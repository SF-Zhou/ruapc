use std::{net::SocketAddr, sync::Arc};

use ruapc_bufpool::RemoteBufferInfo;
use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{
    Buffer, Error, RemoteIoError, RemoteReadOptions, Result, Router, Socket, SocketPoolConfig,
    SocketTrait, State,
    msg::{MsgFlags, MsgMeta},
};

/// Socket endpoint information for RPC contexts.
///
/// Represents the connection endpoint for an RPC operation, which can be:
/// - Invalid: No endpoint specified
/// - Connected: An existing socket connection
/// - Address: A socket address to connect to
#[derive(Clone, Debug, Default)]
pub enum SocketEndpoint {
    /// No valid endpoint (default state).
    #[default]
    Invalid,
    /// An established socket connection.
    Connected(Socket),
    /// A socket address to establish a connection to.
    Address(SocketAddr),
}

/// RPC context carrying request metadata and connection information.
///
/// The `Context` is passed to all RPC service methods and contains:
/// - Shared state (router, socket pool, etc.)
/// - Connection endpoint information
/// - Lifecycle management through drop guards
///
/// # Examples
///
/// Creating a client context:
///
/// ```rust,no_run
/// # use ruapc::{Context, SocketPoolConfig};
/// # use std::{net::SocketAddr, str::FromStr};
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
/// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
/// let ctx = ctx.with_addr(addr);
/// ```
#[derive(Clone)]
pub struct Context {
    pub(crate) drop_guard: Option<Arc<DropGuard>>,
    /// Shared state containing router and socket pool.
    pub state: Arc<State>,
    pub(crate) endpoint: SocketEndpoint,
    /// Message metadata for the current RPC operation.
    pub msg_meta: MsgMeta,
}

impl Context {
    /// Creates a new context with the given socket pool configuration.
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        Self::create_with_router(Router::default(), config)
    }

    /// Creates a new context with a custom router and configuration.
    pub fn create_with_router(router: Router, config: &SocketPoolConfig) -> Result<Self> {
        let (state, drop_guard) = State::create(router, config)?;
        Ok(Self {
            state,
            endpoint: SocketEndpoint::Invalid,
            drop_guard: Some(Arc::new(drop_guard)),
            msg_meta: MsgMeta::default(),
        })
    }

    /// Creates a context with a specific state and address.
    ///
    /// Internal method used by RDMA implementation.
    #[cfg(feature = "rdma")]
    pub(crate) fn create_with_state_and_addr(state: &Arc<State>, addr: &SocketAddr) -> Self {
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Address(*addr),
            drop_guard: None,
            msg_meta: MsgMeta::default(),
        }
    }

    /// Creates a new context with the specified target address.
    #[must_use]
    pub fn with_addr(&self, addr: SocketAddr) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: SocketEndpoint::Address(addr),
            drop_guard: self.drop_guard.clone(),
            msg_meta: MsgMeta::default(),
        }
    }

    /// Creates a server-side context with an established socket connection.
    #[must_use]
    pub(crate) fn server_ctx(state: &Arc<State>, socket: Socket, msg_meta: MsgMeta) -> Self {
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Connected(socket),
            drop_guard: None,
            msg_meta,
        }
    }

    /// Sends an RPC response back to the client.
    pub async fn send_rsp<Rsp, E>(&mut self, rsp: std::result::Result<Rsp, E>)
    where
        Rsp: Serialize,
        E: std::error::Error + From<Error> + Serialize,
    {
        // Responses are correlated by msgid alone: drop the request's method
        // and buffer_info so the response meta encodes as a fixed prefix
        // with no serde tail (decoded allocation-free on the hot path).
        let mut meta = MsgMeta {
            method: String::new(),
            flags: self.msg_meta.flags,
            msgid: self.msg_meta.msgid,
            buffer_info: None,
        };
        meta.flags.remove(MsgFlags::IsReq);
        meta.flags.insert(MsgFlags::IsRsp);
        match &mut self.endpoint {
            SocketEndpoint::Connected(socket) => {
                let _ = socket.send(&mut meta, &rsp, &self.state).await;
            }
            _ => {
                tracing::error!("invalid argument: send rsp without connected socket");
            }
        }
    }

    /// Sends an error response back to the client.
    pub async fn send_err_rsp(&mut self, err: Error) {
        self.send_rsp::<(), Error>(Err(err)).await;
    }

    /// Returns the buffer info the client attached to the current request.
    ///
    /// A client attaches a buffer via [`Client::with_read_buffer`], which
    /// puts its `RemoteBufferInfo` into the request metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::MissingBufferInfo`](crate::ErrorKind::MissingBufferInfo)
    /// if the request carries no buffer info.
    pub fn request_buffer_info(&self) -> Result<&RemoteBufferInfo> {
        self.msg_meta.buffer_info.as_ref().ok_or_else(|| {
            Error::new(
                crate::ErrorKind::MissingBufferInfo,
                "request carries no buffer_info; client must attach a buffer \
                 via with_read_buffer()"
                    .into(),
            )
        })
    }

    /// Reads the client's buffer attached to the current request.
    ///
    /// Convenience wrapper around [`request_buffer_info`](Self::request_buffer_info)
    /// and [`remote_read`](Self::remote_read): allocates a right-sized buffer
    /// from the local pool and fills it with the client's data. The returned
    /// buffer's logical length equals the number of bytes transferred.
    ///
    /// # Errors
    ///
    /// - [`ErrorKind::MissingBufferInfo`](crate::ErrorKind::MissingBufferInfo)
    ///   if the request carries no buffer info.
    /// - Any error from allocation or the underlying transfer.
    pub async fn remote_read_request(&self) -> Result<Buffer> {
        let remote = *self.request_buffer_info()?;
        let local = self
            .state
            .buffer_pool
            .allocate((remote.len as usize).max(1))
            .map_err(|e| Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;
        // The buffer is pool-allocated internally, so there is nothing for
        // the caller to recover on failure: flatten to a plain Error.
        Ok(self.remote_read(&remote, local).await?)
    }

    /// Reads data from a remote peer's registered memory.
    ///
    /// Transfers exactly `remote.len` bytes (the peer's valid data length)
    /// and sets the returned buffer's logical length accordingly. Fails with
    /// [`ErrorKind::BufferTooSmall`](crate::ErrorKind::BufferTooSmall) if
    /// `local` cannot hold `remote.len` bytes.
    ///
    /// On failure the local buffer is handed back inside [`RemoteIoError`]
    /// whenever it survived the operation, so it can be reused (e.g. for a
    /// retry). Propagating with `?` converts to [`Error`] and drops the
    /// buffer back to the pool.
    ///
    /// For TCP: sends a `MemoryService/read` reverse RPC request.
    /// For RDMA: issues a one-sided RDMA Read via QP verbs.
    pub async fn remote_read(
        &self,
        remote: &RemoteBufferInfo,
        local: Buffer,
    ) -> std::result::Result<Buffer, RemoteIoError> {
        self.remote_read_with_options(remote, local, &Default::default())
            .await
    }

    /// Reads data from a remote peer's registered memory with custom options.
    ///
    /// `options.skip_verify` can only be set within this crate, preventing
    /// external code from bypassing the UUID liveness check.
    pub(crate) async fn remote_read_with_options(
        &self,
        remote: &RemoteBufferInfo,
        local: Buffer,
        options: &RemoteReadOptions,
    ) -> std::result::Result<Buffer, RemoteIoError> {
        let socket = match &self.endpoint {
            SocketEndpoint::Connected(s) => s,
            _ => {
                return Err(RemoteIoError::new(
                    Error::new(
                        crate::ErrorKind::NotConnected,
                        "remote_read requires a connected socket (server-side handler context)"
                            .into(),
                    ),
                    Some(local),
                ));
            }
        };
        socket.remote_read(self, local, remote, options).await
    }

    /// Pushes `local` to the remote peer (client) and returns a
    /// [`SentBuffer`](crate::SentBuffer) witness of the completed transfer.
    ///
    /// Transfers `local.len()` bytes (the buffer's logical length); call
    /// [`Buffer::set_len`](ruapc_bufpool::Buffer::set_len) first when the
    /// buffer is only partially filled.
    ///
    /// For TCP: sends data inline via `tcp_push`.
    /// For RDMA: lets the client pull data via `rdma_pull`.
    ///
    /// The push happens *here*, inside the handler, so its latency and
    /// errors are directly observable. Pair the witness with a response
    /// value afterwards — the response type can depend on information only
    /// available once the transfer finished:
    ///
    /// ```rust,ignore
    /// let t0 = std::time::Instant::now();
    /// let sent = ctx.remote_write(buf).await?;
    /// Ok(sent.reply(Stats { push_micros: t0.elapsed().as_micros() as u64 }))
    /// ```
    ///
    /// For zero-length buffers (including those created by
    /// [`Buffer::empty`](ruapc_bufpool::Buffer::empty)), no transfer takes
    /// place: the call returns immediately with a [`SentBuffer`] witness.
    ///
    /// The client stores the received buffer in the waiter; it is delivered
    /// atomically with the response. At most one buffer is delivered per
    /// request (the last push wins).
    ///
    /// On failure the local buffer is handed back inside [`RemoteIoError`]
    /// whenever it survived the operation, so the handler can retry.
    pub async fn remote_write(
        &self,
        local: Buffer,
    ) -> std::result::Result<crate::SentBuffer, RemoteIoError> {
        if local.is_empty() {
            return Ok(crate::SentBuffer::new(local));
        }
        let socket = match &self.endpoint {
            SocketEndpoint::Connected(s) => s,
            _ => {
                return Err(RemoteIoError::new(
                    Error::new(
                        crate::ErrorKind::NotConnected,
                        "remote_write requires a connected socket (server-side handler context)"
                            .into(),
                    ),
                    Some(local),
                ));
            }
        };
        let buffer = socket.remote_write(self, local).await?;
        Ok(crate::SentBuffer::new(buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, ErrorKind, SocketPoolConfig};

    #[tokio::test]
    async fn test_send_rsp_invalid_endpoint_logs_and_does_not_panic() {
        // Context starts with SocketEndpoint::Invalid by default.
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        // This should log an error and silently return (no panic).
        ctx.send_err_rsp(Error::kind(ErrorKind::Timeout)).await;
    }

    #[tokio::test]
    async fn test_remote_read_invalid_endpoint_returns_err() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let remote = RemoteBufferInfo {
            key: ruapc_bufpool::MemoryKey { lkey: 0, rkey: 0 },
            addr: 0,
            len: 1,
        };
        let local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        let result = ctx.remote_read(&remote, local).await;
        assert!(result.is_err());
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::NotConnected);
        // The consumed buffer is recoverable from the error.
        let recovered = err.take_buffer().expect("buffer should be recovered");
        assert_eq!(recovered.capacity(), 1024 * 1024);
    }

    #[tokio::test]
    async fn test_remote_write_invalid_endpoint_returns_err() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        let result = ctx.remote_write(local).await;
        assert!(result.is_err());
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::NotConnected);
        assert!(err.take_buffer().is_some());
        // Once taken, the buffer is gone.
        assert!(err.take_buffer().is_none());
        // Converting to Error drops any remaining buffer and keeps the kind.
        let plain: Error = err.into();
        assert_eq!(plain.kind, ErrorKind::NotConnected);
    }

    #[tokio::test]
    async fn test_request_buffer_info_missing_returns_err() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let err = ctx.request_buffer_info().unwrap_err();
        assert_eq!(err.kind, ErrorKind::MissingBufferInfo);
        // remote_read_request surfaces the same error.
        let err = ctx.remote_read_request().await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::MissingBufferInfo);
    }
}
