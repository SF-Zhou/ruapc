use std::{net::SocketAddr, sync::Arc};

use ruapc_bufpool::RemoteBufferInfo;
use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{
    Buffer, Error, RemoteReadOptions, Result, Router, Socket, SocketPoolConfig, SocketTrait, State,
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
        let mut meta = self.msg_meta.clone();
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

    /// Reads data from a remote peer's registered memory.
    ///
    /// For TCP: sends a `MemoryService/read` reverse RPC request.
    /// For RDMA: issues a one-sided RDMA Read via QP verbs.
    pub async fn remote_read(&self, remote: &RemoteBufferInfo, local: Buffer) -> Result<Buffer> {
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
    ) -> Result<Buffer> {
        let socket = match &self.endpoint {
            SocketEndpoint::Connected(s) => s,
            _ => {
                return Err(Error::new(
                    crate::ErrorKind::InvalidArgument,
                    "remote read requires a connected socket".into(),
                ));
            }
        };
        socket.remote_read(self, local, remote, options).await
    }

    /// Writes data to the remote peer (client) via reverse RPC.
    ///
    /// For TCP: sends data inline via `tcp_push`.
    /// For RDMA: lets the client pull data via `rdma_pull`.
    ///
    /// The client stores the received buffer in the waiter and returns it
    /// to the caller via [`ClientWithBuffer::take_write_buffer`].
    pub async fn remote_write(&self, local: Buffer) -> Result<Buffer> {
        let socket = match &self.endpoint {
            SocketEndpoint::Connected(s) => s,
            _ => {
                return Err(Error::new(
                    crate::ErrorKind::InvalidArgument,
                    "remote write requires a connected socket".into(),
                ));
            }
        };
        socket.remote_write(self, local).await
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
        let local = ctx.state.buffer_pool.allocate().unwrap();
        let result = ctx.remote_read(&remote, local).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn test_remote_write_invalid_endpoint_returns_err() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let local = ctx.state.buffer_pool.allocate().unwrap();
        let result = ctx.remote_write(local).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
    }
}
