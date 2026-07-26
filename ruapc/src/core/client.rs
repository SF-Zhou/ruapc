use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{
    Buffer, Context, SocketEndpoint, SocketTrait, SocketType,
    core::{WriteTarget, scatter::MAX_REGIONS},
    error::{Error, ErrorKind},
    msg::{MsgFlags, MsgMeta},
};

/// RPC client configuration and request handler.
///
/// The `Client` struct is used to make RPC requests to remote services.
/// It handles connection management, request serialization, and response
/// deserialization with configurable timeout and serialization format.
///
/// # Examples
///
/// ```rust,ignore
/// let client = Client::default();
/// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap().with_addr(addr);
///
/// let rsp = client.echo(&ctx, &"hello".into()).await;
/// ```
#[serde_inline_default]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Client {
    /// Timeout duration for RPC requests. Default is 1 second.
    ///
    /// The effective per-request budget is the minimum of this value and
    /// the remaining deadline of the context (for nested RPCs issued while
    /// handling a request). The budget travels with the request so the
    /// server can drop work the client no longer waits for.
    #[serde_inline_default(Duration::from_secs(1))]
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Whether to use MessagePack serialization. Default is true.
    /// When false, JSON serialization is used.
    #[serde_inline_default(true)]
    pub use_msgpack: bool,
    /// Optional socket type to override the default from context.
    /// If None, uses the socket type from the context's socket pool.
    #[serde_inline_default(None)]
    pub socket_type: Option<SocketType>,
    /// Maximum number of retries for failures that occur *before the
    /// request reaches the wire* (connection acquire or send-queue
    /// failures) — always safe, even for non-idempotent methods. With a
    /// multi-address context (`Context::with_addrs`) each retry moves to
    /// the next address. Waiting-phase failures (timeout, connection
    /// closed mid-flight) are never retried automatically. Default is 2.
    #[serde_inline_default(2u32)]
    pub max_retries: u32,
}

impl Default for Client {
    fn default() -> Self {
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap()
    }
}

impl Client {
    /// Creates a [`ClientWithBuffers`] wrapper attaching one *read* buffer
    /// to requests; see [`with_read_buffers`](Self::with_read_buffers).
    pub fn with_read_buffer<'a>(&'a self, buffer: &'a Buffer) -> ClientWithBuffers<'a> {
        self.buffers().with_read_buffer(buffer)
    }

    /// Creates a [`ClientWithBuffers`] wrapper attaching *read* buffers to
    /// requests.
    ///
    /// The buffers' `RemoteBufferInfo` (one region per buffer, in order)
    /// is included in each request's metadata as the request's *read
    /// space*: a logically contiguous concatenation the server can read
    /// from with [`Context::remote_read`](crate::Context::remote_read).
    ///
    /// Each buffer contributes its logical length (`Buffer::len()`): call
    /// `set_len` after filling so the space covers exactly the valid data
    /// bytes. The buffers stay borrowed by the caller for the duration of
    /// the call.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let rsp = client.with_read_buffers(&bufs).upload(&ctx, &req).await?;
    /// ```
    pub fn with_read_buffers<'a>(&'a self, buffers: &'a [Buffer]) -> ClientWithBuffers<'a> {
        self.buffers().with_read_buffers(buffers)
    }

    /// Creates a [`ClientWithBuffers`] wrapper attaching *write* buffers
    /// to requests.
    ///
    /// Ownership of the buffers moves into the request: they are pinned
    /// (registered memory held alive) until the call resolves, forming the
    /// request's *write space* — a logically contiguous concatenation the
    /// server can write into with
    /// [`Context::remote_write`](crate::Context::remote_write). Each
    /// buffer contributes its logical length (`Buffer::len()`); set it to
    /// the receivable size before attaching.
    ///
    /// All buffers come back through the call's return value when the
    /// method's return type is `Result<WithBuffers<T>, E>`; after a failed
    /// call they can be recovered with
    /// [`ClientWithBuffers::take_write_buffers`].
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let (rsp, bufs) = client
    ///     .with_write_buffers(vec![buf_a, buf_b])
    ///     .download(&ctx, &req)
    ///     .await?
    ///     .into_parts();
    /// ```
    pub fn with_write_buffers(&self, buffers: Vec<Buffer>) -> ClientWithBuffers<'_> {
        self.buffers().with_write_buffers(buffers)
    }

    /// Creates an empty [`ClientWithBuffers`] wrapper.
    fn buffers(&self) -> ClientWithBuffers<'_> {
        ClientWithBuffers {
            client: self,
            read_buffers: Vec::new(),
            write_buffers: Mutex::new(None),
        }
    }

    /// Makes an RPC request to a remote service.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The RPC context containing connection information
    /// * `req` - The request payload to send
    /// * `read_buffers` - Registered buffers forming the request's read
    ///   space (borrowed for the call).
    /// * `write_target` - Pinned destination buffers forming the request's
    ///   write space; taken (and consumed) on success.
    /// * `write_buffers_slot` - Optional slot receiving all write buffers
    ///   back once the response arrived.
    /// * `method_name` - The name of the RPC method to invoke
    pub(crate) async fn ruapc_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        read_buffers: &[&Buffer],
        write_target: &mut Option<Arc<WriteTarget>>,
        write_buffers_slot: Option<&mut Vec<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<crate::Error> + for<'c> Deserialize<'c>,
    {
        let metrics = ctx.state.metrics.client_method(method_name);
        metrics.requests.increment(1);
        metrics.inflight.increment(1.0);
        let start = std::time::Instant::now();
        let result = self
            .request_inner(
                ctx,
                req,
                read_buffers,
                write_target,
                write_buffers_slot,
                method_name,
            )
            .await;
        metrics.latency.record(start.elapsed().as_secs_f64());
        metrics.inflight.decrement(1.0);
        if result.is_err() {
            metrics.errors.increment(1);
        }
        result
    }

    async fn request_inner<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        read_buffers: &[&Buffer],
        write_target: &mut Option<Arc<WriteTarget>>,
        write_buffers_slot: Option<&mut Vec<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<crate::Error> + for<'c> Deserialize<'c>,
    {
        // Effective budget: the configured timeout, shrunk to the remaining
        // deadline when issuing a nested RPC from within a handler.
        let timeout = match ctx.remaining_time() {
            Some(remaining) => {
                if remaining.is_zero() {
                    return Err(Error::new(
                        ErrorKind::Timeout,
                        "request deadline already expired".to_string(),
                    )
                    .into());
                }
                self.timeout.min(remaining)
            }
            None => self.timeout,
        };

        if read_buffers.len() > MAX_REGIONS {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!(
                    "too many read buffers: {} (limit {MAX_REGIONS})",
                    read_buffers.len()
                ),
            )
            .into());
        }

        let mut flags = MsgFlags::IsReq;
        if self.use_msgpack {
            flags |= MsgFlags::UseMessagePack;
        }

        // 1.+2. acquire a socket and send the request, retrying failures
        // that provably happen before the request reaches the wire. Each
        // attempt allocates its waiter entry *after* the connection is
        // established, so connection setup (which can be slow, e.g. RDMA QP
        // negotiation with path failover) does not consume the budget. A
        // failed attempt drops its receiver, cleaning the entry up (and,
        // with it, the entry's write-target pin — the caller-held clone in
        // `write_target` keeps the buffers available for the next attempt).
        let addr_base = match &ctx.endpoint {
            SocketEndpoint::Addresses(set) => set.next_base(),
            _ => 0,
        };
        let mut attempt = 0usize;
        let receiver = loop {
            let addr = attempt_addr(ctx, addr_base, attempt)?;
            let result = self
                .try_send(
                    ctx,
                    req,
                    read_buffers,
                    write_target.as_ref(),
                    method_name,
                    flags,
                    timeout,
                    addr,
                )
                .await;
            match result {
                Ok(receiver) => break receiver,
                Err(err) => {
                    if attempt >= self.max_retries as usize {
                        return Err(err.into());
                    }
                    tracing::warn!("attempt {attempt} for {method_name} failed, retrying: {err}");
                    attempt += 1;
                }
            }
        };

        // 3. recv response (fails with Timeout once the entry expires).
        let (response, returned_target) = receiver.recv().await?;
        // Hand every attached write buffer back to the caller. Dropping
        // our own clone first makes the returned target unique in the
        // normal case; a pull/push handler racing the response keeps the
        // buffers alive until it finishes, after which they fall back to
        // the pool.
        drop(write_target.take());
        if let Some(slot) = write_buffers_slot {
            *slot = returned_target
                .and_then(WriteTarget::try_into_buffers)
                .unwrap_or_default();
        }
        response.payload.deserialize(&response.meta)?
    }

    /// One connect + send attempt. Failures here are always safe to retry:
    /// an error from `acquire` or `send` means the request was never handed
    /// to the transport.
    ///
    /// The waiter entry is allocated between the two phases so that
    /// connection setup does not eat into the response budget; it is
    /// returned as a [`Receiver`](crate::Receiver) whose drop (on send
    /// failure) removes the entry again.
    #[allow(clippy::too_many_arguments)]
    async fn try_send<'a, Req>(
        &self,
        ctx: &'a Context,
        req: &Req,
        read_buffers: &[&Buffer],
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
        flags: MsgFlags,
        timeout: Duration,
        addr: Option<std::net::SocketAddr>,
    ) -> std::result::Result<crate::Receiver<'a>, Error>
    where
        Req: Serialize + JsonSchema,
    {
        let socket = match (&ctx.endpoint, addr) {
            (SocketEndpoint::Connected(socket), _) => socket.clone(),
            (_, Some(socket_addr)) => {
                let socket_type = self
                    .socket_type
                    .unwrap_or(ctx.state.socket_pool.socket_type());
                #[cfg(feature = "rdma")]
                let socket = match &ctx.rdma_path {
                    Some(selector) if socket_type == SocketType::RDMA => {
                        ctx.state
                            .socket_pool
                            .acquire_rdma_path(&socket_addr, selector, &ctx.state)
                            .await?
                    }
                    _ => {
                        ctx.state
                            .socket_pool
                            .acquire(&socket_addr, socket_type, &ctx.state)
                            .await?
                    }
                };
                #[cfg(not(feature = "rdma"))]
                let socket = ctx
                    .state
                    .socket_pool
                    .acquire(&socket_addr, socket_type, &ctx.state)
                    .await?;
                socket
            }
            _ => unreachable!("attempt_addr rejects endpoints without an address"),
        };

        // Export the attached buffers as regions for the device the
        // connection actually runs on (TCP device, or the specific RDMA
        // NIC of this connection).
        let mut read_regions = Vec::new();
        let mut write_regions = Vec::new();
        if !read_buffers.is_empty() || write_target.is_some() {
            let device_index = socket.device_index(&ctx.state);
            for buf in read_buffers {
                read_regions.push(
                    buf.remote_buffer_info(&device_index)
                        .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?,
                );
            }
            if let Some(target) = write_target {
                write_regions = target.export_regions(&device_index)?;
            }
        }

        // The waiter entry expires after `timeout` (coarse, swept
        // periodically); no per-request timer is registered.
        let (msgid, receiver) = ctx.state.waiter.alloc(timeout);
        if let Some(target) = write_target {
            // Pin the write buffers to the pending request so push/pull
            // handlers can reach (and keep alive) the destination memory.
            ctx.state.waiter.bind_write_target(msgid, target.clone());
        }
        let mut meta = MsgMeta {
            method: method_name.into(),
            flags,
            msgid,
            read_regions,
            write_regions,
            // Ship the *effective* budget so the whole downstream call
            // tree inherits the shrunk deadline.
            timeout_ms: Some(u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX)),
        };
        socket.send(&mut meta, req, &ctx.state).await?;
        Ok(receiver)
    }
}

/// Resolves the target address for the `attempt`-th try, or `None` for an
/// already-connected endpoint.
fn attempt_addr(
    ctx: &Context,
    base: usize,
    attempt: usize,
) -> std::result::Result<Option<std::net::SocketAddr>, Error> {
    match &ctx.endpoint {
        SocketEndpoint::Invalid => Err(Error::new(
            ErrorKind::InvalidArgument,
            "client context without address".to_string(),
        )),
        SocketEndpoint::Connected(_) => Ok(None),
        SocketEndpoint::Address(addr) => Ok(Some(*addr)),
        SocketEndpoint::Addresses(set) => set.pick(base, attempt).map(Some).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                "client context with empty address set".to_string(),
            )
        }),
    }
}

/// A client wrapper that attaches registered buffers to RPC calls.
///
/// Created via [`Client::with_read_buffers`] /
/// [`Client::with_write_buffers`]. Implements the same service traits as
/// `Client` (generated by the `#[service]` macro).
///
/// # Read buffers
///
/// The attached read buffers form the request's *read space*, advertised
/// in the request metadata; the server reads from it via
/// [`Context::remote_read`](crate::Context::remote_read). They remain
/// borrowed by the caller.
///
/// # Write buffers
///
/// The attached write buffers form the request's *write space*; ownership
/// moves into the call, the memory stays pinned until the call resolves,
/// and the server writes into it via
/// [`Context::remote_write`](crate::Context::remote_write). Methods
/// returning `Result<WithBuffers<T>, E>` deliver all of them back through
/// the return value; after a failed call, recover them with
/// [`take_write_buffers`](Self::take_write_buffers).
///
/// # Examples
///
/// ```rust,ignore
/// // Server reads from the client's buffers:
/// let rsp = client.with_read_buffers(&bufs).upload(&ctx, &req).await?;
///
/// // Upload and download in a single call:
/// let (rsp, out) = client
///     .with_read_buffers(&src_bufs)
///     .with_write_buffers(dst_bufs)
///     .transform(&ctx, &req)
///     .await?
///     .into_parts();
/// ```
pub struct ClientWithBuffers<'a> {
    client: &'a Client,
    read_buffers: Vec<&'a Buffer>,
    write_buffers: Mutex<Option<Vec<Buffer>>>,
}

impl<'a> ClientWithBuffers<'a> {
    /// Appends one buffer to the request's read space.
    #[must_use]
    pub fn with_read_buffer(mut self, buffer: &'a Buffer) -> Self {
        self.read_buffers.push(buffer);
        self
    }

    /// Appends buffers to the request's read space.
    #[must_use]
    pub fn with_read_buffers(mut self, buffers: &'a [Buffer]) -> Self {
        self.read_buffers.extend(buffers.iter());
        self
    }

    /// Attaches the request's write buffers (replacing any previous set).
    #[must_use]
    pub fn with_write_buffers(self, buffers: Vec<Buffer>) -> Self {
        *self.write_buffers.lock().unwrap() = Some(buffers);
        self
    }

    /// Recovers the attached write buffers after a *failed* call (they are
    /// consumed by a successful one and returned through its
    /// `WithBuffers` result instead). Returns `None` when nothing is
    /// recoverable — e.g. a transfer is still in flight; the buffers then
    /// drop back to the pool once it finishes.
    pub fn take_write_buffers(&self) -> Option<Vec<Buffer>> {
        self.write_buffers.lock().unwrap().take()
    }

    /// Makes an RPC request with the configured buffers.
    pub(crate) async fn ruapc_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        write_buffers_slot: Option<&mut Vec<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<crate::Error> + for<'c> Deserialize<'c>,
    {
        let mut target = match self.write_buffers.lock().unwrap().take() {
            Some(buffers) => Some(WriteTarget::new(buffers)?),
            None => None,
        };
        let mut returned: Vec<Buffer> = Vec::new();
        let result = self
            .client
            .ruapc_request(
                ctx,
                req,
                &self.read_buffers,
                &mut target,
                Some(&mut returned),
                method_name,
            )
            .await;
        if result.is_ok() {
            if let Some(slot) = write_buffers_slot {
                *slot = returned;
            }
        } else {
            // On failure, make the write buffers recoverable through
            // `take_write_buffers` whenever nothing keeps them pinned:
            // either the request never consumed the target (pre-wire
            // failure) or an error *response* handed the buffers back.
            let recovered = match target.take() {
                Some(arc) => WriteTarget::try_into_buffers(arc),
                None if !returned.is_empty() => Some(returned),
                None => None,
            };
            if let Some(buffers) = recovered {
                *self.write_buffers.lock().unwrap() = Some(buffers);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let client = Client::default();
        assert_eq!(client.timeout, Duration::from_secs(1));
        assert!(client.use_msgpack);
        assert!(client.socket_type.is_none());
    }

    #[test]
    fn test_client_serde_roundtrip() {
        let client = Client {
            timeout: Duration::from_millis(500),
            use_msgpack: false,
            socket_type: Some(SocketType::TCP),
            max_retries: 4,
        };
        let json = serde_json::to_string(&client).unwrap();
        let recovered: Client = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, client);
    }

    #[test]
    fn test_client_serde_defaults_from_empty_object() {
        let client: Client =
            serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap();
        assert_eq!(client.timeout, Duration::from_secs(1));
        assert!(client.use_msgpack);
        assert!(client.socket_type.is_none());
    }

    #[test]
    fn test_client_debug_format() {
        let client = Client::default();
        let debug = format!("{:?}", client);
        assert!(debug.contains("Client"));
    }

    #[tokio::test]
    async fn test_ruapc_request_invalid_endpoint_returns_err() {
        use crate::{SocketPoolConfig, services::MetaService as _};
        let ctx = crate::Context::create(&SocketPoolConfig::default()).unwrap();
        let client = Client::default();
        let result = client.list_methods(&ctx, &()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn test_write_buffers_recoverable_after_failed_call() {
        use crate::{SocketPoolConfig, services::MetaService as _};
        let ctx = crate::Context::create(&SocketPoolConfig::default()).unwrap();
        let client = Client::default();
        let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
        buf.set_len(16);
        let wrapper = client.with_write_buffers(vec![buf]);
        // Invalid endpoint: the call fails before reaching the wire.
        let result = wrapper.list_methods(&ctx, &()).await;
        assert!(result.is_err());
        let recovered = wrapper.take_write_buffers().expect("buffers recoverable");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].len(), 16);
    }
}
