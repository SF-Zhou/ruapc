use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use std::time::Duration;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use crate::{
    Buffer, Context, Socket, SocketTrait, State,
    core::{EndpointState, WriteTarget, context::ContextEndpoint, scatter::MAX_REGIONS},
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
/// let endpoint = "tcp://127.0.0.1:8000".parse().unwrap();
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap().with_endpoint(endpoint);
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
    /// Maximum number of retries for failures that occur *before the
    /// request reaches the wire* (connection acquire or send-queue
    /// failures) — always safe, even for non-idempotent methods. With a
    /// multi-endpoint context (`Context::with_endpoints`) each retry moves
    /// to the next endpoint, cycling back to the first when the retry
    /// budget exceeds the endpoint count. Waiting-phase failures (timeout,
    /// connection closed mid-flight) are never retried automatically.
    /// Default is 2.
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
        let mut direct_endpoint = None;
        let mut candidates = match &ctx.endpoint {
            ContextEndpoint::Invalid => {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "client context without address".to_string(),
                )
                .into());
            }
            ContextEndpoint::Connected(_) => Vec::new(),
            ContextEndpoint::Endpoints(set) => {
                if let Some(endpoint) = set.singleton() {
                    direct_endpoint = Some(endpoint);
                    Vec::new()
                } else {
                    let candidates = set.candidates(&ctx.state);
                    if candidates.is_empty() {
                        return Err(Error::new(
                            ErrorKind::InvalidArgument,
                            "client context with empty endpoint set".to_string(),
                        )
                        .into());
                    }
                    self.preconnect(ctx, &candidates[1..]);
                    candidates
                }
            }
        };
        let max_attempts = self.max_retries as usize + 1;
        let mut attempt = 0usize;
        let mut tried = HashSet::new();
        let (receiver, endpoint_state, conn_id) = loop {
            let endpoint_state = if candidates.is_empty() {
                None
            } else {
                if attempt != 0 {
                    candidates.sort_by_key(|candidate| candidate.selection_rank());
                }
                if tried.len() == candidates.len() {
                    tried.clear();
                }
                let candidate = candidates
                    .iter()
                    .find(|candidate| !tried.contains(&candidate.endpoint()))
                    .expect("a non-empty candidate cycle has an untried endpoint")
                    .clone();
                Some(candidate)
            };
            let result = self
                .try_send(
                    ctx,
                    req,
                    read_buffers,
                    write_target.as_ref(),
                    method_name,
                    flags,
                    timeout,
                    direct_endpoint,
                    endpoint_state.clone(),
                )
                .await;
            match result {
                Ok((receiver, conn_id)) => break (receiver, endpoint_state, conn_id),
                Err(failure) => {
                    if let Some(endpoint_state) = &endpoint_state {
                        tried.insert(endpoint_state.endpoint());
                    }
                    let has_untried_endpoint = tried.len() < candidates.len();
                    let retry = should_retry_attempt(
                        failure.disposition,
                        attempt,
                        max_attempts,
                        has_untried_endpoint,
                    );
                    if !retry {
                        return Err(failure.error.into());
                    }
                    tracing::warn!(
                        "attempt {attempt} for {method_name} failed, retrying: {}",
                        failure.error
                    );
                    attempt += 1;
                }
            }
        };
        // 3. recv response (fails with Timeout once the entry expires).
        let (response, returned_target) = match receiver.recv().await {
            Ok(response) => response,
            Err(err) => {
                if matches!(err.kind, ErrorKind::ConnectionClosed)
                    && let Some(state) = &endpoint_state
                    && let Some(conn_id) = conn_id
                {
                    state.record_connection_failure(conn_id);
                }
                return Err(err.into());
            }
        };
        if let Some(state) = &endpoint_state
            && let Some(conn_id) = conn_id
        {
            state.record_request_success(conn_id);
        }
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
        direct_endpoint: Option<crate::Endpoint>,
        endpoint_state: Option<Arc<EndpointState>>,
    ) -> std::result::Result<(crate::Receiver<'a>, Option<u64>), AttemptFailure>
    where
        Req: Serialize + JsonSchema,
    {
        let socket = match (&ctx.endpoint, &endpoint_state) {
            (ContextEndpoint::Connected(socket), _) => socket.clone(),
            (ContextEndpoint::Endpoints(_), Some(endpoint_state)) => acquire_endpoint(
                &ctx.state,
                endpoint_state,
                #[cfg(feature = "rdma")]
                ctx.rdma_path.as_ref(),
            )
            .await
            .map_err(AttemptFailure::acquire)?,
            (ContextEndpoint::Endpoints(_), None) => {
                let endpoint =
                    direct_endpoint.expect("a singleton endpoint was resolved before try_send");
                acquire_direct(
                    &ctx.state,
                    endpoint,
                    #[cfg(feature = "rdma")]
                    ctx.rdma_path.as_ref(),
                )
                .await
                .map_err(AttemptFailure::acquire)?
            }
            _ => unreachable!("request endpoint was validated before try_send"),
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
                        .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))
                        .map_err(AttemptFailure::failover)?,
                );
            }
            if let Some(target) = write_target {
                write_regions = target
                    .export_regions(&device_index)
                    .map_err(AttemptFailure::failover)?;
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
        if let Err(err) = socket.send(&mut meta, req, &ctx.state).await {
            if is_connection_failure(&err)
                && let Some(state) = &endpoint_state
                && let Some(conn_id) = socket.conn_id()
            {
                state.record_connection_failure(conn_id);
            }
            return Err(AttemptFailure {
                disposition: if is_connection_failure(&err) {
                    AttemptDisposition::Retryable
                } else {
                    AttemptDisposition::Terminal
                },
                error: err,
            });
        }
        Ok((receiver, socket.conn_id()))
    }

    fn preconnect(&self, ctx: &Context, candidates: &[Arc<EndpointState>]) {
        let supervisor = ctx.state.socket_pool.task_supervisor_handle();
        for endpoint_state in candidates {
            let Some(activity) = endpoint_state.try_begin_preconnect() else {
                continue;
            };
            let state = ctx.state.clone();
            let endpoint_state = endpoint_state.clone();
            #[cfg(feature = "rdma")]
            let selector = ctx.rdma_path.clone();
            let _ = supervisor.try_spawn(async move {
                let result = acquire_direct(
                    &state,
                    endpoint_state.endpoint(),
                    #[cfg(feature = "rdma")]
                    selector.as_ref(),
                )
                .await;
                match &result {
                    Ok(socket) => activity.record_connection(socket),
                    Err(err) if is_connection_failure(err) => activity.record_failure(),
                    Err(_) => {}
                }
                if let Err(err) = result {
                    tracing::debug!(
                        endpoint = %endpoint_state.endpoint(),
                        %err,
                        "background connection failed"
                    );
                }
            });
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AttemptDisposition {
    Retryable,
    FailoverOnly,
    Terminal,
}

struct AttemptFailure {
    disposition: AttemptDisposition,
    error: Error,
}

impl AttemptFailure {
    fn acquire(error: Error) -> Self {
        Self {
            disposition: if is_connection_failure(&error) {
                AttemptDisposition::Retryable
            } else {
                AttemptDisposition::FailoverOnly
            },
            error,
        }
    }

    fn failover(error: Error) -> Self {
        Self {
            disposition: AttemptDisposition::FailoverOnly,
            error,
        }
    }
}

impl From<Error> for AttemptFailure {
    fn from(error: Error) -> Self {
        Self {
            disposition: AttemptDisposition::Terminal,
            error,
        }
    }
}

fn should_retry_attempt(
    disposition: AttemptDisposition,
    attempt: usize,
    max_attempts: usize,
    has_untried_endpoint: bool,
) -> bool {
    if attempt + 1 >= max_attempts {
        return false;
    }
    match disposition {
        AttemptDisposition::Retryable => true,
        AttemptDisposition::FailoverOnly => has_untried_endpoint,
        AttemptDisposition::Terminal => false,
    }
}

async fn acquire_endpoint(
    state: &Arc<State>,
    endpoint_state: &Arc<EndpointState>,
    #[cfg(feature = "rdma")] selector: Option<&crate::rdma::RdmaPathSelector>,
) -> std::result::Result<Socket, Error> {
    let endpoint = endpoint_state.endpoint();
    let result = acquire_direct(
        state,
        endpoint,
        #[cfg(feature = "rdma")]
        selector,
    )
    .await;

    match &result {
        Ok(socket) if !endpoint_state.is_current(socket) => {
            endpoint_state.begin_connect().record_connection(socket);
        }
        Err(err) if is_connection_failure(err) => {
            endpoint_state.begin_connect().record_failure();
        }
        Err(_) => {}
        Ok(_) => {}
    }
    result
}

async fn acquire_direct(
    state: &Arc<State>,
    endpoint: crate::Endpoint,
    #[cfg(feature = "rdma")] selector: Option<&crate::rdma::RdmaPathSelector>,
) -> std::result::Result<Socket, Error> {
    #[cfg(feature = "rdma")]
    if let Some(selector) = selector
        && endpoint.transport() == crate::Transport::RDMA
    {
        return state
            .socket_pool
            .acquire_rdma_path(&endpoint.addr(), selector, state)
            .await;
    }
    state.socket_pool.acquire(endpoint, state).await
}

fn is_connection_failure(err: &Error) -> bool {
    let common = matches!(
        err.kind,
        ErrorKind::TcpConnectFailed
            | ErrorKind::TcpSendMsgFailed
            | ErrorKind::TcpRecvMsgFailed
            | ErrorKind::WebSocketConnectFailed
            | ErrorKind::WebSocketSendFailed
            | ErrorKind::WebSocketRecvFailed
            | ErrorKind::WebSocketClosed
            | ErrorKind::HttpWaitRspFailed
            | ErrorKind::HttpSendReqFailed
            | ErrorKind::ConnectionClosed
            | ErrorKind::RdmaSendFailed
            | ErrorKind::RdmaRecvFailed
            | ErrorKind::RdmaReadTimeout
    );
    #[cfg(feature = "rdma")]
    let rdma = matches!(err.kind, ErrorKind::RdmaError(_));
    #[cfg(not(feature = "rdma"))]
    let rdma = false;
    common || rdma
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
    }

    #[test]
    fn test_client_serde_roundtrip() {
        let client = Client {
            timeout: Duration::from_millis(500),
            use_msgpack: false,
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
    }

    #[test]
    fn test_client_debug_format() {
        let client = Client::default();
        let debug = format!("{:?}", client);
        assert!(debug.contains("Client"));
    }

    #[test]
    fn unknown_errors_do_not_penalize_endpoints() {
        assert!(!is_connection_failure(&Error::new(
            ErrorKind::Unknown("other".into()),
            "unclassified".into(),
        )));
        assert!(is_connection_failure(&Error::kind(
            ErrorKind::TcpConnectFailed
        )));
    }

    #[test]
    fn retry_disposition_respects_failure_scope() {
        assert!(should_retry_attempt(
            AttemptDisposition::Retryable,
            0,
            3,
            false,
        ));
        assert!(should_retry_attempt(
            AttemptDisposition::FailoverOnly,
            0,
            3,
            true,
        ));
        assert!(!should_retry_attempt(
            AttemptDisposition::FailoverOnly,
            0,
            3,
            false,
        ));
        assert!(!should_retry_attempt(
            AttemptDisposition::Terminal,
            0,
            3,
            true,
        ));
    }

    #[test]
    fn invalid_acquire_error_only_fails_over() {
        let failure = AttemptFailure::acquire(Error::kind(ErrorKind::InvalidArgument));
        assert_eq!(failure.disposition, AttemptDisposition::FailoverOnly);
        let failure = AttemptFailure::failover(Error::kind(ErrorKind::InvalidArgument));
        assert_eq!(failure.disposition, AttemptDisposition::FailoverOnly);
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
