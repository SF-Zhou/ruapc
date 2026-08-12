mod attempt;
mod with_buffers;

pub use with_buffers::ClientWithBuffers;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use std::{sync::Arc, time::Duration};

use crate::{
    Buffer, Context, MAX_REGIONS, SocketTrait,
    core::{ContextEndpoint, EndpointState, WriteTarget},
    error::{Error, ErrorKind},
    msg::{MsgFlags, MsgMeta},
    sockets::AcquireOptions,
};

use attempt::{
    AcquiredSocket, AttemptCycle, AttemptEndpoint, AttemptFailure, AttemptOptions, acquire_direct,
    acquire_for_attempt, capped_deadline, export_attached_regions, is_connection_failure,
    wire_timeout_ms,
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
    /// Response timeout after a connection is available. Default is 1 second.
    ///
    /// The effective response budget is the minimum of this value and
    /// the remaining deadline of the context (for nested RPCs issued while
    /// handling a request). The budget travels with the request so the
    /// server can drop work the client no longer waits for.
    #[serde_inline_default(Duration::from_secs(1))]
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Total budget for connection establishment and endpoint failover.
    /// Connection setup does not consume the response timeout; nested calls
    /// still cap both budgets at the parent context's remaining deadline.
    #[serde_inline_default(Duration::from_secs(5))]
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
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
    /// connection closed mid-flight) are never retried automatically:
    /// whether a request is safe to re-issue after an ambiguous outcome is
    /// application knowledge, so that retry loop belongs to the caller.
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
        ClientWithBuffers::new(self).with_read_buffer(buffer)
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
        ClientWithBuffers::new(self).with_read_buffers(buffers)
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
        ClientWithBuffers::new(self).with_write_buffers(buffers)
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
        if ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "request deadline already expired".to_string(),
            )
            .into());
        }

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

        // 1.+2. acquire a socket and send the request, retrying failures
        // that provably happen before the request reaches the wire. Each
        // attempt allocates its waiter entry *after* the connection is
        // established, so connection setup (which can be slow, e.g. RDMA QP
        // negotiation with path failover) does not consume the budget. A
        // failed attempt drops its receiver, cleaning the entry up (and,
        // with it, the entry's write-target pin — the caller-held clone in
        // `write_target` keeps the buffers available for the next attempt).
        let connect_deadline = capped_deadline(
            std::time::Instant::now(),
            self.connect_timeout,
            ctx.deadline,
        );
        let mut cycle = AttemptCycle::new(
            self.initial_candidates(ctx)?,
            u64::from(self.max_retries) + 1,
        );
        let (sent, endpoint_state) = loop {
            let endpoint_state = cycle.next_candidate();
            let endpoint = match (&ctx.endpoint, endpoint_state.clone()) {
                (ContextEndpoint::Connected(socket), _) => {
                    AttemptEndpoint::Connected(socket.clone())
                }
                (ContextEndpoint::Endpoints(_), Some(state)) => AttemptEndpoint::Endpoint(state),
                (ContextEndpoint::Endpoints(_), None) => {
                    unreachable!("an endpoint candidate was selected before try_send")
                }
                (ContextEndpoint::Invalid, _) => {
                    unreachable!("request endpoint was validated before try_send")
                }
            };
            let attempted_addr = endpoint_state.as_ref().map(|state| state.endpoint().addr());
            let result = self
                .try_send(
                    ctx,
                    req,
                    read_buffers,
                    write_target.as_ref(),
                    method_name,
                    AttemptOptions {
                        endpoint,
                        connect_deadline,
                        remaining_acquire_attempts: cycle.remaining_attempts(),
                        avoided_remote_nics: cycle.avoided_remote_nics(attempted_addr),
                    },
                )
                .await;
            match result {
                Ok(sent) => break (sent, endpoint_state),
                Err(failure) => {
                    let attempt = cycle.attempt_index();
                    if !cycle.note_failure(endpoint_state.as_ref(), &failure) {
                        return Err(failure.error.into());
                    }
                    tracing::warn!(
                        "attempt {attempt} for {method_name} failed, retrying: {}",
                        failure.error
                    );
                }
            }
        };
        // 3. recv the single response (fails with Timeout once the waiter
        // entry expires). Ambiguous waiting-phase failures are surfaced to
        // the caller instead of being retried: the request may have
        // executed, and only the application knows whether re-issuing it
        // is safe.
        let (response, returned_target) = match sent.receiver.recv().await {
            Ok(response) => response,
            Err(err) => {
                if matches!(err.kind, ErrorKind::ConnectionClosed)
                    && let Some(state) = &endpoint_state
                    && let Some(conn_id) = sent.conn_id
                {
                    state.record_connection_failure(conn_id);
                }
                return Err(err.into());
            }
        };
        if let Some(state) = &endpoint_state
            && let Some(conn_id) = sent.conn_id
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

    /// Validates the context's destination and resolves the ranked
    /// endpoint candidates (empty for an already-connected context);
    /// alternatives beyond the first start connecting in the background.
    fn initial_candidates(&self, ctx: &Context) -> Result<Vec<Arc<EndpointState>>, Error> {
        match &ctx.endpoint {
            ContextEndpoint::Invalid => Err(Error::new(
                ErrorKind::InvalidArgument,
                "client context without address".to_string(),
            )),
            ContextEndpoint::Connected(_) => Ok(Vec::new()),
            ContextEndpoint::Endpoints(set) => {
                let candidates = set.candidates(&ctx.state);
                if candidates.is_empty() {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "client context with empty endpoint set".to_string(),
                    ));
                }
                self.preconnect(ctx, &candidates[1..]);
                Ok(candidates)
            }
        }
    }

    /// One connect + send attempt. Failures here are always safe to retry:
    /// an error from `acquire` or `send` means the request was never handed
    /// to the transport.
    ///
    /// The waiter entry is allocated between the two phases so that
    /// connection setup does not eat into the response budget; it is
    /// returned as a [`Receiver`](crate::Receiver) whose drop (on send
    /// failure) removes the entry again.
    async fn try_send<'a, Req>(
        &self,
        ctx: &'a Context,
        req: &Req,
        read_buffers: &[&Buffer],
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
        attempt: AttemptOptions<'_>,
    ) -> std::result::Result<SentRequest<'a>, AttemptFailure>
    where
        Req: Serialize + JsonSchema,
    {
        let AttemptOptions {
            endpoint,
            connect_deadline,
            remaining_acquire_attempts,
            avoided_remote_nics,
        } = attempt;
        let AcquiredSocket {
            socket,
            endpoint_state,
        } = acquire_for_attempt(
            &ctx.state,
            endpoint,
            connect_deadline,
            remaining_acquire_attempts,
            avoided_remote_nics,
        )
        .await?;
        // The response budget starts once a connection is available, still
        // capped by the parent context's remaining deadline (nested RPCs).
        let now = std::time::Instant::now();
        let response_deadline = capped_deadline(now, self.timeout, ctx.deadline);
        let timeout = response_deadline.saturating_duration_since(now);
        if timeout.is_zero() {
            return Err(AttemptFailure::deadline());
        }

        let (read_regions, write_regions) =
            export_attached_regions(&socket, &ctx.state, read_buffers, write_target)?;

        // The waiter entry expires after `timeout` (coarse, swept
        // periodically); no per-request timer is registered.
        let (msgid, receiver) = ctx.state.waiter.alloc(timeout);
        if let Some(target) = write_target {
            // Pin the write buffers to the pending request so push/pull
            // handlers can reach (and keep alive) the destination memory.
            ctx.state.waiter.bind_write_target(msgid, target.clone());
        }
        let mut flags = MsgFlags::IsReq;
        if self.use_msgpack {
            flags |= MsgFlags::UseMessagePack;
        }
        let mut meta = MsgMeta {
            method: method_name.into(),
            flags,
            msgid,
            read_regions,
            write_regions,
            // Ship the *effective* budget so the whole downstream call
            // tree inherits the shrunk deadline.
            timeout_ms: wire_timeout_ms(timeout),
        };
        if let Err(err) = socket.send(&mut meta, req, &ctx.state).await {
            if is_connection_failure(&err)
                && let Some(state) = &endpoint_state
                && let Some(conn_id) = socket.conn_id()
            {
                state.record_connection_failure(conn_id);
            }
            return Err(AttemptFailure::send(
                err,
                socket.rdma_remote_device().map(str::to_owned),
            ));
        }
        Ok(SentRequest {
            receiver,
            conn_id: socket.conn_id(),
        })
    }

    /// Starts background connections to alternative endpoints so a later
    /// failover finds them established.
    fn preconnect(&self, ctx: &Context, candidates: &[Arc<EndpointState>]) {
        let supervisor = ctx.state.socket_pool.task_supervisor_handle();
        for endpoint_state in candidates {
            let Some(activity) = endpoint_state.try_begin_preconnect() else {
                continue;
            };
            let state = ctx.state.clone();
            let endpoint_state = endpoint_state.clone();
            let _ = supervisor.try_spawn(async move {
                let result =
                    acquire_direct(&state, endpoint_state.endpoint(), AcquireOptions::default())
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

/// A successfully sent request attempt, waiting for its response.
struct SentRequest<'a> {
    receiver: crate::Receiver<'a>,
    conn_id: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let client = Client::default();
        assert_eq!(client.timeout, Duration::from_secs(1));
        assert_eq!(client.connect_timeout, Duration::from_secs(5));
        assert!(client.use_msgpack);
        assert_eq!(client.max_retries, 2);
    }

    #[test]
    fn test_client_serde_roundtrip() {
        let client = Client {
            timeout: Duration::from_millis(500),
            connect_timeout: Duration::from_secs(2),
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
        assert_eq!(client.connect_timeout, Duration::from_secs(5));
        assert!(client.use_msgpack);
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
}
