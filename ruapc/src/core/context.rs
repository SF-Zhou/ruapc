use std::sync::Arc;

use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{
    Endpoint, Error, Result, Router, Socket, SocketPoolConfig, State,
    msg::{MsgFlags, MsgMeta},
};

/// An established connection or one logical endpoint for an RPC.
#[derive(Clone, Debug, Default)]
pub(crate) enum ContextEndpoint {
    /// No valid endpoint (default state).
    #[default]
    Invalid,
    /// An established socket connection.
    Connected(Socket),
    /// One server's transport and equivalent addresses.
    Endpoint(Endpoint),
}

/// RPC context carrying request metadata and connection information.
///
/// Handlers use it to access shared state, inspect their deadline, and issue
/// nested or reverse RPCs. Clones share state and connection lifetime guards;
/// [`with_endpoint`](Self::with_endpoint) selects a destination for a new call.
///
/// # Examples
///
/// Creating a client context:
///
/// ```rust,no_run
/// # use ruapc::{Context, Endpoint, SocketPoolConfig};
/// # #[tokio::main]
/// # async fn main() -> ruapc::Result<()> {
/// let ctx = Context::create(&SocketPoolConfig::default())?;
/// let endpoint: Endpoint = "tcp://127.0.0.1:8000".parse()?;
/// let ctx = ctx.with_endpoint(endpoint);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Context {
    pub(crate) drop_guard: Option<Arc<DropGuard>>,
    /// Shared state containing router and socket pool.
    pub state: Arc<State>,
    pub(crate) endpoint: ContextEndpoint,
    /// Message metadata for the current RPC operation.
    pub msg_meta: MsgMeta,
    /// Deadline of the request being handled, derived from the client's
    /// `timeout_ms` budget on arrival. `None` for client-created contexts.
    pub(crate) deadline: Option<std::time::Instant>,
}

impl Context {
    /// Creates client state and connection pools with the default [`Router`].
    ///
    /// Requires a Tokio runtime. Select a destination with
    /// [`with_endpoint`](Self::with_endpoint) before making calls. Configuration,
    /// schema generation and socket-pool initialization errors are returned.
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        Self::create_with_router(Router::default(), config)
    }

    /// Creates client state with handlers for incoming reverse RPCs.
    ///
    /// Register services on `router` before calling this. Runtime requirements
    /// and initialization errors are the same as [`create`](Self::create).
    pub fn create_with_router(router: Router, config: &SocketPoolConfig) -> Result<Self> {
        let (state, drop_guard) = State::create(router, config)?;
        Ok(Self {
            state,
            endpoint: ContextEndpoint::Invalid,
            drop_guard: Some(Arc::new(drop_guard)),
            msg_meta: MsgMeta::default(),
            deadline: None,
        })
    }

    /// Reuses shared state for bootstrap RPCs to an RDMA peer.
    #[cfg(feature = "rdma")]
    pub(crate) fn create_with_state_and_endpoint(state: &Arc<State>, endpoint: Endpoint) -> Self {
        Self {
            state: state.clone(),
            endpoint: ContextEndpoint::Endpoint(endpoint),
            drop_guard: None,
            msg_meta: MsgMeta::default(),
            deadline: None,
        }
    }

    /// Shares state while selecting a destination and resetting message metadata.
    ///
    /// The deadline (if any) is inherited: nested RPCs issued while handling
    /// a request keep the caller's remaining time budget. All addresses in
    /// `endpoint` identify the same server; the connection pool selects an
    /// address without changing the logical destination.
    #[must_use]
    pub fn with_endpoint(&self, endpoint: Endpoint) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: ContextEndpoint::Endpoint(endpoint),
            drop_guard: self.drop_guard.clone(),
            msg_meta: MsgMeta::default(),
            deadline: self.deadline,
        }
    }

    /// Deadline derived from the incoming time budget; client-created contexts
    /// have none. A missing or zero wire budget expires on arrival.
    #[must_use]
    pub fn deadline(&self) -> Option<std::time::Instant> {
        self.deadline
    }

    /// Remaining time budget of the request being handled. Returns
    /// `Duration::ZERO` after the deadline and `None` for a context without
    /// an inherited or incoming request deadline.
    #[must_use]
    pub fn remaining_time(&self) -> Option<std::time::Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(std::time::Instant::now()))
    }

    /// Whether the local request deadline has passed. Running handlers are
    /// not cancelled on expiry; poll this to stop work cooperatively.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.remaining_time() == Some(std::time::Duration::ZERO)
    }

    /// Identity of the connection that delivered the request being handled.
    /// `None` for client-created contexts and unary HTTP exchanges.
    pub(crate) fn conn_id(&self) -> Option<u64> {
        match &self.endpoint {
            ContextEndpoint::Connected(socket) => socket.conn_id(),
            _ => None,
        }
    }

    /// Creates a server-side context with an established socket connection.
    ///
    /// Derives the request deadline from the client-provided `timeout_ms`
    /// budget, anchored at arrival time.
    #[must_use]
    pub(crate) fn server_ctx(state: &Arc<State>, socket: Socket, msg_meta: MsgMeta) -> Self {
        let deadline = Some(
            std::time::Instant::now()
                + std::time::Duration::from_millis(u64::from(msg_meta.timeout_ms)),
        );
        Self {
            state: state.clone(),
            endpoint: ContextEndpoint::Connected(socket),
            drop_guard: None,
            msg_meta,
            deadline,
        }
    }

    /// Attempts to send a response on this context's connection.
    ///
    /// Generated service handlers normally call this. It preserves the request
    /// ID and payload encoding; transport send errors are discarded. An `Err`
    /// here uses the application's declared error type. Use
    /// [`send_err_rsp`](Self::send_err_rsp) for framework-generated errors.
    pub async fn send_rsp<Rsp, E>(&mut self, rsp: std::result::Result<Rsp, E>)
    where
        Rsp: Serialize,
        E: std::error::Error + From<Error> + Serialize,
    {
        self.send_rsp_inner(rsp, MsgFlags::empty()).await;
    }

    async fn send_rsp_inner<Rsp, E>(&mut self, rsp: std::result::Result<Rsp, E>, flags: MsgFlags)
    where
        Rsp: Serialize,
        E: std::error::Error + From<Error> + Serialize,
    {
        // Error accounting for the method being handled (server side).
        if rsp.is_err() && !self.msg_meta.method.is_empty() {
            self.state
                .metrics
                .server_method(&self.msg_meta.method)
                .errors
                .increment(1);
        }

        // Responses are correlated by msgid alone: drop the request's method
        // and regions so the response meta stays minimal (flags + msgid
        // only; absent fields are skipped by the meta encoding).
        let mut meta = MsgMeta {
            method: String::new(),
            flags: self.msg_meta.flags,
            msgid: self.msg_meta.msgid,
            read_regions: Default::default(),
            write_regions: Default::default(),
            timeout_ms: 0,
        };
        meta.flags
            .remove(MsgFlags::IsReq | MsgFlags::IsFrameworkError);
        meta.flags.insert(MsgFlags::IsRsp | flags);
        match &mut self.endpoint {
            ContextEndpoint::Connected(socket) => {
                let _ = socket.send(&mut meta, &rsp, &self.state).await;
            }
            _ => {
                tracing::error!("invalid argument: send rsp without connected socket");
            }
        }
    }

    /// Sends a framework-generated error with best-effort delivery.
    ///
    /// Marks the response so typed clients decode [`Error`] first, then convert
    /// it through the application's `From<Error>` implementation. The payload
    /// retains the same `Result::Err` envelope as [`send_rsp`](Self::send_rsp).
    pub async fn send_err_rsp(&mut self, err: Error) {
        self.send_rsp_inner::<(), Error>(Err(err), MsgFlags::IsFrameworkError)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorKind, Message, msg::frame::FrameDecoder, tcp::TcpSocket};

    #[tokio::test]
    async fn response_flags_preserve_extensions_and_classify_errors_by_origin() {
        let base = client_context();
        let extension_flag = MsgFlags::from_bits_retain(0x80);
        for format in [MsgFlags::empty(), MsgFlags::UseMessagePack] {
            let (sender, mut queue) = tokio::sync::mpsc::channel(1);
            let request_meta = MsgMeta {
                method: "Test/reply".into(),
                // Even a request carrying the response-only flag must not
                // cause ordinary application replies to be misclassified.
                flags: MsgFlags::IsReq | format | MsgFlags::IsFrameworkError | extension_flag,
                msgid: 42,
                timeout_ms: 30_000,
                ..Default::default()
            };
            let mut ctx = Context::server_ctx(
                &base.state,
                Socket::TCP(TcpSocket::new(sender)),
                request_meta.clone(),
            );
            let error = Error::new(ErrorKind::HandlerPanic, "handler panicked".into());
            for (framework_error, expected) in [
                (true, Err(error.clone())),
                (false, Err(error.clone())),
                (false, Ok(7_u32)),
            ] {
                if framework_error {
                    ctx.send_err_rsp(expected.clone().unwrap_err()).await;
                } else {
                    ctx.send_rsp(expected.clone()).await;
                }
                let mut encoded = queue.try_recv().unwrap();
                let body = FrameDecoder::default()
                    .decode(&mut encoded)
                    .unwrap()
                    .unwrap();
                let response = Message::parse(body).unwrap();
                let mut flags = MsgFlags::IsRsp | format | extension_flag;
                flags.set(MsgFlags::IsFrameworkError, framework_error);
                assert_eq!(
                    response.meta,
                    MsgMeta {
                        flags,
                        msgid: request_meta.msgid,
                        ..Default::default()
                    }
                );
                // Framework errors retain the Result envelope understood by
                // existing clients whose declared error type is Error.
                assert_eq!(
                    response
                        .payload
                        .deserialize::<Result<u32>>(&response.meta)
                        .unwrap(),
                    expected
                );
                assert_eq!(ctx.msg_meta, request_meta);
                assert!(encoded.is_empty());
            }
        }
    }

    fn client_context() -> Context {
        Context::create(&SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn selecting_endpoint_preserves_group_state_and_parent_deadline() {
        let mut parent = client_context();
        parent.deadline = Some(std::time::Instant::now() + std::time::Duration::from_secs(5));
        parent.msg_meta.method = "Parent/call".into();
        parent.msg_meta.msgid = 42;
        let endpoint: Endpoint = "tcp://127.0.0.1:8001,127.0.0.1:8000".parse().unwrap();

        let ctx = parent.with_endpoint(endpoint.clone());
        let ContextEndpoint::Endpoint(actual) = &ctx.endpoint else {
            panic!("with_endpoint must retain one logical endpoint");
        };
        assert_eq!(actual, &endpoint);
        assert_eq!(actual.addrs().len(), 2);
        assert!(Arc::ptr_eq(&ctx.state, &parent.state));
        assert!(Arc::ptr_eq(
            ctx.drop_guard.as_ref().unwrap(),
            parent.drop_guard.as_ref().unwrap(),
        ));
        assert_eq!(ctx.deadline, parent.deadline);
        assert!(ctx.msg_meta.method.is_empty());
        assert_eq!(ctx.msg_meta.msgid, 0);
        assert_eq!(parent.msg_meta.msgid, 42);
        assert!(matches!(parent.endpoint, ContextEndpoint::Invalid));

        let replacement: Endpoint = "tcp://127.0.0.1:9000".parse().unwrap();
        let replaced = ctx.with_endpoint(replacement.clone());
        assert!(matches!(
            replaced.endpoint,
            ContextEndpoint::Endpoint(actual) if actual == replacement
        ));
        assert!(matches!(ctx.endpoint, ContextEndpoint::Endpoint(actual) if actual == endpoint));
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn bootstrap_context_keeps_every_equivalent_address() {
        let base = client_context();
        let endpoint: Endpoint = "tcp://127.0.0.1:8000,127.0.0.1:8001".parse().unwrap();
        let ctx = Context::create_with_state_and_endpoint(&base.state, endpoint.clone());

        assert!(matches!(ctx.endpoint, ContextEndpoint::Endpoint(actual) if actual == endpoint));
        assert!(Arc::ptr_eq(&ctx.state, &base.state));
        assert!(ctx.drop_guard.is_none());
        assert!(ctx.deadline.is_none());
    }
}
