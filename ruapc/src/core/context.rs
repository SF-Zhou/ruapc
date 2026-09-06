#[cfg(feature = "rdma")]
use std::net::SocketAddr;
use std::sync::Arc;

use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{
    Endpoint, Error, Result, Router, Socket, SocketPoolConfig, SocketTrait, State,
    core::EndpointSet,
    msg::{MsgFlags, MsgMeta},
};

/// Socket endpoint information for RPC contexts.
///
/// Represents the connection endpoint for an RPC operation, which can be:
/// - Invalid: No endpoint specified
/// - Connected: An existing socket connection
/// - Endpoints: One or more equivalent transport-bearing destinations
#[derive(Clone, Debug, Default)]
pub(crate) enum ContextEndpoint {
    /// No valid endpoint (default state).
    #[default]
    Invalid,
    /// An established socket connection.
    Connected(Socket),
    /// Several equivalent server endpoints with shared connection health.
    Endpoints(Arc<EndpointSet>),
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
/// # use ruapc::{Context, Endpoint, SocketPoolConfig};
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
/// let endpoint: Endpoint = "tcp://127.0.0.1:8000".parse().unwrap();
/// let ctx = ctx.with_endpoint(endpoint);
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
    /// Creates a new context with the given socket pool configuration.
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        Self::create_with_router(Router::default(), config)
    }

    /// Creates a new context with a custom router and configuration.
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

    /// Creates a context with a specific state and address.
    ///
    /// Internal method used by RDMA implementation.
    #[cfg(feature = "rdma")]
    pub(crate) fn create_with_state_and_addr(state: &Arc<State>, addr: &SocketAddr) -> Self {
        Self {
            state: state.clone(),
            endpoint: ContextEndpoint::Endpoints(Arc::new(EndpointSet::new(vec![Endpoint::tcp(
                *addr,
            )]))),
            drop_guard: None,
            msg_meta: MsgMeta::default(),
            deadline: None,
        }
    }

    /// Creates a new context with the specified target endpoint.
    ///
    /// The deadline (if any) is inherited: nested RPCs issued while handling
    /// a request keep the caller's remaining time budget.
    #[must_use]
    pub fn with_endpoint(&self, endpoint: Endpoint) -> Self {
        self.with_endpoints(vec![endpoint])
    }

    /// Creates a new context that load-balances across equivalent endpoints.
    ///
    /// Healthy established connections are preferred, ties are distributed
    /// round-robin, and the remaining addresses are connected in the
    /// background. Connect/send failures apply exponential cooldown to that
    /// transport/address pair; connect-phase retries re-rank and use an
    /// untried candidate first. Cooldown does not sleep a foreground request
    /// when no alternative exists (see
    /// [`Client::max_retries`](crate::Client::max_retries)).
    #[must_use]
    pub fn with_endpoints(&self, endpoints: Vec<Endpoint>) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: ContextEndpoint::Endpoints(Arc::new(EndpointSet::new(endpoints))),
            drop_guard: self.drop_guard.clone(),
            msg_meta: MsgMeta::default(),
            deadline: self.deadline,
        }
    }

    /// Deadline of the request being handled, if the client sent a time
    /// budget.
    #[must_use]
    pub fn deadline(&self) -> Option<std::time::Instant> {
        self.deadline
    }

    /// Remaining time budget of the request being handled. Returns
    /// `Duration::ZERO` when the deadline already passed and `None` when
    /// the request carries no budget.
    #[must_use]
    pub fn remaining_time(&self) -> Option<std::time::Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(std::time::Instant::now()))
    }

    /// Whether the request's deadline has passed. Handlers of long-running
    /// methods can poll this to stop work the client no longer waits for.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.remaining_time() == Some(std::time::Duration::ZERO)
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

    /// Sends an RPC response back to the client.
    pub async fn send_rsp<Rsp, E>(&mut self, rsp: std::result::Result<Rsp, E>)
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
            read_regions: Vec::new(),
            write_regions: Vec::new(),
            timeout_ms: 0,
        };
        meta.flags.remove(MsgFlags::IsReq);
        meta.flags.insert(MsgFlags::IsRsp);
        match &mut self.endpoint {
            ContextEndpoint::Connected(socket) => {
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
}
