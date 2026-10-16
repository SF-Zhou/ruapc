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

/// An established connection or equivalent destinations for an RPC.
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

    /// Reuses shared state for TCP bootstrap RPCs to an RDMA peer.
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

    /// Shares state while selecting a destination and resetting message metadata.
    ///
    /// The deadline (if any) is inherited: nested RPCs issued while handling
    /// a request keep the caller's remaining time budget.
    #[must_use]
    pub fn with_endpoint(&self, endpoint: Endpoint) -> Self {
        self.with_endpoints(vec![endpoint])
    }

    /// Creates a new context that load-balances across equivalent endpoints.
    ///
    /// Shares state and inherits the request deadline, with fresh message
    /// metadata, just like [`with_endpoint`](Self::with_endpoint).
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
    /// ID and payload encoding; transport send errors are discarded.
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

    /// Sends an error through [`send_rsp`](Self::send_rsp), with the same
    /// best-effort delivery behavior.
    pub async fn send_err_rsp(&mut self, err: Error) {
        self.send_rsp::<(), Error>(Err(err)).await;
    }
}
