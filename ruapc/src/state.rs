use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{
    Context, Message, RawStream, Result, Router, Socket, SocketPool, SocketPoolConfig, Waiter,
};

/// Shared state for the RPC system.
///
/// The `State` contains shared components used throughout the RPC system:
/// - Router for method dispatch
/// - Waiter for request/response correlation
/// - Socket pool for connection management
///
/// This state is shared between the server and all active connections.
#[derive(Default)]
pub struct State {
    /// Router containing registered service methods.
    pub router: Router,
    /// Waiter for correlating requests with responses.
    pub(crate) waiter: Arc<Waiter>,
    /// Socket pool for managing connections.
    pub(crate) socket_pool: SocketPool,
}

impl State {
    /// Creates a new state with the given router and configuration.
    ///
    /// This method:
    /// 1. Builds the OpenAPI specification from the router
    /// 2. Creates the socket pool with the specified configuration
    /// 3. Initializes the waiter for request/response correlation
    ///
    /// # Arguments
    ///
    /// * `router` - Router containing registered service methods
    /// * `config` - Socket pool configuration
    ///
    /// # Returns
    ///
    /// Returns an Arc-wrapped state and a drop guard for lifecycle management.
    ///
    /// # Errors
    ///
    /// Returns an error if OpenAPI generation or socket pool creation fails.
    pub(crate) fn create(
        mut router: Router,
        config: &SocketPoolConfig,
    ) -> Result<(Arc<Self>, DropGuard)> {
        router.build_open_api()?;
        let state = Self {
            router,
            waiter: Arc::default(),
            socket_pool: SocketPool::create(config)?,
        };
        let state = Arc::new(state);
        let drop_guard = state.drop_guard();
        Ok((state, drop_guard))
    }

    /// Handles a received message from a socket.
    ///
    /// This method routes the message based on whether it's a request or response:
    /// - Requests are dispatched to the router
    /// - Responses are posted to the waiter
    ///
    /// # Arguments
    ///
    /// * `socket` - The socket that received the message
    /// * `msg` - The received message
    ///
    /// # Errors
    ///
    /// Currently always returns Ok, but has Result type for future extensibility.
    pub fn handle_recv(self: &Arc<Self>, socket: &Socket, msg: Message) -> Result<()> {
        if msg.meta.is_req() {
            let ctx = Context::server_ctx(self, socket.clone());
            self.router.dispatch(ctx, msg);
        } else if msg.meta.is_rsp() {
            self.waiter.post(msg.meta.msgid, msg);
        } else {
            tracing::warn!("invalid msg type {:?}", msg.meta);
        }
        Ok(())
    }

    /// Handles a new incoming stream connection.
    ///
    /// This method delegates stream handling to the socket pool, which will
    /// perform any necessary protocol negotiation and start message processing.
    ///
    /// # Arguments
    ///
    /// * `stream` - The raw network stream
    /// * `addr` - The remote address
    pub async fn handle_new_stream(self: Arc<Self>, stream: RawStream, addr: SocketAddr) {
        if let Err(e) = self
            .socket_pool
            .handle_new_stream(&self, stream, addr)
            .await
        {
            tracing::error!("handle new tcp stream error: {e}");
        }
    }

    /// Creates a drop guard for state lifecycle management.
    ///
    /// The drop guard ensures proper cleanup when the state is no longer needed.
    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.socket_pool.drop_guard()
    }
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State").finish()
    }
}
