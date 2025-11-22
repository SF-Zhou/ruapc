use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{Listener, Result, Router, SocketPoolConfig, State};

/// RPC server that listens for and handles incoming requests.
///
/// The `Server` manages the lifecycle of an RPC server, including:
/// - Service registration through a [`Router`]
/// - Network listening on specified addresses
/// - Connection management through socket pools
/// - Request dispatching to registered service implementations
///
/// # Examples
///
/// ```rust,no_run
/// # #![feature(return_type_notation)]
/// # use ruapc::{Server, Router, SocketPoolConfig, Context};
/// # use std::{net::SocketAddr, str::FromStr, sync::Arc};
/// # use schemars::JsonSchema;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, JsonSchema)]
/// # struct Request(String);
/// # #[ruapc::service]
/// # trait EchoService {
/// #     async fn echo(&self, ctx: &Context, req: &Request) -> ruapc::Result<String>;
/// # }
/// # struct DemoImpl;
/// # impl EchoService for DemoImpl {
/// #     async fn echo(&self, _ctx: &Context, req: &Request) -> ruapc::Result<String> {
/// #         Ok(req.0.clone())
/// #     }
/// # }
/// # #[tokio::main]
/// # async fn main() {
/// let mut router = Router::default();
/// EchoService::ruapc_export(Arc::new(DemoImpl), &mut router);
///
/// let server = Server::create(router, &SocketPoolConfig::default()).unwrap();
/// let server = Arc::new(server);
///
/// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
/// server.listen(addr).await.unwrap();
/// server.join().await;
/// # }
/// ```
pub struct Server {
    state: Arc<State>,
    listener: Listener,
    _drop_guard: DropGuard,
}

impl Server {
    /// Creates a new RPC server with the given router and configuration.
    ///
    /// # Arguments
    ///
    /// * `router` - The router containing registered service methods
    /// * `config` - Socket pool configuration specifying transport protocol and options
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Socket pool creation fails
    /// - State initialization fails
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Server, Router, SocketPoolConfig};
    /// let router = Router::default();
    /// let server = Server::create(router, &SocketPoolConfig::default()).unwrap();
    /// ```
    pub fn create(router: Router, config: &SocketPoolConfig) -> Result<Self> {
        let (state, drop_guard) = State::create(router, config)?;

        Ok(Self {
            state,
            listener: Listener::default(),
            _drop_guard: drop_guard,
        })
    }

    /// Stops the server, closing all listeners and connections.
    ///
    /// This method initiates a graceful shutdown by:
    /// - Stopping the listener from accepting new connections
    /// - Stopping the socket pool to close existing connections
    ///
    /// Call [`join`](Self::join) after this to wait for shutdown to complete.
    pub fn stop(&self) {
        self.listener.stop();
        self.state.socket_pool.stop();
    }

    /// Waits for the server to fully shut down.
    ///
    /// This method blocks until:
    /// - All listeners have stopped accepting connections
    /// - All socket pools have closed their connections
    ///
    /// Should be called after [`stop`](Self::stop).
    pub async fn join(&self) {
        self.listener.join().await;
        self.state.socket_pool.join().await;
    }

    /// Starts listening for incoming connections on the specified address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address to bind to
    ///
    /// # Returns
    ///
    /// Returns the actual address the server is listening on, which may differ
    /// from the requested address if port 0 was specified.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The address cannot be bound
    /// - The listener fails to start
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Server, Router, SocketPoolConfig};
    /// # use std::{net::SocketAddr, str::FromStr};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let server = Server::create(Router::default(), &SocketPoolConfig::default()).unwrap();
    /// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
    /// let listening_addr = server.listen(addr).await.unwrap();
    /// println!("Server listening on {}", listening_addr);
    /// # }
    /// ```
    pub async fn listen(&self, addr: SocketAddr) -> Result<SocketAddr> {
        self.listener.start_listen(addr, &self.state).await
    }
}
