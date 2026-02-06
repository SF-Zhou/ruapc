use std::{net::SocketAddr, sync::Arc};

use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::DropGuard;

#[cfg(feature = "rdma")]
use crate::{
    Error, ErrorKind,
    rdma::{Endpoint, RdmaSocketPool},
};
use crate::{
    Result, Socket, SocketPoolConfig, SocketType, State, http::HttpSocketPool, tcp::TcpSocketPool,
    unified::UnifiedSocketPool, ws::WebSocketPool,
};

/// Socket pool managing connections for different transport protocols.
///
/// The socket pool is responsible for:
/// - Managing connection pooling and reuse
/// - Acquiring new connections when needed
/// - Handling protocol-specific connection logic
/// - Lifecycle management (stop/join)
#[derive(Debug)]
pub enum SocketPool {
    /// TCP socket pool.
    TCP(Arc<TcpSocketPool>),
    /// WebSocket pool.
    WS(Arc<WebSocketPool>),
    /// HTTP socket pool.
    HTTP(Arc<HttpSocketPool>),
    /// Unified socket pool supporting multiple protocols.
    UNIFIED(UnifiedSocketPool),
    /// RDMA socket pool (requires "rdma" feature).
    #[cfg(feature = "rdma")]
    RDMA(Arc<RdmaSocketPool>),
}

/// Raw network stream types.
///
/// Represents the underlying transport stream for different protocols.
pub enum RawStream {
    /// Raw TCP stream.
    TCP(TcpStream),
    /// WebSocket stream over upgraded HTTP connection.
    WS(Box<WebSocketStream<TokioIo<Upgraded>>>),
}

impl SocketPool {
    /// Creates a new socket pool with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Socket pool configuration specifying the protocol type
    ///
    /// # Errors
    ///
    /// Returns an error if the socket pool for the specified type cannot be created.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{SocketPool, SocketPoolConfig, SocketType};
    /// let config = SocketPoolConfig { socket_type: SocketType::TCP };
    /// let pool = SocketPool::create(&config).unwrap();
    /// ```
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        match config.socket_type {
            SocketType::TCP => Ok(SocketPool::TCP(TcpSocketPool::new())),
            SocketType::WS => Ok(SocketPool::WS(WebSocketPool::new())),
            SocketType::HTTP => Ok(SocketPool::HTTP(HttpSocketPool::new())),
            SocketType::UNIFIED => Ok(SocketPool::UNIFIED(UnifiedSocketPool::create()?)),
            #[cfg(feature = "rdma")]
            SocketType::RDMA => Ok(SocketPool::RDMA(RdmaSocketPool::create()?)),
            #[cfg(not(feature = "rdma"))]
            SocketType::RDMA => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "RDMA feature is not enabled".into(),
            )),
        }
    }

    /// Returns the socket type of this pool.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{SocketPool, SocketPoolConfig, SocketType};
    /// # let pool = SocketPool::create(&SocketPoolConfig::default()).unwrap();
    /// let socket_type = pool.socket_type();
    /// assert_eq!(socket_type, SocketType::TCP);
    /// ```
    #[must_use]
    pub fn socket_type(&self) -> SocketType {
        match self {
            SocketPool::TCP(_) => SocketType::TCP,
            SocketPool::WS(_) => SocketType::WS,
            SocketPool::HTTP(_) => SocketType::HTTP,
            SocketPool::UNIFIED(_) => SocketType::UNIFIED,
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(_) => SocketType::RDMA,
        }
    }

    /// Acquires a socket connection to the specified address.
    ///
    /// This method attempts to reuse an existing connection from the pool,
    /// or creates a new connection if needed.
    ///
    /// # Arguments
    ///
    /// * `addr` - The target socket address
    /// * `socket_type` - The protocol type to use for the connection
    /// * `state` - Shared state for connection management
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The connection cannot be established
    /// - The requested socket type is not supported by this pool
    pub async fn acquire(
        &self,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<Socket> {
        match self {
            SocketPool::TCP(tcp_socket_pool) => tcp_socket_pool
                .acquire(addr, socket_type, state)
                .await
                .map(Socket::TCP),
            SocketPool::WS(web_socket_pool) => web_socket_pool
                .acquire(addr, socket_type, state)
                .await
                .map(Socket::WS),
            SocketPool::HTTP(http_socket_pool) => http_socket_pool
                .acquire(addr, socket_type, state)
                .await
                .map(Socket::HTTP),
            SocketPool::UNIFIED(unified_socket_pool) => {
                unified_socket_pool.acquire(addr, socket_type, state).await
            }
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(rdma_socket_pool) => rdma_socket_pool
                .acquire(addr, socket_type, state)
                .await
                .map(Socket::RDMA),
        }
    }

    /// Handles a new incoming connection stream.
    ///
    /// This method is called by the listener when a new connection is accepted.
    /// It processes the stream according to the pool's protocol type.
    ///
    /// # Arguments
    ///
    /// * `state` - Shared state for handling the connection
    /// * `stream` - The raw network stream to handle
    /// * `addr` - The remote address of the connection
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Stream handling fails
    /// - The protocol doesn't support incoming connections (e.g., RDMA)
    pub async fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        match self {
            SocketPool::TCP(tcp_socket_pool) => {
                tcp_socket_pool.handle_new_stream(state, stream, addr)
            }
            SocketPool::WS(web_socket_pool) => {
                web_socket_pool.handle_new_stream(state, stream, addr).await
            }
            SocketPool::HTTP(http_socket_pool) => {
                http_socket_pool.handle_new_stream(state, stream, addr)
            }
            SocketPool::UNIFIED(unified_socket_pool) => {
                unified_socket_pool
                    .handle_new_stream(state, stream, addr)
                    .await
            }
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(_) => Err(Error::new(
                ErrorKind::InvalidArgument,
                "invalid socket type".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    /// Returns RDMA connection information.
    ///
    /// This method is only available when the RDMA feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket pool doesn't support RDMA.
    pub fn rdma_info(&self) -> Result<crate::rdma::RdmaInfo> {
        match self {
            SocketPool::RDMA(rdma_socket_pool) => Ok(rdma_socket_pool.rdma_info()),
            SocketPool::UNIFIED(unified_socket_pool) => Ok(unified_socket_pool.rdma_info()),
            _ => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not supported: invalid socket type".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    /// Establishes an RDMA connection to the specified endpoint.
    ///
    /// This method is only available when the RDMA feature is enabled.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The remote RDMA endpoint to connect to
    /// * `state` - Shared state for connection management
    ///
    /// # Returns
    ///
    /// Returns the local endpoint information for the established connection.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The socket pool doesn't support RDMA
    /// - The RDMA connection fails
    pub fn rdma_connect(&self, endpoint: &Endpoint, state: &Arc<State>) -> Result<Endpoint> {
        use crate::{Error, ErrorKind};

        match self {
            SocketPool::RDMA(rdma_socket_pool) => rdma_socket_pool.rdma_connect(endpoint, state),
            SocketPool::UNIFIED(unified_socket_pool) => {
                unified_socket_pool.rdma_connect(endpoint, state)
            }
            _ => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not supported: invalid socket type".into(),
            )),
        }
    }

    /// Stops the socket pool and initiates connection cleanup.
    ///
    /// This signals all connections to gracefully close.
    pub fn stop(&self) {
        match self {
            SocketPool::TCP(tcp_socket_pool) => tcp_socket_pool.stop(),
            SocketPool::WS(web_socket_pool) => web_socket_pool.stop(),
            SocketPool::HTTP(http_socket_pool) => http_socket_pool.stop(),
            SocketPool::UNIFIED(unified_socket_pool) => unified_socket_pool.stop(),
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(rdma_socket_pool) => rdma_socket_pool.stop(),
        }
    }

    /// Creates a drop guard for this socket pool.
    ///
    /// The returned guard will call `stop()` when dropped, ensuring
    /// graceful shutdown.
    ///
    /// # Returns
    ///
    /// Returns a `DropGuard` that calls `stop()` on drop.
    #[must_use]
    pub fn drop_guard(&self) -> DropGuard {
        match self {
            SocketPool::TCP(tcp_socket_pool) => tcp_socket_pool.drop_guard(),
            SocketPool::WS(web_socket_pool) => web_socket_pool.drop_guard(),
            SocketPool::HTTP(http_socket_pool) => http_socket_pool.drop_guard(),
            SocketPool::UNIFIED(unified_socket_pool) => unified_socket_pool.drop_guard(),
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(rdma_socket_pool) => rdma_socket_pool.drop_guard(),
        }
    }

    /// Waits for all connections in the pool to close.
    ///
    /// This method blocks until the socket pool has fully shut down
    /// and all connections are closed.
    pub async fn join(&self) {
        match self {
            SocketPool::TCP(tcp_socket_pool) => tcp_socket_pool.join().await,
            SocketPool::WS(web_socket_pool) => web_socket_pool.join().await,
            SocketPool::HTTP(http_socket_pool) => http_socket_pool.join().await,
            SocketPool::UNIFIED(unified_socket_pool) => unified_socket_pool.join().await,
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(rdma_socket_pool) => rdma_socket_pool.join().await,
        }
    }
}

impl Default for SocketPool {
    fn default() -> Self {
        SocketPool::TCP(TcpSocketPool::new())
    }
}
