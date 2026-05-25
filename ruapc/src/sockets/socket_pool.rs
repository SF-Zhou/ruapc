use std::{net::SocketAddr, sync::Arc};

use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::DropGuard;

#[cfg(feature = "rdma")]
use crate::rdma::{ConnectRequest, Endpoint, RdmaSocketPool};
#[cfg(feature = "rdma")]
use crate::{Error, ErrorKind};
use crate::{
    Result, Socket, State, http::HttpSocketPool, tcp::TcpSocketPool, unified::UnifiedSocketPool,
    ws::WebSocketPool,
};

/// Transport protocol types supported by RuaPC.
///
/// Each socket type represents a different transport protocol with its own characteristics:
/// - **TCP**: Raw TCP sockets with custom protocol
/// - **WS**: WebSocket over HTTP
/// - **HTTP**: HTTP/1.1 and HTTP/2 (h2c), supports bidirectional streaming for reverse RPC
/// - **UNIFIED**: Accepts all protocol types on the same port
/// - **RDMA**: High-performance RDMA (requires "rdma" feature)
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone, Copy, clap::ValueEnum)]
pub enum SocketType {
    /// Raw TCP transport.
    TCP,
    /// WebSocket transport.
    WS,
    /// HTTP transport.
    HTTP,
    /// Unified transport supporting multiple protocols.
    UNIFIED,
    /// RDMA transport (requires "rdma" feature).
    #[cfg(feature = "rdma")]
    RDMA,
}

impl std::fmt::Display for SocketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

/// Socket pool configuration.
///
/// Specifies which transport protocol to use for the socket pool.
///
/// # Examples
///
/// ```rust
/// use ruapc::{SocketPoolConfig, SocketType};
///
/// let config = SocketPoolConfig {
///     socket_type: SocketType::TCP,
/// };
/// ```
#[serde_inline_default]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub struct SocketPoolConfig {
    /// The transport protocol type to use. Default is TCP.
    #[serde_inline_default(SocketType::TCP)]
    pub socket_type: SocketType,
}

impl Default for SocketPoolConfig {
    fn default() -> Self {
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap()
    }
}

/// Socket pool managing connections for different transport protocols.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum SocketPool {
    /// TCP socket pool.
    TCP(TcpSocketPool),
    /// WebSocket pool.
    WS(WebSocketPool),
    /// HTTP socket pool.
    HTTP(HttpSocketPool),
    /// Unified socket pool supporting multiple protocols.
    UNIFIED(UnifiedSocketPool),
    /// RDMA socket pool (requires "rdma" feature).
    #[cfg(feature = "rdma")]
    RDMA(RdmaSocketPool),
}

/// Raw network stream types.
pub enum RawStream {
    /// Raw TCP stream.
    TCP(TcpStream),
    /// WebSocket stream over upgraded HTTP connection.
    WS(Box<WebSocketStream<TokioIo<Upgraded>>>),
}

/// Trait defining the interface for individual socket pool implementations.
///
/// Used by `TcpSocketPool`, `WebSocketPool`, `HttpSocketPool`, etc.
/// `SocketPool` (the enum) dispatches to these via its own methods.
pub trait SocketPoolTrait: Sized {
    fn create(
        config: &SocketPoolConfig,
        devices: &Arc<crate::Devices>,
        buffer_pool: &Arc<crate::BufferPool>,
    ) -> Result<Self>;

    async fn acquire(
        &self,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<Socket>;

    async fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()>;

    fn stop(&self);

    fn drop_guard(&self) -> DropGuard;

    async fn join(&self);

    #[cfg(feature = "rdma")]
    fn rdma_device_list(&self) -> Result<crate::rdma::RdmaInfo> {
        Err(Error::new(
            ErrorKind::InvalidArgument,
            "RDMA is not supported: invalid socket type".into(),
        ))
    }

    #[cfg(feature = "rdma")]
    #[allow(unused_variables)]
    fn rdma_accept(&self, request: &ConnectRequest, state: &Arc<State>) -> Result<Endpoint> {
        Err(Error::new(
            ErrorKind::InvalidArgument,
            "RDMA is not supported: invalid socket type".into(),
        ))
    }
}

impl SocketPool {
    /// Returns the socket type of this pool.
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

    /// Creates a socket pool with the given configuration, devices, and buffer pool.
    pub fn create(
        config: &SocketPoolConfig,
        devices: &Arc<crate::Devices>,
        buffer_pool: &Arc<crate::BufferPool>,
    ) -> Result<Self> {
        match config.socket_type {
            SocketType::TCP => Ok(SocketPool::TCP(TcpSocketPool::create(
                config,
                devices,
                buffer_pool,
            )?)),
            SocketType::WS => Ok(SocketPool::WS(WebSocketPool::create(
                config,
                devices,
                buffer_pool,
            )?)),
            SocketType::HTTP => Ok(SocketPool::HTTP(HttpSocketPool::create(
                config,
                devices,
                buffer_pool,
            )?)),
            SocketType::UNIFIED => Ok(SocketPool::UNIFIED(UnifiedSocketPool::create(
                config,
                devices,
                buffer_pool,
            )?)),
            #[cfg(feature = "rdma")]
            SocketType::RDMA => Ok(SocketPool::RDMA(RdmaSocketPool::create(
                config,
                devices,
                buffer_pool,
            )?)),
        }
    }

    /// Acquires a socket connection to the specified address.
    pub async fn acquire(
        &self,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<Socket> {
        match self {
            SocketPool::TCP(p) => p.acquire(addr, socket_type, state).await,
            SocketPool::WS(p) => p.acquire(addr, socket_type, state).await,
            SocketPool::HTTP(p) => p.acquire(addr, socket_type, state).await,
            SocketPool::UNIFIED(p) => p.acquire(addr, socket_type, state).await,
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(p) => p.acquire(addr, socket_type, state).await,
        }
    }

    /// Handles a new incoming connection stream.
    pub async fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        match self {
            SocketPool::TCP(p) => p.handle_new_stream(state, stream, addr).await,
            SocketPool::WS(p) => p.handle_new_stream(state, stream, addr).await,
            SocketPool::HTTP(p) => p.handle_new_stream(state, stream, addr).await,
            SocketPool::UNIFIED(p) => p.handle_new_stream(state, stream, addr).await,
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(_) => Err(Error::new(
                ErrorKind::InvalidArgument,
                "invalid socket type".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_device_list(&self) -> Result<crate::rdma::RdmaInfo> {
        match self {
            SocketPool::RDMA(p) => p.rdma_device_list(),
            SocketPool::UNIFIED(p) => p.rdma_device_list(),
            _ => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not supported: invalid socket type".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_accept(&self, request: &ConnectRequest, state: &Arc<State>) -> Result<Endpoint> {
        match self {
            SocketPool::RDMA(p) => p.rdma_accept(request, state),
            SocketPool::UNIFIED(p) => p.rdma_accept(request, state),
            _ => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not supported: invalid socket type".into(),
            )),
        }
    }

    /// Stops the socket pool and initiates connection cleanup.
    pub fn stop(&self) {
        match self {
            SocketPool::TCP(p) => p.stop(),
            SocketPool::WS(p) => p.stop(),
            SocketPool::HTTP(p) => p.stop(),
            SocketPool::UNIFIED(p) => p.stop(),
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(p) => p.stop(),
        }
    }

    /// Creates a drop guard for this socket pool.
    pub fn drop_guard(&self) -> DropGuard {
        match self {
            SocketPool::TCP(p) => p.drop_guard(),
            SocketPool::WS(p) => p.drop_guard(),
            SocketPool::HTTP(p) => p.drop_guard(),
            SocketPool::UNIFIED(p) => p.drop_guard(),
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(p) => p.drop_guard(),
        }
    }

    /// Waits for all connections in the pool to close.
    pub async fn join(&self) {
        match self {
            SocketPool::TCP(p) => p.join().await,
            SocketPool::WS(p) => p.join().await,
            SocketPool::HTTP(p) => p.join().await,
            SocketPool::UNIFIED(p) => p.join().await,
            #[cfg(feature = "rdma")]
            SocketPool::RDMA(p) => p.join().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_socket_pool_config_default_is_tcp() {
        let config = SocketPoolConfig::default();
        assert_eq!(config.socket_type, SocketType::TCP);
    }

    #[test]
    fn test_socket_pool_config_serde_roundtrip() {
        let config = SocketPoolConfig {
            socket_type: SocketType::WS,
        };
        let json = serde_json::to_string(&config).unwrap();
        let recovered: SocketPoolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, config);
    }

    #[test]
    fn test_socket_type_display() {
        assert_eq!(SocketType::TCP.to_string(), "TCP");
        assert_eq!(SocketType::WS.to_string(), "WS");
        assert_eq!(SocketType::HTTP.to_string(), "HTTP");
        assert_eq!(SocketType::UNIFIED.to_string(), "UNIFIED");
    }

    #[tokio::test]
    async fn test_socket_pool_tcp_socket_type() {
        let config = SocketPoolConfig::default();
        let devices = std::sync::Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        assert_eq!(pool.socket_type(), SocketType::TCP);
        pool.stop();
        drop(pool.drop_guard());
        pool.join().await;
    }

    #[tokio::test]
    async fn test_socket_pool_ws_socket_type() {
        let config = SocketPoolConfig {
            socket_type: SocketType::WS,
        };
        let devices = std::sync::Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        assert_eq!(pool.socket_type(), SocketType::WS);
        pool.stop();
        drop(pool.drop_guard());
        pool.join().await;
    }

    #[tokio::test]
    async fn test_socket_pool_http_socket_type() {
        let config = SocketPoolConfig {
            socket_type: SocketType::HTTP,
        };
        let devices = std::sync::Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        assert_eq!(pool.socket_type(), SocketType::HTTP);
        // Verify stop/drop_guard/join can be called without panicking.
        pool.stop();
        drop(pool.drop_guard());
        pool.join().await;
    }

    #[tokio::test]
    async fn test_socket_pool_unified_socket_type() {
        let config = SocketPoolConfig {
            socket_type: SocketType::UNIFIED,
        };
        let devices = std::sync::Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        assert_eq!(pool.socket_type(), SocketType::UNIFIED);
        pool.stop();
    }

    #[cfg(feature = "rdma")]
    fn make_rdma_devices() -> std::sync::Arc<crate::Devices> {
        let active_devices =
            ruapc_rdma::ActiveDevice::available().expect("RDMA devices should be available");
        let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
        let mut devices = crate::Devices::default();
        for dev in active_devices {
            if prefer_rxe && !dev.info().name.starts_with("rxe") {
                continue;
            }
            devices.add_rdma_device(dev);
        }
        assert!(!devices.rdma_devices().is_empty(), "no RDMA device found");
        std::sync::Arc::new(devices)
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_socket_pool_rdma_socket_type() {
        let devices = make_rdma_devices();
        let config = SocketPoolConfig {
            socket_type: SocketType::RDMA,
        };
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        assert_eq!(pool.socket_type(), SocketType::RDMA);
        pool.stop();
        drop(pool.drop_guard());
        pool.join().await;
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_socket_pool_rdma_device_list_from_rdma_pool() {
        let devices = make_rdma_devices();
        let config = SocketPoolConfig {
            socket_type: SocketType::RDMA,
        };
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        let info = pool.rdma_device_list().unwrap();
        assert!(!info.devices.is_empty());
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_socket_pool_rdma_device_list_from_non_rdma_returns_err() {
        let config = SocketPoolConfig::default(); // TCP pool
        let devices = std::sync::Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        assert!(pool.rdma_device_list().is_err());
        pool.stop();
        pool.join().await;
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_socket_pool_rdma_accept_non_rdma_returns_err() {
        let config = SocketPoolConfig::default(); // TCP pool
        let devices = std::sync::Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        let (state, _guard) = crate::State::create(crate::Router::default(), &config).unwrap();
        let request = crate::rdma::ConnectRequest {
            target: crate::rdma::DeviceSelection {
                device_name: "missing".into(),
                port_num: 1,
                gid_index: 0,
            },
            endpoint: crate::rdma::Endpoint {
                qp_num: 0,
                port_num: 1,
                gid_index: 0,
                gid: ruapc_rdma::ibv_gid::default(),
                lid: 0,
                link_layer: ruapc_rdma::LinkLayer::Ethernet,
                active_mtu: ruapc_rdma::ibv_mtu::IBV_MTU_512,
            },
        };
        assert!(pool.rdma_accept(&request, &state).is_err());
        pool.stop();
        pool.join().await;
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_socket_pool_handle_new_stream_rdma_returns_err() {
        use tokio::net::TcpListener;
        let devices = make_rdma_devices();
        let config = SocketPoolConfig {
            socket_type: SocketType::RDMA,
        };
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        let (state, _guard) = crate::State::create(crate::Router::default(), &config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Connect a client to get a stream.
        let client_task = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (server_stream, _) = listener.accept().await.unwrap();
        let _ = client_task.await;
        let result = pool
            .handle_new_stream(&state, RawStream::TCP(server_stream), addr)
            .await;
        assert!(result.is_err());
    }
}
