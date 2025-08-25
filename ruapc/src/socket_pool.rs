use std::{net::SocketAddr, sync::Arc};

use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::DropGuard;

#[cfg(feature = "rdma")]
use crate::{
    Error, ErrorKind,
    rdma::{Endpoint, RdmaSocketPool},
};
use crate::{
    Result, Socket, State, http::HttpSocketPool, tcp::TcpSocketPool, unified::UnifiedSocketPool,
    ws::WebSocketPool,
};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone, Copy, clap::ValueEnum)]
pub enum SocketType {
    TCP,
    WS,
    HTTP,
    UNIFIED,
    #[cfg(feature = "rdma")]
    RDMA,
}

impl std::fmt::Display for SocketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[serde_inline_default]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub struct SocketPoolConfig {
    #[serde_inline_default(SocketType::TCP)]
    pub socket_type: SocketType,
}

impl Default for SocketPoolConfig {
    fn default() -> Self {
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap()
    }
}

#[derive(Debug)]
pub enum SocketPool {
    TCP(Arc<TcpSocketPool>),
    WS(Arc<WebSocketPool>),
    HTTP(Arc<HttpSocketPool>),
    UNIFIED(UnifiedSocketPool),
    #[cfg(feature = "rdma")]
    RDMA(Arc<RdmaSocketPool>),
}

pub enum RawStream {
    TCP(TcpStream),
    WS(Box<WebSocketStream<TokioIo<Upgraded>>>),
}

impl SocketPool {
    /// # Errors
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        match config.socket_type {
            SocketType::TCP => Ok(SocketPool::TCP(TcpSocketPool::new())),
            SocketType::WS => Ok(SocketPool::WS(WebSocketPool::new())),
            SocketType::HTTP => Ok(SocketPool::HTTP(HttpSocketPool::new())),
            SocketType::UNIFIED => Ok(SocketPool::UNIFIED(UnifiedSocketPool::create()?)),
            #[cfg(feature = "rdma")]
            SocketType::RDMA => Ok(SocketPool::RDMA(RdmaSocketPool::create()?)),
        }
    }

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

    /// # Errors
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

    /// # Errors
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
    /// # Errors
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
    /// # Errors
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
