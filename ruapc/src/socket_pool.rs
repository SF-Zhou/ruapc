use std::{net::SocketAddr, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use tokio_util::sync::DropGuard;

use crate::{Result, Socket, State, tcp::TcpSocketPool, ws::WebSocketPool};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone, clap::ValueEnum)]
pub enum SocketType {
    TCP,
    WebSocket,
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

#[derive(Clone, Debug)]
pub enum SocketPool {
    Tcp(Arc<TcpSocketPool>),
    WebSocket(Arc<WebSocketPool>),
}

impl SocketPool {
    #[must_use]
    pub fn create(config: &SocketPoolConfig) -> Self {
        match config.socket_type {
            SocketType::TCP => SocketPool::Tcp(TcpSocketPool::new()),
            SocketType::WebSocket => SocketPool::WebSocket(WebSocketPool::new()),
        }
    }

    /// # Errors
    pub async fn acquire(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket> {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => {
                tcp_socket_pool.acquire(addr, state).await.map(Socket::Tcp)
            }
            SocketPool::WebSocket(web_socket_pool) => {
                web_socket_pool.acquire(addr, state).await.map(Socket::WS)
            }
        }
    }

    /// # Errors
    pub async fn start_listen(&self, addr: SocketAddr, state: &Arc<State>) -> Result<SocketAddr> {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => tcp_socket_pool.start_listen(addr, state).await,
            SocketPool::WebSocket(web_socket_pool) => {
                web_socket_pool.start_listen(addr, state).await
            }
        }
    }

    pub fn stop(&self) {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => tcp_socket_pool.stop(),
            SocketPool::WebSocket(web_socket_pool) => web_socket_pool.stop(),
        }
    }

    #[must_use]
    pub fn drop_guard(&self) -> DropGuard {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => tcp_socket_pool.drop_guard(),
            SocketPool::WebSocket(web_socket_pool) => web_socket_pool.drop_guard(),
        }
    }

    pub async fn join(&self) {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => tcp_socket_pool.join().await,
            SocketPool::WebSocket(web_socket_pool) => web_socket_pool.join().await,
        }
    }
}

impl Default for SocketPool {
    fn default() -> Self {
        SocketPool::Tcp(TcpSocketPool::new())
    }
}
