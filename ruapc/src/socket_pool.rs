use std::{net::SocketAddr, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use tokio::net::TcpStream;
use tokio_util::sync::DropGuard;

use crate::{Result, Socket, State, http::HttpSocketPool, tcp::TcpSocketPool, ws::WebSocketPool};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone, clap::ValueEnum)]
pub enum SocketType {
    TCP,
    WS,
    HTTP,
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
    TCP(Arc<TcpSocketPool>),
    WS(Arc<WebSocketPool>),
    HTTP(Arc<HttpSocketPool>),
}

impl SocketPool {
    #[must_use]
    pub fn create(config: &SocketPoolConfig) -> Self {
        match config.socket_type {
            SocketType::TCP => SocketPool::TCP(TcpSocketPool::new()),
            SocketType::WS => SocketPool::WS(WebSocketPool::new()),
            SocketType::HTTP => SocketPool::HTTP(HttpSocketPool::new()),
        }
    }

    /// # Errors
    pub async fn acquire(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket> {
        match self {
            SocketPool::TCP(tcp_socket_pool) => {
                tcp_socket_pool.acquire(addr, state).await.map(Socket::TCP)
            }
            SocketPool::WS(web_socket_pool) => {
                web_socket_pool.acquire(addr, state).await.map(Socket::WS)
            }
            SocketPool::HTTP(http_socket_pool) => http_socket_pool
                .acquire(addr, state)
                .await
                .map(Socket::HTTP),
        }
    }

    /// # Errors
    pub async fn handle_new_tcp_stream(
        &self,
        state: &Arc<State>,
        tcp_stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<()> {
        match self {
            SocketPool::TCP(tcp_socket_pool) => {
                tcp_socket_pool.handle_new_tcp_stream(state, tcp_stream, addr);
                Ok(())
            }
            SocketPool::WS(web_socket_pool) => {
                web_socket_pool
                    .handle_new_tcp_stream(state, tcp_stream, addr)
                    .await
            }
            SocketPool::HTTP(http_socket_pool) => {
                http_socket_pool.handle_new_tcp_stream(state, tcp_stream, addr);
                Ok(())
            }
        }
    }

    pub fn stop(&self) {
        match self {
            SocketPool::TCP(tcp_socket_pool) => tcp_socket_pool.stop(),
            SocketPool::WS(web_socket_pool) => web_socket_pool.stop(),
            SocketPool::HTTP(http_socket_pool) => http_socket_pool.stop(),
        }
    }

    #[must_use]
    pub fn drop_guard(&self) -> DropGuard {
        match self {
            SocketPool::TCP(tcp_socket_pool) => tcp_socket_pool.drop_guard(),
            SocketPool::WS(web_socket_pool) => web_socket_pool.drop_guard(),
            SocketPool::HTTP(http_socket_pool) => http_socket_pool.drop_guard(),
        }
    }

    pub async fn join(&self) {
        match self {
            SocketPool::TCP(tcp_socket_pool) => tcp_socket_pool.join().await,
            SocketPool::WS(web_socket_pool) => web_socket_pool.join().await,
            SocketPool::HTTP(http_socket_pool) => http_socket_pool.join().await,
        }
    }
}

impl Default for SocketPool {
    fn default() -> Self {
        SocketPool::TCP(TcpSocketPool::new())
    }
}
