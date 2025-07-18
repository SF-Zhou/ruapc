use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{Result, Socket, State, tcp::TcpSocketPool};

#[derive(Clone, Debug)]
pub enum SocketPool {
    Tcp(Arc<TcpSocketPool>),
}

impl SocketPool {
    /// # Errors
    pub async fn acquire(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket> {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => {
                tcp_socket_pool.acquire(addr, state).await.map(Socket::Tcp)
            }
        }
    }

    /// # Errors
    pub async fn start_listen(&self, addr: SocketAddr, state: &Arc<State>) -> Result<SocketAddr> {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => tcp_socket_pool.start_listen(addr, state).await,
        }
    }

    pub fn stop(&self) {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => tcp_socket_pool.stop(),
        }
    }

    #[must_use]
    pub fn drop_guard(&self) -> DropGuard {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => tcp_socket_pool.drop_guard(),
        }
    }

    pub async fn join(&self) {
        match self {
            SocketPool::Tcp(tcp_socket_pool) => tcp_socket_pool.join().await,
        }
    }
}

impl Default for SocketPool {
    fn default() -> Self {
        SocketPool::Tcp(TcpSocketPool::new())
    }
}
