use std::{net::SocketAddr, sync::Arc};

use futures_util::TryFutureExt;
use tokio_util::sync::DropGuard;

#[cfg(feature = "rdma")]
use crate::rdma::{Endpoint, RdmaInfo, RdmaSocketPool};
use crate::{
    Error, ErrorKind, RawStream, Result, Socket, SocketType, State, TaskSupervisor,
    http::HttpSocketPool,
    tcp::{self, TcpSocketPool},
    ws::WebSocketPool,
};

pub struct UnifiedSocketPool {
    pub tcp_socket_pool: Arc<TcpSocketPool>,
    pub web_socket_pool: Arc<WebSocketPool>,
    pub http_socket_pool: Arc<HttpSocketPool>,
    #[cfg(feature = "rdma")]
    pub rdma_socket_pool: Arc<RdmaSocketPool>,
    task_supervisor: TaskSupervisor,
}

impl UnifiedSocketPool {
    pub fn create() -> Result<Self> {
        let this = Self {
            tcp_socket_pool: TcpSocketPool::new(),
            web_socket_pool: WebSocketPool::new(),
            http_socket_pool: HttpSocketPool::new(),
            #[cfg(feature = "rdma")]
            rdma_socket_pool: RdmaSocketPool::create()?,
            task_supervisor: TaskSupervisor::create(),
        };

        let task_guard = this.task_supervisor.start_async_task();
        let tcp_guard = this.tcp_socket_pool.drop_guard();
        let web_guard = this.web_socket_pool.drop_guard();
        let http_guard = this.http_socket_pool.drop_guard();
        #[cfg(feature = "rdma")]
        let rdma_guard = this.rdma_socket_pool.drop_guard();
        tokio::spawn(async move {
            task_guard.stopped().await;
            drop(http_guard);
            drop(web_guard);
            drop(tcp_guard);
            #[cfg(feature = "rdma")]
            drop(rdma_guard);
        });

        Ok(this)
    }

    pub async fn acquire(
        &self,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<Socket> {
        match socket_type {
            SocketType::TCP | SocketType::UNIFIED => self
                .tcp_socket_pool
                .acquire(addr, SocketType::TCP, state)
                .await
                .map(Socket::TCP),
            SocketType::WS => self
                .web_socket_pool
                .acquire(addr, SocketType::WS, state)
                .await
                .map(Socket::WS),
            SocketType::HTTP => self
                .http_socket_pool
                .acquire(addr, SocketType::HTTP, state)
                .await
                .map(Socket::HTTP),
            #[cfg(feature = "rdma")]
            SocketType::RDMA => self
                .rdma_socket_pool
                .acquire(addr, SocketType::RDMA, state)
                .await
                .map(Socket::RDMA),
        }
    }

    pub async fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        match &stream {
            RawStream::TCP(tcp_stream) => {
                const S: usize = std::mem::size_of_val(&tcp::MAGIC_NUM);
                let mut buf = [0u8; S];
                tcp_stream
                    .peek(&mut buf)
                    .map_err(|e| Error::new(ErrorKind::TcpRecvMsgFailed, e.to_string()))
                    .await?;

                if buf == tcp::MAGIC_NUM.to_be_bytes() {
                    self.tcp_socket_pool.handle_new_stream(state, stream, addr)
                } else {
                    self.http_socket_pool.handle_new_stream(state, stream, addr)
                }
            }
            RawStream::WS(_) => {
                self.web_socket_pool
                    .handle_new_stream(state, stream, addr)
                    .await
            }
        }
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_info(&self) -> RdmaInfo {
        self.rdma_socket_pool.rdma_info()
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_connect(&self, endpoint: &Endpoint, state: &Arc<State>) -> Result<Endpoint> {
        self.rdma_socket_pool.rdma_connect(endpoint, state)
    }

    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    pub fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    pub async fn join(&self) {
        self.http_socket_pool.join().await;
        self.web_socket_pool.join().await;
        self.tcp_socket_pool.join().await;
        #[cfg(feature = "rdma")]
        self.rdma_socket_pool.join().await;
        self.task_supervisor.all_stopped().await;
    }
}

impl std::fmt::Debug for UnifiedSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedSocketPool").finish()
    }
}
