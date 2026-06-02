use std::{net::SocketAddr, sync::Arc};

use futures_util::TryFutureExt;
use tokio_util::sync::DropGuard;

#[cfg(feature = "rdma")]
use crate::rdma::{ConnectRequest, Endpoint, RdmaInfo, RdmaSocketPool};
use crate::{
    BufferPool, Devices, Error, ErrorKind, RawStream, Result, Socket, SocketPoolConfig,
    SocketPoolTrait, SocketType, State, TaskSupervisor,
    http::HttpSocketPool,
    tcp::{self, TcpSocketPool},
    ws::WebSocketPool,
};

pub struct UnifiedSocketPool {
    pub tcp_socket_pool: TcpSocketPool,
    pub web_socket_pool: WebSocketPool,
    pub http_socket_pool: HttpSocketPool,
    #[cfg(feature = "rdma")]
    pub rdma_socket_pool: RdmaSocketPool,
    task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for UnifiedSocketPool {
    fn create(
        config: &SocketPoolConfig,
        devices: &Arc<Devices>,
        buffer_pool: &Arc<BufferPool>,
    ) -> Result<Self> {
        let this = Self {
            tcp_socket_pool: TcpSocketPool::create(config, devices, buffer_pool)?,
            web_socket_pool: WebSocketPool::create(config, devices, buffer_pool)?,
            http_socket_pool: HttpSocketPool::create(config, devices, buffer_pool)?,
            #[cfg(feature = "rdma")]
            rdma_socket_pool: RdmaSocketPool::create(config, devices, buffer_pool)?,
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

    async fn acquire(
        &self,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<Socket> {
        match socket_type {
            SocketType::TCP | SocketType::UNIFIED => {
                self.tcp_socket_pool
                    .acquire(addr, SocketType::TCP, state)
                    .await
            }
            SocketType::WS => {
                self.web_socket_pool
                    .acquire(addr, SocketType::WS, state)
                    .await
            }
            SocketType::HTTP => {
                self.http_socket_pool
                    .acquire(addr, SocketType::HTTP, state)
                    .await
            }
            #[cfg(feature = "rdma")]
            SocketType::RDMA => {
                self.rdma_socket_pool
                    .acquire(addr, SocketType::RDMA, state)
                    .await
            }
        }
    }

    async fn handle_new_stream(
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
                    self.tcp_socket_pool
                        .handle_new_stream(state, stream, addr)
                        .await
                } else {
                    self.http_socket_pool
                        .handle_new_stream(state, stream, addr)
                        .await
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
    fn rdma_device_list(&self) -> Result<RdmaInfo> {
        self.rdma_socket_pool.rdma_device_list()
    }

    #[cfg(feature = "rdma")]
    fn rdma_accept(&self, request: &ConnectRequest, state: &Arc<State>) -> Result<Endpoint> {
        self.rdma_socket_pool.rdma_accept(request, state)
    }

    fn stop(&self) {
        self.task_supervisor.stop();
    }

    fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    async fn join(&self) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "rdma")]
    fn make_rdma_devices() -> Arc<crate::Devices> {
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
        Arc::new(devices)
    }

    #[tokio::test]
    async fn test_unified_socket_pool_debug_format() {
        let config = crate::SocketPoolConfig {
            socket_type: crate::SocketType::UNIFIED,
            ..Default::default()
        };
        let devices = Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = UnifiedSocketPool::create(&config, &devices, &buffer_pool).unwrap();
        let debug = format!("{pool:?}");
        assert!(debug.contains("UnifiedSocketPool"));
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_unified_socket_pool_rdma_device_list() {
        let devices = make_rdma_devices();
        let config = crate::SocketPoolConfig {
            socket_type: crate::SocketType::UNIFIED,
            ..Default::default()
        };
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = UnifiedSocketPool::create(&config, &devices, &buffer_pool).unwrap();
        let info = pool.rdma_device_list().unwrap();
        assert!(!info.devices.is_empty());
        pool.stop();
        pool.join().await;
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_unified_socket_pool_rdma_accept_no_device_returns_err() {
        // Unified pool without RDMA devices: rdma_accept should return an error.
        let config = crate::SocketPoolConfig {
            socket_type: crate::SocketType::UNIFIED,
            ..Default::default()
        };
        let devices = Arc::new(crate::Devices::default()); // TCP only
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = UnifiedSocketPool::create(&config, &devices, &buffer_pool).unwrap();
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
            config: crate::rdma::RdmaConnectionConfig {
                qp: crate::RdmaQueuePairConfig::default(),
                cq_len: 128,
                recv_queue_len: 64,
                recv_buffer_size: 64 * 1024,
            },
        };
        assert!(pool.rdma_accept(&request, &state).is_err());
        pool.stop();
        pool.join().await;
    }
}
