use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use foldhash::fast::RandomState;
use ruapc_rdma::{Devices, QueuePair, verbs};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::{Endpoint, EventLoop, RdmaInfo, RdmaService, RdmaSocket};
use crate::{
    BufferPool, Client, Context, Error, ErrorKind, Result, Socket, SocketPoolConfig,
    SocketPoolTrait, SocketType, State, TaskSupervisor,
};

/// A pool managing RDMA sockets and their associated resources.
///
/// This structure maintains a collection of RDMA sockets, manages their lifecycle,
/// and handles the shared resources like buffer pools and device access.
pub struct RdmaSocketPool {
    /// Client used for acquiring RDMA connections
    pub acquire_client: Client,
    /// Shared buffer pool for RDMA send/recv buffers (from State).
    pub rdmabuf_pool: Arc<BufferPool>,
    /// Available RDMA devices in the system
    pub devices: Devices,
    /// Thread-safe map of active RDMA sockets indexed by their addresses
    pub socket_map: RwLock<HashMap<SocketAddr, Arc<RdmaSocket>, RandomState>>,
    /// Supervisor for managing asynchronous tasks
    pub task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for RdmaSocketPool {
    /// Creates a new RDMA socket pool with default configuration.
    ///
    /// Uses a minimal dummy buffer pool (no RDMA devices registered).
    /// This path is only hit when no RDMA devices are present, so the
    /// pool is never exercised for actual RDMA operations.
    fn create(_: &SocketPoolConfig) -> Result<Self> {
        let devices = Devices::availables().unwrap_or_else(|_| Devices::from_arcs(vec![]));
        // No shared state pool available here; create a placeholder with no
        // RDMA registrations. Actual RDMA connections use create_from_devices().
        let dummy_pool = BufferPool::new(
            Arc::new(crate::Devices::new()),
            4 * 1024 * 1024,
            8 * 1024 * 1024,
            0,
        );
        Self::create_from_devices(devices, dummy_pool)
    }

    fn stop(&self) {
        self.task_supervisor.stop();
    }

    fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

    fn rdma_info(&self) -> Result<RdmaInfo> {
        Ok(RdmaInfo {
            devices: self.devices.iter().map(|d| d.info()).cloned().collect(),
        })
    }

    fn rdma_connect(&self, endpoint: &Endpoint, state: &Arc<State>) -> Result<Endpoint> {
        let cap = verbs::ibv_qp_cap {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        };
        let queue_pair = QueuePair::create(&self.devices[0], cap)?;
        queue_pair.connect(endpoint)?;
        let local_endpoint = queue_pair.endpoint();
        let (tx, rx) = tokio::sync::mpsc::channel::<u64>(1024);
        let rdma_device = state.devices().rdma_device(0)?;
        let rdma_socket = RdmaSocket::new(queue_pair, self.rdmabuf_pool.clone(), rdma_device, tx);
        self.start_event_loop(Arc::new(rdma_socket), state.clone(), rx)?;
        Ok(local_endpoint)
    }

    async fn acquire(
        &self,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<Socket> {
        if socket_type != SocketType::RDMA {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("invalid socket type {socket_type} for RdmaSocketPool"),
            ));
        }

        // Check if the socket is already in the socket map.
        if let Ok(socket_map) = self.socket_map.try_read()
            && let Some(socket) = socket_map.get(addr)
        {
            return Ok(socket.into());
        }

        // If not, create a new socket and insert it into the socket map.
        let mut socket_map = self.socket_map.write().await;
        if let Some(socket) = socket_map.get(addr) {
            return Ok(socket.into());
        }

        let cap = verbs::ibv_qp_cap {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        };
        let queue_pair = QueuePair::create(&self.devices[0], cap)?;
        let local_endpoint = queue_pair.endpoint();

        let acquire_ctx = Context::create_with_state_and_addr(state, addr);
        let remote_endpoint =
            Box::pin(self.acquire_client.connect(&acquire_ctx, &local_endpoint)).await?;
        queue_pair.connect(&remote_endpoint)?;
        tracing::info!("acquired socket: {:?}", queue_pair);

        let (tx, rx) = tokio::sync::mpsc::channel::<u64>(1024);
        let rdma_device = state.devices().rdma_device(0)?;
        let socket = RdmaSocket::new(queue_pair, self.rdmabuf_pool.clone(), rdma_device, tx);
        let socket = Arc::new(socket);
        socket_map.insert(*addr, socket.clone());
        self.start_event_loop(socket.clone(), state.clone(), rx)?;
        Ok(socket.into())
    }

    async fn handle_new_stream(
        &self,
        _state: &Arc<State>,
        _stream: crate::RawStream,
        _addr: SocketAddr,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::InvalidArgument,
            "invalid socket type".into(),
        ))
    }
}

impl RdmaSocketPool {
    /// Creates a pool using the provided RDMA devices and shared buffer pool.
    ///
    /// The `rdmabuf_pool` must be registered on the same RDMA devices so that
    /// QP send/recv buffers share the same protection domain.
    pub fn create_from_devices(devices: Devices, rdmabuf_pool: Arc<BufferPool>) -> Result<Self> {
        Ok(Self {
            acquire_client: Client {
                timeout: std::time::Duration::from_secs(5),
                use_msgpack: true,
                socket_type: Some(SocketType::TCP),
            },
            rdmabuf_pool,
            devices,
            socket_map: RwLock::default(),
            task_supervisor: TaskSupervisor::create(),
        })
    }

    pub fn start_event_loop(
        &self,
        socket: Arc<RdmaSocket>,
        state: Arc<State>,
        pending_receiver: tokio::sync::mpsc::Receiver<u64>,
    ) -> Result<()> {
        let socket_clone = socket.clone();
        let mut event_loop = EventLoop::new(socket, state, pending_receiver);
        event_loop.submit_recv_tasks(64)?;

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            task_supervisor.stopped().await;
            socket_clone.set_error();
        });

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            match event_loop.run().await {
                Ok(()) => tracing::info!("rdma socket event loop stopped"),
                Err(e) => tracing::error!("rdma socket event loop error: {}", e),
            }
            drop(task_supervisor);
        });

        Ok(())
    }
}

impl std::fmt::Debug for RdmaSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaSocketPool").finish()
    }
}
