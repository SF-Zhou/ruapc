use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use foldhash::fast::RandomState;
use ruapc_rdma::{BufferPool, Devices, QueuePair, verbs};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::{Endpoint, EventLoop, RdmaInfo, RdmaService, RdmaSocket};
use crate::{Client, Context, Error, ErrorKind, Result, SocketType, State, TaskSupervisor};

/// A pool managing RDMA sockets and their associated resources.
///
/// This structure maintains a collection of RDMA sockets, manages their lifecycle,
/// and handles the shared resources like buffer pools and device access.
pub struct RdmaSocketPool {
    /// Client used for acquiring RDMA connections
    pub acquire_client: Client,
    /// Shared buffer pool for RDMA operations
    pub rdmabuf_pool: Arc<BufferPool>,
    /// Available RDMA devices in the system
    pub devices: Devices,
    /// Thread-safe map of active RDMA sockets indexed by their addresses
    pub socket_map: RwLock<HashMap<SocketAddr, Arc<RdmaSocket>, RandomState>>,
    /// Supervisor for managing asynchronous tasks
    pub task_supervisor: TaskSupervisor,
}

impl RdmaSocketPool {
    /// Creates a new RDMA socket pool with default configuration.
    ///
    /// This function:
    /// 1. Initializes available RDMA devices
    /// 2. Creates a shared buffer pool
    /// 3. Sets up a default client configuration
    /// 4. Initializes the socket map and task supervisor
    ///
    /// # Returns
    /// * `Ok(Arc<Self>)` - A new thread-safe RDMA socket pool instance
    /// * `Err(Error)` - If RDMA devices cannot be initialized or buffer pool creation fails
    pub fn create() -> Result<Arc<Self>> {
        let devices = Devices::availables()?;
        let rdmabuf_pool = BufferPool::create(4096, 4096, &devices)?;
        let this = Arc::new(Self {
            acquire_client: Client {
                timeout: std::time::Duration::from_secs(5),
                use_msgpack: true,
                socket_type: Some(SocketType::TCP),
            },
            rdmabuf_pool,
            devices,
            socket_map: RwLock::default(),
            task_supervisor: TaskSupervisor::create(),
        });
        Ok(this)
    }

    /// Stops all tasks managed by the socket pool.
    /// This initiates the shutdown process for all active RDMA connections.
    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    /// Returns a guard that will stop all tasks when dropped.
    /// This is useful for implementing RAII-style cleanup of RDMA resources.
    pub fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    /// Waits for all tasks to complete.
    /// This should be called after `stop()` to ensure clean shutdown.
    pub async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

    /// Returns information about the available RDMA devices.
    ///
    /// # Returns
    /// `RdmaInfo` containing details of all available RDMA devices
    pub fn rdma_info(&self) -> RdmaInfo {
        RdmaInfo {
            devices: self.devices.iter().map(|d| d.info()).cloned().collect(),
        }
    }

    /// Establishes a new RDMA connection with the specified endpoint.
    ///
    /// This method:
    /// 1. Creates a new queue pair with default capabilities
    /// 2. Connects to the remote endpoint
    /// 3. Sets up the event loop for handling RDMA events
    ///
    /// # Arguments
    /// * `endpoint` - The remote endpoint to connect to
    /// * `state` - The shared state for the connection
    ///
    /// # Returns
    /// * `Ok(Endpoint)` - The local endpoint information if connection succeeds
    /// * `Err(Error)` - If connection setup fails
    pub fn rdma_connect(
        self: &Arc<Self>,
        endpoint: &Endpoint,
        state: &Arc<State>,
    ) -> Result<Endpoint> {
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
        let rdma_socket = RdmaSocket::new(queue_pair, self.rdmabuf_pool.clone(), tx);
        self.start_event_loop(Arc::new(rdma_socket), state.clone(), rx)?;
        Ok(local_endpoint)
    }

    /// Acquires or creates an RDMA socket for the specified address.
    ///
    /// This method first checks if a socket already exists for the given address.
    /// If not, it creates a new RDMA connection by:
    /// 1. Creating a new queue pair
    /// 2. Exchanging endpoint information with the remote peer
    /// 3. Establishing the RDMA connection
    /// 4. Setting up the event loop
    ///
    /// # Arguments
    /// * `addr` - The socket address to connect to
    /// * `socket_type` - Must be `SocketType::RDMA`
    /// * `state` - The shared state for the connection
    ///
    /// # Returns
    /// * `Ok(Arc<RdmaSocket>)` - A thread-safe reference to the RDMA socket
    /// * `Err(Error)` - If socket creation fails or if socket type is invalid
    pub async fn acquire(
        self: &Arc<Self>,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<Arc<RdmaSocket>> {
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
            return Ok(socket.clone());
        }

        // If not, create a new socket and insert it into the socket map.
        let mut socket_map = self.socket_map.write().await;
        if let Some(socket) = socket_map.get(addr) {
            return Ok(socket.clone());
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
        let socket = RdmaSocket::new(queue_pair, self.rdmabuf_pool.clone(), tx);
        let socket = Arc::new(socket);
        socket_map.insert(*addr, socket.clone());
        self.start_event_loop(socket.clone(), state.clone(), rx)?;
        Ok(socket)
    }

    /// Starts the event loop for handling RDMA events on a socket.
    ///
    /// This method:
    /// 1. Initializes receive tasks for the socket
    /// 2. Sets up error handling for task supervisor shutdown
    /// 3. Spawns the main event loop task
    ///
    /// # Arguments
    /// * `socket` - The RDMA socket to manage
    /// * `state` - Shared state for the connection
    /// * `pending_receiver` - Channel for receiving pending operation notifications
    ///
    /// # Returns
    /// * `Ok(())` - If event loop setup succeeds
    /// * `Err(Error)` - If setup fails
    pub fn start_event_loop(
        self: &Arc<Self>,
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
