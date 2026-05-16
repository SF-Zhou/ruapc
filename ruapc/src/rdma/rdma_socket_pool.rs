use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
};

use foldhash::fast::RandomState;
use ruapc_bufpool::Device as _;
use ruapc_rdma::{
    CompChannel, CompletionQueue, QueuePair, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type,
};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::{Endpoint, EventLoop, RdmaDevice, RdmaInfo, RdmaService, RdmaSocket};
use crate::{
    BufferPool, Client, Context, Devices, Error, ErrorKind, Result, Socket, SocketPoolConfig,
    SocketPoolTrait, SocketType, State, TaskSupervisor, rdma::event_loop::SendMsg,
};

/// A pool managing RDMA sockets and their associated resources.
///
/// # Drop order
///
/// RDMA resources **must** be destroyed in this order:
///   1. Stop all async tasks (so EventLoop drops its `Arc<RdmaSocket>`)
///   2. Destroy QPs (`socket_map`)
///   3. Deregister MRs (`buffer_pool`)
///   4. Destroy PD / close device context (`devices`)
///
/// Rust drops struct fields in **declaration order**, so the fields below
/// are intentionally ordered to satisfy the ibverbs requirement.
pub struct RdmaSocketPool {
    /// Client used for acquiring RDMA connections (no RDMA resources).
    pub acquire_client: Client,
    /// Supervisor for managing asynchronous tasks — dropped first so that
    /// all spawned EventLoop tasks finish and release their `Arc<RdmaSocket>`.
    pub task_supervisor: TaskSupervisor,
    /// Thread-safe map of active RDMA sockets indexed by their addresses.
    /// Dropped after tasks stop → destroys QPs.
    pub socket_map: RwLock<HashMap<SocketAddr, Arc<RdmaSocket>, RandomState>>,
    /// Shared buffer pool (owned by State, kept alive via Arc).
    /// Dropped after QPs → deregisters MRs.
    pub buffer_pool: Arc<BufferPool>,
    /// Global device collection (owned by State, kept alive via Arc).
    /// Dropped last → destroys PD and closes device context.
    pub devices: Arc<Devices>,
    /// Counter for round-robin device selection.
    next_device: AtomicUsize,
}

impl SocketPoolTrait for RdmaSocketPool {
    fn create(
        _config: &SocketPoolConfig,
        devices: &Arc<Devices>,
        buffer_pool: &Arc<BufferPool>,
    ) -> Result<Self> {
        Ok(Self::new(devices.clone(), buffer_pool.clone()))
    }

    /// Stops all tasks managed by the socket pool.
    fn stop(&self) {
        self.task_supervisor.stop();
    }

    /// Returns a guard that will stop all tasks when dropped.
    fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    /// Waits for all tasks to complete.
    async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

    /// Returns information about the available RDMA devices.
    fn rdma_info(&self) -> Result<RdmaInfo> {
        Ok(RdmaInfo {
            devices: self
                .devices
                .rdma_devices()
                .iter()
                .map(|d| d.inner().info().clone())
                .collect(),
        })
    }

    /// Establishes a new RDMA connection with the specified endpoint.
    fn rdma_connect(&self, endpoint: &Endpoint, state: &Arc<State>) -> Result<Endpoint> {
        let device = self.pick_rdma_device()?;
        let (queue_pair, comp_channel, cq) = self.create_queue_pair(device)?;
        queue_pair
            .connect(endpoint.qp_num, endpoint.gid, endpoint.lid)
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let local_endpoint = self.make_endpoint(&queue_pair, device);
        let (tx, rx) = tokio::sync::mpsc::channel::<SendMsg>(1024);
        let rdma_socket = RdmaSocket::new(queue_pair, self.buffer_pool.clone(), tx);
        self.start_event_loop(Arc::new(rdma_socket), state.clone(), comp_channel, cq, rx)?;
        Ok(local_endpoint)
    }

    /// Acquires or creates an RDMA socket for the specified address.
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

        let device = self.pick_rdma_device()?;
        let (queue_pair, comp_channel, cq) = self.create_queue_pair(device)?;
        let local_endpoint = self.make_endpoint(&queue_pair, device);

        let acquire_ctx = Context::create_with_state_and_addr(state, addr);
        let remote_endpoint =
            Box::pin(self.acquire_client.connect(&acquire_ctx, &local_endpoint)).await?;
        queue_pair
            .connect(
                remote_endpoint.qp_num,
                remote_endpoint.gid,
                remote_endpoint.lid,
            )
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        tracing::info!("acquired socket: {:?}", queue_pair);

        let (tx, rx) = tokio::sync::mpsc::channel::<SendMsg>(1024);
        let socket = RdmaSocket::new(queue_pair, self.buffer_pool.clone(), tx);
        let socket = Arc::new(socket);
        socket_map.insert(*addr, socket.clone());
        self.start_event_loop(socket.clone(), state.clone(), comp_channel, cq, rx)?;
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
    /// Creates a new pool using the shared devices and buffer pool.
    pub fn new(devices: Arc<Devices>, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            acquire_client: Client {
                timeout: std::time::Duration::from_secs(5),
                use_msgpack: true,
                socket_type: Some(SocketType::TCP),
            },
            task_supervisor: TaskSupervisor::create(),
            socket_map: RwLock::default(),
            buffer_pool,
            devices,
            next_device: AtomicUsize::new(0),
        }
    }

    /// Returns an RDMA device and its index using round-robin selection.
    fn pick_rdma_device(&self) -> Result<&RdmaDevice> {
        let rdma_devices = self.devices.rdma_devices();
        if rdma_devices.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "no RDMA device available".into(),
            ));
        }
        let idx = self.next_device.fetch_add(1, Ordering::Relaxed) % rdma_devices.len();
        Ok(&rdma_devices[idx])
    }

    /// Creates a QueuePair along with its CompChannel and CompletionQueue.
    fn create_queue_pair(
        &self,
        device: &RdmaDevice,
    ) -> Result<(QueuePair, Arc<CompChannel>, Arc<CompletionQueue>)> {
        let rdma = device;
        let ctx = rdma.inner().context();
        let pd = rdma.pd();

        let comp_channel = CompChannel::create(ctx)
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        comp_channel
            .set_nonblock()
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let cq = CompletionQueue::create(ctx, 128, Some(&comp_channel))
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 64,
                max_recv_wr: 64,
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 0,
            },
            ..Default::default()
        };

        let queue_pair = QueuePair::create(pd, &cq, &cq, &mut init_attr, device.index())
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        Ok((queue_pair, comp_channel, cq))
    }

    /// Constructs an Endpoint from a QueuePair.
    fn make_endpoint(&self, qp: &QueuePair, device: &RdmaDevice) -> Endpoint {
        let info = device.inner().info();
        Endpoint {
            qp_num: qp.qp_num(),
            lid: 0,
            gid: info.ports[0].gids[1].gid,
        }
    }

    /// Starts the event loop for handling RDMA events on a socket.
    pub fn start_event_loop(
        &self,
        socket: Arc<RdmaSocket>,
        state: Arc<State>,
        comp_channel: Arc<CompChannel>,
        cq: Arc<CompletionQueue>,
        pending_receiver: tokio::sync::mpsc::Receiver<SendMsg>,
    ) -> Result<()> {
        let socket_clone = socket.clone();
        let mut event_loop = EventLoop::new(socket, state, comp_channel, cq, pending_receiver);
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

impl Drop for RdmaSocketPool {
    fn drop(&mut self) {
        self.task_supervisor.stop();
        self.socket_map.get_mut().clear();
    }
}

impl std::fmt::Debug for RdmaSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaSocketPool").finish()
    }
}
