use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use foldhash::fast::RandomState;
use ruapc_rdma::{BufferPool, Devices, QueuePair, verbs};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::{Endpoint, EventLoop, RdmaInfo, RdmaService, RdmaSocket};
use crate::{Client, Context, Error, ErrorKind, Result, SocketType, State, TaskSupervisor};

pub struct RdmaSocketPool {
    pub acquire_client: Client,
    pub rdmabuf_pool: Arc<BufferPool>,
    pub devices: Devices,
    pub socket_map: RwLock<HashMap<SocketAddr, Arc<RdmaSocket>, RandomState>>,
    pub task_supervisor: TaskSupervisor,
}

impl RdmaSocketPool {
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

    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    pub fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    pub async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

    pub fn rdma_info(&self) -> RdmaInfo {
        RdmaInfo {
            devices: self.devices.iter().map(|d| d.info()).cloned().collect(),
        }
    }

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
            if let Err(e) = event_loop.run().await {
                tracing::info!("result is {}", e);
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
