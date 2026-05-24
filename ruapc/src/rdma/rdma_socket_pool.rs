use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use foldhash::fast::RandomState;
use ruapc_bufpool::Device as _;
use ruapc_rdma::{
    CompChannel, CompletionQueue, DeviceInfo, Gid, GidType, LinkLayer, Port, QueuePair, ibv_mtu,
    ibv_port_state, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type,
};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::{
    ConnectRequest, DeviceSelection, Endpoint, EventLoop, RdmaDevice, RdmaDeviceRefresher,
    RdmaInfo, RdmaService, RdmaSocket,
};
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
    /// Background port/GID cache refresher. Dropped before RDMA devices.
    port_refresher: RdmaDeviceRefresher,
    /// Per-peer connection locks prevent duplicate in-flight RDMA handshakes.
    connect_locks: dashmap::DashMap<SocketAddr, Arc<tokio::sync::Mutex<()>>, RandomState>,
    /// Short-lived cache for the first-stage server RDMA device query.
    query_cache: RwLock<HashMap<SocketAddr, CachedRdmaInfo, RandomState>>,
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
        Self::new(devices.clone(), buffer_pool.clone())
    }

    /// Stops all tasks managed by the socket pool.
    fn stop(&self) {
        self.task_supervisor.stop();
        self.port_refresher.stop();
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
                .map(|d| (*d.info()).clone())
                .collect(),
        })
    }

    /// Establishes a new RDMA connection with the specified endpoint.
    fn rdma_connect(&self, request: &ConnectRequest, state: &Arc<State>) -> Result<Endpoint> {
        let device = self.find_rdma_device(&request.target)?;
        let (queue_pair, comp_channel, cq) = self.create_queue_pair(device)?;
        let local_endpoint = self.make_endpoint(
            &queue_pair,
            device,
            request.target.port_num,
            request.target.gid_index,
        )?;
        self.connect_queue_pair(&queue_pair, &local_endpoint, &request.endpoint)?;

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

        let connect_lock = self
            .connect_locks
            .entry(*addr)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let guard = connect_lock.lock().await;
        let result = self.acquire_with_connect_lock(addr, state).await;
        self.connect_locks.remove_if(addr, |_, value| {
            Arc::ptr_eq(value, &connect_lock) && Arc::strong_count(value) == 2
        });
        drop(guard);
        result
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
    const QUERY_CACHE_TTL: Duration = Duration::from_secs(5);

    /// Creates a new pool using the shared devices and buffer pool.
    pub fn new(devices: Arc<Devices>, buffer_pool: Arc<BufferPool>) -> Result<Self> {
        Ok(Self {
            acquire_client: Client {
                timeout: std::time::Duration::from_secs(5),
                use_msgpack: true,
                socket_type: Some(SocketType::TCP),
            },
            task_supervisor: TaskSupervisor::create(),
            port_refresher: RdmaDeviceRefresher::start(devices.clone())?,
            connect_locks: dashmap::DashMap::default(),
            query_cache: RwLock::default(),
            socket_map: RwLock::default(),
            buffer_pool,
            devices,
            next_device: AtomicUsize::new(0),
        })
    }

    fn find_rdma_device(&self, selection: &DeviceSelection) -> Result<&RdmaDevice> {
        self.devices
            .rdma_devices()
            .iter()
            .find(|device| device.info().name.as_str() == selection.device_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("RDMA device {} not found", selection.device_name),
                )
            })
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

    async fn acquire_with_connect_lock(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
    ) -> Result<Socket> {
        if let Some(socket) = self.socket_map.read().await.get(addr).cloned() {
            return Ok(socket.into());
        }

        let acquire_ctx = Context::create_with_state_and_addr(state, addr);
        let remote_info = self.query_remote_info(addr, &acquire_ctx).await?;
        let selection = match self.select_device(&remote_info) {
            Ok(selection) => selection,
            Err(err) => {
                self.invalidate_query_cache(addr).await;
                return Err(err);
            }
        };

        let device = self
            .devices
            .rdma_devices()
            .get(selection.local_device_index)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    "selected RDMA device disappeared".into(),
                )
            })?;
        let (queue_pair, comp_channel, cq) = self.create_queue_pair(device)?;
        let local_endpoint = self.make_endpoint(
            &queue_pair,
            device,
            selection.local_port_num,
            selection.local_gid_index,
        )?;

        let connect_request = ConnectRequest {
            endpoint: local_endpoint,
            target: selection.remote.clone(),
        };
        let remote_endpoint =
            match Box::pin(self.acquire_client.connect(&acquire_ctx, &connect_request)).await {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    self.invalidate_query_cache(addr).await;
                    return Err(err);
                }
            };
        self.connect_queue_pair(&queue_pair, &local_endpoint, &remote_endpoint)?;

        let (tx, rx) = tokio::sync::mpsc::channel::<SendMsg>(1024);
        let socket = RdmaSocket::new(queue_pair, self.buffer_pool.clone(), tx);
        let socket = Arc::new(socket);
        self.start_event_loop(socket.clone(), state.clone(), comp_channel, cq, rx)?;
        {
            let mut socket_map = self.socket_map.write().await;
            socket_map.insert(*addr, socket.clone());
        }
        let local_info = device.info();
        tracing::info!(
            local_device = %local_info.name,
            local_port = selection.local_port_num,
            local_gid_index = selection.local_gid_index,
            remote_device = %selection.remote.device_name,
            remote_port = selection.remote.port_num,
            remote_gid_index = selection.remote.gid_index,
            "acquired RDMA socket"
        );
        Ok(socket.into())
    }

    async fn query_remote_info(&self, addr: &SocketAddr, ctx: &Context) -> Result<RdmaInfo> {
        if let Some(info) = self.cached_remote_info(addr).await {
            return Ok(info);
        }

        let info = Box::pin(self.acquire_client.info(ctx, &())).await?;
        self.query_cache.write().await.insert(
            *addr,
            CachedRdmaInfo {
                info: info.clone(),
                cached_at: Instant::now(),
            },
        );
        Ok(info)
    }

    async fn cached_remote_info(&self, addr: &SocketAddr) -> Option<RdmaInfo> {
        let cache = self.query_cache.read().await;
        let cached = cache.get(addr)?;
        if cached.cached_at.elapsed() < Self::QUERY_CACHE_TTL {
            Some(cached.info.clone())
        } else {
            None
        }
    }

    async fn invalidate_query_cache(&self, addr: &SocketAddr) {
        self.query_cache.write().await.remove(addr);
    }

    fn select_device(&self, remote_info: &RdmaInfo) -> Result<SelectedDevice> {
        let local_devices = self.devices.rdma_devices();
        if local_devices.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "no local RDMA device available".into(),
            ));
        }

        let mut ib_matches = Vec::new();
        let mut roce_matches = Vec::new();
        let mut remote_usable_ports = 0usize;
        let mut local_usable_ports = 0usize;
        let mut link_layer_matches = 0usize;

        for remote_device in &remote_info.devices {
            for remote_port in &remote_device.ports {
                if !Self::port_is_usable(remote_port) {
                    continue;
                }
                remote_usable_ports += 1;

                for (local_device_index, local_device) in local_devices.iter().enumerate() {
                    let local_info = local_device.info();
                    for local_port in &local_info.ports {
                        if !Self::port_is_usable(local_port) {
                            continue;
                        }
                        local_usable_ports += 1;
                        if local_port.port_attr.link_layer != remote_port.port_attr.link_layer {
                            continue;
                        }
                        link_layer_matches += 1;

                        let Some((local_gid_index, remote_gid_index)) =
                            Self::select_gid_pair(local_port, remote_port)
                        else {
                            continue;
                        };

                        let selected = SelectedDevice {
                            local_device_index,
                            local_port_num: local_port.port_num,
                            local_gid_index,
                            remote: DeviceSelection {
                                device_name: remote_device.name.clone(),
                                port_num: remote_port.port_num,
                                gid_index: remote_gid_index,
                            },
                        };

                        match local_port.port_attr.link_layer {
                            LinkLayer::InfiniBand => ib_matches.push(selected),
                            LinkLayer::Ethernet => roce_matches.push(selected),
                            LinkLayer::Unspecified => {}
                        }
                    }
                }
            }
        }

        let matches = if ib_matches.is_empty() {
            &roce_matches
        } else {
            &ib_matches
        };
        if matches.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "no compatible RDMA device/port/GID pair found: remote_devices={} local_devices={} remote_usable_ports={} local_usable_ports={} link_layer_matches={}",
                    remote_info.devices.len(),
                    local_devices.len(),
                    remote_usable_ports,
                    local_usable_ports,
                    link_layer_matches
                ),
            ));
        }

        let idx = self.next_device.fetch_add(1, Ordering::Relaxed) % matches.len();
        Ok(matches[idx].clone())
    }

    fn port_is_usable(port: &Port) -> bool {
        matches!(
            port.port_attr.state,
            ibv_port_state::IBV_PORT_ACTIVE | ibv_port_state::IBV_PORT_ACTIVE_DEFER
        ) && matches!(
            port.port_attr.link_layer,
            LinkLayer::InfiniBand | LinkLayer::Ethernet
        )
    }

    fn select_gid_pair(local_port: &Port, remote_port: &Port) -> Option<(u8, u8)> {
        match local_port.port_attr.link_layer {
            LinkLayer::InfiniBand => Some((
                Self::first_gid_index(local_port, |_| true).unwrap_or(0),
                Self::first_gid_index(remote_port, |_| true).unwrap_or(0),
            )),
            LinkLayer::Ethernet => Self::select_roce_gid_pair(local_port, remote_port),
            LinkLayer::Unspecified => None,
        }
    }

    fn select_roce_gid_pair(local_port: &Port, remote_port: &Port) -> Option<(u8, u8)> {
        let preferred = [
            (
                Self::first_gid_index(local_port, |gid| matches!(gid.gid_type, GidType::RoCEv2)),
                Self::first_gid_index(remote_port, |gid| matches!(gid.gid_type, GidType::RoCEv2)),
            ),
            (
                Self::first_gid_index(local_port, |gid| matches!(gid.gid_type, GidType::RoCEv1)),
                Self::first_gid_index(remote_port, |gid| matches!(gid.gid_type, GidType::RoCEv1)),
            ),
        ];

        for (local, remote) in preferred {
            if let (Some(local), Some(remote)) = (local, remote) {
                return Some((local, remote));
            }
        }

        for local_gid in &local_port.gids {
            for remote_gid in &remote_port.gids {
                if local_gid.gid_type == remote_gid.gid_type {
                    return Some((
                        u8::try_from(local_gid.index).ok()?,
                        u8::try_from(remote_gid.index).ok()?,
                    ));
                }
            }
        }

        Some((
            Self::first_gid_index(local_port, |_| true)?,
            Self::first_gid_index(remote_port, |_| true)?,
        ))
    }

    fn first_gid_index(port: &Port, mut predicate: impl FnMut(&Gid) -> bool) -> Option<u8> {
        port.gids
            .iter()
            .find(|gid| predicate(gid))
            .and_then(|gid| u8::try_from(gid.index).ok())
    }

    /// Constructs an Endpoint from a QueuePair and selected local port/GID.
    fn make_endpoint(
        &self,
        qp: &QueuePair,
        device: &RdmaDevice,
        port_num: u8,
        gid_index: u8,
    ) -> Result<Endpoint> {
        let info = device.info();
        let port = Self::find_port(&info, port_num)?;
        if !Self::port_is_usable(port) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("RDMA port {}:{} is not active", info.name, port_num),
            ));
        }

        let gid = port
            .gids
            .iter()
            .find(|gid| u8::try_from(gid.index).ok() == Some(gid_index))
            .map(|gid| gid.gid);
        if port.port_attr.link_layer.is_ethernet() && gid.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA port {}:{} does not have GID index {}",
                    info.name, port_num, gid_index
                ),
            ));
        }

        Ok(Endpoint {
            qp_num: qp.qp_num(),
            port_num,
            gid_index,
            lid: port.port_attr.lid,
            gid: gid.unwrap_or_default(),
            link_layer: port.port_attr.link_layer,
            active_mtu: port.port_attr.active_mtu,
        })
    }

    fn find_port(info: &DeviceInfo, port_num: u8) -> Result<&Port> {
        info.ports
            .iter()
            .find(|port| port.port_num == port_num)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("RDMA port {}:{} not found", info.name, port_num),
                )
            })
    }

    fn connect_queue_pair(
        &self,
        qp: &QueuePair,
        local: &Endpoint,
        remote: &Endpoint,
    ) -> Result<()> {
        if local.link_layer != remote.link_layer {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA link layer mismatch: local {} remote {}",
                    local.link_layer, remote.link_layer
                ),
            ));
        }

        let path_mtu = Self::min_mtu(local.active_mtu, remote.active_mtu);
        qp.connect(
            local.port_num,
            local.gid_index,
            local.link_layer,
            path_mtu,
            remote.qp_num,
            remote.gid,
            remote.lid,
        )
        .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
    }

    fn min_mtu(a: ibv_mtu, b: ibv_mtu) -> ibv_mtu {
        if (a as u32) <= (b as u32) { a } else { b }
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
        self.port_refresher.stop();
        self.socket_map.get_mut().clear();
    }
}

impl std::fmt::Debug for RdmaSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaSocketPool").finish()
    }
}

#[derive(Clone)]
struct SelectedDevice {
    local_device_index: usize,
    local_port_num: u8,
    local_gid_index: u8,
    remote: DeviceSelection,
}

struct CachedRdmaInfo {
    info: RdmaInfo,
    cached_at: Instant,
}
