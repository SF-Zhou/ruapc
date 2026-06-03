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
    CompChannel, CompletionQueue, DeviceInfo, GidType, LinkLayer, Port, QueuePair, ibv_mtu,
    ibv_port_state, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type,
};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::rdma_service::{RdmaDeviceInfo, RdmaGidInfo, RdmaPortInfo};
use super::{
    ConnState, ConnectRequest, CqPool, DeviceSelection, Endpoint, RdmaConnectionConfig, RdmaDevice,
    RdmaDeviceRefresher, RdmaInfo, RdmaService, RdmaSocket, SendMsg, SharedCq,
};
use crate::{
    BufferPool, Client, Context, Devices, Error, ErrorKind, RdmaQueuePairConfig,
    RdmaSocketPoolConfig, Result, Socket, SocketPoolConfig, SocketPoolTrait, SocketType, State,
    TaskSupervisor,
};

/// A pool managing RDMA sockets and their associated resources.
///
/// # Drop order
///
/// RDMA resources **must** be destroyed in this order:
///   1. Stop async tasks and **join the CQ poller threads** (`cq_pool`), so no
///      thread polls a CQ/QP after it is destroyed and every poller releases
///      its `ConnState` (and thus its `Arc<RdmaSocket>`).
///   2. Destroy QPs (`socket_map`)
///   3. Deregister MRs (`buffer_pool`)
///   4. Destroy PD / close device context (`devices`)
///
/// Rust drops struct fields in **declaration order**, so the fields below
/// are intentionally ordered to satisfy the ibverbs requirement.
pub struct RdmaSocketPool {
    /// Client used for acquiring RDMA connections (no RDMA resources).
    pub acquire_client: Client,
    /// Supervisor for managing asynchronous helper tasks (port refresher).
    pub task_supervisor: TaskSupervisor,
    /// Background port/GID cache refresher. Dropped before RDMA devices.
    _port_refresher: RdmaDeviceRefresher,
    /// Per-peer connection locks prevent duplicate in-flight RDMA handshakes.
    connect_locks: dashmap::DashMap<SocketAddr, Arc<tokio::sync::Mutex<()>>, RandomState>,
    /// Short-lived cache for the first-stage server RDMA device query.
    device_list_cache: RwLock<HashMap<SocketAddr, CachedRdmaInfo, RandomState>>,
    /// Shared completion-queue pool + dedicated poller threads. Dropped first
    /// (joins poller threads) so nothing polls a CQ/QP during teardown.
    cq_pool: CqPool,
    /// Thread-safe map of active RDMA sockets indexed by their addresses.
    /// Dropped after pollers stop → destroys QPs.
    pub socket_map: RwLock<HashMap<SocketAddr, Arc<RdmaSocket>, RandomState>>,
    /// Shared buffer pool (owned by State, kept alive via Arc).
    /// Dropped after QPs → deregisters MRs.
    pub buffer_pool: Arc<BufferPool>,
    /// Global device collection (owned by State, kept alive via Arc).
    /// Dropped last → destroys PD and closes device context.
    pub devices: Arc<Devices>,
    /// RDMA Queue Pair and connection settings.
    pub config: RdmaSocketPoolConfig,
    /// Counter for round-robin device selection.
    next_device: AtomicUsize,
    /// Counter for spreading connections across the shared CQs.
    next_cq: AtomicUsize,
}

impl SocketPoolTrait for RdmaSocketPool {
    fn create(
        config: &SocketPoolConfig,
        devices: &Arc<Devices>,
        buffer_pool: &Arc<BufferPool>,
    ) -> Result<Self> {
        Self::new(devices.clone(), buffer_pool.clone(), config.rdma.clone())
    }

    /// Stops all tasks managed by the socket pool.
    fn stop(&self) {
        self.task_supervisor.stop();
        // Signal the CQ pollers to drain their connections and join the poller
        // threads. After this returns no thread is polling, so the QPs/CQs can
        // be safely destroyed.
        self.cq_pool.shutdown();
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
    fn rdma_device_list(&self) -> Result<RdmaInfo> {
        Ok(RdmaInfo::from_devices(
            self.devices.rdma_devices(),
            &self.config,
        ))
    }

    /// Establishes a new RDMA connection with the specified endpoint.
    fn rdma_accept(&self, request: &ConnectRequest, state: &Arc<State>) -> Result<Endpoint> {
        let (device_index, device) = self.find_device_by_name(&request.target)?;
        let connection_config = self.clamp_connection_config(device, request.config);
        let shared = self.pick_cq(device_index);
        let queue_pair = self.create_queue_pair(device, &connection_config, &shared)?;
        let local_endpoint = self.build_endpoint(
            &queue_pair,
            device,
            request.target.port_num,
            request.target.gid_index,
        )?;
        self.bring_qp_to_rts(
            &queue_pair,
            &local_endpoint,
            &request.endpoint,
            self.config.pkey_index,
        )?;

        self.establish_connection(queue_pair, &shared, state, &connection_config)?;
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
        let result = self.handshake(addr, state).await;
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
    const DEVICE_LIST_CACHE_TTL: Duration = Duration::from_secs(5);

    /// Creates a new pool using the shared devices and buffer pool.
    pub fn new(
        devices: Arc<Devices>,
        buffer_pool: Arc<BufferPool>,
        config: RdmaSocketPoolConfig,
    ) -> Result<Self> {
        let task_supervisor = TaskSupervisor::create();
        let port_refresher = RdmaDeviceRefresher::start(devices.clone(), &task_supervisor);

        // Build the shared completion-queue pool: `cq_count` CQs (each with a
        // dedicated poller thread) per RDMA device. Connections are pinned to
        // one CQ for their lifetime, so a fixed (O(cores)) number of poller
        // threads serves an unbounded number of connections.
        let rdma_devices = devices.rdma_devices();
        let device_count = rdma_devices.len();
        let cq_size = config.effective_cq_size();
        let runtime = tokio::runtime::Handle::current();
        let cq_pool = CqPool::new(device_count, config.cq_count as usize, runtime, |dev| {
            let device = &rdma_devices[dev];
            let ctx = device.context();
            let comp_channel = CompChannel::create(ctx)
                .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
            let cqe = cq_size.min(device.info().device_attr.max_cqe as usize);
            let cq = CompletionQueue::create(ctx, cqe as _, Some(&comp_channel))
                .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
            Ok((cq, comp_channel))
        })?;

        Ok(Self {
            acquire_client: Client {
                timeout: std::time::Duration::from_secs(5),
                use_msgpack: true,
                socket_type: Some(SocketType::TCP),
            },
            task_supervisor,
            _port_refresher: port_refresher,
            connect_locks: dashmap::DashMap::default(),
            device_list_cache: RwLock::default(),
            cq_pool,
            socket_map: RwLock::default(),
            buffer_pool,
            devices,
            config,
            next_device: AtomicUsize::new(0),
            next_cq: AtomicUsize::new(0),
        })
    }

    fn find_device_by_name(&self, selection: &DeviceSelection) -> Result<(usize, &RdmaDevice)> {
        self.devices
            .rdma_devices()
            .iter()
            .enumerate()
            .find(|(_, device)| device.info().name.as_str() == selection.device_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("RDMA device {} not found", selection.device_name),
                )
            })
    }

    /// Creates a QueuePair attached to the given shared completion queue (used
    /// for both send and recv).
    fn create_queue_pair(
        &self,
        device: &RdmaDevice,
        config: &RdmaConnectionConfig,
        shared: &SharedCq,
    ) -> Result<QueuePair> {
        let pd = device.pd();

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: config.qp.max_send_wr,
                max_recv_wr: config.qp.max_recv_wr,
                max_send_sge: config.qp.max_send_sge,
                max_recv_sge: config.qp.max_recv_sge,
                max_inline_data: 0,
            },
            ..Default::default()
        };

        let queue_pair =
            QueuePair::create(pd, &shared.cq, &shared.cq, &mut init_attr, device.index())
                .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        Ok(queue_pair)
    }

    /// Builds the `RdmaSocket` + per-connection `ConnState`, pre-posts recv
    /// buffers, and registers the connection with the poller that owns
    /// `shared`. Inserting into `socket_map` is the caller's responsibility.
    fn establish_connection(
        &self,
        queue_pair: QueuePair,
        shared: &SharedCq,
        state: &Arc<State>,
        connection_config: &RdmaConnectionConfig,
    ) -> Result<Arc<RdmaSocket>> {
        let qp_num = queue_pair.qp_num();
        let (tx, rx) = tokio::sync::mpsc::channel::<SendMsg>(1024);
        let socket = RdmaSocket::new(
            shared.registry.clone(),
            queue_pair,
            self.buffer_pool.clone(),
            tx,
            connection_config.recv_buffer_size,
            connection_config.qp.max_send_wr,
        );

        let mut conn = ConnState::new(
            socket.clone(),
            state.clone(),
            rx,
            connection_config.recv_buffer_size,
        );
        conn.submit_recv_tasks(connection_config.recv_queue_len as usize)?;

        self.cq_pool.register_conn(shared, qp_num, conn);
        Ok(socket)
    }

    async fn handshake(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket> {
        if let Some(socket) = self.socket_map.read().await.get(addr).cloned() {
            return Ok(socket.into());
        }

        let acquire_ctx = Context::create_with_state_and_addr(state, addr);
        let remote_info = self.fetch_remote_device_list(addr, &acquire_ctx).await?;
        let selection = match self.match_remote_device(&remote_info) {
            Ok(selection) => selection,
            Err(err) => {
                self.invalidate_device_list_cache(addr).await;
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
        let connection_config = self.negotiate_connection_config(device, &selection.remote_device);
        let shared = self.pick_cq(selection.local_device_index);
        let queue_pair = self.create_queue_pair(device, &connection_config, &shared)?;
        let local_endpoint = self.build_endpoint(
            &queue_pair,
            device,
            selection.local_port_num,
            selection.local_gid_index,
        )?;

        let connect_request = ConnectRequest {
            endpoint: local_endpoint,
            target: selection.remote.clone(),
            config: connection_config,
        };
        let remote_endpoint =
            match Box::pin(self.acquire_client.connect(&acquire_ctx, &connect_request)).await {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    self.invalidate_device_list_cache(addr).await;
                    return Err(err);
                }
            };
        self.bring_qp_to_rts(
            &queue_pair,
            &local_endpoint,
            &remote_endpoint,
            self.config.pkey_index,
        )?;

        let socket = self.establish_connection(queue_pair, &shared, state, &connection_config)?;
        {
            let mut socket_map = self.socket_map.write().await;
            socket_map.insert(*addr, socket.clone());
        }
        Ok(socket.into())
    }

    /// Picks a shared CQ for a new connection on the given device position,
    /// spreading load across the per-device CQs round-robin.
    fn pick_cq(&self, device_index: usize) -> SharedCq {
        let sel = self.next_cq.fetch_add(1, Ordering::Relaxed);
        self.cq_pool.pick(device_index, sel)
    }

    async fn fetch_remote_device_list(&self, addr: &SocketAddr, ctx: &Context) -> Result<RdmaInfo> {
        if let Some(info) = self.get_cached_device_list(addr).await {
            return Ok(info);
        }

        let info = Box::pin(self.acquire_client.info(ctx, &())).await?;
        self.device_list_cache.write().await.insert(
            *addr,
            CachedRdmaInfo {
                info: info.clone(),
                cached_at: Instant::now(),
            },
        );
        Ok(info)
    }

    async fn get_cached_device_list(&self, addr: &SocketAddr) -> Option<RdmaInfo> {
        let cache = self.device_list_cache.read().await;
        let cached = cache.get(addr)?;
        if cached.cached_at.elapsed() < Self::DEVICE_LIST_CACHE_TTL {
            Some(cached.info.clone())
        } else {
            None
        }
    }

    async fn invalidate_device_list_cache(&self, addr: &SocketAddr) {
        self.device_list_cache.write().await.remove(addr);
    }

    fn match_remote_device(&self, remote_info: &RdmaInfo) -> Result<DeviceMatch> {
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
                if !Self::remote_port_is_usable(remote_port) {
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
                        if local_port.port_attr.link_layer != remote_port.link_layer {
                            continue;
                        }
                        link_layer_matches += 1;

                        let Some((local_gid_index, remote_gid_index)) =
                            Self::match_gid_pair(local_port, remote_port)
                        else {
                            continue;
                        };

                        let selected = DeviceMatch {
                            local_device_index,
                            local_port_num: local_port.port_num,
                            local_gid_index,
                            remote: DeviceSelection {
                                device_name: remote_device.name.clone(),
                                port_num: remote_port.port_num,
                                gid_index: remote_gid_index,
                            },
                            remote_device: remote_device.clone(),
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

    fn remote_port_is_usable(port: &RdmaPortInfo) -> bool {
        matches!(
            port.state,
            ibv_port_state::IBV_PORT_ACTIVE | ibv_port_state::IBV_PORT_ACTIVE_DEFER
        ) && matches!(port.link_layer, LinkLayer::InfiniBand | LinkLayer::Ethernet)
    }

    fn match_gid_pair(local_port: &Port, remote_port: &RdmaPortInfo) -> Option<(u8, u8)> {
        match local_port.port_attr.link_layer {
            LinkLayer::InfiniBand => Some((
                Self::first_local_gid(local_port, |_| true).unwrap_or(0),
                Self::first_remote_gid(remote_port, |_| true).unwrap_or(0),
            )),
            LinkLayer::Ethernet => Self::match_roce_gid_pair(local_port, remote_port),
            LinkLayer::Unspecified => None,
        }
    }

    fn match_roce_gid_pair(local_port: &Port, remote_port: &RdmaPortInfo) -> Option<(u8, u8)> {
        let preferred = [
            (
                Self::first_local_gid(local_port, |gid| matches!(gid.gid_type, GidType::RoCEv2)),
                Self::first_remote_gid(remote_port, |gid| matches!(gid.gid_type, GidType::RoCEv2)),
            ),
            (
                Self::first_local_gid(local_port, |gid| matches!(gid.gid_type, GidType::RoCEv1)),
                Self::first_remote_gid(remote_port, |gid| matches!(gid.gid_type, GidType::RoCEv1)),
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
            Self::first_local_gid(local_port, |_| true)?,
            Self::first_remote_gid(remote_port, |_| true)?,
        ))
    }

    fn first_local_gid(
        port: &Port,
        mut predicate: impl FnMut(&ruapc_rdma::Gid) -> bool,
    ) -> Option<u8> {
        port.gids
            .iter()
            .find(|gid| predicate(gid))
            .and_then(|gid| u8::try_from(gid.index).ok())
    }

    fn first_remote_gid(
        port: &RdmaPortInfo,
        mut predicate: impl FnMut(&RdmaGidInfo) -> bool,
    ) -> Option<u8> {
        port.gids
            .iter()
            .find(|gid| predicate(gid))
            .and_then(|gid| u8::try_from(gid.index).ok())
    }

    fn negotiate_connection_config(
        &self,
        local_device: &RdmaDevice,
        remote_device: &RdmaDeviceInfo,
    ) -> RdmaConnectionConfig {
        let local = self.local_connection_config(local_device);
        let remote = remote_device.connection;
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: local.qp.max_send_wr.min(remote.qp.max_recv_wr),
                max_recv_wr: local.qp.max_recv_wr.min(remote.qp.max_send_wr),
                max_send_sge: local.qp.max_send_sge.min(remote.qp.max_recv_sge),
                max_recv_sge: local.qp.max_recv_sge.min(remote.qp.max_send_sge),
            },
            cq_len: local.cq_len.min(remote.cq_len),
            recv_queue_len: local.recv_queue_len.min(remote.recv_queue_len),
            recv_buffer_size: local.recv_buffer_size.min(remote.recv_buffer_size),
        }
    }

    fn clamp_connection_config(
        &self,
        device: &RdmaDevice,
        requested: RdmaConnectionConfig,
    ) -> RdmaConnectionConfig {
        let local = self.local_connection_config(device);
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: requested.qp.max_send_wr.min(local.qp.max_send_wr),
                max_recv_wr: requested.qp.max_recv_wr.min(local.qp.max_recv_wr),
                max_send_sge: requested.qp.max_send_sge.min(local.qp.max_send_sge),
                max_recv_sge: requested.qp.max_recv_sge.min(local.qp.max_recv_sge),
            },
            cq_len: requested.cq_len.min(local.cq_len),
            recv_queue_len: requested.recv_queue_len.min(local.recv_queue_len),
            recv_buffer_size: requested.recv_buffer_size.min(local.recv_buffer_size),
        }
    }

    fn local_connection_config(&self, device: &RdmaDevice) -> RdmaConnectionConfig {
        let info = device.info();
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: self
                    .config
                    .qp
                    .max_send_wr
                    .min(info.device_attr.max_qp_wr as u32),
                max_recv_wr: self
                    .config
                    .qp
                    .max_recv_wr
                    .min(info.device_attr.max_qp_wr as u32),
                max_send_sge: self
                    .config
                    .qp
                    .max_send_sge
                    .min(info.device_attr.max_sge as u32),
                max_recv_sge: self
                    .config
                    .qp
                    .max_recv_sge
                    .min(info.device_attr.max_sge as u32),
            },
            cq_len: self.config.cq_len.min(info.device_attr.max_cqe as u32),
            recv_queue_len: self.config.recv_queue_len,
            recv_buffer_size: self.config.recv_buffer_size,
        }
    }

    /// Constructs an Endpoint from a QueuePair and selected local port/GID.
    fn build_endpoint(
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

    fn bring_qp_to_rts(
        &self,
        qp: &QueuePair,
        local: &Endpoint,
        remote: &Endpoint,
        pkey_index: u16,
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
            pkey_index,
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
}

impl Drop for RdmaSocketPool {
    fn drop(&mut self) {
        // Stop helper tasks, then stop+join the CQ poller threads BEFORE the
        // QPs/CQs are destroyed. `cq_pool` is also dropped after this (it is
        // declared before `socket_map`), and its Drop calls `shutdown` again
        // (idempotent); doing it explicitly here makes the ordering obvious:
        //   pollers joined → QPs destroyed (socket_map) → MRs → PD/context.
        self.task_supervisor.stop();
        self.cq_pool.shutdown();
        self.socket_map.get_mut().clear();
    }
}

impl std::fmt::Debug for RdmaSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaSocketPool").finish()
    }
}

#[derive(Clone)]
struct DeviceMatch {
    local_device_index: usize,
    local_port_num: u8,
    local_gid_index: u8,
    remote: DeviceSelection,
    remote_device: RdmaDeviceInfo,
}

struct CachedRdmaInfo {
    info: RdmaInfo,
    cached_at: Instant,
}
