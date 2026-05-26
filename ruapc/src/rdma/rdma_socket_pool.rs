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
    CompChannel, CompletionQueue, DeviceInfo, GidType, LinkLayer, Port, QpConnectParams, QueuePair,
    ibv_mtu, ibv_port_state, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type,
};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::rdma_service::{RdmaGidInfo, RdmaPortInfo, RdmaQpCaps, RdmaQpParams};
use super::{
    ConnectRequest, DeviceSelection, Endpoint, EventLoop, RdmaConfig, RdmaDevice,
    RdmaDeviceRefresher, RdmaInfo, RdmaService, RdmaSocket,
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
    /// RDMA-specific configuration for QP creation and state transitions.
    pub config: RdmaConfig,
    /// Client used for acquiring RDMA connections (no RDMA resources).
    pub acquire_client: Client,
    /// Supervisor for managing asynchronous tasks — dropped first so that
    /// all spawned EventLoop tasks finish and release their `Arc<RdmaSocket>`.
    pub task_supervisor: TaskSupervisor,
    /// Background port/GID cache refresher. Dropped before RDMA devices.
    _port_refresher: RdmaDeviceRefresher,
    /// Per-peer connection locks prevent duplicate in-flight RDMA handshakes.
    connect_locks: dashmap::DashMap<SocketAddr, Arc<tokio::sync::Mutex<()>>, RandomState>,
    /// Short-lived cache for the first-stage server RDMA device query.
    device_list_cache: RwLock<HashMap<SocketAddr, CachedRdmaInfo, RandomState>>,
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
        config: &SocketPoolConfig,
        devices: &Arc<Devices>,
        buffer_pool: &Arc<BufferPool>,
    ) -> Result<Self> {
        let rdma_config = config.rdma.clone().unwrap_or_default();
        Self::new(rdma_config, devices.clone(), buffer_pool.clone())
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
    fn rdma_device_list(&self) -> Result<RdmaInfo> {
        Ok(RdmaInfo::from_devices(
            self.devices.rdma_devices(),
            &self.config,
        ))
    }

    /// Establishes a new RDMA connection with the specified endpoint.
    fn rdma_accept(&self, request: &ConnectRequest, state: &Arc<State>) -> Result<Endpoint> {
        let device = self.find_device_by_name(&request.target)?;
        let (queue_pair, comp_channel, cq) = self.create_queue_pair(device, &request.qp_caps)?;
        let local_endpoint = self.build_endpoint(
            &queue_pair,
            device,
            request.target.port_num,
            request.target.gid_index,
        )?;
        let params = QpConnectParams::from(&request.qp_params);
        self.bring_qp_to_rts(&queue_pair, &local_endpoint, &request.endpoint, &params)?;

        let (tx, rx) = tokio::sync::mpsc::channel::<SendMsg>(1024);
        let rdma_socket = RdmaSocket::new(queue_pair, self.buffer_pool.clone(), tx);
        self.spawn_event_loop(
            Arc::new(rdma_socket),
            state.clone(),
            comp_channel,
            cq,
            rx,
            request.qp_caps.max_recv_wr,
        )?;
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
        config: RdmaConfig,
        devices: Arc<Devices>,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self> {
        let task_supervisor = TaskSupervisor::create();
        let port_refresher = RdmaDeviceRefresher::start(devices.clone(), &task_supervisor);
        Ok(Self {
            config,
            acquire_client: Client {
                timeout: std::time::Duration::from_secs(5),
                use_msgpack: true,
                socket_type: Some(SocketType::TCP),
            },
            task_supervisor,
            _port_refresher: port_refresher,
            connect_locks: dashmap::DashMap::default(),
            device_list_cache: RwLock::default(),
            socket_map: RwLock::default(),
            buffer_pool,
            devices,
            next_device: AtomicUsize::new(0),
        })
    }

    fn find_device_by_name(&self, selection: &DeviceSelection) -> Result<&RdmaDevice> {
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
        caps: &RdmaQpCaps,
    ) -> Result<(QueuePair, Arc<CompChannel>, Arc<CompletionQueue>)> {
        let rdma = device;
        let ctx = rdma.context();
        let pd = rdma.pd();

        let comp_channel = CompChannel::create(ctx)
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        comp_channel
            .set_nonblock()
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let cq = CompletionQueue::create(ctx, self.config.cq_size as _, Some(&comp_channel))
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: caps.max_send_wr,
                max_recv_wr: caps.max_recv_wr,
                max_send_sge: caps.max_send_sge,
                max_recv_sge: caps.max_recv_sge,
                max_inline_data: caps.max_inline_data,
            },
            ..Default::default()
        };

        let queue_pair = QueuePair::create(pd, &cq, &cq, &mut init_attr, device.index())
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        Ok((queue_pair, comp_channel, cq))
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

        // Negotiate QP caps: use the minimum of local config and server's advertised caps.
        let negotiated_caps = self.negotiate_caps(&remote_info.qp_caps);
        let negotiated_params = self.negotiate_params(&remote_info.qp_params);

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
        let (queue_pair, comp_channel, cq) = self.create_queue_pair(device, &negotiated_caps)?;
        let local_endpoint = self.build_endpoint(
            &queue_pair,
            device,
            selection.local_port_num,
            selection.local_gid_index,
        )?;

        let connect_request = ConnectRequest {
            endpoint: local_endpoint,
            target: selection.remote.clone(),
            qp_caps: negotiated_caps,
            qp_params: negotiated_params,
        };
        let remote_endpoint =
            match Box::pin(self.acquire_client.connect(&acquire_ctx, &connect_request)).await {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    self.invalidate_device_list_cache(addr).await;
                    return Err(err);
                }
            };
        let params = QpConnectParams::from(&negotiated_params);
        self.bring_qp_to_rts(&queue_pair, &local_endpoint, &remote_endpoint, &params)?;

        let (tx, rx) = tokio::sync::mpsc::channel::<SendMsg>(1024);
        let socket = RdmaSocket::new(queue_pair, self.buffer_pool.clone(), tx);
        let socket = Arc::new(socket);
        self.spawn_event_loop(
            socket.clone(),
            state.clone(),
            comp_channel,
            cq,
            rx,
            self.config.recv_buffer_count,
        )?;
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
        params: &QpConnectParams,
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
            params,
        )
        .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
    }

    fn min_mtu(a: ibv_mtu, b: ibv_mtu) -> ibv_mtu {
        if (a as u32) <= (b as u32) { a } else { b }
    }

    /// Negotiate QP capabilities by taking the minimum of local config and server's caps.
    fn negotiate_caps(&self, remote_caps: &RdmaQpCaps) -> RdmaQpCaps {
        RdmaQpCaps {
            max_send_wr: self.config.max_send_wr.min(remote_caps.max_send_wr),
            max_recv_wr: self.config.max_recv_wr.min(remote_caps.max_recv_wr),
            max_send_sge: self.config.max_send_sge.min(remote_caps.max_send_sge),
            max_recv_sge: self.config.max_recv_sge.min(remote_caps.max_recv_sge),
            max_inline_data: self.config.max_inline_data.min(remote_caps.max_inline_data),
        }
    }

    /// Negotiate QP state transition params; for most fields use the server's values
    /// since they describe the server's capabilities. The client uses its own config
    /// as a preference that should not exceed the server's.
    fn negotiate_params(&self, remote_params: &RdmaQpParams) -> RdmaQpParams {
        RdmaQpParams {
            pkey_index: remote_params.pkey_index,
            max_dest_rd_atomic: self
                .config
                .max_dest_rd_atomic
                .min(remote_params.max_dest_rd_atomic),
            min_rnr_timer: remote_params.min_rnr_timer,
            timeout: remote_params.timeout,
            retry_cnt: remote_params.retry_cnt,
            rnr_retry: remote_params.rnr_retry,
            max_rd_atomic: self.config.max_rd_atomic.min(remote_params.max_rd_atomic),
        }
    }

    /// Starts the event loop for handling RDMA events on a socket.
    pub fn spawn_event_loop(
        &self,
        socket: Arc<RdmaSocket>,
        state: Arc<State>,
        comp_channel: Arc<CompChannel>,
        cq: Arc<CompletionQueue>,
        pending_receiver: tokio::sync::mpsc::Receiver<SendMsg>,
        recv_buffer_count: u32,
    ) -> Result<()> {
        let socket_clone = socket.clone();
        let mut event_loop = EventLoop::new(socket, state, comp_channel, cq, pending_receiver);
        event_loop.submit_recv_tasks(recv_buffer_count as usize)?;

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

impl From<&RdmaQpParams> for QpConnectParams {
    fn from(p: &RdmaQpParams) -> Self {
        Self {
            pkey_index: p.pkey_index,
            max_dest_rd_atomic: p.max_dest_rd_atomic,
            min_rnr_timer: p.min_rnr_timer,
            timeout: p.timeout,
            retry_cnt: p.retry_cnt,
            rnr_retry: p.rnr_retry,
            max_rd_atomic: p.max_rd_atomic,
        }
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

#[derive(Clone)]
struct DeviceMatch {
    local_device_index: usize,
    local_port_num: u8,
    local_gid_index: u8,
    remote: DeviceSelection,
}

struct CachedRdmaInfo {
    info: RdmaInfo,
    cached_at: Instant,
}
