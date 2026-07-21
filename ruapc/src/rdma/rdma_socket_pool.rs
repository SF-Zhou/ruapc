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
    DeviceInfo, GidType, LinkLayer, Port, QueuePair, ibv_mtu, ibv_port_state, ibv_qp_cap,
    ibv_qp_init_attr, ibv_qp_type,
};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::rdma_service::{RdmaDeviceInfo, RdmaGidInfo, RdmaPortInfo};
use super::{
    ConnectRequest, DevicePollers, DeviceSelection, Endpoint, PollerConfig, RdmaConnectionConfig,
    RdmaDevice, RdmaDeviceRefresher, RdmaInfo, RdmaService, RdmaSocket, RegisterConn,
};
use crate::{
    Buffer, BufferPool, Client, Context, Devices, Error, ErrorKind, RdmaQueuePairConfig,
    RdmaSocketPoolConfig, Result, Socket, SocketPoolConfig, SocketPoolTrait, SocketType, State,
    TaskSupervisor,
};

/// A pool managing RDMA sockets and their associated resources.
///
/// # Drop order
///
/// RDMA resources **must** be destroyed in this order:
///   1. Stop all async tasks and join the device poll threads (so all
///      per-connection state drops its `Arc<RdmaSocket>`)
///   2. Destroy QPs (`socket_map`) — the shared CQs are destroyed via their
///      `Arc` chain once the last QP referencing them is gone
///   3. Deregister MRs (`buffer_pool`)
///   4. Destroy PD / close device context (`devices`)
///
/// Rust drops struct fields in **declaration order**, so the fields below
/// are intentionally ordered to satisfy the ibverbs requirement.
pub struct RdmaSocketPool {
    /// Client used for acquiring RDMA connections (no RDMA resources).
    pub acquire_client: Client,
    /// Supervisor for managing asynchronous tasks — dropped first so that
    /// watcher tasks finish and connections are marked for teardown.
    pub task_supervisor: TaskSupervisor,
    /// Per-device completion poll threads. Dropped before `socket_map` so
    /// the threads are joined and release their `Arc<RdmaSocket>` clones.
    pollers: DevicePollers,
    /// Background port/GID cache refresher. Dropped before RDMA devices.
    _port_refresher: RdmaDeviceRefresher,
    /// Per-peer connection locks prevent duplicate in-flight RDMA handshakes.
    connect_locks: dashmap::DashMap<SocketAddr, Arc<tokio::sync::Mutex<()>>, RandomState>,
    /// Short-lived cache for the first-stage server RDMA device query.
    device_list_cache: RwLock<HashMap<SocketAddr, CachedRdmaInfo, RandomState>>,
    /// Thread-safe map of active RDMA connection stripes indexed by peer
    /// address; requests are spread round-robin across the stripes.
    /// Dropped after tasks stop → destroys QPs.
    pub socket_map: RwLock<HashMap<SocketAddr, Vec<Arc<RdmaSocket>>, RandomState>>,
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
    /// Counter for round-robin stripe selection in `acquire`.
    next_stripe: AtomicUsize,
    /// Buffer pool bytes pinned by the receive rings of live connections.
    ring_bytes: Arc<AtomicUsize>,
    /// Ensures the pool-undersized warning fires at most once.
    pool_capacity_warned: std::sync::atomic::AtomicBool,
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
        let connection_config = self.clamp_connection_config(device, request.config);
        let poller = self.pollers.get_or_start(
            device,
            self.poller_config(),
            self.config.poll_threads_per_device,
        )?;
        let queue_pair = self.create_queue_pair(device, &connection_config, &poller)?;
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

        let socket = self.register_socket(queue_pair, state, &poller, &connection_config)?;
        tracing::debug!(
            local_qp = socket.queue_pair.qp_num(),
            remote_qp = request.endpoint.qp_num,
            "accepted RDMA connection"
        );
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
            && let Some(socket) = socket_map.get(addr).and_then(|s| self.pick_stripe(s))
        {
            return Ok(socket);
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
        Ok(Self {
            acquire_client: Client {
                timeout: std::time::Duration::from_secs(5),
                use_msgpack: true,
                socket_type: Some(SocketType::TCP),
            },
            task_supervisor,
            pollers: DevicePollers::default(),
            _port_refresher: port_refresher,
            connect_locks: dashmap::DashMap::default(),
            device_list_cache: RwLock::default(),
            socket_map: RwLock::default(),
            buffer_pool,
            devices,
            config,
            next_device: AtomicUsize::new(0),
            next_stripe: AtomicUsize::new(0),
            ring_bytes: Arc::new(AtomicUsize::new(0)),
            pool_capacity_warned: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Picks one healthy connection stripe round-robin, skipping stripes
    /// that entered the error state.
    fn pick_stripe(&self, stripes: &[Arc<RdmaSocket>]) -> Option<Socket> {
        if stripes.is_empty() {
            return None;
        }
        let start = self.next_stripe.fetch_add(1, Ordering::Relaxed);
        (0..stripes.len())
            .map(|i| &stripes[(start + i) % stripes.len()])
            .find(|s| s.state.is_ok())
            .map(|s| s.into())
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

    /// Poll thread tunables derived from the pool configuration.
    fn poller_config(&self) -> PollerConfig {
        PollerConfig {
            cq_len: self.config.device_cq_len,
            spin_us: self.config.poll_spin_us,
            dispatch_workers: self.config.dispatch_workers,
        }
    }

    /// Creates a QueuePair attached to the device's shared completion queue.
    fn create_queue_pair(
        &self,
        device: &RdmaDevice,
        config: &RdmaConnectionConfig,
        poller: &super::poller::DevicePoller,
    ) -> Result<QueuePair> {
        let pd = device.pd();
        let cq = poller.cq();

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: config.qp.max_send_wr,
                max_recv_wr: config.qp.max_recv_wr,
                max_send_sge: config.qp.max_send_sge,
                max_recv_sge: config.qp.max_recv_sge,
                max_inline_data: config.qp.max_inline_data,
            },
            ..Default::default()
        };

        let mut queue_pair = QueuePair::create(pd, cq, cq, &mut init_attr, device.index())
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        queue_pair
            .set_send_signal_interval(self.config.send_signal_interval, config.qp.max_send_wr);

        Ok(queue_pair)
    }

    /// Wraps a connected QueuePair into an `RdmaSocket`, pre-posts receive
    /// buffers and registers the connection with the device poll thread.
    fn register_socket(
        &self,
        mut queue_pair: QueuePair,
        state: &Arc<State>,
        poller: &super::poller::DevicePoller,
        config: &RdmaConnectionConfig,
    ) -> Result<Arc<RdmaSocket>> {
        // Reserve the poller slot first: its tag must be stamped into the
        // QP before any work request is posted, since completions map back
        // to the connection through the tag in their `wr_id`.
        let qp_depth = (config.qp.max_send_wr + config.qp.max_recv_wr).saturating_mul(2);
        let reservation = poller.reserve(qp_depth)?;
        queue_pair.set_wr_tag(reservation.tag());

        // Account the registered memory this connection's receive ring
        // pins in the shared buffer pool, and warn when the pool is
        // undersized for the connection count (before the ring allocation
        // below starts failing under load). Steady-state traffic needs a
        // multiple of the ring size: zero-copy dispatch holds ring-sized
        // chunks for in-flight messages (each triggering a fresh repost
        // allocation), and send serialization draws from the same pool —
        // so rings exceeding a quarter of the pool are a reliable
        // exhaustion predictor.
        let ring_bytes = config.recv_queue_len as usize * config.max_msg_size as usize;
        let (ring_reservation, ring_total) =
            super::poller::RingReservation::add(&self.ring_bytes, ring_bytes);
        let pool_capacity = self.buffer_pool.max_memory();
        if ring_total.saturating_mul(4) >= pool_capacity
            && !self.pool_capacity_warned.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                "RDMA buffer pool likely undersized: receive rings pin {ring_total}B of the \
                 {pool_capacity}B pool (each connection pins recv_queue_len ({}) x \
                 max_msg_size ({}) = {ring_bytes}B, and in-flight messages typically need a \
                 multiple of that); raise SocketPoolConfig::buffer_pool_memory to >= 4x the \
                 ring total, or lower rdma.recv_queue_len / rdma.max_msg_size / \
                 rdma.connections_per_peer",
                config.recv_queue_len,
                config.max_msg_size,
            );
        }

        // In-flight data WRs are bounded by the peer's receive ring; half
        // the (negotiated) ring keeps ample headroom for ACK latency (and
        // in-flight standalone ACKs) before the receiver could be overrun.
        let send_window = (config.recv_queue_len / 2).max(1);

        let (tx, rx) = tokio::sync::mpsc::channel::<Buffer>(1024);
        let socket = Arc::new(RdmaSocket::new(
            queue_pair,
            self.buffer_pool.clone(),
            tx,
            poller.waker(),
            config.max_msg_size as usize,
            send_window,
        ));

        // Pre-post receive buffers *before* the remote can send: the
        // registration is picked up asynchronously by the poll thread, but
        // the recv ring must be ready as soon as the handshake response
        // reaches the peer.
        for _ in 0..config.recv_queue_len {
            let buf = self.buffer_pool.allocate(config.max_msg_size as usize)?;
            socket
                .queue_pair
                .recv(buf)
                .map_err(|e| Error::new(ErrorKind::RdmaRecvFailed, e.to_string()))?;
        }

        poller.register(
            reservation,
            RegisterConn {
                socket: socket.clone(),
                state: state.clone(),
                pending_receiver: rx,
                recv_submitted: u64::from(config.recv_queue_len),
                recv_buf_size: config.max_msg_size as usize,
                send_window,
                // Local-only send-side toggle; receivers walk the same
                // frame loop either way.
                msg_aggregation: self.config.msg_aggregation,
                supervisor_guard: self.task_supervisor.start_async_task(),
                ring_reservation,
            },
        )?;

        // Mark the socket as failed when the pool shuts down so the poll
        // thread tears the connection down.
        let socket_clone = socket.clone();
        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            task_supervisor.stopped().await;
            socket_clone.set_error();
        });

        Ok(socket)
    }

    async fn handshake(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket> {
        // Re-check under the connect lock: another task may have connected,
        // or every stripe may have failed and must be replaced.
        {
            let mut socket_map = self.socket_map.write().await;
            if let Some(stripes) = socket_map.get(addr) {
                if let Some(socket) = self.pick_stripe(stripes) {
                    return Ok(socket);
                }
                // All stripes are dead: drop them and reconnect. The poll
                // thread reclaims the connections independently.
                tracing::info!("all RDMA stripes to {addr} failed, reconnecting");
                socket_map.remove(addr);
            }
        }

        // Establish `connections_per_peer` stripes towards this peer;
        // requests are spread round-robin across them (and, with poll
        // thread shards, across cores). All stripes share one device/GID
        // selection: verified connectivity matters more than NIC spreading.
        let acquire_ctx = Context::create_with_state_and_addr(state, addr);
        let remote_info = self.fetch_remote_device_list(addr, &acquire_ctx).await?;
        let selection = match self.match_remote_device(&remote_info) {
            Ok(selection) => selection,
            Err(err) => {
                self.invalidate_device_list_cache(addr).await;
                return Err(err);
            }
        };

        let stripe_count = self.config.connections_per_peer.max(1);
        let mut stripes = Vec::with_capacity(stripe_count as usize);
        for _ in 0..stripe_count {
            match self
                .connect_stripe(addr, state, &acquire_ctx, &selection)
                .await
            {
                Ok(stripe) => stripes.push(stripe),
                Err(err) => {
                    // Don't leak the stripes established so far: mark them
                    // failed so the poll thread tears them down.
                    for stripe in &stripes {
                        stripe.set_error();
                    }
                    return Err(err);
                }
            }
        }

        let socket = self
            .pick_stripe(&stripes)
            .expect("stripe count is at least 1");
        self.socket_map.write().await.insert(*addr, stripes);
        Ok(socket)
    }

    /// Establishes one RDMA connection (stripe) towards `addr` using the
    /// given device/GID selection.
    async fn connect_stripe(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
        acquire_ctx: &Context,
        selection: &DeviceMatch,
    ) -> Result<Arc<RdmaSocket>> {
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
        let poller = self.pollers.get_or_start(
            device,
            self.poller_config(),
            self.config.poll_threads_per_device,
        )?;
        let queue_pair = self.create_queue_pair(device, &connection_config, &poller)?;
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
            match Box::pin(self.acquire_client.connect(acquire_ctx, &connect_request)).await {
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

        let socket = self.register_socket(queue_pair, state, &poller, &connection_config)?;
        let local_info = device.info();
        tracing::info!(
            local_device = %local_info.name,
            local_port = selection.local_port_num,
            local_gid_index = selection.local_gid_index,
            remote_device = %selection.remote.device_name,
            remote_port = selection.remote.port_num,
            remote_gid_index = selection.remote.gid_index,
            local_qp = socket.queue_pair.qp_num(),
            remote_qp = remote_endpoint.qp_num,
            "acquired RDMA socket"
        );
        Ok(socket)
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
                // Scatter/gather lists are purely local WQE properties: a
                // gather-list SEND arrives as one contiguous message no
                // matter how many SGEs composed it, so neither side's SGE
                // capability constrains the other.
                max_send_sge: local.qp.max_send_sge,
                max_recv_sge: local.qp.max_recv_sge,
                max_inline_data: local.qp.max_inline_data.min(remote.qp.max_inline_data),
            },
            cq_len: local.cq_len.min(remote.cq_len),
            recv_queue_len: local.recv_queue_len.min(remote.recv_queue_len),
            max_msg_size: local.max_msg_size.min(remote.max_msg_size),
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
                // SGE lists are local WQE properties (see
                // `negotiate_connection_config`): use our own capabilities
                // regardless of what the initiator requested for itself.
                max_send_sge: local.qp.max_send_sge,
                max_recv_sge: local.qp.max_recv_sge,
                max_inline_data: requested.qp.max_inline_data.min(local.qp.max_inline_data),
            },
            cq_len: requested.cq_len.min(local.cq_len),
            recv_queue_len: requested.recv_queue_len.min(local.recv_queue_len),
            max_msg_size: requested.max_msg_size.min(local.max_msg_size),
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
                max_inline_data: self.config.qp.max_inline_data,
            },
            cq_len: self.config.cq_len.min(info.device_attr.max_cqe as u32),
            recv_queue_len: self.config.recv_queue_len,
            // Enforce a small floor so a tiny misconfiguration cannot break
            // the RPC control plane.
            max_msg_size: self.config.max_msg_size.max(16 * 1024),
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
            psn: Self::random_psn(qp.qp_num()),
        })
    }

    /// Generates a pseudo-random 24-bit initial packet sequence number.
    ///
    /// Uniqueness across QP incarnations is what matters: drivers recycle
    /// qp numbers, and a fresh QP reusing the (qp_num, GID) pair of a
    /// recently destroyed one with a predictable PSN can silently blackhole
    /// against stale peer state.
    fn random_psn(qp_num: u32) -> u32 {
        use std::hash::BuildHasher as _;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        (RandomState::default().hash_one((qp_num, nanos)) as u32) & 0xFF_FFFF
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
            local.psn,
            remote.psn,
        )
        .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
    }

    fn min_mtu(a: ibv_mtu, b: ibv_mtu) -> ibv_mtu {
        if (a as u32) <= (b as u32) { a } else { b }
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
    remote_device: RdmaDeviceInfo,
}

struct CachedRdmaInfo {
    info: RdmaInfo,
    cached_at: Instant,
}
