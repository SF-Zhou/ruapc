use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    sync::{Arc, OnceLock, Weak},
    time::{Duration, Instant},
};

use foldhash::fast::RandomState;
use ruapc_bufpool::Device as _;
use ruapc_rdma::{
    DeviceInfo, Gid, GidType, LinkLayer, Port, QueuePair, ibv_mtu, ibv_qp_cap, ibv_qp_init_attr,
    ibv_qp_type,
};
use tokio_util::sync::DropGuard;

use super::path::{RdmaNicInfo, RdmaPathInfo, gid_ip};
use super::rdma_service::RdmaPortInfo;
use super::{
    ConnectRequest, ConnectionControl, DevicePollers, DeviceSelection, Endpoint, PollerConfig,
    RdmaConnectionConfig, RdmaDevice, RdmaDeviceRefresher, RdmaInfo, RdmaService, RdmaSocket,
    RegisterConn,
};
use crate::{
    Buffer, BufferPool, Client, Context, Devices, Error, ErrorKind, RdmaQueuePairConfig,
    RdmaSocketPoolConfig, Result, Socket, SocketPoolConfig, SocketPoolTrait, State, TaskSupervisor,
};

mod peer;
pub(crate) use peer::PeerState;
mod accept;
use accept::{AcceptLease, AcceptLeaseEvent, AcceptLeaseState, advance_accept_lease};
mod connect;
use connect::{EstablishedSocket, SocketRegistrationGuard};
mod placement;
use placement::{PathClass, ReconcileAction};
mod maintenance;
mod report;
use maintenance::{RetryBackoff, preconnect_backoff_delay};

type PathKey = (String, u8, u8, String, u8, u8);
type PeerMap = dashmap::DashMap<SocketAddr, Arc<PeerState>, RandomState>;

/// One RDMA connection towards a peer, tagged with bookkeeping metadata.
///
/// Each stripe carries its own path (NIC pair): stripes of one peer are
/// placed independently, spreading load across the NICs of both sides.
#[derive(Clone)]
pub(crate) struct Stripe {
    pub(crate) socket: Arc<RdmaSocket>,
}

pub(crate) type RdmaPeerHealth = PeerState;

/// RAII increment of one device's live-connection counter; decremented
/// when the poll thread tears the connection down (the guard travels
/// through [`RegisterConn`] into the poller's per-connection state).
pub(crate) struct ConnCountGuard {
    counts: Arc<Vec<AtomicUsize>>,
    index: usize,
}

impl ConnCountGuard {
    pub(crate) fn acquire(counts: &Arc<Vec<AtomicUsize>>, index: usize) -> Self {
        if let Some(count) = counts.get(index) {
            count.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            counts: counts.clone(),
            index,
        }
    }
}

impl Drop for ConnCountGuard {
    fn drop(&mut self) {
        if let Some(count) = self.counts.get(self.index) {
            count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// A pool managing RDMA sockets and their associated resources.
///
/// # Drop order
///
/// RDMA resources **must** be destroyed in this order:
///   1. Stop all async tasks and join the device poll threads (so all
///      per-connection state drops its `Arc<RdmaSocket>`)
///   2. Destroy QPs (`peers`) — the shared CQs are destroyed via their
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
    /// Per-device completion poll threads. Dropped before `peers` so
    /// the threads are joined and release their `Arc<RdmaSocket>` clones.
    pollers: DevicePollers,
    /// Background port/GID cache refresher. Dropped before RDMA devices.
    _port_refresher: RdmaDeviceRefresher,
    /// Per-peer connection state. Dropped after tasks stop, before memory
    /// registration and device resources.
    peers: PeerMap,
    /// Live connection count per local RDMA device (outbound + inbound),
    /// indexed like `devices.rdma_devices()`. Drives least-connections
    /// placement and is advertised to peers via `RdmaInfo`.
    conn_counts: Arc<Vec<AtomicUsize>>,
    /// Per local RDMA device budget of in-flight RDMA READ work requests
    /// (`rdma.max_inflight_read_wrs`), indexed like
    /// `devices.rdma_devices()` and shared by every connection on the
    /// device — the congestion control for read traffic (server-side
    /// `remote_read` and client-side `pull` alike).
    read_permits: Vec<Arc<tokio::sync::Semaphore>>,
    /// Inbound (accepted) connections, for path reporting and the
    /// port-down watchdog; dead entries are pruned opportunistically.
    inbound: std::sync::Mutex<Vec<Weak<RdmaSocket>>>,
    /// Accepted connections remain leased until confirmation and a data-plane
    /// receive have both occurred. Active entries remain briefly as tombstones
    /// so a lost confirmation response can be retried idempotently.
    accept_leases: dashmap::DashMap<u64, AcceptLease, RandomState>,
    lease_sweeper_started: AtomicBool,
    /// Shared buffer pool (owned by State, kept alive via Arc).
    /// Dropped after QPs → deregisters MRs.
    pub buffer_pool: Arc<BufferPool>,
    /// Global device collection (owned by State, kept alive via Arc).
    /// Dropped last → destroys PD and closes device context.
    pub devices: Arc<Devices>,
    /// RDMA Queue Pair and connection settings.
    pub config: RdmaSocketPoolConfig,
    /// Counter for round-robin stripe selection in `acquire`.
    next_stripe: AtomicUsize,
    /// Sequence for the pool's cheap pseudo-random draws (P2C, jitter).
    rng_seq: AtomicUsize,
    /// Whether the background maintenance task has been started.
    maintenance_started: AtomicBool,
    /// Round-robin cursor selecting the next peer to rebalance.
    rebalance_cursor: AtomicUsize,
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
        let rdma = config.rdma.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                "RDMA socket pool requires RDMA configuration".into(),
            )
        })?;
        Self::new(devices.clone(), buffer_pool.clone(), rdma)
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

    /// Acquires or creates an RDMA socket for the specified address.
    async fn acquire(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket> {
        self.acquire_with_deadline(addr, &HashSet::new(), None, state)
            .await
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
    pub(crate) fn rdma_device_list(&self) -> Result<RdmaInfo> {
        Ok(RdmaInfo::from_devices(
            self.devices.rdma_devices(),
            &self.config,
            &self.conn_counts,
        ))
    }

    pub(crate) fn rdma_accept(
        &self,
        request: &ConnectRequest,
        state: &Arc<State>,
    ) -> Result<Endpoint> {
        if request.connection_id == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA connection id must be non-zero".into(),
            ));
        }
        if let dashmap::mapref::entry::Entry::Occupied(entry) =
            self.accept_leases.entry(request.connection_id)
        {
            if entry.get().expires_at > Instant::now() && entry.get().socket.strong_count() > 0 {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!("duplicate RDMA connection id {}", request.connection_id),
                ));
            }
            let (_, expired) = entry.remove_entry();
            if let Some(socket) = expired.socket.upgrade() {
                socket.set_error();
            }
        }
        let (device_index, device) = self.find_device_by_name(&request.target)?;
        let connection_config = self.clamp_connection_config(device, request.config);
        let poller = self.pollers.get_or_start(
            device,
            self.poller_config(),
            self.config.poll_threads_per_device,
        )?;
        let queue_pair = self.create_queue_pair(device, &connection_config, &poller)?;
        let mut local_endpoint = self.build_endpoint(
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
            connection_config.traffic_class,
        )?;

        let (info, gid_zones) = device.info_with_zones();
        let local_ip = Self::find_port(&info, request.target.port_num)
            .ok()
            .and_then(|port| port.find_gid(request.target.gid_index))
            .and_then(|gid| gid_ip(&gid.gid));
        let path = RdmaPathInfo {
            local: RdmaNicInfo {
                device: info.name.clone(),
                port_num: request.target.port_num,
                gid_index: request.target.gid_index,
                ip: local_ip,
                zones: gid_zones
                    .get(&(request.target.port_num, request.target.gid_index))
                    .cloned()
                    .unwrap_or_default(),
            },
            remote: RdmaNicInfo {
                device: request.source_device.clone(),
                port_num: request.endpoint.port_num,
                gid_index: request.endpoint.gid_index,
                ip: gid_ip(&request.endpoint.gid),
                zones: request.source_zones.clone(),
            },
        };

        let socket = self.register_socket(
            queue_pair,
            state,
            &poller,
            &connection_config,
            path,
            device_index,
        )?;
        local_endpoint.connection_cookie = socket.conn_id;
        {
            let mut inbound = self.inbound.lock().unwrap();
            inbound.retain(|conn| conn.strong_count() > 0);
            inbound.push(Arc::downgrade(&socket));
        }
        match self.accept_leases.entry(request.connection_id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(AcceptLease {
                    socket: Arc::downgrade(&socket),
                    server_connection_cookie: socket.conn_id,
                    state: AcceptLeaseState::Pending,
                    expires_at: Instant::now()
                        + Duration::from_millis(self.config.connect_lease_ms),
                });
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                socket.set_error();
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!("duplicate RDMA connection id {}", request.connection_id),
                ));
            }
        }
        socket.set_accept_lease(request.connection_id);
        self.ensure_accept_lease_sweeper(state);
        self.ensure_maintenance_task(state);
        tracing::debug!(
            local_qp = socket.queue_pair.qp_num(),
            remote_qp = request.endpoint.qp_num,
            "accepted RDMA connection"
        );
        Ok(local_endpoint)
    }

    pub(crate) fn rdma_confirm(&self, control: &ConnectionControl) -> Result<()> {
        match self.accept_leases.entry(control.connection_id) {
            dashmap::mapref::entry::Entry::Vacant(_) => Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "unknown or expired RDMA connection id {}",
                    control.connection_id
                ),
            )),
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if !Self::lease_matches_control(entry.get(), control) {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!(
                            "RDMA connection {} identity mismatch",
                            control.connection_id
                        ),
                    ));
                }
                if entry.get().expires_at <= Instant::now() {
                    let (_, expired) = entry.remove_entry();
                    if let Some(socket) = expired.socket.upgrade() {
                        socket.set_error();
                    }
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!("expired RDMA connection id {}", control.connection_id),
                    ));
                }
                if !entry
                    .get()
                    .socket
                    .upgrade()
                    .is_some_and(|socket| socket.state.is_ok())
                {
                    entry.remove();
                    return Err(Error::new(
                        ErrorKind::ConnectionClosed,
                        format!("RDMA connection {} already closed", control.connection_id),
                    ));
                }
                entry.get_mut().state =
                    advance_accept_lease(entry.get().state, AcceptLeaseEvent::Confirm);
                entry.get_mut().expires_at =
                    Instant::now() + Duration::from_millis(self.config.connect_lease_ms);
                Ok(())
            }
        }
    }

    pub(crate) fn rdma_abort(&self, control: &ConnectionControl) {
        if let Some((_, lease)) = self
            .accept_leases
            .remove_if(&control.connection_id, |_, lease| {
                Self::lease_matches_control(lease, control)
            })
            && let Some(socket) = lease.socket.upgrade()
        {
            socket.set_error();
        }
    }

    pub(crate) fn rdma_receive_observed(&self, connection_id: u64, socket: &Arc<RdmaSocket>) {
        let weak_socket = Arc::downgrade(socket);
        self.observe_accept_receive(connection_id, &weak_socket);
    }

    fn observe_accept_receive(&self, connection_id: u64, socket: &Weak<RdmaSocket>) {
        let dashmap::mapref::entry::Entry::Occupied(mut entry) =
            self.accept_leases.entry(connection_id)
        else {
            return;
        };
        if !entry.get().socket.ptr_eq(socket) {
            return;
        }
        if entry.get().expires_at <= Instant::now() {
            let (_, expired) = entry.remove_entry();
            if let Some(socket) = expired.socket.upgrade() {
                socket.set_error();
            }
            return;
        }
        entry.get_mut().state = advance_accept_lease(entry.get().state, AcceptLeaseEvent::Receive);
    }

    fn lease_matches_control(lease: &AcceptLease, control: &ConnectionControl) -> bool {
        lease.server_connection_cookie == control.server_connection_cookie
    }

    const DEVICE_LIST_CACHE_TTL: Duration = Duration::from_secs(30);
    const DESIRED_PEER_IDLE_TTL: Duration = Duration::from_secs(60);
    /// How long a NIC pair stays penalized after its QP setup failed.
    const PATH_BLACKLIST_TTL: Duration = Duration::from_secs(30);

    /// Creates a new pool using the shared devices and buffer pool.
    pub fn new(
        devices: Arc<Devices>,
        buffer_pool: Arc<BufferPool>,
        config: RdmaSocketPoolConfig,
    ) -> Result<Self> {
        for (index, zone) in config.zones.iter().enumerate() {
            if zone.name.is_empty()
                || config.zones[..index]
                    .iter()
                    .any(|existing| existing.name == zone.name)
            {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "RDMA zone names must be non-empty and unique: {:?}",
                        zone.name
                    ),
                ));
            }
        }
        if config.connect_lease_ms < 15_000 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "rdma.connect_lease_ms must be at least 15000".into(),
            ));
        }
        if config.preconnect_max_per_peer.max(1) < config.connections_per_peer.max(1) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "rdma.preconnect_max_per_peer must cover connections_per_peer".into(),
            ));
        }
        let task_supervisor = TaskSupervisor::create();
        let port_refresher = RdmaDeviceRefresher::start(devices.clone(), &task_supervisor);
        Ok(Self {
            acquire_client: Client {
                timeout: std::time::Duration::from_secs(5),
                connect_timeout: std::time::Duration::from_secs(5),
                use_msgpack: true,
                max_retries: 2,
            },
            task_supervisor,
            pollers: DevicePollers::default(),
            _port_refresher: port_refresher,
            peers: PeerMap::default(),
            conn_counts: Arc::new(
                (0..devices.rdma_devices().len())
                    .map(|_| AtomicUsize::new(0))
                    .collect(),
            ),
            read_permits: (0..devices.rdma_devices().len())
                .map(|_| {
                    Arc::new(tokio::sync::Semaphore::new(
                        config.max_inflight_read_wrs.max(1) as usize,
                    ))
                })
                .collect(),
            inbound: std::sync::Mutex::new(Vec::new()),
            accept_leases: dashmap::DashMap::default(),
            lease_sweeper_started: AtomicBool::new(false),
            buffer_pool,
            devices,
            config,
            next_stripe: AtomicUsize::new(0),
            rng_seq: AtomicUsize::new(0),
            maintenance_started: AtomicBool::new(false),
            rebalance_cursor: AtomicUsize::new(0),
            ring_bytes: Arc::new(AtomicUsize::new(0)),
            pool_capacity_warned: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(crate) async fn acquire_with_deadline(
        &self,
        addr: &SocketAddr,
        avoided_remote_nics: &HashSet<String>,
        deadline: Option<Instant>,
        state: &Arc<State>,
    ) -> Result<Socket> {
        if let Some(socket) = self.try_acquire(addr, avoided_remote_nics) {
            return Ok(socket);
        }

        let peer = self.peer(*addr);
        let guard = match deadline {
            Some(deadline) => tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                peer.connect.lock(),
            )
            .await
            .map_err(|_| Error::new(ErrorKind::Timeout, "request deadline expired".into())),
            None => Ok(peer.connect.lock().await),
        };
        let guard = guard?;
        let result = self
            .handshake(&peer, state, avoided_remote_nics, deadline)
            .await;
        drop(guard);
        if result.is_ok() {
            peer.touch(Instant::now());
        }
        result
    }

    pub(crate) fn try_acquire(
        &self,
        addr: &SocketAddr,
        avoided_remote_nics: &HashSet<String>,
    ) -> Option<Socket> {
        let peer = self.existing_peer(addr)?;
        // The fast path must never block: a writer in the way means
        // connection churn, which the slow path handles anyway. A
        // poisoned lock likewise falls through to the slow path.
        let stripes = peer.stripes.try_read().ok()?;
        let socket = self.pick_stripe(&stripes.active, avoided_remote_nics)?;
        drop(stripes);
        peer.touch(Instant::now());
        Some(socket)
    }

    pub(crate) async fn acquire_existing(
        &self,
        addr: &SocketAddr,
        avoided_remote_nics: &HashSet<String>,
    ) -> Option<Socket> {
        let peer = self.existing_peer(addr)?;
        let stripes = peer.stripes.read().unwrap();
        let socket = self.pick_stripe(&stripes.active, avoided_remote_nics)?;
        drop(stripes);
        peer.touch(Instant::now());
        Some(socket)
    }

    fn peer(&self, addr: SocketAddr) -> Arc<PeerState> {
        self.peers
            .entry(addr)
            .or_insert_with(|| Arc::new(PeerState::new(addr)))
            .clone()
    }

    fn existing_peer(&self, addr: &SocketAddr) -> Option<Arc<PeerState>> {
        self.peers.get(addr).map(|peer| peer.clone())
    }

    /// Picks one healthy connection stripe round-robin.
    fn pick_stripe(
        &self,
        stripes: &[Stripe],
        avoided_remote_nics: &HashSet<String>,
    ) -> Option<Socket> {
        if stripes.is_empty() {
            return None;
        }
        let start = self.next_stripe.fetch_add(1, Ordering::Relaxed);
        (0..stripes.len())
            .map(|i| &stripes[(start + i) % stripes.len()])
            .find(|s| {
                s.socket.state.is_ok()
                    && !avoided_remote_nics.contains(&s.socket.path.remote.device)
            })
            .map(|s| (&s.socket).into())
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

    /// Cheap pseudo-random draw for P2C sampling and interval jitter;
    /// uniqueness/quality requirements are modest, so a hash of a
    /// (sequence, nanos) pair suffices — no RNG dependency.
    fn pseudo_random(&self) -> u64 {
        pseudo_random(self.rng_seq.fetch_add(1, Ordering::Relaxed))
    }

    /// Penalizes a NIC pair towards `addr` after its QP setup failed.
    fn blacklist_path(&self, peer: &PeerState, path: &RdmaPathInfo) {
        peer.meta.lock().unwrap().blacklist.insert(
            Self::path_key(path),
            Instant::now() + Self::PATH_BLACKLIST_TTL,
        );
    }

    fn path_key(path: &RdmaPathInfo) -> PathKey {
        (
            path.local.device.clone(),
            path.local.port_num,
            path.local.gid_index,
            path.remote.device.clone(),
            path.remote.port_num,
            path.remote.gid_index,
        )
    }

    /// Whether the candidate's NIC pair towards `addr` is currently
    /// penalized; expired entries are pruned on the way.
    fn is_blacklisted(&self, peer: &PeerState, candidate: &PathCandidate) -> bool {
        let mut meta = peer.meta.lock().unwrap();
        let now = Instant::now();
        meta.blacklist.retain(|_, until| *until > now);
        meta.blacklist
            .contains_key(&Self::path_key(&candidate.path))
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
                max_inline_data: 0,
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
        path: RdmaPathInfo,
        device_index: usize,
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
        // Software timeout for RDMA READ completions (0 disables).
        let read_timeout = (self.config.read_timeout_ms > 0)
            .then(|| Duration::from_millis(self.config.read_timeout_ms));
        // In-flight READ budget: shared per local NIC (congestion
        // control), plus a per-connection SQ-overflow guard (the send
        // queue is shared with regular sends, so leave half to them).
        let read_permits = self
            .read_permits
            .get(device_index)
            .cloned()
            .unwrap_or_else(|| {
                Arc::new(tokio::sync::Semaphore::new(
                    self.config.max_inflight_read_wrs.max(1) as usize,
                ))
            });
        let sq_read_cap = (config.qp.max_send_wr / 2).max(1);
        let socket = Arc::new(RdmaSocket::new(
            queue_pair,
            self.buffer_pool.clone(),
            tx,
            poller.waker(),
            config.max_msg_size as usize,
            send_window,
            path,
            read_timeout,
            read_permits,
            sq_read_cap,
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
                conn_count_guard: ConnCountGuard::acquire(&self.conn_counts, device_index),
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

    async fn handshake(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        avoided_remote_nics: &HashSet<String>,
        deadline: Option<Instant>,
    ) -> Result<Socket> {
        let addr = &peer.addr;
        self.ensure_maintenance_task(state);
        // Re-check under the connect lock: another task may have connected,
        // or every stripe may have failed and must be replaced.
        let existing = {
            let mut stripes = peer.stripes.write().unwrap();
            if let Some(socket) = self.pick_stripe(&stripes.active, avoided_remote_nics) {
                return Ok(socket);
            }
            if stripes
                .active
                .iter()
                .any(|stripe| stripe.socket.state.is_ok())
            {
                stripes.active.clone()
            } else {
                if !stripes.active.is_empty() {
                    tracing::info!("all RDMA stripes to {addr} failed, reconnecting");
                    stripes.active.clear();
                }
                Vec::new()
            }
        };

        let fallback = (!avoided_remote_nics.is_empty())
            .then(|| self.pick_stripe(&existing, &HashSet::new()))
            .flatten();
        let plan = match self.prepare_connect_plan(peer, state, deadline).await {
            Ok(plan) => plan,
            Err(_) if fallback.is_some() => return Ok(fallback.expect("checked above")),
            Err(err) => return Err(err),
        };
        if !avoided_remote_nics.is_empty()
            && !plan
                .candidates
                .iter()
                .any(|candidate| !avoided_remote_nics.contains(&candidate.path.remote.device))
            && let Some(socket) = fallback
        {
            return Ok(socket);
        }
        let preference = PathPreference {
            remote_device: None,
            avoided_remote_nics,
        };

        if !existing.is_empty() {
            let max_connections = self.config.preconnect_max_per_peer.max(1) as usize;
            if existing
                .iter()
                .filter(|stripe| stripe.socket.state.is_ok())
                .count()
                >= max_connections
            {
                return fallback.ok_or_else(|| {
                    Error::new(
                        ErrorKind::Overloaded,
                        format!(
                            "RDMA peer {addr} reached preconnect_max_per_peer ({max_connections})"
                        ),
                    )
                });
            }
            let established = match self
                .connect_with_failover(peer, state, &plan, preference, &existing)
                .await
            {
                Ok(established) => established,
                Err(_) if fallback.is_some() => return Ok(fallback.expect("checked above")),
                Err(err) => return Err(err),
            };
            let stripe = self.admit_established(peer, established);
            return Ok(Socket::from(&stripe.socket));
        }

        // Establish `connections_per_peer` stripes towards this peer;
        // requests are spread round-robin across them (and, with poll
        // thread shards, across cores). Each stripe picks its own path:
        // local side by least connections, remote side by
        // power-of-two-choices over the peer's advertised per-NIC load.
        let stripe_count = self.config.connections_per_peer.max(1);
        let mut stripes: Vec<Stripe> = Vec::with_capacity(stripe_count as usize);
        let mut established_sockets = Vec::with_capacity(stripe_count as usize);
        for _ in 0..stripe_count {
            match self
                .connect_with_failover(peer, state, &plan, preference, &stripes)
                .await
            {
                Ok(established) => {
                    stripes.push(Stripe {
                        socket: established.socket.clone(),
                    });
                    established_sockets.push(established);
                }
                Err(err) => return Err(err),
            }
        }

        let socket = self
            .pick_stripe(&stripes, avoided_remote_nics)
            .or_else(|| self.pick_stripe(&stripes, &HashSet::new()))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::ConnectionClosed,
                    format!("all freshly established RDMA stripes to {addr} failed"),
                )
            })?;
        self.admit_initial(peer, established_sockets);
        Ok(socket)
    }

    /// Fetches the peer's device list and enumerates the compatible path
    /// candidates: everything a placement decision towards `addr` needs.
    async fn prepare_connect_plan(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        deadline: Option<Instant>,
    ) -> Result<ConnectPlan> {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::new(
                ErrorKind::Timeout,
                "request deadline expired".into(),
            ));
        }
        let mut acquire_ctx = Context::create_with_state_and_addr(state, &peer.addr);
        acquire_ctx.deadline = deadline;
        let remote_info = self.fetch_remote_device_list(peer, &acquire_ctx).await?;
        let candidates = match self.enumerate_path_candidates(&remote_info) {
            Ok(candidates) => candidates,
            Err(err) => {
                self.invalidate_device_list_cache(peer);
                return Err(err);
            }
        };
        Ok(ConnectPlan {
            acquire_ctx,
            remote_info,
            candidates,
            deadline,
        })
    }

    /// Establishes one stripe towards `addr`, falling over to the next
    /// best candidate when a NIC pair turns out to be unreachable
    /// (device matching cannot verify routability). Each failed pair is
    /// blacklisted by `connect_stripe`, so later placements avoid it too.
    async fn connect_with_failover(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        plan: &ConnectPlan,
        preference: PathPreference<'_>,
        existing: &[Stripe],
    ) -> Result<EstablishedSocket> {
        let mut remaining: Vec<PathCandidate> = plan.candidates.clone();
        loop {
            if plan
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(Error::new(
                    ErrorKind::Timeout,
                    "request deadline expired".into(),
                ));
            }
            let candidate =
                self.select_candidate(peer, &remaining, preference, &plan.remote_info, existing)?;
            match self
                .connect_stripe(peer, state, &plan.acquire_ctx, &candidate)
                .await
            {
                Ok(socket) => return Ok(socket),
                Err(err) => {
                    remaining.retain(|remaining| remaining.path != candidate.path);
                    if remaining.is_empty() {
                        return Err(err);
                    }
                    tracing::warn!(
                        peer = %peer.addr,
                        local = %candidate.path.local.device,
                        remote = %candidate.path.remote.device,
                        "RDMA path failed ({err}); trying another NIC pair"
                    );
                }
            }
        }
    }

    /// Establishes one RDMA connection (stripe) towards `addr` on the
    /// given path candidate.
    async fn connect_stripe(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        acquire_ctx: &Context,
        candidate: &PathCandidate,
    ) -> Result<EstablishedSocket> {
        let device = self
            .devices
            .rdma_devices()
            .get(candidate.local_device_index)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    "selected RDMA device disappeared".into(),
                )
            })?;
        let connection_config = self.negotiate_connection_config(device, &candidate.remote_limits);
        let poller = self.pollers.get_or_start(
            device,
            self.poller_config(),
            self.config.poll_threads_per_device,
        )?;
        let queue_pair = self.create_queue_pair(device, &connection_config, &poller)?;
        let local_endpoint = self.build_endpoint(
            &queue_pair,
            device,
            candidate.path.local.port_num,
            candidate.path.local.gid_index,
        )?;

        let connection_id = next_connection_id();
        let connect_request = ConnectRequest {
            connection_id,
            endpoint: local_endpoint,
            source_device: candidate.path.local.device.clone(),
            source_zones: candidate.path.local.zones.clone(),
            target: candidate.remote.clone(),
            config: connection_config,
        };
        // Box the recursive RPC call: `Client::connect` is generated by
        // `#[service]` and its future (through `SocketPool::acquire`)
        // contains this pool's futures — without the indirection this
        // coroutine's type would be infinitely sized. (No `Send`-proof
        // cycle arises from the recursion: the macro emits client impls
        // as `fn -> impl Future + Send`, so callers take `Send` from the
        // signature instead of inspecting the client bodies.)
        let remote_endpoint =
            match Box::pin(self.acquire_client.connect(acquire_ctx, &connect_request)).await {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    self.invalidate_device_list_cache(peer);
                    return Err(err);
                }
            };
        let control = ConnectionControl {
            connection_id,
            server_connection_cookie: remote_endpoint.connection_cookie,
        };
        if let Err(err) = self.bring_qp_to_rts(
            &queue_pair,
            &local_endpoint,
            &remote_endpoint,
            self.config.pkey_index,
            connection_config.traffic_class,
        ) {
            // QP setup failures are typically path problems (no route
            // between the selected NIC pair): penalize the pair so
            // placement falls over to other candidates.
            self.blacklist_path(peer, &candidate.path);
            self.schedule_abort_connection(&peer.addr, state, control);
            return Err(err);
        }

        let socket = match self.register_socket(
            queue_pair,
            state,
            &poller,
            &connection_config,
            candidate.path.clone(),
            candidate.local_device_index,
        ) {
            Ok(socket) => socket,
            Err(err) => {
                self.schedule_abort_connection(&peer.addr, state, control);
                return Err(err);
            }
        };
        let registration = SocketRegistrationGuard::new(
            &socket,
            self.task_supervisor.handle(),
            self.acquire_client.clone(),
            Context::create_with_state_and_addr(state, &peer.addr),
            control,
        );
        if acquire_ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "RDMA acquire deadline expired before confirmation".into(),
            ));
        }
        self.confirm_connection(acquire_ctx, &control).await?;
        if acquire_ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "RDMA acquire deadline expired after confirmation".into(),
            ));
        }
        tracing::info!(
            local_device = %candidate.path.local.device,
            local_port = candidate.path.local.port_num,
            local_gid_index = candidate.path.local.gid_index,
            remote_device = %candidate.remote.device_name,
            remote_port = candidate.remote.port_num,
            remote_gid_index = candidate.remote.gid_index,
            local_qp = socket.queue_pair.qp_num(),
            remote_qp = remote_endpoint.qp_num,
            "acquired RDMA socket"
        );
        Ok(EstablishedSocket {
            socket,
            registration,
        })
    }

    /// Runs the confirmation RPC, retrying once after an ambiguous
    /// response timeout. `confirm` is idempotent on the server (the lease
    /// state machine absorbs duplicates), so this local retry rescues an
    /// otherwise healthy QP from a lost response on the bootstrap
    /// connection instead of tearing it down and failing over paths. The
    /// generic client deliberately never retries after send — idempotency
    /// is knowledge only this call site has.
    async fn confirm_connection(&self, ctx: &Context, control: &ConnectionControl) -> Result<()> {
        match Box::pin(self.acquire_client.confirm(ctx, control)).await {
            Err(err) if matches!(err.kind, ErrorKind::Timeout) && !ctx.is_expired() => {
                tracing::debug!(
                    connection_id = control.connection_id,
                    "RDMA confirm timed out, retrying once"
                );
                Box::pin(self.acquire_client.confirm(ctx, control)).await
            }
            result => result,
        }
    }

    /// Admits one confirmed connection into request rotation.
    /// Ordering is always track -> commit -> publish -> activate.
    fn admit_established(&self, peer: &Arc<PeerState>, established: EstablishedSocket) -> Stripe {
        let EstablishedSocket {
            socket,
            mut registration,
        } = established;
        let stripe = Stripe { socket };
        stripe.socket.set_peer_health(peer);
        registration.commit();
        peer.stripes.write().unwrap().active.push(stripe.clone());
        stripe.socket.request_activation();
        stripe
    }

    /// Atomically publishes an initial stripe set after every handshake
    /// succeeded. Dropping before commit aborts every unadmitted connection.
    fn admit_initial(&self, peer: &Arc<PeerState>, established: Vec<EstablishedSocket>) {
        let mut registrations = Vec::with_capacity(established.len());
        let stripes: Vec<Stripe> = established
            .into_iter()
            .map(|established| {
                let EstablishedSocket {
                    socket,
                    registration,
                } = established;
                socket.set_peer_health(peer);
                registrations.push(registration);
                Stripe { socket }
            })
            .collect();
        for registration in &mut registrations {
            registration.commit();
        }
        peer.stripes.write().unwrap().active = stripes.clone();
        for stripe in &stripes {
            stripe.socket.request_activation();
        }
    }

    /// Replaces `victim` only if it is still in rotation. The replacement is
    /// never visible unless its registration can be committed.
    fn admit_replacing(
        &self,
        peer: &Arc<PeerState>,
        victim: &Arc<RdmaSocket>,
        established: EstablishedSocket,
    ) -> bool {
        let EstablishedSocket {
            socket,
            mut registration,
        } = established;
        let mut stripes = peer.stripes.write().unwrap();
        let Some(position) = stripes
            .active
            .iter()
            .position(|stripe| Arc::ptr_eq(&stripe.socket, victim))
        else {
            return false;
        };
        socket.set_peer_health(peer);
        registration.commit();
        stripes.active.push(Stripe {
            socket: socket.clone(),
        });
        let victim = stripes.active.remove(position);
        stripes.draining.push(victim.clone());
        drop(stripes);
        socket.request_activation();
        self.drain_then_close(peer, victim.socket);
        true
    }

    fn schedule_abort_connection(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
        control: ConnectionControl,
    ) {
        let abort_ctx = Context::create_with_state_and_addr(state, addr);
        let abort_client = self.acquire_client.clone();
        let _ = self.task_supervisor.handle().try_spawn(async move {
            if let Err(err) = abort_client.abort(&abort_ctx, &control).await {
                tracing::debug!(connection_id = control.connection_id, %err, "RDMA abort cleanup failed");
            }
        });
    }

    async fn fetch_remote_device_list(
        &self,
        peer: &Arc<PeerState>,
        ctx: &Context,
    ) -> Result<RdmaInfo> {
        if let Some(info) = self.get_cached_device_list(peer) {
            return Ok(info);
        }

        // Boxed for the same reason as the `connect` call in
        // `connect_stripe`: keeps this coroutine's type finite.
        let info = Box::pin(self.acquire_client.info(ctx, &())).await?;
        peer.meta.lock().unwrap().device_cache = Some(CachedRdmaInfo {
            info: info.clone(),
            cached_at: Instant::now(),
        });
        Ok(info)
    }

    fn get_cached_device_list(&self, peer: &PeerState) -> Option<RdmaInfo> {
        let meta = peer.meta.lock().unwrap();
        let cached = meta.device_cache.as_ref()?;
        if cached.cached_at.elapsed() < Self::DEVICE_LIST_CACHE_TTL {
            Some(cached.info.clone())
        } else {
            None
        }
    }

    fn invalidate_device_list_cache(&self, peer: &PeerState) {
        peer.meta.lock().unwrap().device_cache = None;
    }

    /// Enumerates every compatible (local NIC, remote NIC) pair.
    ///
    /// One candidate is produced per compatible port pair (the GID within
    /// a port pair is chosen by the existing preference logic). Link class
    /// preference is applied after request constraints and reachability
    /// filters, keeping lower classes available as fallbacks.
    fn enumerate_path_candidates(&self, remote_info: &RdmaInfo) -> Result<Vec<PathCandidate>> {
        let local_devices = self.devices.rdma_devices();
        if local_devices.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "no local RDMA device available".into(),
            ));
        }

        let mut matches = Vec::new();
        let mut remote_ports = 0usize;
        let mut local_usable_ports = 0usize;
        let mut link_layer_matches = 0usize;

        // Remote ports are pre-filtered: peers only advertise usable ports.
        for remote_device in &remote_info.devices {
            for remote_port in &remote_device.ports {
                remote_ports += 1;

                for (local_device_index, local_device) in local_devices.iter().enumerate() {
                    let (local_info, gid_zones) = local_device.info_with_zones();
                    for local_port in &local_info.ports {
                        if !local_port.is_usable() {
                            continue;
                        }
                        local_usable_ports += 1;
                        if local_port.port_attr.link_layer != remote_port.link_layer {
                            continue;
                        }
                        link_layer_matches += 1;

                        let gid_pairs = Self::match_gid_pairs(local_port, remote_port);
                        let pair_limit = if self.config.zones.is_empty() {
                            1
                        } else {
                            gid_pairs.len()
                        };
                        for (local_gid_index, remote_gid_index) in
                            gid_pairs.into_iter().take(pair_limit)
                        {
                            let local_ip = local_port
                                .find_gid(local_gid_index)
                                .and_then(|gid| gid_ip(&gid.gid));
                            let remote_ip = remote_port
                                .gids
                                .iter()
                                .find(|gid| gid.index == remote_gid_index)
                                .and_then(|gid| gid_ip(&gid.gid));
                            let class = match local_port.port_attr.link_layer {
                                LinkLayer::InfiniBand => PathClass::InfiniBand,
                                LinkLayer::Ethernet
                                    if Self::gid_index_is_rocev2(local_port, local_gid_index) =>
                                {
                                    PathClass::RoceV2
                                }
                                LinkLayer::Ethernet => PathClass::RoceOther,
                                LinkLayer::Unspecified => continue,
                            };
                            matches.push(PathCandidate {
                                local_device_index,
                                remote: DeviceSelection {
                                    device_name: remote_device.name.clone(),
                                    port_num: remote_port.port_num,
                                    gid_index: remote_gid_index,
                                },
                                remote_limits: remote_device.connection,
                                class,
                                path: RdmaPathInfo {
                                    local: RdmaNicInfo {
                                        device: local_info.name.clone(),
                                        port_num: local_port.port_num,
                                        gid_index: local_gid_index,
                                        ip: local_ip,
                                        zones: gid_zones
                                            .get(&(local_port.port_num, local_gid_index))
                                            .cloned()
                                            .unwrap_or_default(),
                                    },
                                    remote: RdmaNicInfo {
                                        device: remote_device.name.clone(),
                                        port_num: remote_port.port_num,
                                        gid_index: remote_gid_index,
                                        ip: remote_ip,
                                        zones: remote_port
                                            .gid_zones
                                            .get(&remote_gid_index)
                                            .cloned()
                                            .unwrap_or_default(),
                                    },
                                },
                            });
                        }
                    }
                }
            }
        }

        if matches.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "no compatible RDMA device/port/GID pair found: remote_devices={} local_devices={} remote_ports={} local_usable_ports={} link_layer_matches={}",
                    remote_info.devices.len(),
                    local_devices.len(),
                    remote_ports,
                    local_usable_ports,
                    link_layer_matches
                ),
            ));
        }
        Ok(matches)
    }

    /// Picks the path for a new stripe.
    ///
    /// Remote NIC: power-of-two-choices over the peer's advertised per-NIC
    /// connection counts (plus our own healthy stripes to this peer, which
    /// the possibly-stale advertisement may not include yet). P2C keeps
    /// clients from herding onto the same "least loaded" server NIC when
    /// they all act on the same cached snapshot.
    ///
    /// Local NIC: plain least-connections over the live per-device
    /// counters — they are exact, and equal counts self-balance because
    /// every placement increments the chosen device's counter.
    fn select_candidate(
        &self,
        peer: &PeerState,
        candidates: &[PathCandidate],
        preference: PathPreference<'_>,
        remote_info: &RdmaInfo,
        peer_stripes: &[Stripe],
    ) -> Result<PathCandidate> {
        let views: Vec<placement::Candidate<'_>> = candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| {
                let advertised = remote_info
                    .devices
                    .iter()
                    .find(|device| device.name == candidate.path.remote.device)
                    .map_or(0, |device| u64::from(device.active_connections));
                let ours = peer_stripes
                    .iter()
                    .filter(|stripe| {
                        stripe.socket.state.is_ok()
                            && stripe.socket.path.remote.device == candidate.path.remote.device
                    })
                    .count() as u64;
                placement::Candidate {
                    index,
                    local_index: candidate.local_device_index,
                    remote: &candidate.path.remote.device,
                    same_zone: candidate.has_same_zone(),
                    class: candidate.class,
                    blacklisted: self.is_blacklisted(peer, candidate),
                    local_load: self
                        .conn_counts
                        .get(candidate.local_device_index)
                        .map_or(0, |count| count.load(Ordering::Acquire) as u64),
                    remote_load: advertised + ours,
                }
            })
            .collect();
        let index = placement::choose_path(
            &views,
            placement::Selection {
                required_remote: preference.remote_device,
                avoided_remotes: preference.avoided_remote_nics,
            },
            [self.pseudo_random(), self.pseudo_random()],
        )
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "no compatible RDMA path matches remote device {:?}",
                    preference.remote_device
                ),
            )
        })?;
        Ok(candidates[index].clone())
    }

    fn gid_index_is_rocev2(port: &Port, gid_index: u8) -> bool {
        port.find_gid(gid_index)
            .is_some_and(|gid| gid.gid_type == GidType::RoCEv2)
    }

    /// Selects a (local, remote) GID index pair for the given port pair.
    ///
    /// Both GID tables only contain usable GIDs — unusable ones (RoCE v2
    /// loopback / link-local) are filtered out at collection time on each
    /// side (see `query_device_info`).
    fn match_gid_pairs(local_port: &Port, remote_port: &RdmaPortInfo) -> Vec<(u8, u8)> {
        let (local_gids, remote_gids) = (&local_port.gids[..], &remote_port.gids[..]);
        match local_port.port_attr.link_layer {
            LinkLayer::InfiniBand => vec![(
                Self::first_gid(local_gids, |_| true).unwrap_or(0),
                Self::first_gid(remote_gids, |_| true).unwrap_or(0),
            )],
            LinkLayer::Ethernet => Self::match_roce_gid_pairs(local_gids, remote_gids),
            LinkLayer::Unspecified => Vec::new(),
        }
    }

    fn match_roce_gid_pairs(local_gids: &[Gid], remote_gids: &[Gid]) -> Vec<(u8, u8)> {
        let mut pairs = Vec::new();
        // Prefer RoCE v2 pairs, then RoCE v1 pairs, while retaining every
        // compatible pair so zone preference can select the right GID.
        for wanted in [GidType::RoCEv2, GidType::RoCEv1] {
            for local in local_gids.iter().filter(|gid| gid.gid_type == wanted) {
                for remote in remote_gids.iter().filter(|gid| gid.gid_type == wanted) {
                    pairs.push((local.index, remote.index));
                }
            }
        }

        // Then every other pair with matching GID types.
        for local in local_gids
            .iter()
            .filter(|gid| !matches!(gid.gid_type, GidType::RoCEv2 | GidType::RoCEv1))
        {
            for remote in remote_gids
                .iter()
                .filter(|remote| remote.gid_type == local.gid_type)
            {
                pairs.push((local.index, remote.index));
            }
        }

        // Preserve the previous best-effort fallback for unusual providers
        // whose two sides report different GID type names.
        if pairs.is_empty()
            && let (Some(local), Some(remote)) = (
                Self::first_gid(local_gids, |_| true),
                Self::first_gid(remote_gids, |_| true),
            )
        {
            pairs.push((local, remote));
        }
        pairs
    }

    /// Returns the index of the first GID matching `predicate`.
    fn first_gid(gids: &[Gid], mut predicate: impl FnMut(&Gid) -> bool) -> Option<u8> {
        gids.iter().find(|gid| predicate(gid)).map(|gid| gid.index)
    }

    fn negotiate_connection_config(
        &self,
        local_device: &RdmaDevice,
        remote: &RdmaConnectionConfig,
    ) -> RdmaConnectionConfig {
        let local = self.local_connection_config(local_device);
        let remote = *remote;
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
            },
            cq_len: local.cq_len.min(remote.cq_len),
            recv_queue_len: local.recv_queue_len.min(remote.recv_queue_len),
            max_msg_size: local.max_msg_size.min(remote.max_msg_size),
            // The connecting side dictates the traffic class; the remote
            // advertisement is irrelevant here.
            traffic_class: self.config.traffic_class,
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
            },
            cq_len: requested.cq_len.min(local.cq_len),
            recv_queue_len: requested.recv_queue_len.min(local.recv_queue_len),
            max_msg_size: requested.max_msg_size.min(local.max_msg_size),
            // Client-chosen: applied verbatim so both directions of the
            // connection share one traffic class.
            traffic_class: requested.traffic_class,
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
            // Enforce a small floor so a tiny misconfiguration cannot break
            // the RPC control plane.
            max_msg_size: self.config.max_msg_size.max(16 * 1024),
            traffic_class: self.config.traffic_class,
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
        if !port.is_usable() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("RDMA port {}:{} is not active", info.name, port_num),
            ));
        }

        let gid = port.find_gid(gid_index).map(|gid| gid.gid);
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
            connection_cookie: 0,
            qp_num: qp.qp_num(),
            port_num,
            gid_index,
            lid: port.port_attr.lid,
            gid: gid.unwrap_or_default(),
            link_layer: port.port_attr.link_layer,
            active_mtu: port.port_attr.active_mtu,
            psn: Self::random_psn(qp.qp_num()),
            rd_atomic_cap: Self::rd_atomic_cap(&info),
        })
    }

    /// The device cap on concurrent RDMA READs per QP, advertised to the
    /// peer via the endpoint exchange.
    ///
    /// The minimum of the initiator-side and responder-side device limits
    /// is used for both directions, clamped to a sane ceiling — beyond ~16
    /// the returns diminish while responder resources grow.
    fn rd_atomic_cap(info: &DeviceInfo) -> u8 {
        const RD_ATOMIC_CEILING: i32 = 16;
        let cap = info
            .device_attr
            .max_qp_rd_atom
            .min(info.device_attr.max_qp_init_rd_atom)
            .clamp(1, RD_ATOMIC_CEILING);
        u8::try_from(cap).unwrap_or(1)
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
        traffic_class: u8,
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
        // Both sides advertise their device cap and program the minimum
        // for both `max_rd_atomic` (outbound reads) and
        // `max_dest_rd_atomic` (inbound reads): the two ends compute the
        // same value, which keeps the RC requirement
        // `initiator.max_rd_atomic <= responder.max_dest_rd_atomic`
        // trivially satisfied.
        let rd_atomic = local.rd_atomic_cap.min(remote.rd_atomic_cap).max(1);
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
            rd_atomic,
            rd_atomic,
            traffic_class,
        )
        .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
    }

    fn min_mtu(a: ibv_mtu, b: ibv_mtu) -> ibv_mtu {
        if (a as u32) <= (b as u32) { a } else { b }
    }

    /// Starts the background maintenance task on first use (when an
    /// `Arc<State>` is available). The task holds only a `Weak<State>`;
    /// it exits when the pool's supervisor stops or the state is dropped.
    fn ensure_maintenance_task(&self, state: &Arc<State>) {
        let interval_ms = self.config.maintenance_interval_ms;
        if interval_ms == 0 || self.maintenance_started.swap(true, Ordering::Relaxed) {
            return;
        }
        let weak_state = Arc::downgrade(state);
        let guard = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            let mut seq = 0usize;
            loop {
                // 0.5x..1.5x jitter decorrelates clients that would
                // otherwise all rebalance against the same (cached) server
                // load snapshot.
                seq = seq.wrapping_add(1);
                let sleep_ms = interval_ms / 2 + pseudo_random(seq) % interval_ms.max(1);
                tokio::select! {
                    () = guard.stopped() => break,
                    () = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
                }
                let Some(state) = weak_state.upgrade() else {
                    break;
                };
                let Some(pool) = state.socket_pool.rdma_pool() else {
                    break;
                };
                pool.run_maintenance(&state).await;
            }
        });
    }

    fn ensure_accept_lease_sweeper(&self, state: &Arc<State>) {
        if self.lease_sweeper_started.swap(true, Ordering::Relaxed) {
            return;
        }
        let interval = Duration::from_millis((self.config.connect_lease_ms / 4).clamp(100, 1_000));
        let weak_state = Arc::downgrade(state);
        if self
            .task_supervisor
            .handle()
            .try_spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(state) = weak_state.upgrade() else {
                        break;
                    };
                    let Some(pool) = state.socket_pool.rdma_pool() else {
                        break;
                    };
                    let now = Instant::now();
                    pool.accept_leases.retain(|connection_id, lease| {
                        let Some(socket) = lease.socket.upgrade() else {
                            return false;
                        };
                        if lease.expires_at > now {
                            return true;
                        }
                        if lease.state == AcceptLeaseState::Active {
                            tracing::debug!(
                                connection_id,
                                qp = socket.queue_pair.qp_num(),
                                "RDMA active lease tombstone expired"
                            );
                        } else {
                            tracing::warn!(
                                connection_id,
                                qp = socket.queue_pair.qp_num(),
                                state = ?lease.state,
                                "RDMA accept lease expired"
                            );
                            socket.set_error();
                        }
                        false
                    });
                }
            })
            .is_none()
        {
            self.lease_sweeper_started.store(false, Ordering::Relaxed);
        }
    }

    /// One maintenance tick: fail connections on dead local ports, prune
    /// dead stripes, and replenish peers below `connections_per_peer`.
    pub(crate) async fn run_maintenance(&self, state: &Arc<State>) {
        self.fail_paths_on_dead_ports().await;
        self.prune_dead().await;
        let now = Instant::now();
        let snapshot: Vec<Arc<PeerState>> =
            self.peers.iter().map(|peer| peer.value().clone()).collect();
        let mut peers = Vec::new();
        for peer in snapshot {
            let has_stripes = !peer.stripes.read().unwrap().active.is_empty();
            let recently_used = peer
                .meta
                .lock()
                .unwrap()
                .last_used
                .is_some_and(|last_used| {
                    now.saturating_duration_since(last_used) < Self::DESIRED_PEER_IDLE_TTL
                });
            if has_stripes || recently_used {
                peers.push(peer);
                continue;
            }
            let Ok(_connect) = peer.connect.try_lock() else {
                continue;
            };
            if Arc::strong_count(&peer) == 2 {
                self.peers.remove_if(&peer.addr, |_, current| {
                    Arc::ptr_eq(current, &peer)
                        && Arc::strong_count(current) == 2
                        && current.stripes.read().unwrap().active.is_empty()
                });
            }
        }
        if peers.is_empty() {
            return;
        }
        for peer in &peers {
            self.replenish_peer(peer, state).await;
        }
        let target =
            peers[self.rebalance_cursor.fetch_add(1, Ordering::Relaxed) % peers.len()].clone();
        self.rebalance_peer(&target, state).await;
    }

    /// Whether the local NIC of `nic` can no longer carry traffic
    /// (device gone or port not usable, per the refresher's snapshot).
    fn local_nic_dead(&self, nic: &RdmaNicInfo) -> bool {
        let Some(device) = self
            .devices
            .rdma_devices()
            .iter()
            .find(|d| d.info().name == nic.device)
        else {
            return true;
        };
        !device
            .info()
            .ports
            .iter()
            .any(|port| port.port_num == nic.port_num && port.is_usable())
    }

    /// Proactively fails connections whose local port went down; the port
    /// state comes from the periodic device refresher, so failures are
    /// detected even on idle connections that see no completion errors.
    async fn fail_paths_on_dead_ports(&self) {
        let fail_if_dead = |socket: &Arc<RdmaSocket>| {
            if socket.state.is_ok() && self.local_nic_dead(&socket.path.local) {
                tracing::warn!(
                    device = %socket.path.local.device,
                    qp = socket.queue_pair.qp_num(),
                    "local RDMA port down; failing connection"
                );
                socket.set_error();
            }
        };
        for peer in self.peers.iter() {
            for socket in peer.value().all_sockets() {
                fail_if_dead(&socket);
            }
        }
        let inbound: Vec<Arc<RdmaSocket>> = self
            .inbound
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for socket in &inbound {
            fail_if_dead(socket);
        }
    }

    /// Removes dead stripes (and fully dead peers) from the socket map and
    /// prunes released inbound connections.
    async fn prune_dead(&self) {
        for peer in self.peers.iter() {
            let mut stripes = peer.value().stripes.write().unwrap();
            stripes.active.retain(|s| s.socket.state.is_ok());
            stripes.draining.retain(|s| s.socket.state.is_ok());
        }
        self.inbound
            .lock()
            .unwrap()
            .retain(|conn| conn.strong_count() > 0);
    }

    /// Tops a desired peer up to `connections_per_peer` healthy stripes.
    async fn replenish_peer(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        let addr = &peer.addr;
        const PEER_BACKOFF_KEY: &str = "";
        if !self.preconnect_ready(peer, PEER_BACKOFF_KEY) {
            return;
        }
        let Ok(guard) = peer.connect.try_lock() else {
            // An acquire is already connecting to this peer; retry next tick.
            return;
        };
        let result: Result<()> = async {
            let plan = self.prepare_connect_plan(peer, state, None).await?;
            let mut existing = peer.active_snapshot();
            let max_connections = self.config.preconnect_max_per_peer.max(1) as usize;
            let min_per_remote = self.config.min_connections_per_remote_nic as usize;
            let avoided_remote_nics = HashSet::new();
            let mut remote_names = Vec::new();
            for candidate in &plan.candidates {
                if !remote_names.contains(&candidate.path.remote.device) {
                    remote_names.push(candidate.path.remote.device.clone());
                }
            }
            let healthy_remotes: Vec<String> = existing
                .iter()
                .filter(|stripe| stripe.socket.state.is_ok())
                .map(|stripe| stripe.socket.path.remote.device.clone())
                .collect();
            let coverage_blocked: HashSet<String> = remote_names
                .iter()
                .filter(|remote| !self.preconnect_ready(peer, remote))
                .cloned()
                .collect();
            let actions = placement::plan_connections(
                &remote_names,
                &healthy_remotes,
                &coverage_blocked,
                min_per_remote,
                self.config.connections_per_peer.max(1) as usize,
                max_connections,
            );
            for action in actions {
                match action {
                    ReconcileAction::ConnectCoverage(remote) => {
                    if !self.preconnect_ready(peer, &remote) {
                            continue;
                    }
                    let preference = PathPreference {
                        remote_device: Some(&remote),
                        avoided_remote_nics: &avoided_remote_nics,
                    };
                    match self
                        .connect_with_failover(peer, state, &plan, preference, &existing)
                        .await
                    {
                        Ok(established) => {
                            self.clear_preconnect_failure(peer, &remote);
                            let stripe = self.admit_established(peer, established);
                            existing.push(stripe);
                        }
                        Err(err) => {
                            self.record_preconnect_failure(peer, &remote);
                            tracing::debug!(peer = %addr, remote_device = %remote, %err, "RDMA coverage connection failed");
                        }
                    }
                    }
                    ReconcileAction::ConnectTarget => {
                        let established = self
                            .connect_with_failover(
                                peer,
                                state,
                                &plan,
                                PathPreference {
                                    remote_device: None,
                                    avoided_remote_nics: &avoided_remote_nics,
                                },
                                &existing,
                            )
                            .await?;
                        let stripe = self.admit_established(peer, established);
                        existing.push(stripe);
                    }
                }
            }
            // Coverage actions may fail after the pure plan is built. Fill
            // the normal target from the resulting actual state rather than
            // assuming every planned action succeeded. The iteration bound
            // is computed upfront so connections that die right after
            // admission cannot keep this loop alive; the next maintenance
            // tick retries them (under backoff).
            let healthy_count = |stripes: &[Stripe]| {
                stripes
                    .iter()
                    .filter(|stripe| stripe.socket.state.is_ok())
                    .count()
            };
            let target = self.config.connections_per_peer.max(1) as usize;
            for _ in 0..target.saturating_sub(healthy_count(&existing)) {
                if healthy_count(&existing) >= max_connections {
                    break;
                }
                let established = self
                    .connect_with_failover(
                        peer,
                        state,
                        &plan,
                        PathPreference {
                            remote_device: None,
                            avoided_remote_nics: &avoided_remote_nics,
                        },
                        &existing,
                    )
                    .await?;
                let stripe = self.admit_established(peer, established);
                existing.push(stripe);
            }
            Ok(())
        }
        .await;
        if let Err(err) = result {
            self.record_preconnect_failure(peer, PEER_BACKOFF_KEY);
            tracing::debug!("replenishing RDMA stripes to {addr} failed: {err}");
        } else {
            self.clear_preconnect_failure(peer, PEER_BACKOFF_KEY);
        }
        drop(guard);
    }

    fn preconnect_ready(&self, peer: &PeerState, remote: &str) -> bool {
        peer.meta
            .lock()
            .unwrap()
            .backoff
            .get(remote)
            .is_none_or(|state| Instant::now() >= state.retry_at)
    }

    fn record_preconnect_failure(&self, peer: &PeerState, remote: &str) {
        let mut meta = peer.meta.lock().unwrap();
        let failures = meta
            .backoff
            .get(remote)
            .map_or(1, |state| state.failures.saturating_add(1));
        let base = preconnect_backoff_delay(failures);
        let jitter =
            Duration::from_millis(self.pseudo_random() % (base.as_millis() as u64 / 2 + 1));
        meta.backoff.insert(
            remote.to_owned(),
            RetryBackoff {
                failures,
                retry_at: Instant::now() + base + jitter,
            },
        );
    }

    fn clear_preconnect_failure(&self, peer: &PeerState, remote: &str) {
        peer.meta.lock().unwrap().backoff.remove(remote);
    }

    async fn rebalance_peer(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        let Ok(plan) = self.prepare_connect_plan(peer, state, None).await else {
            return;
        };
        let advertised_remotes: HashSet<&str> = plan
            .remote_info
            .devices
            .iter()
            .map(|device| device.name.as_str())
            .collect();
        let stripes: Vec<Stripe> = peer
            .active_snapshot()
            .into_iter()
            .filter(|stripe| stripe.socket.state.is_ok())
            .collect();
        if stripes.is_empty() {
            return;
        }

        let remote_info = &plan.remote_info;
        let views: Vec<placement::Candidate<'_>> = plan
            .candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| placement::Candidate {
                index,
                local_index: candidate.local_device_index,
                remote: &candidate.path.remote.device,
                same_zone: candidate.has_same_zone(),
                class: candidate.class,
                blacklisted: self.is_blacklisted(peer, candidate),
                local_load: 0,
                remote_load: 0,
            })
            .collect();
        let indices = placement::eligible_paths(
            &views,
            &placement::Selection {
                required_remote: None,
                avoided_remotes: &HashSet::new(),
            },
            false,
        );
        if indices.is_empty() {
            return;
        }

        const GONE: u64 = u64::MAX / 4;
        let remote_count = |name: &str| -> u64 {
            remote_info
                .devices
                .iter()
                .find(|device| device.name == name)
                .map_or(GONE, |device| u64::from(device.active_connections))
        };
        let local_count_by_index = |index: usize| -> u64 {
            self.conn_counts
                .get(index)
                .map_or(0, |count| count.load(Ordering::Acquire) as u64)
        };
        let local_count = |name: &str| -> u64 {
            self.devices
                .rdma_devices()
                .iter()
                .enumerate()
                .find(|(_, device)| device.info().name == name)
                .map_or(GONE, |(index, _)| local_count_by_index(index))
        };
        let stripe_views: Vec<placement::ExistingStripe<'_>> = stripes
            .iter()
            .enumerate()
            .map(|(index, stripe)| placement::ExistingStripe {
                index,
                local: &stripe.socket.path.local.device,
                remote: &stripe.socket.path.remote.device,
                local_load: local_count(&stripe.socket.path.local.device),
                remote_load: remote_count(&stripe.socket.path.remote.device),
                remote_healthy: stripes
                    .iter()
                    .filter(|other| {
                        other.socket.path.remote.device == stripe.socket.path.remote.device
                    })
                    .count(),
                remote_advertised: advertised_remotes
                    .contains(stripe.socket.path.remote.device.as_str()),
            })
            .collect();
        let replacements: Vec<placement::Replacement<'_>> = indices
            .iter()
            .map(|&index| {
                let candidate = &plan.candidates[index];
                placement::Replacement {
                    index,
                    local: &candidate.path.local.device,
                    remote: &candidate.path.remote.device,
                    local_load: local_count_by_index(candidate.local_device_index),
                    remote_load: remote_count(&candidate.path.remote.device),
                }
            })
            .collect();
        let Some((victim_index, best_index)) = placement::choose_rebalance(
            &stripe_views,
            &replacements,
            self.config.min_connections_per_remote_nic as usize,
            u64::from(self.config.rebalance_threshold.max(1)),
            !self.pseudo_random().is_multiple_of(2),
        ) else {
            return;
        };
        let victim = &stripes[victim_index];
        let best = &plan.candidates[best_index];

        let Ok(guard) = peer.connect.try_lock() else {
            return;
        };
        match self
            .connect_stripe(peer, state, &plan.acquire_ctx, best)
            .await
        {
            Ok(established) => {
                self.admit_replacing(peer, &victim.socket, established);
            }
            Err(err) => tracing::debug!(peer = %peer.addr, %err, "RDMA rebalance failed"),
        }
        drop(guard);
    }

    fn drain_then_close(&self, peer: &Arc<PeerState>, socket: Arc<RdmaSocket>) {
        let drain = Duration::from_millis(self.config.drain_timeout_ms);
        let guard = self.task_supervisor.start_async_task();
        let peer = peer.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = guard.stopped() => {}
                () = tokio::time::sleep(drain) => {}
            }
            socket.set_error();
            peer.stripes
                .write()
                .unwrap()
                .draining
                .retain(|stripe| !Arc::ptr_eq(&stripe.socket, &socket));
        });
    }
}

impl Drop for RdmaSocketPool {
    fn drop(&mut self) {
        self.task_supervisor.stop();
        self.peers.clear();
    }
}

impl std::fmt::Debug for RdmaSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaSocketPool").finish()
    }
}

/// Everything a placement decision towards one peer needs: the bootstrap
/// context, the peer's advertised device list and the compatible path
/// candidates derived from it.
struct ConnectPlan {
    acquire_ctx: Context,
    remote_info: RdmaInfo,
    candidates: Vec<PathCandidate>,
    deadline: Option<Instant>,
}

#[derive(Clone, Copy)]
struct PathPreference<'a> {
    remote_device: Option<&'a str>,
    avoided_remote_nics: &'a HashSet<String>,
}

/// One compatible (local NIC, remote NIC) pair a new connection could use.
#[derive(Clone, Debug)]
struct PathCandidate {
    /// Index of the local device in `devices.rdma_devices()`.
    local_device_index: usize,
    /// Remote device/port/GID to request in the `connect` RPC.
    remote: DeviceSelection,
    /// Remote per-connection resource limits advertised for that device.
    remote_limits: RdmaConnectionConfig,
    /// Preferred transport class, considered after hard constraints and
    /// reachability filters so lower classes remain valid fallbacks.
    class: PathClass,
    /// Full NIC-pair identity of this candidate.
    path: RdmaPathInfo,
}

impl PathCandidate {
    fn has_same_zone(&self) -> bool {
        !self.path.local.zones.is_empty()
            && self
                .path
                .local
                .zones
                .iter()
                .any(|zone| self.path.remote.zones.contains(zone))
    }
}

struct CachedRdmaInfo {
    info: RdmaInfo,
    cached_at: Instant,
}

/// Cheap pseudo-random draw; see [`RdmaSocketPool::pseudo_random`].
fn pseudo_random(seq: usize) -> u64 {
    use std::hash::BuildHasher as _;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    RandomState::default().hash_one((seq, nanos))
}

fn next_connection_id() -> u64 {
    static BASE: OnceLock<u64> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let base = *BASE.get_or_init(|| pseudo_random(0));
    loop {
        let id = base.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed));
        if id != 0 {
            return id;
        }
    }
}

#[cfg(test)]
mod path_selection_tests {
    use super::*;
    use crate::rdma::rdma_service::RdmaDeviceInfo;

    fn make_pool() -> RdmaSocketPool {
        let devices = crate::rdma::test_utils::make_rdma_devices();
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        RdmaSocketPool::new(devices, buffer_pool, RdmaSocketPoolConfig::default()).unwrap()
    }

    fn connection_limits() -> RdmaConnectionConfig {
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig::default(),
            cq_len: 128,
            recv_queue_len: 8,
            max_msg_size: 64 * 1024,
            traffic_class: 0,
        }
    }

    fn candidate(local_index: usize, remote_dev: &str) -> PathCandidate {
        PathCandidate {
            local_device_index: local_index,
            remote: DeviceSelection {
                device_name: remote_dev.into(),
                port_num: 1,
                gid_index: 0,
            },
            remote_limits: connection_limits(),
            class: PathClass::InfiniBand,
            path: RdmaPathInfo {
                local: RdmaNicInfo {
                    device: format!("local{local_index}"),
                    port_num: 1,
                    gid_index: 0,
                    ip: None,
                    zones: Vec::new(),
                },
                remote: RdmaNicInfo {
                    device: remote_dev.into(),
                    port_num: 1,
                    gid_index: 0,
                    ip: None,
                    zones: Vec::new(),
                },
            },
        }
    }

    fn remote_info(devices: &[(&str, u32)]) -> RdmaInfo {
        RdmaInfo {
            devices: devices
                .iter()
                .map(|(name, load)| RdmaDeviceInfo {
                    name: (*name).to_string(),
                    active_connections: *load,
                    connection: connection_limits(),
                    ports: Vec::new(),
                })
                .collect(),
        }
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:9999".parse().unwrap()
    }

    fn peer() -> PeerState {
        PeerState::new(addr())
    }

    fn preference<'a>(
        remote_device: Option<&'a str>,
        avoided_remote_nics: &'a HashSet<String>,
    ) -> PathPreference<'a> {
        PathPreference {
            remote_device,
            avoided_remote_nics,
        }
    }

    #[tokio::test]
    async fn test_select_prefers_less_loaded_remote() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 5), ("remoteB", 0)]);
        // With exactly two distinct remote NICs, P2C compares both every
        // time, so the choice is deterministic.
        for _ in 0..8 {
            let chosen = pool
                .select_candidate(
                    &peer(),
                    &candidates,
                    preference(None, &HashSet::new()),
                    &info,
                    &[],
                )
                .unwrap();
            assert_eq!(chosen.path.remote.device, "remoteB");
        }
    }

    #[tokio::test]
    async fn test_select_avoids_remote_nic_before_fallback() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 5), ("remoteB", 0)]);

        let avoided = HashSet::from(["remoteB".to_owned()]);
        let selected = pool
            .select_candidate(&peer(), &candidates, preference(None, &avoided), &info, &[])
            .unwrap();
        assert_eq!(selected.path.remote.device, "remoteA");

        let all_avoided = HashSet::from(["remoteA".to_owned(), "remoteB".to_owned()]);
        let selected = pool
            .select_candidate(
                &peer(),
                &candidates,
                preference(None, &all_avoided),
                &info,
                &[],
            )
            .unwrap();
        assert_eq!(selected.path.remote.device, "remoteB");
    }

    #[tokio::test]
    async fn test_select_respects_internal_remote_constraint() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 5), ("remoteB", 0)]);
        let selected = pool
            .select_candidate(
                &peer(),
                &candidates,
                preference(Some("remoteA"), &HashSet::new()),
                &info,
                &[],
            )
            .unwrap();
        assert_eq!(selected.path.remote.device, "remoteA");
    }

    #[tokio::test]
    async fn test_rejects_too_short_connect_lease() {
        let devices = crate::rdma::test_utils::make_rdma_devices();
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let config = RdmaSocketPoolConfig {
            connect_lease_ms: 14_999,
            ..Default::default()
        };
        let err = RdmaSocketPool::new(devices, buffer_pool, config).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn test_rejects_invalid_zone_names() {
        for zones in [
            vec![crate::RdmaZoneConfig {
                name: String::new(),
                cidrs: Vec::new(),
            }],
            vec![
                crate::RdmaZoneConfig {
                    name: "storage".into(),
                    cidrs: Vec::new(),
                },
                crate::RdmaZoneConfig {
                    name: "storage".into(),
                    cidrs: Vec::new(),
                },
            ],
        ] {
            let devices = crate::rdma::test_utils::make_rdma_devices();
            let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
            let config = RdmaSocketPoolConfig {
                zones,
                ..Default::default()
            };
            let err = RdmaSocketPool::new(devices, buffer_pool, config).unwrap_err();
            assert_eq!(err.kind, ErrorKind::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn test_accept_lease_transitions_are_state_checked() {
        let pool = make_pool();
        let connection_id = 42;
        let socket = Weak::new();
        pool.accept_leases.insert(
            connection_id,
            AcceptLease {
                socket: socket.clone(),
                server_connection_cookie: 7,
                state: AcceptLeaseState::Pending,
                expires_at: Instant::now() + Duration::from_secs(1),
            },
        );
        pool.observe_accept_receive(connection_id, &socket);
        assert!(pool.accept_leases.contains_key(&connection_id));
        assert_eq!(
            pool.accept_leases.get(&connection_id).unwrap().state,
            AcceptLeaseState::ReceiveObserved
        );

        pool.accept_leases.get_mut(&connection_id).unwrap().state = AcceptLeaseState::Confirmed;
        pool.observe_accept_receive(connection_id, &socket);
        assert_eq!(
            pool.accept_leases.get(&connection_id).unwrap().state,
            AcceptLeaseState::Active
        );
        pool.accept_leases.remove(&connection_id);

        pool.accept_leases.insert(
            connection_id,
            AcceptLease {
                socket: Weak::new(),
                server_connection_cookie: 7,
                state: AcceptLeaseState::Pending,
                expires_at: Instant::now() + Duration::from_secs(1),
            },
        );
        let mismatched = ConnectionControl {
            connection_id,
            server_connection_cookie: 8,
        };
        assert_eq!(
            pool.rdma_confirm(&mismatched).unwrap_err().kind,
            ErrorKind::InvalidArgument
        );
        pool.rdma_abort(&mismatched);
        assert!(pool.accept_leases.contains_key(&connection_id));
        pool.accept_leases.remove(&connection_id);

        pool.accept_leases.insert(
            connection_id,
            AcceptLease {
                socket: Weak::new(),
                server_connection_cookie: 7,
                state: AcceptLeaseState::Pending,
                expires_at: Instant::now() - Duration::from_millis(1),
            },
        );
        let control = ConnectionControl {
            connection_id,
            server_connection_cookie: 7,
        };
        let err = pool.rdma_confirm(&control).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
        assert!(!pool.accept_leases.contains_key(&connection_id));
    }

    #[test]
    fn test_accept_lease_events_commit_in_either_order() {
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Pending, AcceptLeaseEvent::Confirm),
            AcceptLeaseState::Confirmed
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Confirmed, AcceptLeaseEvent::Receive),
            AcceptLeaseState::Active
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Pending, AcceptLeaseEvent::Receive),
            AcceptLeaseState::ReceiveObserved
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::ReceiveObserved, AcceptLeaseEvent::Confirm),
            AcceptLeaseState::Active
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Confirmed, AcceptLeaseEvent::Confirm),
            AcceptLeaseState::Confirmed
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Active, AcceptLeaseEvent::Confirm),
            AcceptLeaseState::Active
        );
    }

    #[test]
    fn test_candidate_detects_shared_zone() {
        let mut candidate = candidate(0, "remoteA");
        assert!(!candidate.has_same_zone());

        candidate.path.local.zones = vec!["storage-a".into(), "storage-b".into()];
        candidate.path.remote.zones = vec!["storage-b".into()];
        assert!(candidate.has_same_zone());

        candidate.path.remote.zones = vec!["frontend".into()];
        assert!(!candidate.has_same_zone());
    }

    #[tokio::test]
    async fn test_same_zone_is_preferred() {
        let pool = make_pool();
        let mut same_subnet = candidate(0, "remoteA");
        same_subnet.path.local.zones = vec!["storage".into()];
        same_subnet.path.remote.zones = vec!["storage".into()];
        let other = candidate(0, "remoteB");
        let candidates = [same_subnet, other];
        let info = remote_info(&[("remoteA", 10), ("remoteB", 0)]);

        let selected = pool
            .select_candidate(
                &peer(),
                &candidates,
                preference(None, &HashSet::new()),
                &info,
                &[],
            )
            .unwrap();
        assert_eq!(selected.path.remote.device, "remoteA");
    }

    #[test]
    fn test_preconnect_backoff_is_bounded() {
        assert_eq!(preconnect_backoff_delay(1), Duration::from_millis(100));
        assert_eq!(preconnect_backoff_delay(4), Duration::from_millis(800));
        assert!(preconnect_backoff_delay(100) <= Duration::from_secs(30));
    }

    #[test]
    fn test_connection_ids_are_nonzero_and_unique() {
        let first = next_connection_id();
        let second = next_connection_id();
        assert_ne!(first, 0);
        assert_ne!(second, 0);
        assert_ne!(first, second);
    }

    /// The connecting client dictates the traffic class (its own config,
    /// not min'd with the remote advertisement); the accepting server
    /// applies the client's requested value verbatim.
    #[tokio::test]
    async fn test_traffic_class_client_decides_server_obeys() {
        let devices = crate::rdma::test_utils::make_rdma_devices();
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let config = RdmaSocketPoolConfig {
            traffic_class: 96,
            ..Default::default()
        };
        let pool = RdmaSocketPool::new(devices, buffer_pool, config).unwrap();
        let rdma_devices = pool.devices.rdma_devices();
        let device = &rdma_devices[0];

        // Client path: local config wins over the remote advertisement.
        let mut advertised = connection_limits();
        advertised.traffic_class = 7;
        let negotiated = pool.negotiate_connection_config(device, &advertised);
        assert_eq!(negotiated.traffic_class, 96);

        // Server path: the requested (client-chosen) value passes through.
        let mut requested = connection_limits();
        requested.traffic_class = 42;
        let clamped = pool.clamp_connection_config(device, requested);
        assert_eq!(clamped.traffic_class, 42);
    }

    /// Old peers omit `traffic_class` from the handshake payload; it must
    /// deserialize to 0.
    #[test]
    fn test_connection_config_traffic_class_serde_default() {
        let encoded = rmp_serde::to_vec_named(&serde_json::json!({
            "qp": {
                "max_send_wr": 64,
                "max_recv_wr": 64,
                "max_send_sge": 16,
                "max_recv_sge": 1,
            },
            "cq_len": 128,
            "recv_queue_len": 8,
            "max_msg_size": 65536,
        }))
        .unwrap();
        let config: RdmaConnectionConfig = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(config.traffic_class, 0);
    }

    #[tokio::test]
    async fn test_select_local_least_connections() {
        let pool = make_pool();
        // Simulate load on local device 0; device index 1 may not exist in
        // this environment, but placement only reads its (zero) counter.
        pool.conn_counts[0].fetch_add(2, Ordering::AcqRel);
        let candidates = [candidate(0, "remoteA"), candidate(1, "remoteA")];
        let info = remote_info(&[("remoteA", 0)]);
        let chosen = pool
            .select_candidate(
                &peer(),
                &candidates,
                preference(None, &HashSet::new()),
                &info,
                &[],
            )
            .unwrap();
        assert_eq!(chosen.local_device_index, 1);
    }

    #[tokio::test]
    async fn test_select_avoids_blacklisted_pair() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 0), ("remoteB", 0)]);

        let peer = peer();
        pool.blacklist_path(&peer, &candidates[0].path);
        for _ in 0..8 {
            let chosen = pool
                .select_candidate(
                    &peer,
                    &candidates,
                    preference(None, &HashSet::new()),
                    &info,
                    &[],
                )
                .unwrap();
            assert_eq!(chosen.path.remote.device, "remoteB");
        }
        // The blacklist is per peer: another address is unaffected.
        let other = PeerState::new("127.0.0.1:9998".parse().unwrap());
        assert!(!pool.is_blacklisted(&other, &candidates[0]));

        // Soft fallback: with every candidate blacklisted, selection still
        // returns one instead of failing.
        pool.blacklist_path(&peer, &candidates[1].path);
        let _ = pool
            .select_candidate(
                &peer,
                &candidates,
                preference(None, &HashSet::new()),
                &info,
                &[],
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_conn_count_guard_accounting() {
        let counts: Arc<Vec<AtomicUsize>> = Arc::new(vec![AtomicUsize::new(0)]);
        let a = ConnCountGuard::acquire(&counts, 0);
        let b = ConnCountGuard::acquire(&counts, 0);
        // Out-of-range indices are tolerated and count nothing.
        let c = ConnCountGuard::acquire(&counts, 7);
        assert_eq!(counts[0].load(Ordering::Acquire), 2);
        drop(a);
        assert_eq!(counts[0].load(Ordering::Acquire), 1);
        drop((b, c));
        assert_eq!(counts[0].load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_device_list_advertises_connection_counts() {
        let pool = make_pool();
        pool.conn_counts[0].fetch_add(3, Ordering::AcqRel);
        let info = pool.rdma_device_list().unwrap();
        assert_eq!(info.devices[0].active_connections, 3);
    }
}

#[cfg(test)]
mod gid_match_tests {
    use super::*;
    use std::collections::HashMap;

    fn make_gid(addr: &str) -> ruapc_rdma::ibv_gid {
        let bits = addr.parse::<std::net::Ipv6Addr>().unwrap().to_bits();
        let mut gid = ruapc_rdma::ibv_gid::default();
        gid.global.subnet_prefix = ((bits >> 64) as u64).to_be();
        gid.global.interface_id = (bits as u64).to_be();
        gid
    }

    fn gid(index: u8, addr: &str, gid_type: GidType) -> Gid {
        Gid {
            index,
            gid: make_gid(addr),
            gid_type,
        }
    }

    fn local_port(gids: Vec<Gid>) -> Port {
        Port {
            port_num: 1,
            port_attr: ruapc_rdma::ibv_port_attr::default(),
            gids,
        }
    }

    #[test]
    fn test_prefers_rocev2_over_rocev1() {
        let local = [
            gid(0, "fe80::1", GidType::RoCEv1),
            gid(3, "::ffff:10.0.0.1", GidType::RoCEv2),
        ];
        let remote = [
            gid(0, "fe80::2", GidType::RoCEv1),
            gid(5, "::ffff:10.0.0.2", GidType::RoCEv2),
        ];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &remote)
                .first()
                .copied(),
            Some((3, 5))
        );
    }

    #[test]
    fn test_retains_all_compatible_gid_pairs_for_zone_selection() {
        let local = [
            gid(1, "::ffff:10.0.0.1", GidType::RoCEv2),
            gid(2, "::ffff:10.1.0.1", GidType::RoCEv2),
        ];
        let remote = [
            gid(3, "::ffff:10.0.0.2", GidType::RoCEv2),
            gid(4, "::ffff:10.1.0.2", GidType::RoCEv2),
        ];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &remote),
            [(1, 3), (1, 4), (2, 3), (2, 4)]
        );
    }

    #[test]
    fn test_falls_back_to_rocev1_when_remote_lacks_rocev2() {
        let local = [
            gid(0, "fe80::1", GidType::RoCEv1),
            gid(3, "::ffff:10.0.0.1", GidType::RoCEv2),
        ];
        let remote = [gid(0, "fe80::2", GidType::RoCEv1)];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &remote)
                .first()
                .copied(),
            Some((0, 0))
        );
    }

    #[test]
    fn test_matches_same_gid_type_when_no_roce_pair() {
        let local = [gid(0, "fe80::1", GidType::Other("custom".into()))];
        let remote = [
            gid(0, "::ffff:10.0.0.2", GidType::RoCEv2),
            gid(2, "fe80::2", GidType::Other("custom".into())),
        ];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &remote)
                .first()
                .copied(),
            Some((0, 2))
        );
    }

    #[test]
    fn test_empty_remote_gid_table_returns_none() {
        let local = [gid(3, "::ffff:10.0.0.1", GidType::RoCEv2)];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &[])
                .first()
                .copied(),
            None
        );
    }

    #[test]
    fn test_match_gid_pair_ethernet() {
        let mut local = local_port(vec![gid(0, "::ffff:10.0.0.1", GidType::RoCEv2)]);
        local.port_attr.link_layer = LinkLayer::Ethernet;
        let remote = RdmaPortInfo {
            port_num: 1,
            link_layer: LinkLayer::Ethernet,
            gids: vec![gid(2, "::ffff:10.0.0.2", GidType::RoCEv2)],
            gid_zones: HashMap::new(),
        };
        assert_eq!(
            RdmaSocketPool::match_gid_pairs(&local, &remote)
                .first()
                .copied(),
            Some((0, 2))
        );
    }

    #[test]
    fn test_gid_index_is_rocev2() {
        let port = local_port(vec![
            gid(0, "fe80::1", GidType::RoCEv1),
            gid(3, "::ffff:10.0.0.1", GidType::RoCEv2),
        ]);
        assert!(RdmaSocketPool::gid_index_is_rocev2(&port, 3));
        assert!(!RdmaSocketPool::gid_index_is_rocev2(&port, 0));
        assert!(!RdmaSocketPool::gid_index_is_rocev2(&port, 7));
    }
}
