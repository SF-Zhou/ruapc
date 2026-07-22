use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use foldhash::fast::RandomState;
use ruapc_bufpool::Device as _;
use ruapc_rdma::{
    DeviceInfo, Gid, GidType, LinkLayer, Port, QueuePair, ibv_mtu, ibv_qp_cap, ibv_qp_init_attr,
    ibv_qp_type,
};
use tokio::sync::RwLock;
use tokio_util::sync::DropGuard;

use super::path::{
    RdmaConnDirection, RdmaDeviceLoad, RdmaNicInfo, RdmaPathEntry, RdmaPathInfo, RdmaPathReport,
    RdmaPathSelector, gid_ip,
};
use super::rdma_service::RdmaPortInfo;
use super::{
    ConnectRequest, DevicePollers, DeviceSelection, Endpoint, PollerConfig, RdmaConnectionConfig,
    RdmaDevice, RdmaDeviceRefresher, RdmaInfo, RdmaService, RdmaSocket, RegisterConn,
};
use crate::{
    Buffer, BufferPool, Client, Context, Devices, Error, ErrorKind, RdmaQueuePairConfig,
    RdmaSocketPoolConfig, Result, Socket, SocketPoolConfig, SocketPoolTrait, SocketType, State,
    TaskSupervisor,
};

/// One RDMA connection towards a peer, tagged with bookkeeping metadata.
///
/// Each stripe carries its own path (NIC pair): stripes of one peer are
/// placed independently, spreading load across the NICs of both sides.
#[derive(Clone)]
pub(crate) struct Stripe {
    pub(crate) socket: Arc<RdmaSocket>,
    /// Created for an explicit path selector; exempt from replenishment
    /// accounting and rebalancing.
    pub(crate) pinned: bool,
}

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
    pub(crate) socket_map: RwLock<HashMap<SocketAddr, Vec<Stripe>, RandomState>>,
    /// Live connection count per local RDMA device (outbound + inbound),
    /// indexed like `devices.rdma_devices()`. Drives least-connections
    /// placement and is advertised to peers via `RdmaInfo`.
    conn_counts: Arc<Vec<AtomicUsize>>,
    /// Inbound (accepted) connections, for path reporting and the
    /// port-down watchdog; dead entries are pruned opportunistically.
    inbound: std::sync::Mutex<Vec<Weak<RdmaSocket>>>,
    /// NIC pairs whose QP setup towards a peer recently failed (e.g. no
    /// route between the two subnets), with their retry deadline. Device
    /// matching cannot verify reachability, so failed pairs are penalized
    /// and placement falls over to the remaining candidates.
    path_blacklist: std::sync::Mutex<HashMap<(SocketAddr, String, String), Instant, RandomState>>,
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
    /// Round-robin cursor selecting the rebalance target peer.
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
            &self.conn_counts,
        ))
    }

    /// Establishes a new RDMA connection with the specified endpoint.
    fn rdma_accept(&self, request: &ConnectRequest, state: &Arc<State>) -> Result<Endpoint> {
        let (device_index, device) = self.find_device_by_name(&request.target)?;
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

        let info = device.info();
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
            },
            remote: RdmaNicInfo {
                device: request.source_device.clone(),
                port_num: request.endpoint.port_num,
                gid_index: request.endpoint.gid_index,
                ip: gid_ip(&request.endpoint.gid),
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
        {
            let mut inbound = self.inbound.lock().unwrap();
            inbound.retain(|conn| conn.strong_count() > 0);
            inbound.push(Arc::downgrade(&socket));
        }
        self.ensure_maintenance_task(state);
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
        self.acquire_path(addr, None, state).await
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
    /// How long a NIC pair stays penalized after its QP setup failed.
    const PATH_BLACKLIST_TTL: Duration = Duration::from_secs(30);

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
            conn_counts: Arc::new(
                (0..devices.rdma_devices().len())
                    .map(|_| AtomicUsize::new(0))
                    .collect(),
            ),
            inbound: std::sync::Mutex::new(Vec::new()),
            path_blacklist: std::sync::Mutex::new(HashMap::default()),
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

    /// Acquires a healthy connection to `addr`, optionally constrained to
    /// paths matching `selector`; establishes one when none exists.
    pub(crate) async fn acquire_path(
        &self,
        addr: &SocketAddr,
        selector: Option<&RdmaPathSelector>,
        state: &Arc<State>,
    ) -> Result<Socket> {
        // Check if a matching socket is already in the socket map.
        if let Ok(socket_map) = self.socket_map.try_read()
            && let Some(socket) = socket_map
                .get(addr)
                .and_then(|s| self.pick_stripe(s, selector))
        {
            return Ok(socket);
        }

        let connect_lock = self.peer_lock(addr);
        let guard = connect_lock.lock().await;
        let result = self.handshake(addr, state, selector).await;
        self.release_peer_lock(addr, &connect_lock);
        drop(guard);
        result
    }

    /// Returns (creating if necessary) the per-peer connect lock.
    fn peer_lock(&self, addr: &SocketAddr) -> Arc<tokio::sync::Mutex<()>> {
        self.connect_locks
            .entry(*addr)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Drops the per-peer connect lock entry once no other task holds it.
    fn release_peer_lock(&self, addr: &SocketAddr, lock: &Arc<tokio::sync::Mutex<()>>) {
        self.connect_locks.remove_if(addr, |_, value| {
            Arc::ptr_eq(value, lock) && Arc::strong_count(value) == 2
        });
    }

    /// Picks one healthy connection stripe round-robin, skipping stripes
    /// that entered the error state or do not match `selector`.
    fn pick_stripe(
        &self,
        stripes: &[Stripe],
        selector: Option<&RdmaPathSelector>,
    ) -> Option<Socket> {
        if stripes.is_empty() {
            return None;
        }
        let start = self.next_stripe.fetch_add(1, Ordering::Relaxed);
        (0..stripes.len())
            .map(|i| &stripes[(start + i) % stripes.len()])
            .find(|s| {
                s.socket.state.is_ok() && selector.is_none_or(|sel| sel.matches(&s.socket.path))
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
    fn blacklist_path(&self, addr: &SocketAddr, path: &RdmaPathInfo) {
        self.path_blacklist.lock().unwrap().insert(
            (*addr, path.local.device.clone(), path.remote.device.clone()),
            Instant::now() + Self::PATH_BLACKLIST_TTL,
        );
    }

    /// Whether the candidate's NIC pair towards `addr` is currently
    /// penalized; expired entries are pruned on the way.
    fn is_blacklisted(&self, addr: &SocketAddr, candidate: &PathCandidate) -> bool {
        let mut blacklist = self.path_blacklist.lock().unwrap();
        let now = Instant::now();
        blacklist.retain(|_, until| *until > now);
        blacklist.contains_key(&(
            *addr,
            candidate.path.local.device.clone(),
            candidate.path.remote.device.clone(),
        ))
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
        let socket = Arc::new(RdmaSocket::new(
            queue_pair,
            self.buffer_pool.clone(),
            tx,
            poller.waker(),
            config.max_msg_size as usize,
            send_window,
            path,
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
        addr: &SocketAddr,
        state: &Arc<State>,
        selector: Option<&RdmaPathSelector>,
    ) -> Result<Socket> {
        self.ensure_maintenance_task(state);

        // Re-check under the connect lock: another task may have connected,
        // or every stripe may have failed and must be replaced. `add_one`
        // covers the selector case: the peer may already have healthy
        // stripes, just none on the requested path.
        let mut add_one = selector.is_some();
        {
            let mut socket_map = self.socket_map.write().await;
            if let Some(stripes) = socket_map.get(addr) {
                if let Some(socket) = self.pick_stripe(stripes, selector) {
                    return Ok(socket);
                }
                if !stripes.iter().any(|s| s.socket.state.is_ok()) {
                    // All stripes are dead: drop them and reconnect. The
                    // poll thread reclaims the connections independently.
                    tracing::info!("all RDMA stripes to {addr} failed, reconnecting");
                    socket_map.remove(addr);
                    add_one = selector.is_some();
                } else {
                    debug_assert!(selector.is_some());
                    add_one = true;
                }
            }
        }

        let plan = self.prepare_connect_plan(addr, state).await?;

        if add_one {
            // Append a single (possibly pinned) stripe on the requested
            // path to whatever the peer already has.
            let existing = self
                .socket_map
                .read()
                .await
                .get(addr)
                .cloned()
                .unwrap_or_default();
            let socket = self
                .connect_with_failover(addr, state, &plan, selector, &existing)
                .await?;
            let result = Socket::from(&socket);
            self.socket_map
                .write()
                .await
                .entry(*addr)
                .or_default()
                .push(Stripe {
                    socket,
                    pinned: selector.is_some(),
                });
            return Ok(result);
        }

        // Establish `connections_per_peer` stripes towards this peer;
        // requests are spread round-robin across them (and, with poll
        // thread shards, across cores). Each stripe picks its own path:
        // local side by least connections, remote side by
        // power-of-two-choices over the peer's advertised per-NIC load.
        let stripe_count = self.config.connections_per_peer.max(1);
        let mut stripes: Vec<Stripe> = Vec::with_capacity(stripe_count as usize);
        for _ in 0..stripe_count {
            match self
                .connect_with_failover(addr, state, &plan, None, &stripes)
                .await
            {
                Ok(socket) => stripes.push(Stripe {
                    socket,
                    pinned: false,
                }),
                Err(err) => {
                    // Don't leak the stripes established so far: mark them
                    // failed so the poll thread tears them down.
                    for stripe in &stripes {
                        stripe.socket.set_error();
                    }
                    return Err(err);
                }
            }
        }

        let socket = self
            .pick_stripe(&stripes, None)
            .expect("stripe count is at least 1");
        self.socket_map.write().await.insert(*addr, stripes);
        Ok(socket)
    }

    /// Fetches the peer's device list and enumerates the compatible path
    /// candidates: everything a placement decision towards `addr` needs.
    async fn prepare_connect_plan(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
    ) -> Result<ConnectPlan> {
        let acquire_ctx = Context::create_with_state_and_addr(state, addr);
        let remote_info = self.fetch_remote_device_list(addr, &acquire_ctx).await?;
        let candidates = match self.enumerate_path_candidates(&remote_info) {
            Ok(candidates) => candidates,
            Err(err) => {
                self.invalidate_device_list_cache(addr).await;
                return Err(err);
            }
        };
        Ok(ConnectPlan {
            acquire_ctx,
            remote_info,
            candidates,
        })
    }

    /// Establishes one stripe towards `addr`, falling over to the next
    /// best candidate when a NIC pair turns out to be unreachable
    /// (device matching cannot verify routability). Each failed pair is
    /// blacklisted by `connect_stripe`, so later placements avoid it too.
    async fn connect_with_failover(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
        plan: &ConnectPlan,
        selector: Option<&RdmaPathSelector>,
        existing: &[Stripe],
    ) -> Result<Arc<RdmaSocket>> {
        let mut remaining: Vec<PathCandidate> = plan.candidates.clone();
        loop {
            let candidate =
                self.select_candidate(addr, &remaining, selector, &plan.remote_info, existing)?;
            match self
                .connect_stripe(addr, state, &plan.acquire_ctx, &candidate)
                .await
            {
                Ok(socket) => return Ok(socket),
                Err(err) => {
                    remaining.retain(|c| {
                        c.path.local.device != candidate.path.local.device
                            || c.path.remote.device != candidate.path.remote.device
                    });
                    if remaining.is_empty() {
                        return Err(err);
                    }
                    tracing::warn!(
                        peer = %addr,
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
        addr: &SocketAddr,
        state: &Arc<State>,
        acquire_ctx: &Context,
        candidate: &PathCandidate,
    ) -> Result<Arc<RdmaSocket>> {
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

        let connect_request = ConnectRequest {
            endpoint: local_endpoint,
            source_device: candidate.path.local.device.clone(),
            target: candidate.remote.clone(),
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
        if let Err(err) = self.bring_qp_to_rts(
            &queue_pair,
            &local_endpoint,
            &remote_endpoint,
            self.config.pkey_index,
        ) {
            // QP setup failures are typically path problems (no route
            // between the selected NIC pair): penalize the pair so
            // placement falls over to other candidates.
            self.blacklist_path(addr, &candidate.path);
            return Err(err);
        }

        let socket = self.register_socket(
            queue_pair,
            state,
            &poller,
            &connection_config,
            candidate.path.clone(),
            candidate.local_device_index,
        )?;
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

    /// Enumerates every compatible (local NIC, remote NIC) pair.
    ///
    /// One candidate is produced per compatible port pair (the GID within
    /// a port pair is chosen by the existing preference logic). Candidates
    /// are pre-filtered to the best available class: InfiniBand matches
    /// win over RoCE v2 matches, which win over other RoCE matches.
    fn enumerate_path_candidates(&self, remote_info: &RdmaInfo) -> Result<Vec<PathCandidate>> {
        let local_devices = self.devices.rdma_devices();
        if local_devices.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "no local RDMA device available".into(),
            ));
        }

        let remote_filter = &self.config.remote_device_filter;
        let mut ib_matches = Vec::new();
        let mut roce_v2_matches = Vec::new();
        let mut roce_other_matches = Vec::new();
        let mut remote_ports = 0usize;
        let mut local_usable_ports = 0usize;
        let mut link_layer_matches = 0usize;

        // Remote ports are pre-filtered: peers only advertise usable ports.
        for remote_device in &remote_info.devices {
            if !remote_filter.is_empty() && !remote_filter.contains(&remote_device.name) {
                continue;
            }
            for remote_port in &remote_device.ports {
                remote_ports += 1;

                for (local_device_index, local_device) in local_devices.iter().enumerate() {
                    let local_info = local_device.info();
                    for local_port in &local_info.ports {
                        if !local_port.is_usable() {
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

                        let local_ip = local_port
                            .find_gid(local_gid_index)
                            .and_then(|gid| gid_ip(&gid.gid));
                        let remote_ip = remote_port
                            .gids
                            .iter()
                            .find(|gid| gid.index == remote_gid_index)
                            .and_then(|gid| gid_ip(&gid.gid));
                        let selected = PathCandidate {
                            local_device_index,
                            remote: DeviceSelection {
                                device_name: remote_device.name.clone(),
                                port_num: remote_port.port_num,
                                gid_index: remote_gid_index,
                            },
                            remote_limits: remote_device.connection,
                            path: RdmaPathInfo {
                                local: RdmaNicInfo {
                                    device: local_info.name.clone(),
                                    port_num: local_port.port_num,
                                    gid_index: local_gid_index,
                                    ip: local_ip,
                                },
                                remote: RdmaNicInfo {
                                    device: remote_device.name.clone(),
                                    port_num: remote_port.port_num,
                                    gid_index: remote_gid_index,
                                    ip: remote_ip,
                                },
                            },
                        };

                        match local_port.port_attr.link_layer {
                            LinkLayer::InfiniBand => ib_matches.push(selected),
                            LinkLayer::Ethernet => {
                                // Prefer RoCE v2 matches whenever one exists.
                                if Self::gid_index_is_rocev2(local_port, local_gid_index) {
                                    roce_v2_matches.push(selected);
                                } else {
                                    roce_other_matches.push(selected);
                                }
                            }
                            LinkLayer::Unspecified => {}
                        }
                    }
                }
            }
        }

        let matches = if !ib_matches.is_empty() {
            ib_matches
        } else if !roce_v2_matches.is_empty() {
            roce_v2_matches
        } else {
            roce_other_matches
        };
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
        addr: &SocketAddr,
        candidates: &[PathCandidate],
        selector: Option<&RdmaPathSelector>,
        remote_info: &RdmaInfo,
        peer_stripes: &[Stripe],
    ) -> Result<PathCandidate> {
        let mut filtered: Vec<&PathCandidate> = candidates
            .iter()
            .filter(|c| selector.is_none_or(|sel| sel.matches(&c.path)))
            .collect();
        if filtered.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("no compatible RDMA path matches selector {selector:?}"),
            ));
        }
        // Soft blacklist: skip recently failed pairs, but when every
        // remaining candidate is penalized, try them anyway — the fault
        // may have cleared, and failing fast helps nobody.
        let usable: Vec<&PathCandidate> = filtered
            .iter()
            .copied()
            .filter(|c| !self.is_blacklisted(addr, c))
            .collect();
        if !usable.is_empty() {
            filtered = usable;
        }

        let remote_score = |name: &str| -> u64 {
            let advertised = remote_info
                .devices
                .iter()
                .find(|d| d.name == name)
                .map_or(0, |d| u64::from(d.active_connections));
            let ours = peer_stripes
                .iter()
                .filter(|s| s.socket.state.is_ok() && s.socket.path.remote.device == name)
                .count() as u64;
            advertised + ours
        };

        let mut remote_names: Vec<&str> = Vec::new();
        for candidate in &filtered {
            if !remote_names.contains(&candidate.path.remote.device.as_str()) {
                remote_names.push(candidate.path.remote.device.as_str());
            }
        }
        let chosen_remote = if remote_names.len() == 1 {
            remote_names[0]
        } else {
            let n = remote_names.len();
            let a = self.pseudo_random() as usize % n;
            let mut b = self.pseudo_random() as usize % (n - 1);
            if b >= a {
                b += 1;
            }
            if remote_score(remote_names[b]) < remote_score(remote_names[a]) {
                remote_names[b]
            } else {
                remote_names[a]
            }
        };

        let local_count = |c: &PathCandidate| -> usize {
            self.conn_counts
                .get(c.local_device_index)
                .map_or(0, |count| count.load(Ordering::Acquire))
        };
        Ok(filtered
            .iter()
            .filter(|c| c.path.remote.device == chosen_remote)
            .min_by_key(|c| local_count(c))
            .copied()
            .expect("chosen remote device came from the filtered candidates")
            .clone())
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
    fn match_gid_pair(local_port: &Port, remote_port: &RdmaPortInfo) -> Option<(u8, u8)> {
        let (local_gids, remote_gids) = (&local_port.gids[..], &remote_port.gids[..]);
        match local_port.port_attr.link_layer {
            LinkLayer::InfiniBand => Some((
                Self::first_gid(local_gids, |_| true).unwrap_or(0),
                Self::first_gid(remote_gids, |_| true).unwrap_or(0),
            )),
            LinkLayer::Ethernet => Self::match_roce_gid_pair(local_gids, remote_gids),
            LinkLayer::Unspecified => None,
        }
    }

    fn match_roce_gid_pair(local_gids: &[Gid], remote_gids: &[Gid]) -> Option<(u8, u8)> {
        // Prefer RoCE v2 pairs, then RoCE v1 pairs.
        for wanted in [GidType::RoCEv2, GidType::RoCEv1] {
            let local = Self::first_gid(local_gids, |gid| gid.gid_type == wanted);
            let remote = Self::first_gid(remote_gids, |gid| gid.gid_type == wanted);
            if let (Some(local), Some(remote)) = (local, remote) {
                return Some((local, remote));
            }
        }

        // Then any pair with matching GID types.
        for local_gid in local_gids {
            let remote = Self::first_gid(remote_gids, |remote_gid| {
                remote_gid.gid_type == local_gid.gid_type
            });
            if let Some(remote) = remote {
                return Some((local_gid.index, remote));
            }
        }

        // Finally, fall back to the first GID on each side.
        Some((
            Self::first_gid(local_gids, |_| true)?,
            Self::first_gid(remote_gids, |_| true)?,
        ))
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

    /// One maintenance tick: fail connections on dead local ports, prune
    /// dead stripes, replenish peers below `connections_per_peer`, and
    /// rebalance at most one connection of one peer (rate-limited by the
    /// tick interval, so recovered NICs regain traffic gradually).
    pub(crate) async fn run_maintenance(&self, state: &Arc<State>) {
        self.fail_paths_on_dead_ports().await;
        self.prune_dead().await;
        let peers: Vec<SocketAddr> = self.socket_map.read().await.keys().copied().collect();
        if peers.is_empty() {
            return;
        }
        for addr in &peers {
            self.replenish_peer(addr, state).await;
        }
        let target = peers[self.rebalance_cursor.fetch_add(1, Ordering::Relaxed) % peers.len()];
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
        {
            let socket_map = self.socket_map.read().await;
            for stripes in socket_map.values() {
                for stripe in stripes {
                    fail_if_dead(&stripe.socket);
                }
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
        let mut socket_map = self.socket_map.write().await;
        socket_map.retain(|_, stripes| {
            stripes.retain(|s| s.socket.state.is_ok());
            !stripes.is_empty()
        });
        drop(socket_map);
        self.inbound
            .lock()
            .unwrap()
            .retain(|conn| conn.strong_count() > 0);
    }

    /// Tops a peer with surviving stripes back up to
    /// `connections_per_peer` unpinned stripes (each independently
    /// placed, so replacements land on the currently least-loaded NICs).
    /// Fully dead peers are left to the on-demand reconnect in `acquire`.
    async fn replenish_peer(&self, addr: &SocketAddr, state: &Arc<State>) {
        let target = self.config.connections_per_peer.max(1) as usize;
        let needed = {
            let socket_map = self.socket_map.read().await;
            let Some(stripes) = socket_map.get(addr) else {
                return;
            };
            if !stripes.iter().any(|s| s.socket.state.is_ok()) {
                return;
            }
            let healthy_unpinned = stripes
                .iter()
                .filter(|s| s.socket.state.is_ok() && !s.pinned)
                .count();
            target.saturating_sub(healthy_unpinned)
        };
        if needed == 0 {
            return;
        }

        let lock = self.peer_lock(addr);
        let Ok(guard) = lock.try_lock() else {
            // An acquire is already connecting to this peer; retry next tick.
            self.release_peer_lock(addr, &lock);
            return;
        };
        let result: Result<()> = async {
            let plan = self.prepare_connect_plan(addr, state).await?;
            for _ in 0..needed {
                let existing = self
                    .socket_map
                    .read()
                    .await
                    .get(addr)
                    .cloned()
                    .unwrap_or_default();
                let socket = self
                    .connect_with_failover(addr, state, &plan, None, &existing)
                    .await?;
                self.socket_map
                    .write()
                    .await
                    .entry(*addr)
                    .or_default()
                    .push(Stripe {
                        socket,
                        pinned: false,
                    });
            }
            Ok(())
        }
        .await;
        if let Err(err) = result {
            tracing::debug!("replenishing RDMA stripes to {addr} failed: {err}");
        }
        drop(guard);
        self.release_peer_lock(addr, &lock);
    }

    /// Migrates at most one connection of `addr` onto a strictly less
    /// loaded path (make-before-break: the replacement is established and
    /// enters the rotation before the victim is drained and closed).
    ///
    /// Scores are connection counts with the victim's own contribution
    /// excluded on both sides — i.e. the world as it would look with the
    /// victim removed: `stay = victim's pair load`, `move = candidate
    /// pair load`. Migration requires `move + rebalance_threshold <=
    /// stay`; the victim's own pair scores exactly `stay`, so a balanced
    /// pool never oscillates and the threshold adds hysteresis on top.
    async fn rebalance_peer(&self, addr: &SocketAddr, state: &Arc<State>) {
        let threshold = u64::from(self.config.rebalance_threshold.max(1));
        let stripes: Vec<Stripe> = {
            let socket_map = self.socket_map.read().await;
            match socket_map.get(addr) {
                Some(stripes) => stripes
                    .iter()
                    .filter(|s| s.socket.state.is_ok() && !s.pinned)
                    .cloned()
                    .collect(),
                None => return,
            }
        };
        if stripes.is_empty() {
            return;
        }

        let Ok(plan) = self.prepare_connect_plan(addr, state).await else {
            return;
        };
        let remote_info = &plan.remote_info;
        // Never migrate onto a recently failed pair; unlike placement
        // (which may have no alternative), skipping a rebalance is free.
        let candidates: Vec<PathCandidate> = plan
            .candidates
            .iter()
            .filter(|c| !self.is_blacklisted(addr, c))
            .cloned()
            .collect();
        if candidates.is_empty() {
            return;
        }

        // A NIC that is no longer advertised/present scores prohibitively
        // high, so its connections migrate away as soon as any healthy
        // alternative exists.
        const GONE: u64 = u64::MAX / 4;
        let remote_count = |name: &str| -> u64 {
            remote_info
                .devices
                .iter()
                .find(|d| d.name == name)
                .map_or(GONE, |d| u64::from(d.active_connections))
        };
        let local_count_by_index = |index: usize| -> u64 {
            self.conn_counts
                .get(index)
                .map_or(0, |c| c.load(Ordering::Acquire) as u64)
        };
        let local_count = |name: &str| -> u64 {
            self.devices
                .rdma_devices()
                .iter()
                .enumerate()
                .find(|(_, d)| d.info().name == name)
                .map_or(GONE, |(index, _)| local_count_by_index(index))
        };

        // A stripe's "stay" score: the load on its pair with itself
        // removed from both sides.
        let stripe_score = |s: &Stripe| -> u64 {
            local_count(&s.socket.path.local.device).saturating_sub(1)
                + remote_count(&s.socket.path.remote.device).saturating_sub(1)
        };
        let Some(victim) = stripes.iter().max_by_key(|s| stripe_score(s)) else {
            return;
        };
        let victim_score = stripe_score(victim);
        // A candidate's "move" score: the load the victim would join,
        // likewise with the victim's own contribution excluded (it still
        // occupies its current pair while we measure).
        let candidate_score = |c: &PathCandidate| -> u64 {
            let mut local = local_count_by_index(c.local_device_index);
            if c.path.local.device == victim.socket.path.local.device {
                local = local.saturating_sub(1);
            }
            let mut remote = remote_count(&c.path.remote.device);
            if c.path.remote.device == victim.socket.path.remote.device {
                remote = remote.saturating_sub(1);
            }
            local + remote
        };
        let Some((best_score, best)) = candidates
            .iter()
            .map(|c| (candidate_score(c), c))
            .min_by_key(|(score, _)| *score)
        else {
            return;
        };
        if best_score.saturating_add(threshold) > victim_score {
            return;
        }
        // Herd damping: many clients may decide to move against the same
        // stale load snapshot in the same tick; a coin flip (on top of the
        // jittered tick) halves the worst-case simultaneous moves, and the
        // threshold hysteresis absorbs the overshoot that remains.
        if self.pseudo_random().is_multiple_of(2) {
            return;
        }

        let lock = self.peer_lock(addr);
        let Ok(guard) = lock.try_lock() else {
            self.release_peer_lock(addr, &lock);
            return;
        };
        match self
            .connect_stripe(addr, state, &plan.acquire_ctx, best)
            .await
        {
            Ok(socket) => {
                let mut socket_map = self.socket_map.write().await;
                if let Some(stripes) = socket_map.get_mut(addr) {
                    stripes.push(Stripe {
                        socket: socket.clone(),
                        pinned: false,
                    });
                    if let Some(pos) = stripes
                        .iter()
                        .position(|s| Arc::ptr_eq(&s.socket, &victim.socket))
                    {
                        stripes.remove(pos);
                        drop(socket_map);
                        tracing::info!(
                            peer = %addr,
                            from_local = %victim.socket.path.local.device,
                            from_remote = %victim.socket.path.remote.device,
                            to_local = %socket.path.local.device,
                            to_remote = %socket.path.remote.device,
                            "rebalancing RDMA connection"
                        );
                        // The victim already left the rotation; give its
                        // in-flight responses time to arrive, then close.
                        self.drain_then_close(victim.socket.clone());
                    }
                } else {
                    socket.set_error();
                }
            }
            Err(err) => tracing::debug!("rebalance connect to {addr} failed: {err}"),
        }
        drop(guard);
        self.release_peer_lock(addr, &lock);
    }

    /// Closes `socket` after the drain timeout; used for stripes removed
    /// from the rotation whose in-flight responses must still arrive.
    fn drain_then_close(&self, socket: Arc<RdmaSocket>) {
        let drain = Duration::from_millis(self.config.drain_timeout_ms);
        let guard = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            tokio::select! {
                () = guard.stopped() => {}
                () = tokio::time::sleep(drain) => {}
            }
            socket.set_error();
        });
    }

    /// Snapshot of every live connection with its NIC pair, plus
    /// per-device connection counts.
    pub(crate) async fn path_report(&self) -> RdmaPathReport {
        let devices = self
            .devices
            .rdma_devices()
            .iter()
            .enumerate()
            .map(|(index, device)| RdmaDeviceLoad {
                device: device.info().name.clone(),
                connections: self
                    .conn_counts
                    .get(index)
                    .map_or(0, |c| c.load(Ordering::Acquire)),
            })
            .collect();

        let mut paths = Vec::new();
        {
            let socket_map = self.socket_map.read().await;
            for (addr, stripes) in socket_map.iter() {
                for stripe in stripes {
                    paths.push(RdmaPathEntry {
                        peer: Some(*addr),
                        direction: RdmaConnDirection::Outbound,
                        path: stripe.socket.path.clone(),
                        qp_num: stripe.socket.queue_pair.qp_num(),
                        healthy: stripe.socket.state.is_ok(),
                        pinned: stripe.pinned,
                    });
                }
            }
        }
        let inbound: Vec<Arc<RdmaSocket>> = self
            .inbound
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for socket in inbound {
            paths.push(RdmaPathEntry {
                peer: None,
                direction: RdmaConnDirection::Inbound,
                path: socket.path.clone(),
                qp_num: socket.queue_pair.qp_num(),
                healthy: socket.state.is_ok(),
                pinned: false,
            });
        }
        RdmaPathReport { devices, paths }
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

/// Everything a placement decision towards one peer needs: the bootstrap
/// context, the peer's advertised device list and the compatible path
/// candidates derived from it.
struct ConnectPlan {
    acquire_ctx: Context,
    remote_info: RdmaInfo,
    candidates: Vec<PathCandidate>,
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
    /// Full NIC-pair identity of this candidate.
    path: RdmaPathInfo,
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
            path: RdmaPathInfo {
                local: RdmaNicInfo {
                    device: format!("local{local_index}"),
                    port_num: 1,
                    gid_index: 0,
                    ip: None,
                },
                remote: RdmaNicInfo {
                    device: remote_dev.into(),
                    port_num: 1,
                    gid_index: 0,
                    ip: None,
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

    #[tokio::test]
    async fn test_select_prefers_less_loaded_remote() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 5), ("remoteB", 0)]);
        // With exactly two distinct remote NICs, P2C compares both every
        // time, so the choice is deterministic.
        for _ in 0..8 {
            let chosen = pool
                .select_candidate(&addr(), &candidates, None, &info, &[])
                .unwrap();
            assert_eq!(chosen.path.remote.device, "remoteB");
        }
    }

    #[tokio::test]
    async fn test_select_respects_selector() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 5), ("remoteB", 0)]);

        let selector = RdmaPathSelector::remote_device("remoteA");
        let chosen = pool
            .select_candidate(&addr(), &candidates, Some(&selector), &info, &[])
            .unwrap();
        assert_eq!(chosen.path.remote.device, "remoteA");

        let missing = RdmaPathSelector::remote_device("nonexistent");
        let err = pool
            .select_candidate(&addr(), &candidates, Some(&missing), &info, &[])
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
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
            .select_candidate(&addr(), &candidates, None, &info, &[])
            .unwrap();
        assert_eq!(chosen.local_device_index, 1);
    }

    #[tokio::test]
    async fn test_select_avoids_blacklisted_pair() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 0), ("remoteB", 0)]);

        pool.blacklist_path(&addr(), &candidates[0].path);
        for _ in 0..8 {
            let chosen = pool
                .select_candidate(&addr(), &candidates, None, &info, &[])
                .unwrap();
            assert_eq!(chosen.path.remote.device, "remoteB");
        }
        // The blacklist is per peer: another address is unaffected.
        let other: SocketAddr = "127.0.0.1:9998".parse().unwrap();
        assert!(!pool.is_blacklisted(&other, &candidates[0]));

        // Soft fallback: with every candidate blacklisted, selection still
        // returns one instead of failing.
        pool.blacklist_path(&addr(), &candidates[1].path);
        assert!(
            pool.select_candidate(&addr(), &candidates, None, &info, &[])
                .is_ok()
        );
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
            RdmaSocketPool::match_roce_gid_pair(&local, &remote),
            Some((3, 5))
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
            RdmaSocketPool::match_roce_gid_pair(&local, &remote),
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
            RdmaSocketPool::match_roce_gid_pair(&local, &remote),
            Some((0, 2))
        );
    }

    #[test]
    fn test_empty_remote_gid_table_returns_none() {
        let local = [gid(3, "::ffff:10.0.0.1", GidType::RoCEv2)];
        assert_eq!(RdmaSocketPool::match_roce_gid_pair(&local, &[]), None);
    }

    #[test]
    fn test_match_gid_pair_ethernet() {
        let mut local = local_port(vec![gid(0, "::ffff:10.0.0.1", GidType::RoCEv2)]);
        local.port_attr.link_layer = LinkLayer::Ethernet;
        let remote = RdmaPortInfo {
            port_num: 1,
            link_layer: LinkLayer::Ethernet,
            gids: vec![gid(2, "::ffff:10.0.0.2", GidType::RoCEv2)],
        };
        assert_eq!(
            RdmaSocketPool::match_gid_pair(&local, &remote),
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
