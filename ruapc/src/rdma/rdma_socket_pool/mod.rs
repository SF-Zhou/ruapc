use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    sync::{Arc, OnceLock, Weak},
    time::{Duration, Instant},
};

use foldhash::fast::RandomState;
use tokio_util::sync::DropGuard;

use super::path::RdmaPathInfo;
use super::{DevicePollers, PollerConfig, RdmaDeviceRefresher, RdmaSocket, RdmaSocketPoolConfig};
use crate::{
    BufferPool, Client, Devices, Endpoint, Error, ErrorKind, RawStream, Result, Socket,
    SocketPoolConfig, SocketPoolTrait, State, TaskSupervisor,
};

mod peer;
pub(crate) use peer::PeerState;
mod accept;
use accept::AcceptLease;
mod connect;
mod handshake;
mod placement;
use placement::PathCandidate;
mod maintenance;
mod probe;
mod reconcile;
mod report;
mod setup;

type PathKey = (String, u8, u8, String, u8, u8);
type PeerMap = dashmap::DashMap<Endpoint, Arc<PeerState>, RandomState>;

/// One RDMA connection towards a peer, tagged with bookkeeping metadata.
///
/// Each stripe carries its own path (NIC pair): stripes of one peer are
/// placed independently, spreading load across the NICs of both sides.
#[derive(Clone)]
pub(crate) struct Stripe {
    pub(crate) socket: Arc<RdmaSocket>,
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
///   2. Destroy QPs (`peers`) — the shared CQs are destroyed via their
///      `Arc` chain once the last QP referencing them is gone
///   3. Deregister MRs (`buffer_pool`)
///   4. Destroy PD / close device context (`devices`)
///
/// Rust drops struct fields in **declaration order**, so the fields below
/// are intentionally ordered to satisfy the ibverbs requirement.
pub struct RdmaSocketPool {
    /// Client for bootstrap control RPCs over the endpoint's declared
    /// bootstrap transport (no RDMA resources).
    pub bootstrap_client: Client,
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
    /// indexed like `devices.devices()`. Drives least-connections
    /// placement and is advertised to peers via `RdmaPeerAdvertisement`.
    conn_counts: Arc<Vec<AtomicUsize>>,
    /// Per local RDMA device budget of in-flight RDMA READ work requests
    /// (`rdma.remote_memory.max_inflight_read_wrs`), indexed like
    /// `devices.devices()` and shared by every connection on the
    /// device — the congestion control for read traffic (server-side
    /// `remote_read` and client-side `read_into_target` alike).
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
    /// Whether the independent connection health probe task has started.
    probe_started: AtomicBool,
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

    /// Acquires or creates an RDMA socket for the complete bootstrap address group.
    async fn acquire(&self, endpoint: &Endpoint, state: &Arc<State>) -> Result<Socket> {
        self.acquire_with_options(endpoint, &HashSet::new(), None, state)
            .await
    }

    async fn handle_new_stream(
        &self,
        _state: &Arc<State>,
        _stream: RawStream,
        _addr: SocketAddr,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::InvalidArgument,
            "invalid socket type".into(),
        ))
    }
}

impl RdmaSocketPool {
    /// How long a NIC pair stays penalized after its QP setup failed.
    const PATH_BLACKLIST_TTL: Duration = Duration::from_secs(30);

    /// Creates a new pool using the shared devices and buffer pool.
    pub fn new(
        devices: Arc<Devices>,
        buffer_pool: Arc<BufferPool>,
        config: RdmaSocketPoolConfig,
    ) -> Result<Self> {
        config.validate()?;
        for device in devices.devices() {
            device.configure_bandwidth_limit(&config)?;
        }
        let task_supervisor = TaskSupervisor::create();
        let port_refresher = RdmaDeviceRefresher::start(devices.clone(), &task_supervisor);
        Ok(Self {
            bootstrap_client: Client {
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
                (0..devices.devices().len())
                    .map(|_| AtomicUsize::new(0))
                    .collect(),
            ),
            read_permits: (0..devices.devices().len())
                .map(|_| {
                    Arc::new(tokio::sync::Semaphore::new(
                        config.remote_memory.max_inflight_read_wrs as usize,
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
            probe_started: AtomicBool::new(false),
            rebalance_cursor: AtomicUsize::new(0),
            ring_bytes: Arc::new(AtomicUsize::new(0)),
            pool_capacity_warned: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(crate) async fn acquire_with_options(
        &self,
        endpoint: &Endpoint,
        avoided_remote_nics: &HashSet<String>,
        deadline: Option<Instant>,
        state: &Arc<State>,
    ) -> Result<Socket> {
        if let Some(socket) = self.try_acquire(endpoint, avoided_remote_nics) {
            return Ok(socket);
        }

        let peer = self.peer(endpoint);
        let guard = match deadline {
            Some(deadline) => tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                peer.connect.lock(),
            )
            .await
            .map_err(|_| {
                Error::new(
                    ErrorKind::Timeout,
                    format!(
                        "RDMA connection to {endpoint}: deadline expired waiting for peer connect lock"
                    ),
                )
            }),
            None => Ok(peer.connect.lock().await),
        };
        let guard = guard?;
        let result = self
            .connect_peer(&peer, state, avoided_remote_nics, deadline)
            .await;
        drop(guard);
        if result.is_ok() {
            peer.touch(Instant::now());
            self.schedule_reconcile(&peer, state);
        }
        result
    }

    pub(crate) fn try_acquire(
        &self,
        endpoint: &Endpoint,
        avoided_remote_nics: &HashSet<String>,
    ) -> Option<Socket> {
        let peer = self.existing_peer(endpoint)?;
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
        endpoint: &Endpoint,
        avoided_remote_nics: &HashSet<String>,
    ) -> Option<Socket> {
        let peer = self.existing_peer(endpoint)?;
        let stripes = peer.stripes.read().unwrap();
        let socket = self.pick_stripe(&stripes.active, avoided_remote_nics)?;
        drop(stripes);
        peer.touch(Instant::now());
        Some(socket)
    }

    fn peer(&self, endpoint: &Endpoint) -> Arc<PeerState> {
        self.peers
            .entry(endpoint.clone())
            .or_insert_with(|| Arc::new(PeerState::new(endpoint.clone())))
            .clone()
    }

    fn existing_peer(&self, endpoint: &Endpoint) -> Option<Arc<PeerState>> {
        self.peers.get(endpoint).map(|peer| peer.clone())
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

    /// Cheap pseudo-random draw for P2C sampling and interval jitter;
    /// uniqueness/quality requirements are modest, so a hash of a
    /// (sequence, nanos) pair suffices — no RNG dependency.
    fn pseudo_random(&self) -> u64 {
        pseudo_random(self.rng_seq.fetch_add(1, Ordering::Relaxed))
    }

    /// Penalizes a NIC pair towards this peer after its QP setup failed.
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

    /// Whether the candidate's NIC pair towards this peer is currently
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
            cq_len: self.config.polling.device_cq_len,
            read_limit: self.config.remote_memory.max_inflight_read_wrs,
            spin_us: self.config.polling.poll_spin_us,
            dispatch_workers: self.config.polling.dispatch_workers,
        }
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

/// Cheap pseudo-random draw; see [`RdmaSocketPool::pseudo_random`].
fn pseudo_random(seq: usize) -> u64 {
    use std::hash::BuildHasher as _;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    RandomState::default().hash_one((seq, nanos))
}

fn next_attempt_id() -> u64 {
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
mod group_tests {
    //! Address-group behavior needs no RDMA device or queue pair.

    use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

    use super::*;
    use crate::core::ContextEndpoint;
    use crate::http::HttpSocket;
    use crate::rdma::{RdmaService as _, rdma_service::RDMA_BOOTSTRAP_PROTOCOL_VERSION};
    use crate::{Context, Endpoint, ListenMode, Listener, Router, Server, Transport};

    fn pool_without_rdma_devices() -> RdmaSocketPool {
        let devices = Arc::new(Devices::default());
        let buffers = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        RdmaSocketPool::new(devices, buffers, RdmaSocketPoolConfig::default()).unwrap()
    }

    fn endpoint(ports: &[u16]) -> Endpoint {
        Endpoint::rdma(
            Transport::TCP,
            ports
                .iter()
                .map(|port| SocketAddr::from(([127, 0, 0, 1], *port))),
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normalized_groups_share_one_peer_and_connect_lock() {
        let pool = Arc::new(pool_without_rdma_devices());
        let group = endpoint(&[41001, 41002]);
        let equivalent = endpoint(&[41002, 41001, 41002]);
        let barrier = Arc::new(tokio::sync::Barrier::new(16));
        let mut tasks = Vec::new();
        for index in 0..16 {
            let pool = pool.clone();
            let barrier = barrier.clone();
            let group = if index % 2 == 0 { &group } else { &equivalent }.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                pool.peer(&group)
            }));
        }
        let mut tasks = tasks.into_iter();
        let peer = tasks.next().unwrap().await.unwrap();
        for task in tasks {
            assert!(Arc::ptr_eq(&peer, &task.await.unwrap()));
        }
        assert!(Arc::ptr_eq(&peer, &pool.peer(&group)));
        assert_eq!(pool.peers.len(), 1);
        let now = Instant::now();
        peer.touch(now);
        assert_eq!(
            pool.existing_peer(&equivalent)
                .unwrap()
                .meta
                .lock()
                .unwrap()
                .last_used,
            Some(now),
        );

        // Exercise the actual acquire path while the equivalent spelling owns
        // the connect lock. A separate peer would instead attempt TCP discovery.
        let _guard = peer.connect.lock().await;
        let context = Context::create(&SocketPoolConfig {
            rdma: None,
            ..Default::default()
        })
        .unwrap();
        let error = pool
            .acquire_with_options(
                &equivalent,
                &HashSet::new(),
                Some(Instant::now() + Duration::from_millis(20)),
                &context.state,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(
            error.msg.contains("waiting for peer connect lock"),
            "{error}"
        );
        assert_eq!(pool.peers.len(), 1);
    }

    #[tokio::test]
    async fn overlapping_groups_keep_independent_peer_state() {
        let pool = pool_without_rdma_devices();
        let first = pool.peer(&endpoint(&[41001, 41002]));
        let overlapping = pool.peer(&endpoint(&[41002, 41003]));
        let subset = pool.peer(&endpoint(&[41001]));
        assert!(!Arc::ptr_eq(&first, &overlapping));
        assert!(!Arc::ptr_eq(&first, &subset));
        assert_eq!(pool.peers.len(), 3);

        let _guard = first.connect.lock().await;
        assert!(overlapping.connect.try_lock().is_ok());
        assert!(subset.connect.try_lock().is_ok());
        first.touch(Instant::now());
        assert!(overlapping.meta.lock().unwrap().last_used.is_none());
        assert!(subset.meta.lock().unwrap().last_used.is_none());
    }

    fn mark_closed(socket: &Socket) -> bool {
        match socket {
            Socket::TCP(socket) => socket.mark_closed(),
            Socket::WS(socket) => socket.mark_closed(),
            Socket::HTTP(HttpSocket::Stream(socket)) => socket.mark_closed(),
            Socket::HTTP(HttpSocket::ForResponse(_)) => false,
            #[cfg(feature = "rdma")]
            Socket::RDMA(_) => false,
        }
    }

    fn bootstrap_config() -> SocketPoolConfig {
        // Keep the real bootstrap service and RDMA pool, but register no RDMA
        // devices. Discovery and bootstrap entry-point failover require no verbs
        // work, and a UNIFIED listener can serve every bootstrap transport.
        let mut config = SocketPoolConfig {
            listen_mode: ListenMode::UNIFIED,
            ..Default::default()
        };
        config.rdma.as_mut().unwrap().path.device_filter =
            vec!["ruapc-test-no-matching-rdma-device".into()];
        config
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bootstrap_group_reconnects_through_another_listener_for_every_transport() {
        for bootstrap_transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
            let config = bootstrap_config();
            let server = Server::create(Router::default(), &config).unwrap();
            assert!(server.state().devices.devices().is_empty());
            let first = Listener::new();
            let second = Listener::new();
            let first_addr = first
                .start_listen("127.0.0.1:0".parse().unwrap(), server.state())
                .await
                .unwrap();
            let second_addr = second
                .start_listen("127.0.0.1:0".parse().unwrap(), server.state())
                .await
                .unwrap();
            let unavailable = "127.0.0.1:0".parse().unwrap();
            let group = Endpoint::rdma(bootstrap_transport, [second_addr, unavailable, first_addr])
                .unwrap();
            let peer = PeerState::new(group.clone());
            let context = Context::create(&SocketPoolConfig {
                rdma: None,
                ..Default::default()
            })
            .unwrap();
            let bootstrap = peer.bootstrap_context(&context.state);
            let ContextEndpoint::Endpoint(bootstrap_endpoint) = &bootstrap.endpoint else {
                panic!("bootstrap must retain an address group");
            };
            assert_eq!(bootstrap_endpoint.transport(), bootstrap_transport);
            assert_eq!(bootstrap_endpoint.addrs(), group.addrs());
            let client = Client {
                max_retries: 0,
                ..Default::default()
            };
            let info = client.discover(&bootstrap, &()).await.unwrap();
            assert_eq!(info.protocol_version, RDMA_BOOTSTRAP_PROTOCOL_VERSION);
            assert!(info.devices.is_empty());
            let original = context
                .state
                .socket_pool
                .acquire(bootstrap_endpoint, &context.state)
                .await
                .unwrap();

            // The normalized group tries port zero, then the lower live port.
            // Stop that accept loop and invalidate its cached connection. The same
            // bootstrap context must still discover through the other listener.
            let (closed, remaining) = if first_addr < second_addr {
                (&first, &second)
            } else {
                (&second, &first)
            };
            closed.stop();
            closed.join().await;
            assert!(mark_closed(&original));
            let info = client.discover(&bootstrap, &()).await.unwrap();
            assert_eq!(info.protocol_version, RDMA_BOOTSTRAP_PROTOCOL_VERSION);
            let replacement = context
                .state
                .socket_pool
                .acquire(bootstrap_endpoint, &context.state)
                .await
                .unwrap();
            assert_ne!(replacement.conn_id(), original.conn_id());

            remaining.stop();
            remaining.join().await;
            server.stop();
            tokio::time::timeout(Duration::from_secs(5), server.join())
                .await
                .unwrap();
        }
    }
}
