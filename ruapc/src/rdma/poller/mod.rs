//! RDMA completion polling, with one OS thread per CQ shard.
//!
//! Each RDMA device has configurable CQ shards, each with a dedicated OS
//! poll thread. Admission selects a shard by available completion credits:
//!
//! - **Busy phase**: after any completion, the thread keeps polling the CQ
//!   for a configurable spin window (`poll_spin_us`), eliminating the
//!   interrupt + epoll + task-wakeup latency of the event-driven path.
//! - **Idle phase**: once the spin window expires, the thread arms the CQ
//!   notification (`req_notify`), re-polls to close the race, and then
//!   sleeps in `poll(2)` on the completion channel fd and a wake pipe.
//! - The wake pipe is written by senders that enqueue pending (window
//!   blocked) sends, by `RdmaSocket::set_error`, and by connection
//!   registration/shutdown.
//!
//! # Completion routing
//!
//! Completions carry the provider's local QP number. A poll-thread-owned
//! registry maps that number to its connection, then checks the CQ-issued
//! identity's sequence floor before accounting credits. WRIDs have a fixed
//! 62-bit sequence independent of CQ capacity. QP identities remain leased
//! through destruction, including while registrations are in transit.
//!
//! # Zero-parse poll thread
//!
//! The poll thread never looks inside received bytes. Flow control is
//! accounted per *work completion* (one receive WC = one credit,
//! regardless of how many messages the buffer carries), so received
//! buffers accumulate into per-drain batches that are routed to a fixed
//! pool of long-lived dispatch worker tasks (`rdma.polling.dispatch_workers`),
//! each receiving through a Tokio mpsc queue; workers walk the shared `[RUA!][body_len][body]`
//! frames and parse them on tokio worker threads. Routing is sticky
//! (spill on pressure, see [`Dispatcher`]) and queue sends never wait.
//! When every worker is saturated, a one-shot task handles the batch.

mod budget;
mod conn;
mod dispatch;
mod flow;

use std::{
    collections::{HashMap, hash_map::Entry},
    io::{Read as _, Write as _},
    os::unix::io::AsRawFd as _,
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use foldhash::fast::RandomState;
use ruapc_rdma::{CompChannel, Completion, CompletionBatch, CompletionQueue, poll_readable2};

use budget::{CqBudget, CqDrain, Demand};
use conn::ConnState;
use dispatch::{DispatchBatch, Dispatcher, MAX_DISPATCH_BATCH};

use super::RdmaSocket;
use crate::{Buffer, Error, ErrorKind, Result, State, task::TaskSupervisorGuard};

/// Only the poll thread may move a registered QP to ERR: receive posting and
/// the provider flush must not race. Initial receive publication uses the
/// inbox mutex; established receives are posted by this thread alone.
fn set_qp_error(socket: &RdmaSocket) {
    let mut attr = ruapc_rdma::ibv_qp_attr {
        qp_state: ruapc_rdma::ibv_qp_state::IBV_QPS_ERR,
        ..Default::default()
    };
    let mask = ruapc_rdma::ibv_qp_attr_mask::IBV_QP_STATE;
    if let Err(err) = socket.queue_pair.modify(&mut attr, mask.0 as _) {
        tracing::warn!(conn_id = socket.conn_id, local_qp = socket.queue_pair.qp_num(), %err,
            "failed to move RDMA queue pair to ERR");
    }
}

/// Wakes the poll thread out of its idle `poll(2)` sleep.
#[derive(Clone, Debug)]
pub struct PollerWaker(Arc<UnixStream>, Arc<AtomicBool>);

impl PollerWaker {
    /// Wakes the poll thread. Best-effort: if the pipe is full the thread is
    /// already scheduled to wake up.
    pub fn wake(&self) {
        // Publish before the pipe write. The poll thread consumes this hint
        // before maintenance, so a concurrent wake schedules another pass.
        self.1.store(true, Ordering::Release);
        let _ = (&*self.0).write(&[1u8]);
    }
}

/// Tracks the registered memory pinned by one connection's receive ring
/// (`recv_queue_len × max_msg_size`); the shared counter is decremented
/// when the connection is torn down.
pub struct RingReservation {
    total: Arc<std::sync::atomic::AtomicUsize>,
    bytes: usize,
}

impl RingReservation {
    /// Adds `bytes` to the shared ring total and returns the guard plus
    /// the new total.
    pub fn add(total: &Arc<std::sync::atomic::AtomicUsize>, bytes: usize) -> (Self, usize) {
        let previous = total.fetch_add(bytes, Ordering::AcqRel);
        (
            Self {
                total: total.clone(),
                bytes,
            },
            previous + bytes,
        )
    }
}

impl Drop for RingReservation {
    fn drop(&mut self) {
        self.total.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// Everything the poll thread needs to manage one connection.
pub struct RegisterConn {
    pub socket: Arc<RdmaSocket>,
    pub state: Arc<State>,
    pub pending_receiver: tokio::sync::mpsc::Receiver<Buffer>,
    /// Number of receive work requests already posted by the registrar.
    pub recv_submitted: u64,
    /// Negotiated receive buffer size (`max_msg_size`).
    pub recv_buf_size: usize,
    /// Negotiated send window in data WRs (`recv_queue_len / 2`); the
    /// peer uses the same value, so the ACK cadence derives from it.
    pub send_window: u32,
    /// Whether to aggregate window-blocked sends (local send-side toggle;
    /// receivers walk the same frame loop either way).
    pub msg_aggregation: bool,
    /// Keeps `SocketPool::join` waiting until this connection is torn down.
    pub supervisor_guard: TaskSupervisorGuard,
    /// Buffer pool bytes pinned by this connection's receive ring.
    pub ring_reservation: RingReservation,
    /// Keeps the pool's per-device connection count accurate until the
    /// poll thread tears this connection down.
    pub conn_count_guard: super::ConnCountGuard,
}

/// State shared between registrars and the poll thread: the registration
/// inbox and the shutdown flag. QP identity leases belong to the CQ.
struct PollerShared {
    inner: Mutex<SharedInner>,
    budget: CqBudget,
    /// Fast-path hint that `inner.incoming` is non-empty; written under
    /// the `inner` lock, read lock-free by the poll thread.
    has_incoming: AtomicBool,
    /// Set (under the `inner` lock) when the poller shuts down; after the
    /// poll thread's final inbox drain no registration can be lost.
    shutdown: AtomicBool,
}

impl std::fmt::Debug for PollerShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollerShared")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct SharedInner {
    /// Registered connections awaiting pickup by the poll thread.
    incoming: Vec<RegisterConn>,
}

/// CQ credits owned from before QP creation through destruction. Owners
/// must declare their QP field before this guard, so QP destruction precedes
/// retirement. A poll-to-empty after retirement authorizes credit reuse.
#[derive(Debug)]
pub(crate) struct ConnReservation {
    shared: Arc<PollerShared>,
    demand: Demand,
    waker: PollerWaker,
}

/// This wrapper preserves destruction order even on setup errors and when
/// moved between the handshake and the socket. Do not split its fields.
#[derive(Debug)]
pub(crate) struct ReservedQueuePair {
    qp: ruapc_rdma::QueuePair,
    reservation: ConnReservation,
}

impl std::ops::Deref for ReservedQueuePair {
    type Target = ruapc_rdma::QueuePair;
    fn deref(&self) -> &Self::Target {
        &self.qp
    }
}

impl ConnReservation {
    pub(crate) fn bind(self, qp: ruapc_rdma::QueuePair) -> ReservedQueuePair {
        ReservedQueuePair {
            qp,
            reservation: self,
        }
    }
}

impl Drop for ConnReservation {
    fn drop(&mut self) {
        self.shared.budget.retire(self.demand);
        self.waker.wake();
    }
}

/// Handle to a per-device poll thread.
///
/// Dropping the handle flags shutdown, wakes the thread and joins it
/// (unless the drop happens on the poll thread itself, which can occur
/// when the last `Arc<State>` is released during connection teardown).
pub struct DevicePoller {
    cq: Arc<CompletionQueue>,
    shared: Arc<PollerShared>,
    waker: PollerWaker,
    thread: Option<std::thread::JoinHandle<()>>,
    cq_capacity: u32,
}

/// Tunables for the poll thread, taken from `RdmaSocketPoolConfig`.
#[derive(Debug, Clone, Copy)]
pub struct PollerConfig {
    /// Shared CQ capacity (entries).
    pub cq_len: u32,
    /// Per-NIC READ limit, shared by all connections.
    pub read_limit: u32,
    /// Busy-poll window after the last completion, in microseconds.
    /// `0` disables spinning (pure event-driven mode).
    pub spin_us: u64,
    /// Number of dispatch worker tasks shared by all shards of the pool
    /// (consulted once, when the first shard starts).
    pub dispatch_workers: u32,
}

impl DevicePoller {
    /// Creates the shared CQ and starts the poll thread for one device.
    ///
    /// Must be called from within a tokio runtime: the thread captures the
    /// current runtime handle to spawn request handlers.
    pub fn start(
        ctx: &Arc<ruapc_rdma::Context>,
        device_name: &str,
        config: PollerConfig,
        dispatcher: Dispatcher,
    ) -> Result<Self> {
        let comp_channel = CompChannel::create(ctx)
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        comp_channel
            .set_nonblock()
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        // Clamp the shared CQ length to the device's capability: e.g. the
        // rxe soft-RoCE driver caps max_cqe at 32767, well below the
        // default device_cq_len, and ibv_create_cq fails with EINVAL when
        // asked for more. The provider can round this request up; admission
        // uses the actual returned CQ capacity below.
        let max_cqe = ctx
            .query_device()
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?
            .max_cqe;
        let cq_len = u32::try_from(max_cqe.max(1))
            .map_or(config.cq_len, |max_cqe| config.cq_len.min(max_cqe));
        if cq_len < config.cq_len {
            tracing::info!(
                "clamping shared CQ length {} -> {cq_len} for {device_name} (device max_cqe)",
                config.cq_len,
            );
        }
        let cq = CompletionQueue::create(ctx, cq_len as _, Some(&comp_channel))
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let cq_capacity = cq.capacity();
        let (wake_tx, wake_rx) =
            UnixStream::pair().map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        wake_tx
            .set_nonblocking(true)
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        wake_rx
            .set_nonblocking(true)
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let shared = Arc::new(PollerShared {
            inner: Mutex::new(SharedInner::default()),
            budget: CqBudget::new(cq_capacity, config.read_limit),
            has_incoming: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
        });
        let maintenance_requested = Arc::new(AtomicBool::new(false));
        let handle = tokio::runtime::Handle::current();

        let thread = {
            let cq = cq.clone();
            let comp_channel = comp_channel.clone();
            let shared = shared.clone();
            let maintenance_requested = maintenance_requested.clone();
            std::thread::Builder::new()
                .name(format!("ruapc-rdma-poll-{device_name}"))
                .spawn(move || {
                    let _rt = handle.enter();
                    PollLoop {
                        cq,
                        comp_channel,
                        wake_rx,
                        shared,
                        dispatcher,
                        spin: Duration::from_micros(config.spin_us),
                        conns: HashMap::default(),
                        dirty_qps: Vec::new(),
                        drain: CqDrain::default(),
                        maintenance_requested,
                        unack_cq_events: 0,
                    }
                    .run();
                })
                .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?
        };

        Ok(Self {
            cq,
            shared,
            waker: PollerWaker(Arc::new(wake_tx), maintenance_requested),
            thread: Some(thread),
            cq_capacity,
        })
    }

    /// The shared completion queue for this device.
    pub fn cq(&self) -> &Arc<CompletionQueue> {
        &self.cq
    }

    /// A waker for sockets on this device.
    pub fn waker(&self) -> PollerWaker {
        self.waker.clone()
    }

    /// Reserve before creating a QP, including connections still negotiating.
    fn reserve(&self, demand: Demand) -> Result<ConnReservation> {
        if self.shared.shutdown.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                "RDMA poll thread is not running".into(),
            ));
        }
        if !self.shared.budget.reserve(demand) {
            return Err(Error::new(
                ErrorKind::Overloaded,
                format!(
                    "shared CQ capacity exhausted: {} of {} entries reserved; connection needs {demand:?}",
                    self.shared.budget.snapshot().0,
                    self.cq_capacity
                ),
            ));
        }
        Ok(ConnReservation {
            shared: self.shared.clone(),
            demand,
            waker: self.waker(),
        })
    }

    /// Posts the initial receives and publishes their owner as one transaction.
    /// An early CQE's routing miss waits for this transaction before retrying.
    pub fn register(
        &self,
        conn: RegisterConn,
        post_receives: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let qp = &conn.socket.queue_pair;
        if !Arc::ptr_eq(&conn.socket.queue_pair.reservation.shared, &self.shared)
            || !Arc::ptr_eq(qp.cq(), &self.cq)
        {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "connection and reservation must belong to this RDMA poller's CQ".into(),
            ));
        }
        {
            let mut inner = self.shared.inner.lock().unwrap();
            if self.shared.shutdown.load(Ordering::Acquire) {
                return Err(Error::new(
                    ErrorKind::ConnectionClosed,
                    "RDMA poll thread is not running".into(),
                ));
            }
            post_receives()?;
            inner.incoming.push(conn);
            self.shared.has_incoming.store(true, Ordering::Release);
        }
        self.waker.wake();
        Ok(())
    }
}

impl Drop for DevicePoller {
    fn drop(&mut self) {
        {
            let _inner = self.shared.inner.lock().unwrap();
            self.shared.shutdown.store(true, Ordering::Release);
        }
        self.waker.wake();
        if let Some(thread) = self.thread.take() {
            // Teardown can be triggered from the poll thread itself when the
            // last `Arc<State>` is dropped during connection removal; never
            // join our own thread.
            if std::thread::current().id() == thread.thread().id() {
                drop(thread); // detach; the thread is already exiting
            } else {
                let _ = thread.join();
            }
        }
    }
}

impl std::fmt::Debug for DevicePoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevicePoller")
            .field("cq_capacity", &self.cq_capacity)
            .field("cq_reserved", &self.shared.budget.snapshot())
            .finish()
    }
}

/// The poll thread main loop state.
struct PollLoop {
    cq: Arc<CompletionQueue>,
    comp_channel: Arc<CompChannel>,
    wake_rx: UnixStream,
    shared: Arc<PollerShared>,
    /// Hands received buffers to the pool's dispatch worker tasks.
    dispatcher: Dispatcher,
    spin: Duration,
    /// This CQ's connections, indexed by the provider's local QP number.
    conns: HashMap<u32, ConnState, RandomState>,
    /// QPs needing flow maintenance, deduplicated by `ConnState::dirty`.
    dirty_qps: Vec<u32>,
    /// Undrained retirement snapshots survive scheduling yields.
    drain: CqDrain,
    /// Existing pending/error/activation wakeups request a full scan because
    /// those changes need not produce a CQE identifying the connection.
    maintenance_requested: Arc<AtomicBool>,
    unack_cq_events: u32,
}

impl PollLoop {
    /// Idle sleep timeout; bounds the latency of periodic housekeeping
    /// (5s ACK timer) when no completions arrive.
    const IDLE_TIMEOUT_MS: i32 = 100;
    /// Interval between per-connection state dumps (debug level).
    const DUMP_INTERVAL: Duration = Duration::from_secs(2);
    /// Acknowledge CQ events in batches to amortize the syscall-free ack.
    const ACK_EVENTS_BATCH: u32 = 1024;

    /// Retry receive-buffer allocation during pool pressure while busy. Idle
    /// polling uses a 1 ms timeout, preserving `poll_spin_us = 0` semantics.
    const RECEIVE_RETRY_INTERVAL: Duration = Duration::from_micros(100);

    /// Full scans cover idle keepalives, READ deadlines and error teardown.
    const HOUSEKEEPING_INTERVAL: Duration = Duration::from_millis(100);

    fn run(mut self) {
        if let Err(error) = self.run_until_shutdown() {
            tracing::error!(%error, "stopping RDMA poll thread");
        }
        // Every exit, including provider errors, closes registration and fails
        // outstanding reads before connection ownership is released.
        self.shutdown_cleanup();
    }

    fn run_until_shutdown(&mut self) -> ruapc_rdma::Result<()> {
        // The local owner lets completion proofs borrow the CQ while routing
        // mutably updates this loop. Clone once per thread, never per CQE.
        let cq = self.cq.clone();
        let mut wcs = CompletionBatch::<64>::new();
        let mut batch: DispatchBatch = Vec::new();
        let mut spin_until = Instant::now();
        let mut next_housekeeping = Instant::now();
        let mut next_receive_retry = Instant::now();
        let mut last_dump = Instant::now();

        loop {
            let progressed = self.drain_completions(&cq, &mut wcs, &mut batch)?;

            let now = Instant::now();
            if self.shared.shutdown.load(Ordering::Acquire) {
                return Ok(());
            }
            // Consume before touching connections: wakeups racing maintenance
            // remain set for the next loop, even when their pipe bytes coalesce.
            let requested = self.maintenance_requested.load(Ordering::Acquire)
                && self.maintenance_requested.swap(false, Ordering::AcqRel);
            let registered = self.drain_incoming(false);
            let housekeeping = now >= next_housekeeping;
            let retry_receives = now >= next_receive_retry;
            if retry_receives {
                next_receive_retry = now + Self::RECEIVE_RETRY_INTERVAL;
            }
            if housekeeping || requested {
                if housekeeping {
                    next_housekeeping = now + Self::HOUSEKEEPING_INTERVAL;
                }
                self.maintain_connections(now, housekeeping, retry_receives);
            } else if progressed || registered || retry_receives {
                self.maintain_dirty_connections(now, retry_receives);
            }
            if tracing::enabled!(tracing::Level::DEBUG)
                && now.duration_since(last_dump) >= Self::DUMP_INTERVAL
            {
                last_dump = now;
                self.dump_connections();
            }

            if progressed {
                spin_until = now + self.spin;
                continue;
            }
            if now < spin_until {
                std::hint::spin_loop();
                continue;
            }

            // Arm notifications, then poll again before sleeping. A completion
            // racing the arm must be observed here or signal the channel.
            self.cq.req_notify(false)?;
            if self.poll_completions(&cq, &mut wcs, &mut batch)? > 0 {
                self.dispatcher.flush(&mut batch);
                // These CQEs were polled outside the normal drain. Maintain
                // their credits now, even if the next drain finds an empty CQ.
                self.maintain_dirty_connections(Instant::now(), false);
                spin_until = Instant::now() + self.spin;
                continue;
            }
            if self.wait_for_event()? {
                spin_until = Instant::now() + self.spin;
            }
        }
    }

    /// Drain a bounded burst, then return to maintenance even under sustained
    /// READ traffic. Both this drain and the arm/poll check route CQEs identically.
    fn drain_completions(
        &mut self,
        cq: &CompletionQueue,
        wcs: &mut CompletionBatch<64>,
        batch: &mut DispatchBatch,
    ) -> ruapc_rdma::Result<bool> {
        self.drain.begin(&self.shared.budget);
        let mut progressed = false;
        loop {
            let count = self.poll_completions(cq, wcs, batch)?;
            progressed |= count > 0;
            if !self
                .drain
                .polled(&self.shared.budget, count, wcs.capacity())
            {
                break;
            }
        }
        self.dispatcher.flush(batch);
        Ok(progressed)
    }

    fn poll_completions(
        &mut self,
        cq: &CompletionQueue,
        wcs: &mut CompletionBatch<64>,
        batch: &mut DispatchBatch,
    ) -> ruapc_rdma::Result<usize> {
        let completions = cq.poll_batch(wcs)?;
        let count = completions.len();
        for wc in completions {
            self.dispatch(wc, batch);
        }
        if batch.len() >= MAX_DISPATCH_BATCH {
            self.dispatcher.flush(batch);
        }
        Ok(count)
    }

    fn dump_connections(&self) {
        for conn in self.conns.values() {
            tracing::debug!(
                "conn dump: qp={} ok={} pending={} flow={:?}",
                conn.socket.queue_pair.qp_num(),
                conn.socket.state.is_ok(),
                conn.pending_sends.len(),
                conn.flow,
            );
        }
    }

    fn mark_dirty(conn: &mut ConnState, dirty_qps: &mut Vec<u32>) {
        if !conn.dirty {
            conn.dirty = true;
            dirty_qps.push(conn.identity.qp_num());
        }
    }

    fn maintain_connections(&mut self, now: Instant, sweep_reads: bool, retry_receives: bool) {
        // Every old entry is covered by this scan. Rebuild only the receive
        // deficits that need another timed attempt, without stale QP entries.
        self.dirty_qps.clear();
        self.conns.retain(|&qp_num, conn| {
            let keep = Self::maintain_connection(conn, now, sweep_reads, retry_receives);
            if keep && conn.dirty {
                self.dirty_qps.push(qp_num);
            }
            keep
        });
    }

    fn maintain_dirty_connections(&mut self, now: Instant, retry_receives: bool) {
        // Compact in place: completions cannot append QPs while this same
        // poll thread is maintaining them, so the vector retains its allocation.
        let mut retained = 0;
        for index in 0..self.dirty_qps.len() {
            let qp_num = self.dirty_qps[index];
            if let Entry::Occupied(mut entry) = self.conns.entry(qp_num) {
                if !Self::maintain_connection(entry.get_mut(), now, false, retry_receives) {
                    entry.remove();
                } else if entry.get().dirty {
                    self.dirty_qps[retained] = qp_num;
                    retained += 1;
                }
            }
        }
        self.dirty_qps.truncate(retained);
    }

    /// Returns whether the connection must remain registered. Receive-buffer
    /// pressure leaves its dirty bit set for another timed pass.
    fn maintain_connection(
        conn: &mut ConnState,
        now: Instant,
        sweep_reads: bool,
        retry_receives: bool,
    ) -> bool {
        conn.dirty = false;
        if sweep_reads {
            conn.sweep_read_timeouts(now);
        }
        conn.drain_pending();
        if retry_receives && conn.recv_deficit > 0 && conn.socket.state.is_ok() {
            conn.retry_recv_deficit();
        }
        if let Err(e) = conn.update_flow_control() {
            tracing::error!("flow control update error: {e}");
        }
        if conn.ready_to_remove() {
            // The QP keeps its identity leased even if another owner holds
            // the socket after poller teardown.
            return false;
        }
        conn.dirty = conn.recv_deficit > 0 && conn.socket.state.is_ok();
        true
    }

    /// Sleep until a CQ notification, explicit wake, or housekeeping timeout.
    /// Draining both sources before returning avoids a permanently readable fd.
    fn wait_for_event(&mut self) -> ruapc_rdma::Result<bool> {
        let (cq_ready, wake_ready) = poll_readable2(
            self.comp_channel.fd().as_raw_fd(),
            self.wake_rx.as_raw_fd(),
            if self.dirty_qps.is_empty() {
                Self::IDLE_TIMEOUT_MS
            } else {
                // Only receive deficits survive maintenance. Recover promptly
                // without busy-spinning when the buffer pool remains full.
                1
            },
        )?;
        if cq_ready {
            while self.comp_channel.get_event().is_ok() {
                self.unack_cq_events += 1;
            }
            if self.unack_cq_events >= Self::ACK_EVENTS_BATCH {
                self.cq.ack_events(self.unack_cq_events);
                self.unack_cq_events = 0;
            }
        }
        if wake_ready {
            let mut buf = [0u8; 256];
            while matches!(self.wake_rx.read(&mut buf), Ok(n) if n > 0) {}
        }
        Ok(cq_ready || wake_ready)
    }

    /// Moves newly registered connections from the shared inbox into the
    /// provider QP-number registry.
    fn drain_incoming(&mut self, routing_miss: bool) -> bool {
        if !routing_miss && !self.shared.has_incoming.load(Ordering::Acquire) {
            return false;
        }
        let drained = {
            let mut inner = self.shared.inner.lock().unwrap();
            self.shared.has_incoming.store(false, Ordering::Release);
            std::mem::take(&mut inner.incoming)
        };
        let registered = !drained.is_empty();
        for incoming in drained {
            let qp_num = incoming.socket.queue_pair.qp_num();
            match self.conns.entry(qp_num) {
                Entry::Vacant(entry) => {
                    let mut conn = ConnState::new(incoming);
                    Self::mark_dirty(&mut conn, &mut self.dirty_qps);
                    entry.insert(conn);
                }
                Entry::Occupied(_) => panic!("poller QP {qp_num} already registered"),
            }
        }
        registered
    }

    fn dispatch(&mut self, wc: Completion<'_>, batch: &mut DispatchBatch) {
        let id = wc.info().wr_id;
        let qp_num = wc.qp_num();
        if let Some(conn) = self.conns.get_mut(&qp_num)
            && conn.identity.contains(qp_num, id)
        {
            conn.handle_wc(wc, batch);
            Self::mark_dirty(conn, &mut self.dirty_qps);
            return;
        }
        // Initial receives and inbox publication hold the same mutex. Bypass
        // the empty-inbox hint: a registrar can still be posting the ring.
        // Normal completions never acquire this lock.
        self.drain_incoming(true);
        if let Some(conn) = self.conns.get_mut(&qp_num)
            && conn.identity.contains(qp_num, id)
        {
            conn.handle_wc(wc, batch);
            Self::mark_dirty(conn, &mut self.dirty_qps);
        } else {
            tracing::warn!("dropping completion for unknown or retired QP identity: {wc:?}");
        }
    }

    /// Marks shutdown, fails every connection and drains the inbox so no
    /// registration (with its budget and supervisor guards) is leaked.
    fn shutdown_cleanup(&mut self) {
        let drained = {
            let mut inner = self.shared.inner.lock().unwrap();
            // Under the same lock registrars check the flag, so after this
            // section the inbox stays empty forever.
            self.shared.shutdown.store(true, Ordering::Release);
            std::mem::take(&mut inner.incoming)
        };
        for incoming in &drained {
            incoming.socket.set_error();
            set_qp_error(&incoming.socket);
            incoming.socket.fail_read_batches();
            // Inbox entries have not opened their metric registration yet,
            // but a sender may already have bound a waiter to the socket.
            incoming.state.connection_closed(
                incoming.socket.conn_id,
                &Error::new(
                    ErrorKind::ConnectionClosed,
                    "rdma poll thread stopped".into(),
                ),
            );
        }
        for conn in self.conns.values_mut() {
            conn.socket.set_error();
            conn.start_error_flush();
            // Nobody will poll the flush completions after this thread
            // exits: resolve the waiting tasks now. The memory holds stay
            // parked in the batches and are released when the socket (and
            // its QP, first) is dropped.
            conn.socket.fail_read_batches();
        }
        // Closing every connection must precede dropping any last socket
        // owner. QP destruction can return unconsumed READ permits; no other
        // connection on this stopped CQ may use them to replenish its CQEs.
        drop(drained);
        // RegisteredConnection guards notify ordinary waiters and close the
        // connection gauges exactly once, including already-failing sockets.
        self.conns.clear();
        self.dirty_qps.clear();
        if self.unack_cq_events > 0 {
            self.cq.ack_events(self.unack_cq_events);
            self.unack_cq_events = 0;
        }
    }
}

/// Lazily-created CQ/poll-thread shards, keyed by device name. Connections
/// reserve completion credits on the least utilized shard that can admit
/// them, before QP creation or peer negotiation. All shards share one fixed
/// dispatcher worker pool; the configured thread count bounds CPU usage.
#[derive(Default)]
pub struct DevicePollers(Mutex<PollersInner>);

#[derive(Default)]
struct PollersInner {
    devices: std::collections::HashMap<String, DeviceShards, RandomState>,
    dispatcher: Option<Dispatcher>,
}

#[derive(Default)]
struct DeviceShards {
    shards: Vec<Arc<DevicePoller>>,
    next: usize,
}

impl DevicePollers {
    /// Select and reserve as one setup operation. Creating each configured
    /// shard lazily spreads even a small number of connections across cores.
    pub(crate) fn reserve(
        &self,
        device: &super::RdmaDevice,
        config: PollerConfig,
        shard_count: u32,
        connection: &super::RdmaConnectionConfig,
    ) -> Result<(Arc<DevicePoller>, ConnReservation)> {
        let name = device.info().name.clone();
        let demand = Demand::connection(connection);
        let mut inner = self.0.lock().unwrap();
        let dispatcher = inner
            .dispatcher
            .get_or_insert_with(|| Dispatcher::start(config.dispatch_workers))
            .clone();
        let entry = inner.devices.entry(name.clone()).or_default();
        if entry.shards.len() < shard_count as usize {
            let index = entry.shards.len();
            match DevicePoller::start(
                device.context(),
                &format!("{name}.{index}"),
                config,
                dispatcher,
            ) {
                Ok(poller) => {
                    tracing::info!(
                        device = name,
                        shard = index,
                        capacity = poller.cq_capacity,
                        "started RDMA CQ shard"
                    );
                    entry.shards.push(Arc::new(poller));
                }
                Err(error) if !entry.shards.is_empty() => {
                    // An optional new thread/CQ is not required if an existing
                    // shard has room (e.g. provider CQ resources exhausted).
                    return entry.reserve(demand).map_err(|admission| {
                        Error::new(
                            admission.kind,
                            format!(
                                "RDMA device {name}: {}; new shard failed: {error}",
                                admission.msg
                            ),
                        )
                    });
                }
                Err(error) => return Err(error),
            }
        }
        entry
            .reserve(demand)
            .map_err(|err| Error::new(err.kind, format!("RDMA device {name}: {}", err.msg)))
    }

    pub(crate) fn report(&self) -> Vec<super::path::RdmaCqLoad> {
        let inner = self.0.lock().unwrap();
        let mut result = Vec::new();
        for (device, entry) in &inner.devices {
            for (shard, poller) in entry.shards.iter().enumerate() {
                let (reserved, connections) = poller.shared.budget.snapshot();
                let registry = poller.cq.qp_registry_stats();
                result.push(super::path::RdmaCqLoad {
                    device: device.clone(),
                    shard,
                    capacity: poller.cq_capacity,
                    reserved,
                    connections,
                    registered_qps: registry.active,
                    next_sequence_floor: registry.next_sequence_floor,
                    sequence_bits: ruapc_rdma::WRID::SEQUENCE_BITS,
                });
            }
        }
        result.sort_by(|a, b| (&a.device, a.shard).cmp(&(&b.device, b.shard)));
        result
    }
}

impl DeviceShards {
    fn reserve(&mut self, demand: Demand) -> Result<(Arc<DevicePoller>, ConnReservation)> {
        let count = self.shards.len();
        let start = self.next % count;
        self.next = self.next.wrapping_add(1);
        let mut candidates: Vec<_> = (0..count).map(|offset| (start + offset) % count).collect();
        // Stable sort rotates equally utilized shards. Recheck admission
        // under each budget's mutex; a reclamation can race these snapshots.
        let loads: Vec<_> = self
            .shards
            .iter()
            .map(|p| p.shared.budget.snapshot().0)
            .collect();
        candidates.sort_by(|&a, &b| {
            (u64::from(loads[a]) * u64::from(self.shards[b].cq_capacity))
                .cmp(&(u64::from(loads[b]) * u64::from(self.shards[a].cq_capacity)))
        });
        let mut error = None;
        for index in candidates {
            let poller = &self.shards[index];
            match poller.reserve(demand) {
                Ok(reservation) => return Ok((poller.clone(), reservation)),
                Err(err) => error = Some(err),
            }
        }
        let error = error.expect("at least one configured CQ shard");
        Err(Error::new(
            error.kind,
            format!(
                "all {count} RDMA CQ shards unavailable: {}; raise rdma.polling.device_cq_len or poll_threads_per_device",
                error.msg
            ),
        ))
    }
}

impl std::fmt::Debug for DevicePollers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevicePollers").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruapc_memory::BufferSet;

    use std::sync::{atomic::AtomicUsize, mpsc};

    use ruapc_bufpool::Device as _;
    use ruapc_memory::CopyBatch;
    use ruapc_rdma::{QueuePair, ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_state};

    use crate::{
        BufferPool, MsgFlags, MsgMeta, Router, SocketPoolConfig, SocketTrait, TaskSupervisor,
        rdma::{
            ConnCountGuard, RdmaBandwidthLimiter, RdmaConnectionConfig, RdmaDevice, RdmaNicInfo,
            RdmaPathInfo, RdmaQueuePairConfig, RdmaSocketConfig, SendPermit,
            test_utils::{make_rdma_devices, open_rdma_device},
        },
    };

    /// The shared CQ length must be clamped to the device's `max_cqe`:
    /// drivers reject larger requests with EINVAL (e.g. the rxe soft-RoCE
    /// driver caps `max_cqe` at 32767, below the default `device_cq_len`).
    #[tokio::test]
    async fn test_cq_len_clamped_to_device_max() {
        let device = open_rdma_device();
        let config = PollerConfig {
            cq_len: u32::MAX,
            read_limit: 1,
            spin_us: 0,
            dispatch_workers: 1,
        };
        let poller = DevicePoller::start(
            device.context(),
            "cq-clamp-test",
            config,
            Dispatcher::start(config.dispatch_workers),
        )
        .expect("CQ creation must succeed with a clamped length");
        let max_cqe = device.context().query_device().unwrap().max_cqe;
        assert!(poller.cq_capacity <= u32::try_from(max_cqe.max(1)).unwrap_or(u32::MAX));
        assert!(poller.cq_capacity > 0);
    }

    #[test]
    fn test_ring_reservation_accounting() {
        let total = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (a, after_a) = RingReservation::add(&total, 16);
        assert_eq!(after_a, 16);
        let (b, after_b) = RingReservation::add(&total, 32);
        assert_eq!(after_b, 48);
        drop(a);
        assert_eq!(total.load(Ordering::Acquire), 32);
        drop(b);
        assert_eq!(total.load(Ordering::Acquire), 0);
    }

    #[test]
    fn wake_during_maintenance_survives_draining_the_pipe() {
        let (writer, mut reader) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        reader.set_nonblocking(true).unwrap();
        let requested = Arc::new(AtomicBool::new(false));
        let waker = PollerWaker(Arc::new(writer), requested.clone());
        waker.wake();
        assert!(requested.swap(false, Ordering::AcqRel));
        // The first maintenance pass is in progress when a different sender
        // publishes work. Consuming both pipe bytes must preserve its new hint.
        waker.clone().wake();
        let mut bytes = [0; 2];
        reader.read_exact(&mut bytes).unwrap();
        assert!(requested.swap(false, Ordering::AcqRel));
        assert!(!requested.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shard_admission_tries_other_queues_and_reclaims_failed_setup() {
        let device = open_rdma_device();
        let config = PollerConfig {
            cq_len: 63,
            read_limit: 32,
            spin_us: 0,
            dispatch_workers: 1,
        };
        let dispatcher = Dispatcher::start(1);
        let mut shards = DeviceShards::default();
        for index in 0..2 {
            shards.shards.push(Arc::new(
                DevicePoller::start(
                    device.context(),
                    &format!("admission-test-{index}"),
                    config,
                    dispatcher.clone(),
                )
                .unwrap(),
            ));
        }
        // Give the less loaded shard an unsaturated READ reserve. The larger
        // READ demand below can fit only on the other shard, whose shared READ
        // reserve is already paid. Lowest current usage alone is insufficient.
        let connection = |recv_queue_len, max_send_wr| RdmaConnectionConfig {
            recv_queue_len,
            max_msg_size: 1024,
            max_large_msg_size: 0,
            traffic_class: 0,
            qp: RdmaQueuePairConfig {
                max_send_wr,
                ..Default::default()
            },
        };
        assert_eq!(shards.shards[0].cq_capacity, 63);
        let a = shards.shards[0]
            .reserve(Demand::connection(&connection(8, 2)))
            .unwrap();
        let b = shards.shards[1]
            .reserve(Demand::connection(&connection(2, 64)))
            .unwrap();
        assert_eq!(shards.shards[0].shared.budget.snapshot().0, 17);
        assert_eq!(shards.shards[1].shared.budget.snapshot().0, 37);
        let (chosen, c) = shards
            .reserve(Demand::connection(&connection(8, 64)))
            .unwrap();
        assert!(Arc::ptr_eq(&chosen, &shards.shards[1]));
        assert_eq!(chosen.shared.budget.snapshot().0, 53);
        assert!(
            shards
                .reserve(Demand::connection(&connection(8, 64)))
                .is_err()
        );
        // These reservations model failed setup before QP creation. Their wake
        // causes even idle shards to drain and make the credits available again.
        drop((a, b, c));
        tokio::time::timeout(Duration::from_secs(5), async {
            while shards
                .shards
                .iter()
                .any(|p| p.shared.budget.snapshot().1 != 0)
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("retired setup capacity must be reclaimed");
        assert!(
            shards
                .reserve(Demand::connection(&connection(8, 64)))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn failed_new_shard_does_not_hide_existing_capacity() {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pollers = DevicePollers::default();
        let config = PollerConfig {
            cq_len: 63,
            read_limit: 1,
            spin_us: 0,
            dispatch_workers: 1,
        };
        let connection = RdmaConnectionConfig {
            recv_queue_len: 2,
            max_msg_size: 1024,
            max_large_msg_size: 0,
            traffic_class: 0,
            qp: RdmaQueuePairConfig::default(),
        };
        let (first, a) = pollers.reserve(device, config, 2, &connection).unwrap();
        // Bypass public config validation to deterministically fail only the new
        // CQ creation; the existing shard can still satisfy this reservation.
        let (second, b) = pollers
            .reserve(
                device,
                PollerConfig {
                    cq_len: 0,
                    ..config
                },
                2,
                &connection,
            )
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(pollers.report().len(), 1);
        assert_eq!(pollers.report()[0].connections, 2);
        drop((a, b));
    }

    /// The test controls CQ consumption itself so it can stop a registrar after
    /// posting but before publication, without racing an independently running loop.
    fn manual_poller(context: &Arc<ruapc_rdma::Context>) -> (DevicePoller, PollLoop) {
        let channel = CompChannel::create(context).unwrap();
        let cq = CompletionQueue::create(context, 16, Some(&channel)).unwrap();
        let shared = Arc::new(PollerShared {
            inner: Mutex::new(SharedInner::default()),
            budget: CqBudget::new(cq.capacity(), 1),
            has_incoming: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
        });
        let (wake_tx, wake_rx) = UnixStream::pair().unwrap();
        wake_tx.set_nonblocking(true).unwrap();
        wake_rx.set_nonblocking(true).unwrap();
        let maintenance_requested = Arc::new(AtomicBool::new(false));
        let poller = DevicePoller {
            cq: cq.clone(),
            shared: shared.clone(),
            waker: PollerWaker(Arc::new(wake_tx), maintenance_requested.clone()),
            thread: None,
            cq_capacity: cq.capacity(),
        };
        let poll_loop = PollLoop {
            cq,
            comp_channel: channel,
            wake_rx,
            shared,
            dispatcher: Dispatcher::start(1),
            spin: Duration::ZERO,
            conns: HashMap::default(),
            dirty_qps: Vec::new(),
            drain: CqDrain::default(),
            maintenance_requested,
            unack_cq_events: 0,
        };
        (poller, poll_loop)
    }

    /// An admitted QP with no posted WRs lets maintenance tests control software
    /// credits without inventing CQEs or requiring a connected peer.
    fn register_idle_socket(
        poller: &DevicePoller,
        device: &RdmaDevice,
        buffer_pool: &Arc<BufferPool>,
        state: &Arc<State>,
    ) -> Arc<RdmaSocket> {
        register_idle_socket_with_read_permits(
            poller,
            device,
            buffer_pool,
            state,
            Arc::new(tokio::sync::Semaphore::new(1)),
        )
    }

    fn register_idle_socket_with_read_permits(
        poller: &DevicePoller,
        device: &RdmaDevice,
        buffer_pool: &Arc<BufferPool>,
        state: &Arc<State>,
        read_permits: Arc<tokio::sync::Semaphore>,
    ) -> Arc<RdmaSocket> {
        register_socket_with_config(
            poller,
            device,
            buffer_pool,
            state,
            read_permits,
            SocketTestConfig::default(),
        )
    }

    struct SocketTestConfig {
        capacity: usize,
        window: u32,
        aggregate: bool,
        read_timeout: Option<Duration>,
    }

    impl Default for SocketTestConfig {
        fn default() -> Self {
            Self {
                capacity: 4,
                window: 1,
                aggregate: false,
                read_timeout: None,
            }
        }
    }

    fn register_socket_with_config(
        poller: &DevicePoller,
        device: &RdmaDevice,
        buffer_pool: &Arc<BufferPool>,
        state: &Arc<State>,
        read_permits: Arc<tokio::sync::Semaphore>,
        pending: SocketTestConfig,
    ) -> Arc<RdmaSocket> {
        let connection = RdmaConnectionConfig {
            recv_queue_len: pending.window * 2,
            max_msg_size: 1024,
            max_large_msg_size: 0,
            traffic_class: 0,
            qp: RdmaQueuePairConfig {
                max_send_wr: 4,
                max_recv_wr: 4,
                max_send_sge: 1,
                max_recv_sge: 1,
            },
        };
        let reservation = poller.reserve(Demand::connection(&connection)).unwrap();
        let mut attrs = ruapc_rdma::ibv_qp_init_attr {
            qp_type: ruapc_rdma::ibv_qp_type::IBV_QPT_RC,
            cap: ruapc_rdma::ibv_qp_cap {
                max_send_wr: 4,
                max_recv_wr: 4,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let qp = QueuePair::create(device.pd(), poller.cq(), &mut attrs, device.index()).unwrap();
        let nic = RdmaNicInfo {
            device: device.info().name.clone(),
            port_num: device.info().ports[0].port_num,
            gid_index: 0,
            ip: None,
        };
        let (pending_sender, pending_receiver) = tokio::sync::mpsc::channel(pending.capacity);
        let socket = Arc::new(RdmaSocket::new(
            reservation.bind(qp),
            buffer_pool.clone(),
            pending_sender,
            poller.waker(),
            RdmaSocketConfig {
                max_msg_size: 1024,
                max_large_msg_size: 0,
                max_inflight_large_bytes: 128 << 20,
                send_window: pending.window,
                path: RdmaPathInfo {
                    local: nic.clone(),
                    remote: nic.clone(),
                    same_connectivity_domain: true,
                },
                read_timeout: pending.read_timeout,
                read_permits,
                bandwidth_limiter: Arc::new(RdmaBandwidthLimiter::new(
                    nic.device,
                    nic.port_num,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                )),
                sq_read_cap: 2,
            },
        ));
        poller
            .register(
                RegisterConn {
                    socket: socket.clone(),
                    state: state.clone(),
                    pending_receiver,
                    recv_submitted: 0,
                    recv_buf_size: 1024,
                    send_window: pending.window,
                    msg_aggregation: pending.aggregate,
                    supervisor_guard: TaskSupervisor::create().start_async_task(),
                    ring_reservation: RingReservation::add(&Arc::new(AtomicUsize::new(0)), 0).0,
                    conn_count_guard: ConnCountGuard::acquire(
                        &Arc::new(vec![AtomicUsize::new(0)]),
                        0,
                    ),
                },
                || Ok(()),
            )
            .unwrap();
        socket
    }

    fn connect_read_pair(device: &RdmaDevice, first: &QueuePair, second: &QueuePair) {
        let info = device.info();
        let port = info.ports.iter().find(|p| p.is_usable()).unwrap();
        let gid = port
            .gids
            .iter()
            .find(|g| g.gid_type == ruapc_rdma::GidType::RoCEv2)
            .or_else(|| port.gids.first())
            .unwrap();
        let config = |remote: &QueuePair| ruapc_rdma::QpConnectionConfig {
            local_port_num: port.port_num,
            local_gid_index: gid.index,
            pkey_index: 0,
            link_layer: port.port_attr.link_layer,
            path_mtu: port.port_attr.active_mtu,
            remote_qp_num: remote.qp_num(),
            remote_gid: gid.gid,
            remote_lid: port.port_attr.lid,
            local_psn: 7,
            remote_psn: 7,
            max_rd_atomic: 1,
            max_dest_rd_atomic: 1,
            traffic_class: 0,
        };
        first.connect(&config(second)).unwrap();
        second.connect(&config(first)).unwrap();
    }

    /// Consume exactly one authentic READ completion, leaving later CQEs untouched.
    async fn complete_one_read(poller: &DevicePoller, poll_loop: &mut PollLoop) {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut storage = CompletionBatch::<1>::new();
            loop {
                if let Some(completion) = poller.cq().poll_batch(&mut storage).unwrap().next() {
                    assert!(completion.info().succ());
                    assert!(completion.info().is_read());
                    poll_loop.dispatch(completion, &mut Vec::new());
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("READ must complete on the connected QP");
    }

    #[tokio::test]
    async fn successful_receive_queued_before_close_is_not_reposted() {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (poller, mut poll_loop) = manual_poller(device.context());
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let socket = register_idle_socket(&poller, device, &pool, &state);
        let peer = register_idle_socket(&poller, device, &pool, &state);
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        assert!(poll_loop.drain_incoming(false));
        socket.queue_pair.recv(pool.allocate(64).unwrap()).unwrap();
        poll_loop
            .conns
            .get_mut(&socket.queue_pair.qp_num())
            .unwrap()
            .flow
            .receive_posted();
        let mut message = pool.allocate(64).unwrap();
        message.set_len(64);
        message.fill(0x5a);
        peer.queue_pair
            .send_signaled(message, ruapc_rdma::ibv_send_flags::IBV_SEND_SIGNALED)
            .unwrap();

        let cq = poller.cq().clone();
        let mut storage = CompletionBatch::<1>::new();
        let mut batch = DispatchBatch::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(completion) = cq.poll_batch(&mut storage).unwrap().next() {
                    assert!(completion.info().succ());
                    if completion.info().is_recv() {
                        assert_eq!(completion.qp_num(), socket.queue_pair.qp_num());
                        // The provider has completed a receive, but shutdown wins
                        // before the poller consumes its CQ-issued proof. An ERR
                        // transition must not be followed by a new receive post.
                        socket.set_error();
                        poll_loop.dispatch(completion, &mut batch);
                        break;
                    }
                    poll_loop.dispatch(completion, &mut batch);
                } else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        })
        .await
        .expect("the connected peer must produce a successful receive");

        let conn = poll_loop
            .conns
            .get_mut(&socket.queue_pair.qp_num())
            .unwrap();
        assert!(
            conn.flow.flushed(),
            "a successful CQE consumed after closure must not replenish the receive ring"
        );
        assert!(
            batch.is_empty(),
            "closed connections must not dispatch messages"
        );
        conn.update_flow_control().unwrap();
        assert!(conn.ready_to_remove());
        poll_loop.maintain_connections(Instant::now(), false, false);
        assert!(!poll_loop.conns.contains_key(&socket.queue_pair.qp_num()));
        poll_loop.shutdown_cleanup();
    }

    #[tokio::test]
    async fn close_between_receive_health_check_and_post_is_flushed_by_poller() {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (poller, mut poll_loop) = manual_poller(device.context());
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let socket = register_idle_socket(&poller, device, &pool, &state);
        let peer = register_idle_socket(&poller, device, &pool, &state);
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        assert!(poll_loop.drain_incoming(false));

        // Reproduce the interleaving without a timing-dependent race: the poller
        // has checked health when another task requests closure. Its in-progress
        // receive post must still precede the provider's ERR transition.
        assert!(socket.state.is_ok());
        socket.set_error();
        assert!(
            !poll_loop
                .conns
                .get_mut(&socket.queue_pair.qp_num())
                .unwrap()
                .ready_to_remove(),
            "software closure cannot remove the QP before requesting its provider flush"
        );
        socket.queue_pair.recv(pool.allocate(64).unwrap()).unwrap();
        let conn = poll_loop
            .conns
            .get_mut(&socket.queue_pair.qp_num())
            .unwrap();
        conn.flow.receive_posted();
        assert!(!conn.ready_to_remove());
        conn.update_flow_control().unwrap();
        assert!(
            !conn.ready_to_remove(),
            "the posted receive still needs its CQE"
        );

        let cq = poller.cq().clone();
        let mut storage = CompletionBatch::<1>::new();
        let mut batch = DispatchBatch::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(completion) = cq.poll_batch(&mut storage).unwrap().next() {
                    assert_eq!(completion.qp_num(), socket.queue_pair.qp_num());
                    assert!(completion.info().is_recv());
                    assert_eq!(
                        completion.info().status,
                        ruapc_rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR
                    );
                    poll_loop.dispatch(completion, &mut batch);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("poller closure must flush the receive admitted before its ERR transition");

        let conn = poll_loop
            .conns
            .get_mut(&socket.queue_pair.qp_num())
            .unwrap();
        assert!(conn.flow.flushed());
        assert!(conn.ready_to_remove());
        assert!(batch.is_empty());
        poll_loop.maintain_connections(Instant::now(), false, false);
        assert!(!poll_loop.conns.contains_key(&socket.queue_pair.qp_num()));
        poll_loop.shutdown_cleanup();
    }

    #[tokio::test]
    async fn read_deadlines_bound_first_nic_admission_and_restore_unposted_targets() {
        use crate::remote_memory::{CopyOp, WriteTarget};
        use ruapc_memory::RemoteSpace;

        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let short = Duration::from_millis(50);
        for (read_timeout, parent_remaining, expected) in [
            (Some(short), None, ErrorKind::RdmaReadTimeout),
            (
                Some(Duration::from_secs(5)),
                Some(short),
                ErrorKind::Timeout,
            ),
            (None, Some(short), ErrorKind::Timeout),
        ] {
            let (poller, mut poll_loop) = manual_poller(device.context());
            let permits = Arc::new(tokio::sync::Semaphore::new(1));
            let socket = register_socket_with_config(
                &poller,
                device,
                &pool,
                &state,
                permits.clone(),
                SocketTestConfig {
                    read_timeout,
                    ..Default::default()
                },
            );
            let peer = register_idle_socket_with_read_permits(
                &poller,
                device,
                &pool,
                &state,
                permits.clone(),
            );
            connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
            assert!(poll_loop.drain_incoming(false));
            let mut source = pool.allocate(64).unwrap();
            source.set_len(64);
            source.fill(0x5a);
            let regions = [source.remote_buffer_info(&device.index()).unwrap()];
            let remote = RemoteSpace::new(&regions).unwrap();
            let ops = [CopyOp::new(0, 0, 64)];
            let mut other_destination = pool.allocate(64).unwrap();
            other_destination.set_len(64);
            let other_target =
                WriteTarget::new(BufferSet::try_new(vec![other_destination]).unwrap());
            let validated = CopyBatch::new(&ops).unwrap();
            let mut other_read =
                Box::pin(peer.read_into_target(&remote, &validated, other_target, None));
            assert!(futures_util::poll!(&mut other_read).is_pending());
            assert_eq!(peer.queue_pair.pending_read_count(), 1);

            let mut destination = pool.allocate(64).unwrap();
            destination.set_len(64);
            destination.fill(0xa5);
            let target = WriteTarget::new(BufferSet::try_new(vec![destination]).unwrap());
            let mut read = Box::pin(socket.read_into_target(
                &remote,
                &validated,
                target.clone(),
                parent_remaining.map(|remaining| Instant::now() + remaining),
            ));
            assert!(futures_util::poll!(&mut read).is_pending());
            assert_eq!(socket.queue_pair.pending_read_count(), 0);
            assert_eq!(permits.available_permits(), 0);

            // No completion or housekeeping pass runs: a batch waiting for its
            // first NIC permit has not entered the QP's READ ledger yet.
            let error = tokio::time::timeout(Duration::from_secs(2), read)
                .await
                .expect("READ admission must honor its own and the parent's deadline")
                .unwrap_err();
            assert_eq!(error.kind, expected);
            assert!(
                socket.state.is_ok(),
                "unposted expiry must not close the QP"
            );
            assert_eq!(socket.queue_pair.pending_read_count(), 0);
            let returned = WriteTarget::try_into_buffers(target).unwrap();
            assert_eq!(returned[0].as_slice(), &[0xa5; 64]);
            assert_eq!(permits.available_permits(), 0);
            assert_eq!(peer.queue_pair.pending_read_count(), 1);

            drop(other_read);
            poll_loop.shutdown_cleanup();
            drop(socket);
            assert_eq!(permits.available_permits(), 0);
            drop(peer);
            assert_eq!(permits.available_permits(), 1);
            drop(source);
        }
    }

    #[tokio::test]
    async fn read_deadlines_bound_partial_posting_without_releasing_dma() {
        use crate::remote_memory::{CopyOp, WriteTarget};
        use ruapc_memory::RemoteSpace;

        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let short = Duration::from_millis(500);
        for (read_timeout, parent_remaining, expected) in [
            (Some(short), None, ErrorKind::RdmaReadTimeout),
            (
                Some(Duration::from_secs(5)),
                Some(short),
                ErrorKind::Timeout,
            ),
        ] {
            let (poller, mut poll_loop) = manual_poller(device.context());
            let permits = Arc::new(tokio::sync::Semaphore::new(2));
            let socket = register_socket_with_config(
                &poller,
                device,
                &pool,
                &state,
                permits.clone(),
                SocketTestConfig {
                    read_timeout,
                    ..Default::default()
                },
            );
            let peer = register_idle_socket_with_read_permits(
                &poller,
                device,
                &pool,
                &state,
                permits.clone(),
            );
            connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
            assert!(poll_loop.drain_incoming(false));
            let mut source = pool.allocate(192).unwrap();
            source.set_len(192);
            source.fill(0x5a);
            let regions = [source.remote_buffer_info(&device.index()).unwrap()];
            let remote = RemoteSpace::new(&regions).unwrap();
            let ops = [
                CopyOp::new(0, 0, 64),
                CopyOp::new(64, 64, 64),
                CopyOp::new(128, 128, 64),
            ];
            // A separate destination pool lets the test observe DMA ownership
            // independently of the socket's pool and the remote source.
            let destination_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
            let destination_lifetime = Arc::downgrade(&destination_pool);
            let mut destination = destination_pool.allocate(192).unwrap();
            destination.set_len(192);
            let target = WriteTarget::new(BufferSet::try_new(vec![destination]).unwrap());
            drop(destination_pool);
            let validated = CopyBatch::new(&ops).unwrap();
            let mut read = Box::pin(socket.read_into_target(
                &remote,
                &validated,
                target.clone(),
                parent_remaining.map(|remaining| Instant::now() + remaining),
            ));
            assert!(futures_util::poll!(&mut read).is_pending());
            assert_eq!(socket.queue_pair.pending_read_count(), 2);

            let mut other_destination = pool.allocate(64).unwrap();
            other_destination.set_len(64);
            let other_target =
                WriteTarget::new(BufferSet::try_new(vec![other_destination]).unwrap());
            let other_ops = [CopyOp::new(0, 0, 64)];
            let other_validated = CopyBatch::new(&other_ops).unwrap();
            let mut other_read =
                Box::pin(peer.read_into_target(&remote, &other_validated, other_target, None));
            assert!(futures_util::poll!(&mut other_read).is_pending());
            assert_eq!(peer.queue_pair.pending_read_count(), 0);

            // The first completion gives SQ capacity back to this batch, but
            // its NIC permit goes to the other QP already waiting in the FIFO.
            complete_one_read(&poller, &mut poll_loop).await;
            assert!(futures_util::poll!(&mut other_read).is_pending());
            assert_eq!(peer.queue_pair.pending_read_count(), 1);
            assert!(futures_util::poll!(&mut read).is_pending());
            assert_eq!(socket.queue_pair.pending_read_count(), 1);
            assert_eq!(permits.available_permits(), 0);

            let error = tokio::time::timeout(Duration::from_secs(2), read)
                .await
                .expect("partial posting must stop waiting for NIC capacity at its deadline")
                .unwrap_err();
            assert_eq!(error.kind, expected);
            assert!(socket.state.is_ok());
            assert_eq!(socket.queue_pair.pending_read_count(), 1);
            assert_eq!(permits.available_permits(), 0);
            assert!(WriteTarget::try_into_buffers(target).is_none());
            assert!(destination_lifetime.upgrade().is_some());

            // Admission expiry alone does not prove this QP is faulty. Its
            // outstanding prefix is still subject to the poller's hard watchdog.
            poll_loop.maintain_connections(Instant::now() + Duration::from_secs(10), true, false);
            assert!(!socket.state.is_ok());
            assert_eq!(socket.queue_pair.pending_read_count(), 1);
            assert_eq!(permits.available_permits(), 0);

            drop(other_read);
            poll_loop.shutdown_cleanup();
            assert!(destination_lifetime.upgrade().is_some());
            assert_eq!(permits.available_permits(), 0);
            drop(socket);
            assert!(destination_lifetime.upgrade().is_none());
            assert_eq!(permits.available_permits(), 1);
            drop(peer);
            assert_eq!(permits.available_permits(), 2);
            drop(source);
        }
    }

    #[tokio::test]
    async fn parent_read_completion_deadline_preserves_the_hard_watchdog() {
        completion_watchdog_survives_waiter(false).await;
    }

    #[tokio::test]
    async fn cancelled_read_completion_preserves_the_hard_watchdog() {
        completion_watchdog_survives_waiter(true).await;
    }

    async fn completion_watchdog_survives_waiter(cancel_receiver: bool) {
        use crate::remote_memory::{CopyOp, WriteTarget};
        use ruapc_memory::RemoteSpace;

        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (poller, mut poll_loop) = manual_poller(device.context());
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let socket = register_socket_with_config(
            &poller,
            device,
            &pool,
            &state,
            permits.clone(),
            SocketTestConfig {
                read_timeout: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        );
        let peer =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        assert!(poll_loop.drain_incoming(false));
        let mut source = pool.allocate(64).unwrap();
        source.set_len(64);
        let regions = [source.remote_buffer_info(&device.index()).unwrap()];
        let remote = RemoteSpace::new(&regions).unwrap();
        let mut destination = pool.allocate(64).unwrap();
        destination.set_len(64);
        let target = WriteTarget::new(BufferSet::try_new(vec![destination]).unwrap());
        let ops = [CopyOp::new(0, 0, 64)];
        let validated = CopyBatch::new(&ops).unwrap();
        // An expired absolute request deadline cannot post even when both
        // resources are immediately available, and leaves the target reusable.
        let error = socket
            .read_into_target(&remote, &validated, target.clone(), Some(Instant::now()))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(socket.queue_pair.pending_read_count(), 0);
        assert_eq!(permits.available_permits(), 1);
        let mut read = Box::pin(socket.read_into_target(
            &remote,
            &validated,
            target.clone(),
            Some(Instant::now() + Duration::from_millis(50)),
        ));
        assert!(futures_util::poll!(&mut read).is_pending());
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        let now = Instant::now();
        if cancel_receiver {
            drop(read);
        } else {
            poll_loop.maintain_connections(now + Duration::from_millis(100), true, false);
            let error = tokio::time::timeout(Duration::from_secs(1), read)
                .await
                .expect("the parent deadline must settle an already-posted batch")
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::Timeout);
        }
        assert!(socket.state.is_ok());
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        assert_eq!(permits.available_permits(), 0);
        assert!(WriteTarget::try_into_buffers(target).is_none());

        // Taking the receiver at the parent deadline must not disable the
        // independent watchdog for READs still owned by this QP.
        poll_loop.maintain_connections(now + Duration::from_secs(2), true, false);
        assert!(!socket.state.is_ok());
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        assert_eq!(permits.available_permits(), 0);
        poll_loop.shutdown_cleanup();
        drop(socket);
        assert_eq!(permits.available_permits(), 1);
        drop(peer);
        drop(source);
    }

    #[tokio::test]
    async fn closing_a_posted_read_without_a_watchdog_wakes_before_any_completion() {
        use crate::remote_memory::{CopyOp, WriteTarget};
        use ruapc_memory::RemoteSpace;

        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (poller, mut poll_loop) = manual_poller(device.context());
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let socket =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        let peer =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        assert!(poll_loop.drain_incoming(false));
        let mut source = pool.allocate(64).unwrap();
        source.set_len(64);
        let regions = [source.remote_buffer_info(&device.index()).unwrap()];
        let remote = RemoteSpace::new(&regions).unwrap();
        let destination_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let destination_lifetime = Arc::downgrade(&destination_pool);
        let mut destination = destination_pool.allocate(64).unwrap();
        destination.set_len(64);
        let target = WriteTarget::new(BufferSet::try_new(vec![destination]).unwrap());
        drop(destination_pool);
        let ops = [CopyOp::new(0, 0, 64)];
        let validated = CopyBatch::new(&ops).unwrap();
        let mut read = Box::pin(socket.read_into_target(&remote, &validated, target.clone(), None));
        assert!(futures_util::poll!(&mut read).is_pending());
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        socket.set_error();
        let error = tokio::time::timeout(Duration::from_secs(1), read)
            .await
            .expect("closing a QP must wake its READ even with read_timeout_ms = 0")
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        assert_eq!(permits.available_permits(), 0);
        assert!(WriteTarget::try_into_buffers(target).is_none());
        assert!(destination_lifetime.upgrade().is_some());
        poll_loop.shutdown_cleanup();
        assert!(destination_lifetime.upgrade().is_some());
        drop(socket);
        assert!(destination_lifetime.upgrade().is_none());
        assert_eq!(permits.available_permits(), 1);
        drop(peer);
        drop(source);
    }

    #[tokio::test]
    async fn shutdown_returns_unpolled_read_credits_only_after_qp_destruction() {
        use crate::remote_memory::{CopyOp, WriteTarget};
        use ruapc_memory::RemoteSpace;

        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();

        // Exercise an unpublished connection, an ordinary registered connection,
        // a partially completed batch, and fully completed work (no double refund).
        for (completed, incoming) in [(0, true), (0, false), (1, false), (2, false)] {
            let (poller, mut poll_loop) = manual_poller(device.context());
            let permits = Arc::new(tokio::sync::Semaphore::new(1));
            let socket = register_idle_socket_with_read_permits(
                &poller,
                device,
                &pool,
                &state,
                permits.clone(),
            );
            let peer = register_idle_socket_with_read_permits(
                &poller,
                device,
                &pool,
                &state,
                permits.clone(),
            );
            connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
            if !incoming {
                assert!(poll_loop.drain_incoming(false));
            }
            let mut source = pool.allocate(128).unwrap();
            source.set_len(128);
            source.fill(0x5a);
            let regions = [source.remote_buffer_info(&device.index()).unwrap()];
            let remote = RemoteSpace::new(&regions).unwrap();
            let mut destination = pool.allocate(128).unwrap();
            destination.set_len(128);
            let target = WriteTarget::new(BufferSet::try_new(vec![destination]).unwrap());
            let ops = [CopyOp::new(0, 0, 64), CopyOp::new(64, 64, 64)];
            let validated = CopyBatch::new(&ops).unwrap();
            let mut read =
                Box::pin(socket.read_into_target(&remote, &validated, target.clone(), None));
            assert!(futures_util::poll!(&mut read).is_pending());
            assert_eq!(socket.queue_pair.pending_read_count(), 1);
            assert_eq!(permits.available_permits(), 0);

            for count in 0..completed {
                complete_one_read(&poller, &mut poll_loop).await;
                assert_eq!(socket.queue_pair.pending_read_count(), 0);
                if count == 0 {
                    // The second WR was waiting for the NIC credit returned by
                    // the first CQE. The semaphore has assigned that credit to
                    // its waiter already, before the future is polled again.
                    assert_eq!(permits.available_permits(), 0);
                    assert!(futures_util::poll!(&mut read).is_pending());
                    assert_eq!(socket.queue_pair.pending_read_count(), 1);
                    assert_eq!(permits.available_permits(), 0);
                } else {
                    assert_eq!(permits.available_permits(), 1);
                }
            }
            if completed == 2 {
                tokio::time::timeout(Duration::from_secs(1), read)
                    .await
                    .expect("processed READ completions must settle their waiter")
                    .unwrap();
                let ((), buffers) = target.checkout(|_| Ok(())).unwrap();
                let buffers = buffers.into_buffers();
                assert_eq!(&buffers[0][..], &source[..]);
            } else {
                poll_loop.shutdown_cleanup();
                let error = tokio::time::timeout(Duration::from_secs(1), read)
                    .await
                    .expect("shutdown must settle the READ waiter")
                    .unwrap_err();
                assert_eq!(error.kind, ErrorKind::ConnectionClosed);
                assert_eq!(socket.queue_pair.pending_read_count(), 1);
                assert_eq!(
                    permits.available_permits(),
                    0,
                    "failing the waiter cannot end DMA"
                );
            }
            poll_loop.shutdown_cleanup();
            assert!(socket.read_credits.is_closed());
            assert!(peer.read_credits.is_closed());
            drop(socket);
            assert_eq!(
                permits.available_permits(),
                1,
                "QP destruction must refund only remaining READs"
            );
            drop(peer);
            assert_eq!(permits.available_permits(), 1);
            // The remote source outlives both provider QPs, including unpolled READs.
            drop(source);
        }
    }

    #[tokio::test]
    async fn shutdown_closes_live_connections_before_refunding_incoming_reads() {
        use crate::remote_memory::{CopyOp, WriteTarget};
        use ruapc_memory::RemoteSpace;
        use std::task::{Context, Wake, Waker};

        struct RefundProbe {
            live: Arc<RdmaSocket>,
            woke: AtomicBool,
        }
        impl Wake for RefundProbe {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                assert!(
                    self.live.read_credits.is_closed(),
                    "a stopped CQ must not admit new READs from a refund"
                );
                self.woke.store(true, Ordering::Release);
            }
        }

        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (poller, mut poll_loop) = manual_poller(device.context());
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let live =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        assert!(poll_loop.drain_incoming(false));
        let incoming =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        connect_read_pair(device, &incoming.queue_pair, &live.queue_pair);
        let mut source = pool.allocate(64).unwrap();
        source.set_len(64);
        let regions = [source.remote_buffer_info(&device.index()).unwrap()];
        let remote = RemoteSpace::new(&regions).unwrap();
        let mut destination = pool.allocate(64).unwrap();
        destination.set_len(64);
        let target = WriteTarget::new(BufferSet::try_new(vec![destination]).unwrap());
        let ops = [CopyOp::new(0, 0, 64)];
        let validated = CopyBatch::new(&ops).unwrap();
        let mut read = Box::pin(incoming.read_into_target(&remote, &validated, target, None));
        assert!(futures_util::poll!(&mut read).is_pending());
        drop(read);
        assert_eq!(incoming.queue_pair.pending_read_count(), 1);
        drop(incoming); // The inbox now owns the only socket reference.

        let probe = Arc::new(RefundProbe {
            live,
            woke: AtomicBool::new(false),
        });
        let waker = Waker::from(probe.clone());
        let mut context = Context::from_waker(&waker);
        let mut waiting = Box::pin(permits.acquire());
        assert!(std::future::Future::poll(waiting.as_mut(), &mut context).is_pending());
        poll_loop.shutdown_cleanup();
        assert!(
            probe.woke.load(Ordering::Acquire),
            "incoming QP destruction must wake the NIC waiter"
        );
        drop(waiting.await.unwrap());
        assert_eq!(permits.available_permits(), 1);
        drop(waker);
        drop(probe);
        drop(source);
    }

    #[tokio::test]
    async fn a_read_posted_after_the_shutdown_scan_does_not_wait_for_a_stopped_poller() {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (poller, mut poll_loop) = manual_poller(device.context());
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let socket =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        let peer =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        let mut source = pool.allocate(64).unwrap();
        source.set_len(64);
        let remote = source.remote_buffer_info(&device.index()).unwrap();
        let mut destination = pool.allocate(64).unwrap();
        destination.set_len(64);
        let (mut posting, receiver) = socket
            .queue_pair
            .prepare_reads(
                vec![destination],
                vec![ruapc_rdma::ReadRequest {
                    remote_addr: remote.addr,
                    rkey: remote.key.rkey,
                    segments: vec![ruapc_rdma::ReadSegment {
                        buffer: 0,
                        offset: 0,
                        len: 64,
                    }],
                }],
            )
            .unwrap();
        let device_permit = permits.acquire().await.unwrap();
        assert!(socket.state.is_ok());
        // Pause after the final health check, before publishing to the QP. The
        // cleanup's failure scan sees no batch and will never consume another CQE.
        poll_loop.shutdown_cleanup();
        assert_eq!(socket.queue_pair.pending_read_count(), 0);
        // Providers such as RXE accept WRs on an ERR QP. Publish through the real
        // ownership map, without repeating admission or consuming its CQEs.
        match socket.queue_pair.post_read(&mut posting) {
            Ok(()) => {
                device_permit.forget();
                assert_eq!(socket.queue_pair.pending_read_count(), 1);
                socket.register_read_batch(&posting, None, None);
                let error =
                    tokio::time::timeout(Duration::from_secs(1), socket.await_read_batch(receiver))
                        .await
                        .expect("late posting must not strand its waiter after shutdown")
                        .unwrap_err();
                assert_eq!(error.kind, ErrorKind::ConnectionClosed);
                drop(posting);
                assert_eq!(socket.queue_pair.pending_read_count(), 1);
                assert_eq!(permits.available_permits(), 0);
            }
            Err(_) => {
                // A provider may instead reject the late WR. Ownership must roll
                // back and the still-unposted permit must return through RAII.
                assert_eq!(socket.queue_pair.pending_read_count(), 0);
                assert_eq!(posting.remaining(), 1);
                assert_eq!(posting.cancel().unwrap()[0].len(), 64);
                drop(device_permit);
                assert_eq!(permits.available_permits(), 1);
            }
        }
        drop(socket);
        assert_eq!(permits.available_permits(), 1);
        drop(peer);
        drop(source);
    }

    fn mark_dirty(poll_loop: &mut PollLoop, qp_num: u32) {
        PollLoop::mark_dirty(
            poll_loop.conns.get_mut(&qp_num).unwrap(),
            &mut poll_loop.dirty_qps,
        );
    }

    /// Fill both pending stages without posting to the fixture's unconnected QP.
    /// Byte markers make FIFO order observable without involving frame decoding.
    fn fill_blocked_send_stages(
        poll_loop: &mut PollLoop,
        socket: &RdmaSocket,
        pool: &Arc<BufferPool>,
    ) {
        assert!(matches!(
            socket.state.try_acquire(),
            SendPermit::Granted { .. }
        ));
        let capacity = socket.pending_sender.max_capacity();
        for batch in 0..2 {
            for index in 0..capacity {
                let mut buffer = pool.allocate(1).unwrap();
                buffer.set_len(1);
                buffer[0] = (batch * capacity + index) as u8;
                socket.pending_sender.try_send(buffer).unwrap();
            }
            if batch == 0 {
                poll_loop
                    .conns
                    .get_mut(&socket.queue_pair.qp_num())
                    .unwrap()
                    .drain_pending();
            }
        }
        assert_eq!(socket.pending_sender.capacity(), 0);
        assert_eq!(
            poll_loop.conns[&socket.queue_pair.qp_num()]
                .pending_sends
                .len(),
            capacity
        );
    }

    #[tokio::test]
    async fn pending_send_backlog_stays_bounded_and_cancellation_preserves_capacity() {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let (poller, mut poll_loop) = manual_poller(device.context());
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let socket = register_idle_socket(&poller, device, &pool, &state);
        let qp_num = socket.queue_pair.qp_num();
        assert!(poll_loop.drain_incoming(false));
        fill_blocked_send_stages(&mut poll_loop, &socket, &pool);

        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            ..Default::default()
        };
        let mut sending = Box::pin(socket.send(&mut meta, &27, &state));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());

        // Repeated sender wakes and housekeeping must not move the ingress
        // backlog into an ever-growing FIFO while the SEND window is exhausted.
        for _ in 0..8 {
            poll_loop.maintain_connections(Instant::now(), false, false);
            assert_eq!(poll_loop.conns[&qp_num].pending_sends.len(), 4);
            assert_eq!(socket.pending_sender.capacity(), 0);
            assert!(futures_util::poll!(sending.as_mut()).is_pending());
        }
        drop(sending);
        drop(response);
        assert_eq!(state.waiter.pending_count(), 0);
        assert_eq!(socket.pending_sender.capacity(), 0);

        // Releasing one staging slot permits exactly one ingress dequeue. The
        // cancelled producer must not retain the newly available channel slot.
        let conn = poll_loop.conns.get_mut(&qp_num).unwrap();
        assert_eq!(conn.pending_sends.pop_front().unwrap()[0], 0);
        conn.drain_pending();
        assert_eq!(
            conn.pending_sends
                .iter()
                .map(|frame| frame[0])
                .collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert_eq!(socket.pending_sender.capacity(), 1);
        let slot = socket.pending_sender.try_reserve().unwrap();
        assert_eq!(socket.pending_sender.capacity(), 0);
        drop(slot);
        assert_eq!(socket.pending_sender.capacity(), 1);
        poll_loop.shutdown_cleanup();
    }

    #[tokio::test]
    async fn full_pending_send_backlog_wakes_on_close_and_poller_shutdown() {
        for shutdown in [false, true] {
            let devices = make_rdma_devices();
            let device = &devices.devices()[0];
            let (poller, mut poll_loop) = manual_poller(device.context());
            let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
            let (state, _stop) = State::create(
                Router::default(),
                &SocketPoolConfig {
                    rdma: None,
                    ..Default::default()
                },
            )
            .unwrap();
            let socket = register_idle_socket(&poller, device, &pool, &state);
            let qp_num = socket.queue_pair.qp_num();
            assert!(poll_loop.drain_incoming(false));
            fill_blocked_send_stages(&mut poll_loop, &socket, &pool);
            let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
            let mut meta = MsgMeta {
                msgid,
                flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
                ..Default::default()
            };
            let mut sending = Box::pin(socket.send(&mut meta, &27, &state));
            assert!(futures_util::poll!(sending.as_mut()).is_pending());

            if shutdown {
                poll_loop.shutdown_cleanup();
                assert!(socket.pending_sender.is_closed());
            } else {
                socket.set_error();
                // Keep the receiver and both full stages alive: logical closure
                // must wake admission without depending on queue destruction.
                assert!(!socket.pending_sender.is_closed());
                assert_eq!(socket.pending_sender.capacity(), 0);
                assert_eq!(poll_loop.conns[&qp_num].pending_sends.len(), 4);
            }
            let error = tokio::time::timeout(Duration::from_secs(1), sending.as_mut())
                .await
                .expect("closed pending admission must wake while a socket owner remains")
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::ConnectionClosed);
            drop(sending);
            drop(response);
            poll_loop.shutdown_cleanup();
        }
    }

    #[tokio::test]
    async fn pending_send_refill_spends_remaining_window_in_one_maintenance_pass() {
        for aggregate in [false, true] {
            let devices = make_rdma_devices();
            let device = &devices.devices()[0];
            let (poller, mut poll_loop) = manual_poller(device.context());
            let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
            let (state, _stop) = State::create(
                Router::default(),
                &SocketPoolConfig {
                    rdma: None,
                    ..Default::default()
                },
            )
            .unwrap();
            let capacity = if aggregate { 2 } else { 1 };
            let socket = register_socket_with_config(
                &poller,
                device,
                &pool,
                &state,
                Arc::new(tokio::sync::Semaphore::new(1)),
                SocketTestConfig {
                    capacity,
                    window: 2,
                    aggregate,
                    ..Default::default()
                },
            );
            let peer = register_idle_socket(&poller, device, &pool, &state);
            connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
            assert!(poll_loop.drain_incoming(false));
            for _ in 0..2 {
                peer.queue_pair.recv(pool.allocate(64).unwrap()).unwrap();
            }

            let qp_num = socket.queue_pair.qp_num();
            for batch in 0..2 {
                for index in 0..capacity {
                    let mut buffer = pool.allocate(1).unwrap();
                    buffer.set_len(1);
                    buffer[0] = (batch * capacity + index) as u8;
                    socket.pending_sender.try_send(buffer).unwrap();
                }
                if batch == 0 {
                    poll_loop.conns.get_mut(&qp_num).unwrap().drain_pending();
                }
            }
            assert_eq!(socket.pending_sender.capacity(), 0);
            assert_eq!(poll_loop.conns[&qp_num].pending_sends.len(), capacity);

            // No CQ polling occurs between batches. The second SEND credit must
            // drain the ingress stage after the first FIFO batch is posted.
            poll_loop.maintain_connections(Instant::now(), false, false);
            assert!(poll_loop.conns[&qp_num].pending_sends.is_empty());
            assert_eq!(socket.pending_sender.capacity(), capacity);
            assert_eq!(socket.state.try_acquire(), SendPermit::Full);
            assert!(socket.state.is_ok());

            let mut received = Vec::new();
            let mut sends_completed = 0;
            let mut cursor = ruapc_rdma::CompletionCursor::default();
            let mut storage = CompletionBatch::<1>::new();
            tokio::time::timeout(Duration::from_secs(5), async {
                while received.len() < 2 || sends_completed < 2 {
                    if let Some(completion) = poller.cq().poll_batch(&mut storage).unwrap().next() {
                        assert!(completion.info().succ());
                        if completion.qp_num() == qp_num {
                            assert!(!completion.info().is_recv());
                            poll_loop.dispatch(completion, &mut Vec::new());
                            sends_completed += 1;
                        } else {
                            assert_eq!(completion.qp_num(), peer.queue_pair.qp_num());
                            assert!(completion.info().is_recv());
                            // Consume genuine CQ-issued ownership evidence while
                            // retaining the received bytes for the FIFO assertion.
                            let completed =
                                peer.queue_pair.complete(completion, &mut cursor).unwrap();
                            let buffer = completed.buffer.unwrap().into_single().unwrap();
                            received.push(buffer[..completed.wc.byte_len as usize].to_vec());
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            })
            .await
            .expect("both pending batches must arrive on the connected peer");
            let expected: Vec<Vec<u8>> = (0..2)
                .map(|batch| {
                    (0..capacity)
                        .map(|index| (batch * capacity + index) as u8)
                        .collect()
                })
                .collect();
            assert_eq!(received, expected);
            poll_loop.shutdown_cleanup();
        }
    }

    #[tokio::test]
    async fn dirty_maintenance_is_bounded_and_survives_replacement() {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let (poller, mut poll_loop) = manual_poller(device.context());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let first = register_idle_socket(&poller, device, &buffer_pool, &state);
        let second = register_idle_socket(&poller, device, &buffer_pool, &state);
        let first_qp = first.queue_pair.qp_num();
        let second_qp = second.queue_pair.qp_num();
        assert!(poll_loop.drain_incoming(false));
        assert_eq!(poll_loop.dirty_qps.len(), 2);
        poll_loop.maintain_dirty_connections(Instant::now(), false);
        assert!(poll_loop.dirty_qps.is_empty());

        // A pending send on an unrelated connection must stay untouched when
        // only the first connection has completion work. Exhaust its window
        // first so a later full scan can drain the message without posting it.
        assert!(matches!(
            second.state.try_acquire(),
            SendPermit::Granted { .. }
        ));
        second
            .pending_sender
            .try_send(buffer_pool.allocate(64).unwrap())
            .unwrap();
        mark_dirty(&mut poll_loop, first_qp);
        mark_dirty(&mut poll_loop, first_qp);
        assert_eq!(poll_loop.dirty_qps, [first_qp]);
        poll_loop.maintain_dirty_connections(Instant::now(), false);
        assert!(poll_loop.conns[&second_qp].pending_sends.is_empty());
        poll_loop.maintain_connections(Instant::now(), false, false);
        assert_eq!(poll_loop.conns[&second_qp].pending_sends.len(), 1);
        assert!(
            poll_loop.dirty_qps.is_empty(),
            "a full window must not force retry polling"
        );

        // A receive allocation deficit stays listed once across both kinds of
        // maintenance, including more CQEs before the next timed retry.
        poll_loop.conns.get_mut(&first_qp).unwrap().recv_deficit = 1;
        mark_dirty(&mut poll_loop, first_qp);
        poll_loop.maintain_dirty_connections(Instant::now(), false);
        mark_dirty(&mut poll_loop, first_qp);
        assert_eq!(poll_loop.dirty_qps, [first_qp]);
        poll_loop.maintain_connections(Instant::now(), false, false);
        assert_eq!(poll_loop.dirty_qps, [first_qp]);
        poll_loop.conns.get_mut(&first_qp).unwrap().recv_deficit = 0;

        first.set_error();
        poll_loop.maintain_dirty_connections(Instant::now(), false);
        assert!(!poll_loop.conns.contains_key(&first_qp));
        assert!(poll_loop.dirty_qps.is_empty());
        drop(first);
        poll_loop
            .drain_completions(poller.cq(), &mut CompletionBatch::new(), &mut Vec::new())
            .unwrap();
        let replacement = register_idle_socket(&poller, device, &buffer_pool, &state);
        let replacement_qp = replacement.queue_pair.qp_num();
        assert!(poll_loop.drain_incoming(false));
        assert_eq!(poll_loop.dirty_qps, [replacement_qp]);
        poll_loop.maintain_dirty_connections(Instant::now(), false);
        assert!(!poll_loop.conns[&replacement_qp].dirty);
        assert!(poll_loop.dirty_qps.is_empty());
        poll_loop.shutdown_cleanup();
    }

    #[tokio::test]
    async fn identical_wrids_are_routed_by_their_provider_qp_numbers() {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let (poller, mut poll_loop) = manual_poller(device.context());
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let sockets = [
            register_idle_socket(&poller, device, &pool, &state),
            register_idle_socket(&poller, device, &pool, &state),
        ];
        assert_ne!(
            sockets[0].queue_pair.qp_num(),
            sockets[1].queue_pair.qp_num()
        );
        assert_eq!(
            sockets[0].queue_pair.identity().first_sequence(),
            sockets[1].queue_pair.identity().first_sequence()
        );

        for socket in &sockets {
            let mut init = ibv_qp_attr {
                qp_state: ibv_qp_state::IBV_QPS_INIT,
                port_num: device.info().ports[0].port_num,
                ..Default::default()
            };
            let mask = ibv_qp_attr_mask::IBV_QP_STATE
                | ibv_qp_attr_mask::IBV_QP_PORT
                | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
                | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
            socket.queue_pair.modify(&mut init, mask.0 as _).unwrap();
            socket.queue_pair.recv(pool.allocate(64).unwrap()).unwrap();
            socket.set_error();
        }
        // This manual poller has consumed nothing yet. Account for each real
        // receive before publishing the connections to its local registry.
        for incoming in &mut poller.shared.inner.lock().unwrap().incoming {
            incoming.recv_submitted = 1;
        }
        assert!(poll_loop.drain_incoming(false));
        // The poller owns provider ERR transitions, after receive posting ends.
        poll_loop.maintain_connections(Instant::now(), false, false);
        let mut seen = std::collections::HashSet::new();
        let mut wrid = None;
        let mut storage = CompletionBatch::<8>::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while seen.len() < sockets.len() {
            for completion in poller.cq().poll_batch(&mut storage).unwrap() {
                assert!(completion.info().is_recv());
                assert_eq!(
                    completion.info().status,
                    ruapc_rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR
                );
                if let Some(first) = wrid {
                    assert_eq!(completion.info().wr_id, first);
                } else {
                    wrid = Some(completion.info().wr_id);
                }
                assert!(seen.insert(completion.qp_num()));
                poll_loop.dispatch(completion, &mut Vec::new());
            }
            assert!(
                Instant::now() < deadline,
                "both receive flushes must arrive"
            );
            std::thread::yield_now();
        }
        for socket in &sockets {
            assert!(
                poll_loop
                    .conns
                    .get_mut(&socket.queue_pair.qp_num())
                    .unwrap()
                    .ready_to_remove()
            );
        }
        poll_loop.maintain_dirty_connections(Instant::now(), false);
        assert!(poll_loop.conns.is_empty());
        assert!(poll_loop.dirty_qps.is_empty());
        poll_loop.shutdown_cleanup();
    }

    #[tokio::test]
    async fn early_flush_completion_waits_for_registration_and_settles_receive() {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
        let (poller, mut poll_loop) = manual_poller(device.context());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let mut attrs = ruapc_rdma::ibv_qp_init_attr {
            qp_type: ruapc_rdma::ibv_qp_type::IBV_QPT_RC,
            cap: ruapc_rdma::ibv_qp_cap {
                max_send_wr: 4,
                max_recv_wr: 4,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let qp = QueuePair::create(device.pd(), poller.cq(), &mut attrs, device.index()).unwrap();
        let port = device.info().ports[0].port_num;
        let mut init = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_INIT,
            port_num: port,
            ..Default::default()
        };
        let mask = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        qp.modify(&mut init, mask.0 as _).unwrap();

        let nic = RdmaNicInfo {
            device: device.info().name.clone(),
            port_num: port,
            gid_index: 0,
            ip: None,
        };
        let (pending_sender, pending_receiver) = tokio::sync::mpsc::channel(1);
        let socket = Arc::new(RdmaSocket::new(
            poller
                .reserve(Demand::connection(&RdmaConnectionConfig {
                    qp: RdmaQueuePairConfig::default(),
                    recv_queue_len: 1,
                    max_msg_size: 1024,
                    max_large_msg_size: 0,
                    traffic_class: 0,
                }))
                .unwrap()
                .bind(qp),
            buffer_pool.clone(),
            pending_sender,
            poller.waker(),
            RdmaSocketConfig {
                max_msg_size: 1024,
                max_large_msg_size: 0,
                max_inflight_large_bytes: 128 << 20,
                send_window: 2,
                path: RdmaPathInfo {
                    local: nic.clone(),
                    remote: nic,
                    same_connectivity_domain: true,
                },
                read_timeout: None,
                read_permits: Arc::new(tokio::sync::Semaphore::new(1)),
                bandwidth_limiter: Arc::new(RdmaBandwidthLimiter::new(
                    device.info().name.clone(),
                    port,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                )),
                sq_read_cap: 1,
            },
        ));
        let (state, _stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        let supervisor = TaskSupervisor::create();
        let counts = Arc::new(vec![AtomicUsize::new(0)]);
        let ring_total = Arc::new(AtomicUsize::new(0));
        let registration = RegisterConn {
            socket: socket.clone(),
            state,
            pending_receiver,
            recv_submitted: 1,
            recv_buf_size: 1024,
            send_window: 2,
            msg_aggregation: false,
            supervisor_guard: supervisor.start_async_task(),
            ring_reservation: RingReservation::add(&ring_total, 1024).0,
            conn_count_guard: ConnCountGuard::acquire(&counts, 0),
        };
        let buffer = buffer_pool.allocate(1024).unwrap();
        let cq = poller.cq().clone();
        let (posted_tx, posted_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let (polled_tx, polled_rx) = mpsc::channel();
        let (dispatched_tx, dispatched_rx) = mpsc::channel();
        let timeout = Duration::from_secs(5);

        std::thread::scope(|scope| {
            let poller = &poller;
            let socket = &socket;
            let registrar = scope.spawn(move || {
                poller.register(registration, || {
                    socket.queue_pair.recv(buffer)?;
                    // INIT -> ERR flushes a real posted receive without needing a
                    // peer or routable network. The socket is closed for teardown.
                    socket.set_error();
                    set_qp_error(socket);
                    posted_tx.send(()).map_err(|error| {
                        Error::new(ErrorKind::ConnectionClosed, error.to_string())
                    })?;
                    resume_rx
                        .recv_timeout(timeout)
                        .map_err(|error| Error::new(ErrorKind::Timeout, error.to_string()))?;
                    Ok(())
                })
            });
            posted_rx.recv_timeout(timeout).unwrap();
            assert!(!poller.shared.has_incoming.load(Ordering::Acquire));
            assert!(poller.shared.inner.try_lock().is_err());

            let router = scope.spawn(|| {
                let mut storage = CompletionBatch::<1>::new();
                let mut batch = DispatchBatch::new();
                let deadline = Instant::now() + timeout;
                loop {
                    if let Some(completion) = cq.poll_batch(&mut storage).unwrap().next() {
                        assert!(completion.info().is_recv());
                        assert_eq!(
                            completion.info().status,
                            ruapc_rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR
                        );
                        polled_tx.send(()).unwrap();
                        poll_loop.dispatch(completion, &mut batch);
                        dispatched_tx.send(()).unwrap();
                        return;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "receive did not produce a flush CQE"
                    );
                    std::thread::yield_now();
                }
            });
            polled_rx.recv_timeout(timeout).unwrap();
            let early_dispatch = dispatched_rx.recv_timeout(Duration::from_millis(50));
            // Always release the registrar before asserting, including when a
            // regression lets the CQE escape through the empty-inbox fast path.
            resume_tx.send(()).unwrap();
            registrar.join().unwrap().unwrap();
            router.join().unwrap();
            assert!(matches!(
                early_dispatch,
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
        });

        let conn = poll_loop
            .conns
            .get_mut(&socket.queue_pair.qp_num())
            .expect("the early CQE must find the newly published connection");
        conn.start_error_flush();
        assert!(
            conn.ready_to_remove(),
            "the flush CQE must settle the receive ledger"
        );
        poll_loop.shutdown_cleanup();
        // Removing poller state cannot release CQ capacity while another socket
        // owner may still submit work. Destruction plus a later drain does.
        assert_eq!(poller.shared.budget.snapshot().1, 1);
        drop(socket);
        assert_eq!(poller.shared.budget.snapshot().1, 1);
        poll_loop
            .drain_completions(&cq, &mut CompletionBatch::new(), &mut Vec::new())
            .unwrap();
        assert_eq!(poller.shared.budget.snapshot(), (0, 0));
        assert_eq!(counts[0].load(Ordering::Acquire), 0);
        assert_eq!(ring_total.load(Ordering::Acquire), 0);
    }
}
