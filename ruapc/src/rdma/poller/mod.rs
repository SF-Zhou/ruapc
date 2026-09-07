//! Dedicated per-device RDMA completion poll thread
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
//! each owning one SPSC queue; the workers walk the `[4B len][message]`
//! frames and parse them on tokio worker threads. Routing is sticky
//! (spill on pressure, see [`Dispatcher`]), the enqueue is a non-blocking
//! push, and the poll thread issues no `tokio::spawn` on this path. Only
//! when every worker is saturated does it degrade to spawning a one-shot
//! task per batch, so it still never blocks.

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

use budget::{CqBudget, Demand};
use conn::ConnState;
use dispatch::{DispatchBatch, Dispatcher, MAX_DISPATCH_BATCH};

use super::RdmaSocket;
use crate::{Buffer, Error, ErrorKind, Result, State, task::TaskSupervisorGuard};

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
            || !Arc::ptr_eq(qp.send_cq(), &self.cq)
            || !Arc::ptr_eq(qp.recv_cq(), &self.cq)
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
    /// Their second-scale timers do not need a scan after every CQ drain.
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

    /// Drain a burst completely while bounding each dispatch batch. Both the
    /// normal drain and the arm/poll race check use the same CQE routing path.
    fn drain_completions(
        &mut self,
        cq: &CompletionQueue,
        wcs: &mut CompletionBatch<64>,
        batch: &mut DispatchBatch,
    ) -> ruapc_rdma::Result<bool> {
        let retired = self.shared.budget.take_retired();
        let mut progressed = false;
        loop {
            let count = self.poll_completions(cq, wcs, batch)?;
            progressed |= count > 0;
            if count == 0 {
                self.shared.budget.release_drained(retired);
                break;
            }
            if retired.is_empty() && count < wcs.capacity() {
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
        drop(drained);
        for conn in self.conns.values() {
            conn.socket.set_error();
            // Nobody will poll the flush completions after this thread
            // exits: resolve the waiting tasks now. The memory holds stay
            // parked in the batches and are released when the socket (and
            // its QP, first) is dropped.
            conn.socket.fail_read_batches();
        }
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
mod tests;
