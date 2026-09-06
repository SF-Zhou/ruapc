//! Dedicated per-device RDMA completion poll thread
//!
//! Each RDMA device gets one shared completion queue (send + recv for
//! every connection on that device) and one dedicated OS thread that polls
//! it:
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
//! Every QP receives a route slot from its CQ at creation. A work request
//! carries that slot and a sequence number that continues across slot reuse.
//! Routing is a plain `Vec` index plus a sequence-floor check, with no QPN
//! hash lookup or separate poller identity allocator. The QP keeps its slot
//! reserved until destruction, including while registrations are in transit.
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

mod conn;
mod dispatch;
mod flow;

use std::{
    io::{Read as _, Write as _},
    os::unix::io::AsRawFd as _,
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use foldhash::fast::RandomState;
use ruapc_rdma::{CompChannel, Completion, CompletionBatch, CompletionQueue, poll_readable2};

use conn::ConnState;
use dispatch::{DispatchBatch, Dispatcher, MAX_DISPATCH_BATCH};

use super::RdmaSocket;
use crate::{Buffer, Error, ErrorKind, Result, State, task::TaskSupervisorGuard};

/// Wakes the poll thread out of its idle `poll(2)` sleep.
#[derive(Clone, Debug)]
pub struct PollerWaker(Arc<UnixStream>);

impl PollerWaker {
    /// Wakes the poll thread. Best-effort: if the pipe is full the thread is
    /// already scheduled to wake up.
    pub fn wake(&self) {
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
/// inbox and the shutdown flag. Route allocation belongs to the CQ.
struct PollerShared {
    inner: Mutex<SharedInner>,
    /// Fast-path hint that `inner.incoming` is non-empty; written under
    /// the `inner` lock, read lock-free by the poll thread.
    has_incoming: AtomicBool,
    /// Set (under the `inner` lock) when the poller shuts down; after the
    /// poll thread's final inbox drain no registration can be lost.
    shutdown: AtomicBool,
}

#[derive(Default)]
struct SharedInner {
    /// Registered connections awaiting pickup by the poll thread.
    incoming: Vec<Incoming>,
}

struct Incoming {
    conn: Box<RegisterConn>,
    budget: BudgetGuard,
}

/// CQ budget for a connection about to be registered. Dropping an
/// unconsumed reservation releases the budget; the QP owns its route.
pub struct ConnReservation {
    shared: Arc<PollerShared>,
    budget: BudgetGuard,
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
    /// Sum of (send + recv) queue depths registered on the shared CQ.
    wr_budget: Arc<AtomicU32>,
    cq_capacity: u32,
}

/// Tunables for the poll thread, taken from `RdmaSocketPoolConfig`.
#[derive(Debug, Clone, Copy)]
pub struct PollerConfig {
    /// Shared CQ capacity (entries).
    pub cq_len: u32,
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
        // asked for more. The clamped value also becomes the connection
        // budget (`cq_capacity`), so admission control stays consistent
        // with the actual CQ size.
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
            has_incoming: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
        });
        let handle = tokio::runtime::Handle::current();

        let thread = {
            let cq = cq.clone();
            let comp_channel = comp_channel.clone();
            let shared = shared.clone();
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
                        conns: Vec::new(),
                        unack_cq_events: 0,
                    }
                    .run();
                })
                .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?
        };

        Ok(Self {
            cq,
            shared,
            waker: PollerWaker(Arc::new(wake_tx)),
            thread: Some(thread),
            wr_budget: Arc::new(AtomicU32::new(0)),
            cq_capacity: cq_len,
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

    /// Reserves CQ budget for a new connection.
    ///
    /// `qp_depth` is the connection's total queue depth (send + recv work
    /// requests); the reservation fails if the shared CQ cannot absorb it.
    pub fn reserve(&self, qp_depth: u32) -> Result<ConnReservation> {
        let budget = self.wr_budget.clone();
        if budget
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(qp_depth)
                    .filter(|total| *total <= self.cq_capacity)
            })
            .is_err()
        {
            return Err(Error::new(
                ErrorKind::Overloaded,
                format!(
                    "shared CQ capacity exhausted: {} + {qp_depth} > {} (raise rdma.polling.device_cq_len)",
                    budget.load(Ordering::Acquire),
                    self.cq_capacity
                ),
            ));
        }
        let budget = BudgetGuard {
            budget,
            depth: qp_depth,
        };

        if self.shared.shutdown.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                "RDMA poll thread is not running".into(),
            ));
        }
        Ok(ConnReservation {
            shared: self.shared.clone(),
            budget,
        })
    }

    /// Posts the initial receives and publishes their owner as one transaction.
    /// An early CQE's routing miss waits for this transaction before retrying.
    pub fn register(
        &self,
        reservation: ConnReservation,
        conn: RegisterConn,
        post_receives: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let qp = &conn.socket.queue_pair;
        if !Arc::ptr_eq(&reservation.shared, &self.shared)
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
            inner.incoming.push(Incoming {
                conn: Box::new(conn),
                budget: reservation.budget,
            });
            self.shared.has_incoming.store(true, Ordering::Release);
        }
        self.waker.wake();
        Ok(())
    }
}

struct BudgetGuard {
    budget: Arc<AtomicU32>,
    depth: u32,
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        self.budget.fetch_sub(self.depth, Ordering::AcqRel);
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
            .field("wr_budget", &self.wr_budget.load(Ordering::Acquire))
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
    /// Connections indexed by their slot.
    conns: Vec<Option<ConnState>>,
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

    /// Housekeeping cadence when no completions arrive. Housekeeping is
    /// O(connections) (pending drain, flow control, teardown checks and
    /// their clock reads), so it must not run on every spin iteration.
    const HOUSEKEEPING_INTERVAL: Duration = Duration::from_micros(100);

    /// Cadence of the RDMA READ timeout sweep. Coarse on purpose: read
    /// timeouts are measured in seconds, and per-request timers are
    /// deliberately avoided (a background scan of the small in-flight
    /// read maps costs nearly nothing).
    const READ_SWEEP_INTERVAL: Duration = Duration::from_millis(100);

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
        let mut next_read_sweep = Instant::now();
        let mut last_dump = Instant::now();

        loop {
            let progressed = self.drain_completions(&cq, &mut wcs, &mut batch)?;

            // This O(connections) pass runs after progress or at the periodic
            // cadence, never on every empty iteration of the spin window.
            let now = Instant::now();
            if progressed || now >= next_housekeeping {
                next_housekeeping = now + Self::HOUSEKEEPING_INTERVAL;
                if self.shared.shutdown.load(Ordering::Acquire) {
                    return Ok(());
                }
                self.drain_incoming(false);
                if tracing::enabled!(tracing::Level::DEBUG)
                    && last_dump.elapsed() >= Self::DUMP_INTERVAL
                {
                    last_dump = Instant::now();
                    self.dump_connections();
                }
                let sweep_reads = now >= next_read_sweep;
                if sweep_reads {
                    next_read_sweep = now + Self::READ_SWEEP_INTERVAL;
                }
                self.maintain_connections(now, sweep_reads);
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
        let mut progressed = false;
        loop {
            let count = self.poll_completions(cq, wcs, batch)?;
            progressed |= count > 0;
            if count < wcs.capacity() {
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
        for conn in self.conns.iter().flatten() {
            tracing::debug!(
                "conn dump: qp={} ok={} pending={} flow={:?}",
                conn.socket.queue_pair.qp_num(),
                conn.socket.state.is_ok(),
                conn.pending_sends.len(),
                conn.flow,
            );
        }
    }

    fn maintain_connections(&mut self, now: Instant, sweep_reads: bool) {
        for slot in 0..self.conns.len() {
            let Some(conn) = self.conns[slot].as_mut() else {
                continue;
            };
            if sweep_reads {
                conn.sweep_read_timeouts(now);
            }
            conn.drain_pending();
            if conn.recv_deficit > 0 {
                conn.retry_recv_deficit();
            }
            if let Err(e) = conn.update_flow_control() {
                tracing::error!("flow control update error: {e}");
            }
            if conn.ready_to_remove() {
                // The QP keeps its route reserved even if another owner holds
                // the socket after poller teardown.
                self.conns[slot] = None;
            }
        }
    }

    /// Sleep until a CQ notification, explicit wake, or housekeeping timeout.
    /// Draining both sources before returning avoids a permanently readable fd.
    fn wait_for_event(&mut self) -> ruapc_rdma::Result<bool> {
        let (cq_ready, wake_ready) = poll_readable2(
            self.comp_channel.fd().as_raw_fd(),
            self.wake_rx.as_raw_fd(),
            Self::IDLE_TIMEOUT_MS,
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

    /// Moves newly registered connections from the shared inbox into their
    /// slots.
    fn drain_incoming(&mut self, routing_miss: bool) {
        if !routing_miss && !self.shared.has_incoming.load(Ordering::Acquire) {
            return;
        }
        let drained = {
            let mut inner = self.shared.inner.lock().unwrap();
            self.shared.has_incoming.store(false, Ordering::Release);
            std::mem::take(&mut inner.incoming)
        };
        for incoming in drained {
            let slot = incoming.conn.socket.queue_pair.send_route().slot();
            if self.conns.len() <= slot {
                self.conns.resize_with(slot + 1, || None);
            }
            debug_assert!(self.conns[slot].is_none(), "poller slot {slot} occupied");
            self.conns[slot] = Some(ConnState::new(*incoming.conn, incoming.budget));
        }
    }

    fn dispatch(&mut self, wc: Completion<'_>, batch: &mut DispatchBatch) {
        let id = wc.info().wr_id;
        let slot = id.get_slot();
        if let Some(Some(conn)) = self.conns.get_mut(slot)
            && conn.route.contains(id)
        {
            conn.handle_wc(wc, batch);
            return;
        }
        // Initial receives and inbox publication hold the same mutex. Bypass
        // the empty-inbox hint: a registrar can still be posting the ring.
        // Normal completions never acquire this lock.
        self.drain_incoming(true);
        if let Some(Some(conn)) = self.conns.get_mut(slot)
            && conn.route.contains(id)
        {
            conn.handle_wc(wc, batch);
        } else {
            tracing::warn!("dropping completion for unknown connection route: {wc:?}");
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
            incoming.conn.socket.set_error();
            incoming.conn.socket.fail_read_batches();
            // Inbox entries have not opened their metric registration yet,
            // but a sender may already have bound a waiter to the socket.
            incoming.conn.state.connection_closed(
                incoming.conn.socket.conn_id,
                &Error::new(
                    ErrorKind::ConnectionClosed,
                    "rdma poll thread stopped".into(),
                ),
            );
        }
        drop(drained);
        for conn in self.conns.iter().flatten() {
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
        if self.unack_cq_events > 0 {
            self.cq.ack_events(self.unack_cq_events);
            self.unack_cq_events = 0;
        }
    }
}

/// Lazily-created poller shards, keyed by device name.
///
/// A device may run several (shared CQ + poll thread) shards; connections
/// are assigned round-robin so their completion processing spreads across
/// cores. All shards share one [`Dispatcher`] (created with the first
/// shard), so the pool runs a single fixed set of dispatch worker tasks.
#[derive(Default)]
pub struct DevicePollers(Mutex<PollersInner>);

#[derive(Default)]
struct PollersInner {
    devices: std::collections::HashMap<String, DeviceShards, RandomState>,
    /// Shared dispatch worker pool; started lazily so worker tasks only
    /// exist once RDMA is actually used (and inside a runtime).
    dispatcher: Option<Dispatcher>,
}

#[derive(Default)]
struct DeviceShards {
    shards: Vec<Arc<DevicePoller>>,
    next: usize,
}

impl DevicePollers {
    /// Returns a poller shard for the given device (round-robin across
    /// `shard_count` shards), starting it if necessary.
    pub fn get_or_start(
        &self,
        device: &super::RdmaDevice,
        config: PollerConfig,
        shard_count: u32,
    ) -> Result<Arc<DevicePoller>> {
        let name = device.info().name.clone();
        debug_assert!(shard_count > 0);
        let shard_count = shard_count as usize;
        let mut inner = self.0.lock().unwrap();
        let dispatcher = inner
            .dispatcher
            .get_or_insert_with(|| Dispatcher::start(config.dispatch_workers))
            .clone();
        let entry = inner.devices.entry(name.clone()).or_default();
        let index = entry.next % shard_count;
        entry.next = entry.next.wrapping_add(1);
        if let Some(poller) = entry.shards.get(index) {
            return Ok(poller.clone());
        }
        debug_assert_eq!(index, entry.shards.len());
        tracing::info!(
            "starting RDMA poll thread {name}.{index} (existing shards: {})",
            entry.shards.len()
        );
        let poller = Arc::new(DevicePoller::start(
            device.context(),
            &format!("{name}.{index}"),
            config,
            dispatcher,
        )?);
        entry.shards.push(poller.clone());
        Ok(poller)
    }
}

impl std::fmt::Debug for DevicePollers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevicePollers").finish()
    }
}

#[cfg(test)]
mod tests;
