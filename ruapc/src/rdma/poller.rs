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
//! Every work request carries a connection *tag* (poller slot index +
//! generation) in its `wr_id`, stamped into the QP at registration time.
//! Routing a completion is a plain `Vec` index — no `qp_num` hash lookup,
//! and no orphan window: the slot is reserved before the first work
//! request is posted.
//!
//! # Zero-parse poll thread
//!
//! The poll thread never looks inside received bytes. Flow control is
//! accounted per *work completion* (one receive WC = one credit,
//! regardless of how many messages the buffer carries), so received
//! buffers accumulate into per-drain batches that are routed to a fixed
//! pool of long-lived dispatch worker tasks (`rdma.dispatch_workers`),
//! each owning one SPSC queue; the workers walk the `[4B len][message]`
//! frames and parse them on tokio worker threads. Routing is sticky
//! (spill on pressure, see [`Dispatcher`]), the enqueue is a non-blocking
//! push, and the poll thread issues no `tokio::spawn` on this path. Only
//! when every worker is saturated does it degrade to spawning a one-shot
//! task per batch, so it still never blocks.

use std::{
    collections::VecDeque,
    io::{Read as _, Write as _},
    os::unix::io::AsRawFd as _,
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use foldhash::fast::RandomState;
use ruapc_rdma::{
    CompChannel, CompletionQueue, WRType, WrBuffers, ibv_send_flags, ibv_wc, poll_readable2,
};

use super::{RdmaSocket, SendPermit};
use crate::{Buffer, Error, ErrorKind, Message, Result, Socket, State, task::TaskSupervisorGuard};

/// Size of the per-frame header: a big-endian u32 frame length.
///
/// Every RDMA send is a sequence of `[4B frame_len][4B meta_len][meta]
/// [payload]` frames — usually one. Uniform framing makes messages
/// self-delimiting, so aggregation is plain concatenation and the receive
/// path has a single parse loop.
pub(crate) const FRAME_HEADER: usize = 4;

/// Upper bound for one aggregated send; packing more rarely helps and would
/// only add head-of-line latency for the packed messages.
const MAX_AGG_BYTES: usize = 64 * 1024;

/// Messages up to this size are copied out of the receive buffer so the
/// buffer recycles immediately into the repost cache instead of traveling
/// (zero-copy) into the dispatched message.
///
/// Two reasons:
/// - **Starvation immunity**: zero-copy dispatch holds the receive buffer
///   until user code drops the response/request, and the repost must
///   allocate a fresh buffer from the shared pool. Under pool exhaustion
///   that allocation fails, the receive ring shrinks, and once it empties
///   the connection can no longer receive ACKs or responses — the freed
///   capacity the pool is waiting for never arrives (deadlock spiral).
///   With copy-out the ring sustains itself with zero pool traffic.
/// - **Cost**: copying <= 1 KiB (~50ns) is cheaper than the pool
///   allocate/free round-trip it replaces, and the copy replaces a
///   16 KiB+ registered chunk held for the message's lifetime with a
///   right-sized heap allocation.
///
/// Large messages keep the zero-copy path: their copy cost would dominate
/// and their volume is bounded by the send window.
const SMALL_MSG_COPY_MAX: usize = 1024;

/// Number of bits of a connection tag holding the slot index; the
/// remaining [`WRID::TAG_BITS`] bits hold the slot's generation.
const SLOT_BITS: u32 = 14;
/// Maximum number of live connections per poller shard.
const MAX_SLOTS: usize = 1 << SLOT_BITS;

/// Packs a slot index and its generation into a `wr_id` connection tag.
fn conn_tag(slot: u16, generation: u8) -> u32 {
    (u32::from(generation) << SLOT_BITS) | u32::from(slot)
}

/// Splits a `wr_id` connection tag into (slot index, generation).
fn split_tag(tag: u32) -> (usize, u8) {
    (
        (tag & (MAX_SLOTS as u32 - 1)) as usize,
        (tag >> SLOT_BITS) as u8,
    )
}

/// One received buffer awaiting dispatch: the connection's shared state,
/// the socket it arrived on and the raw `[4B len][message]` frames.
type DispatchItem = (Arc<State>, Socket, Bytes);

/// Received buffers of one completion batch, dispatched together. Handing
/// batches (instead of single buffers) to the queue amortizes the enqueue
/// and — more importantly — the worker wakeup over an entire CQ drain:
/// per-buffer enqueueing measurably collapses throughput because nearly
/// every message then pays one parked-task wakeup.
type DispatchBatch = Vec<DispatchItem>;

/// Flush threshold for a dispatch batch (bounds latency and memory).
const MAX_DISPATCH_BATCH: usize = 256;

/// Batches a sticky worker may have queued before the router spills to
/// the next worker.
///
/// Queueing a backlog on the current (busy, hence running and cache-hot)
/// worker is cheaper than engaging another one: a drained worker is a
/// parked task, and waking it is a cross-thread wake through tokio's
/// remote-injection path — benchmarks show eager spilling (threshold 4)
/// quadruples context switches and costs ~30% QPS at high load. Spilling
/// only under real pressure keeps one hot worker per poll thread in the
/// common case — wakeups coalesce exactly as they would with a dedicated
/// dispatcher task — while still growing parallelism when a worker
/// genuinely falls behind (a threshold of 16 batches is multiple
/// milliseconds of parse backlog).
const SPILL_BACKLOG: usize = 16;

/// Batches queued per worker before the dispatcher considers it saturated
/// and moves on (ultimately to the one-shot spawn fallback). Bounds the
/// standing backlog per worker without a bounded channel.
const MAX_WORKER_BACKLOG: usize = 32;

/// One dispatch worker endpoint: an SPSC queue plus the number of batches
/// sent to it that it has not finished processing yet.
struct DispatchWorker {
    tx: tokio::sync::mpsc::UnboundedSender<DispatchBatch>,
    /// Incremented by the sender before each send, decremented by the
    /// worker *after* processing a batch — `0` therefore means "drained
    /// and done", i.e. sending now cannot queue behind anything.
    backlog: Arc<std::sync::atomic::AtomicUsize>,
}

/// Hands received buffers from the poll threads to a fixed pool of
/// long-lived dispatch worker tasks, each owning one SPSC queue.
///
/// Dispatching (frame walk, parse, request spawn / response oneshot wake)
/// from the poll thread would serialize that work on the shard and — for
/// spawns — go through tokio's remote-injection path, whose shared lock
/// becomes the global throughput ceiling. Spawning a task per batch has
/// the same problem: every spawn from the poll thread is a remote inject
/// plus a task allocation.
///
/// Routing is *home worker + spill on pressure* — not blind round-robin,
/// and deliberately not a shared MPMC queue:
///
/// - Each poll thread has a private home worker; while it keeps up
///   (backlog below [`SPILL_BACKLOG`]) it receives every batch: it stays
///   cache-hot, and its wakeups coalesce exactly like a dedicated
///   dispatcher task's would (a send to a busy worker is just a
///   lock-free push, no wake at all).
/// - Only when it falls genuinely behind do batches spill to the next
///   worker, so parallelism grows with load instead of rotating every
///   batch through a different cold, parked task. (A shared MPMC queue
///   does the opposite — each send wakes the longest-parked consumer —
///   which benchmarked 20-30% slower at high load.)
/// - With every worker past the spill threshold, batches queue (bounded
///   by [`MAX_WORKER_BACKLOG`]) on the least-loaded worker; only beyond
///   that does the poll thread degrade to a one-shot `tokio::spawn` per
///   batch, so it never blocks and no buffer is dropped.
pub(crate) struct Dispatcher {
    workers: Arc<[DispatchWorker]>,
    /// This clone's *home* worker. Every batch is offered to the home
    /// worker first and only spills forward for that single batch, so a
    /// poll thread always returns to its own worker once a burst is over.
    /// A *sticky cursor* that moves on spill was measurably worse: two
    /// poll threads whose cursors land on the same worker herd there —
    /// both then spill in lockstep and keep sharing one worker, halving
    /// dispatch throughput.
    home: usize,
    /// Hands every clone a distinct home, so poll threads stick to
    /// *different* workers instead of piling onto the same one.
    next_home: Arc<std::sync::atomic::AtomicUsize>,
}

impl Clone for Dispatcher {
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
            home: self.next_home.fetch_add(1, Ordering::Relaxed) % self.workers.len(),
            next_home: self.next_home.clone(),
        }
    }
}

impl Dispatcher {
    /// Spawns `workers` long-lived dispatch worker tasks. Must be called
    /// from within a tokio runtime. The workers exit once every
    /// `Dispatcher` clone (one per poll thread, plus the owning pool's)
    /// has been dropped.
    pub fn start(workers: u32) -> Self {
        let workers: Arc<[DispatchWorker]> = (0..workers.max(1))
            .map(|_| {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DispatchBatch>();
                let backlog = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let worker_backlog = backlog.clone();
                tokio::spawn(async move {
                    while let Some(batch) = rx.recv().await {
                        run_dispatch_batch(batch);
                        worker_backlog.fetch_sub(1, Ordering::Release);
                    }
                });
                DispatchWorker { tx, backlog }
            })
            .collect();
        Self {
            workers,
            home: 0,
            next_home: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        }
    }

    /// Enqueues the accumulated buffers of one CQ drain for parsing.
    /// Called from the poll threads; never blocks.
    fn flush(&mut self, batch: &mut DispatchBatch) {
        if batch.is_empty() {
            return;
        }
        let batch = std::mem::take(batch);

        // Stay with the home worker while it is not too far behind;
        // otherwise spill — for this batch only — to the next worker
        // below the spill threshold.
        let n = self.workers.len();
        for i in 0..n {
            let idx = (self.home + i) % n;
            if self.workers[idx].backlog.load(Ordering::Acquire) < SPILL_BACKLOG {
                self.send(idx, batch);
                return;
            }
        }

        // Every worker is backlogged: queue (bounded) on the least loaded.
        let (idx, backlog) = self
            .workers
            .iter()
            .enumerate()
            .map(|(idx, worker)| (idx, worker.backlog.load(Ordering::Acquire)))
            .min_by_key(|(_, backlog)| *backlog)
            .expect("at least one dispatch worker");
        if backlog < MAX_WORKER_BACKLOG {
            self.send(idx, batch);
            return;
        }

        // Workers saturated beyond the backlog cap: fall back to a
        // one-shot task doing the same work rather than blocking.
        tokio::spawn(async move { run_dispatch_batch(batch) });
    }

    /// Sends one batch to the chosen worker, keeping its backlog counter
    /// consistent. Send failures only happen when the runtime is shutting
    /// down (the worker task is gone); the messages are dropped.
    fn send(&self, idx: usize, batch: DispatchBatch) {
        let worker = &self.workers[idx];
        worker.backlog.fetch_add(1, Ordering::AcqRel);
        if let Err(e) = worker.tx.send(batch) {
            worker.backlog.fetch_sub(1, Ordering::AcqRel);
            tracing::debug!("dispatch worker gone; dropping {} buffer(s)", e.0.len());
        }
    }
}

/// Handles one batch of dispatched buffers on a runtime worker thread.
fn run_dispatch_batch(batch: DispatchBatch) {
    for item in batch {
        dispatch_item(item);
    }
}

/// Walks the `[4B len][message]` frames of one received buffer, invoking
/// `f` with each frame (a zero-copy slice of the refcounted buffer).
fn for_each_frame(frames: &Bytes, mut f: impl FnMut(Bytes)) {
    let mut offset = 0;
    while offset < frames.len() {
        let Some(header) = frames.get(offset..offset + FRAME_HEADER) else {
            tracing::error!("truncated frame header at {offset}");
            return;
        };
        let frame_len = u32::from_be_bytes(header.try_into().unwrap()) as usize;
        let start = offset + FRAME_HEADER;
        let Some(end) = start
            .checked_add(frame_len)
            .filter(|end| *end <= frames.len())
        else {
            tracing::error!("truncated frame ({frame_len}B) at {offset}");
            return;
        };
        f(frames.slice(start..end));
        offset = end;
    }
}

/// Handles one dispatched buffer: frame walk + parse + routing to the
/// router (requests) or waiter (responses) on a runtime worker thread.
fn dispatch_item((state, socket, frames): DispatchItem) {
    for_each_frame(&frames, |frame| match Message::parse(frame) {
        Ok(msg) => {
            if let Err(e) = state.handle_recv(&socket, msg) {
                tracing::error!("Failed to handle message: {e}");
            }
        }
        Err(e) => tracing::error!("Failed to parse message: {e}"),
    });
}

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
}

/// State shared between registrars and the poll thread: the slot
/// allocator, the registration inbox and the shutdown flag.
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
    /// Freed slot indices available for reuse.
    free_slots: Vec<u16>,
    /// Current generation per ever-allocated slot; bumped on release so
    /// stale completions of a previous occupant can never be attributed
    /// to a new connection reusing the slot.
    generations: Vec<u8>,
    /// Registered connections awaiting pickup by the poll thread.
    incoming: Vec<Incoming>,
}

struct Incoming {
    slot: u16,
    generation: u8,
    conn: Box<RegisterConn>,
    budget: BudgetGuard,
}

/// Releases a slot for reuse, invalidating its previous generation.
fn release_slot(shared: &PollerShared, slot: u16) {
    let mut inner = shared.inner.lock().unwrap();
    let generation = &mut inner.generations[slot as usize];
    *generation = generation.wrapping_add(1);
    inner.free_slots.push(slot);
}

/// A reserved poller slot (plus CQ budget) for a connection about to be
/// registered. Dropping an unconsumed reservation releases both.
pub struct ConnReservation {
    shared: Arc<PollerShared>,
    slot: u16,
    generation: u8,
    budget: Option<BudgetGuard>,
}

impl ConnReservation {
    /// The connection tag to stamp into the QP's work request IDs
    /// (`QueuePair::set_wr_tag`) before posting anything.
    pub fn tag(&self) -> u32 {
        conn_tag(self.slot, self.generation)
    }
}

impl Drop for ConnReservation {
    fn drop(&mut self) {
        // A consumed reservation (budget moved into the inbox) frees
        // nothing; an abandoned one returns the slot.
        if self.budget.is_some() {
            release_slot(&self.shared, self.slot);
        }
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

    /// Reserves a poller slot and CQ budget for a new connection.
    ///
    /// `qp_depth` is the connection's total queue depth (send + recv work
    /// requests); the reservation fails if the shared CQ cannot absorb it.
    /// The returned reservation's [`tag`](ConnReservation::tag) must be
    /// stamped into the QP before any work request is posted.
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
                ErrorKind::RdmaSendFailed,
                format!(
                    "shared CQ capacity exhausted: {} + {qp_depth} > {} (raise rdma.device_cq_len)",
                    budget.load(Ordering::Acquire),
                    self.cq_capacity
                ),
            ));
        }
        let budget = BudgetGuard {
            budget,
            depth: qp_depth,
        };

        let mut inner = self.shared.inner.lock().unwrap();
        if self.shared.shutdown.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorKind::RdmaSendFailed,
                "RDMA poll thread is not running".into(),
            ));
        }
        let slot = match inner.free_slots.pop() {
            Some(slot) => slot,
            None => {
                if inner.generations.len() >= MAX_SLOTS {
                    return Err(Error::new(
                        ErrorKind::RdmaSendFailed,
                        format!("poller connection slots exhausted ({MAX_SLOTS})"),
                    ));
                }
                inner.generations.push(0);
                (inner.generations.len() - 1) as u16
            }
        };
        let generation = inner.generations[slot as usize];
        drop(inner);

        Ok(ConnReservation {
            shared: self.shared.clone(),
            slot,
            generation,
            budget: Some(budget),
        })
    }

    /// Registers a connection under a previously reserved slot.
    pub fn register(&self, mut reservation: ConnReservation, conn: RegisterConn) -> Result<()> {
        let budget = reservation
            .budget
            .take()
            .expect("connection reservation used twice");
        {
            let mut inner = self.shared.inner.lock().unwrap();
            if self.shared.shutdown.load(Ordering::Acquire) {
                // Put the budget back so the reservation drop frees the slot.
                reservation.budget = Some(budget);
                return Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    "RDMA poll thread is not running".into(),
                ));
            }
            inner.incoming.push(Incoming {
                slot: reservation.slot,
                generation: reservation.generation,
                conn: Box::new(conn),
                budget,
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

/// Flow control configuration for RDMA operations.
#[derive(Debug)]
struct FlowConfig {
    /// Number of unacknowledged receive completions before triggering an
    /// acknowledgment.
    ack_threshold: u32,
    /// Maximum number of unacknowledged receive completions allowed.
    ack_max_limit: u32,
}

impl FlowConfig {
    /// Derives the ACK cadence from the peer's send window (both sides
    /// compute the same negotiated value, `recv_queue_len / 2`).
    ///
    /// The threshold must stay below the window: the peer can never have
    /// more than `window` unacknowledged data WRs in flight, so a larger
    /// threshold would never fire and every credit return would wait for
    /// the 5s keepalive ACK (a de-facto stall). Half the window keeps two
    /// ACK batches per window worth of headroom. With the default window
    /// (32) this reduces to the classic 16/32 cadence.
    ///
    /// `ack_max_limit` caps outstanding standalone ACK WRs; the receive
    /// ring reserves its non-window half for exactly these, so the send
    /// window is the bound.
    fn for_window(send_window: u32) -> Self {
        Self {
            ack_threshold: (send_window / 2).max(1),
            ack_max_limit: send_window.max(2),
        }
    }
}

/// Receive-side statistics for one connection; all counters are per work
/// completion (= per receive-ring buffer), not per logical message.
#[derive(Debug, Default)]
struct RecvStats {
    submitted: u64,
    completed: u64,
    imm_received: u64,
    imm_acked: u64,
    data_received: u64,
    data_acked: u64,
}

/// Send-side statistics for one connection; all counters are per work
/// request.
#[derive(Debug, Default)]
struct SendStats {
    data_completed: u64,
    data_confirmed: u64,
    ack_submitted: u64,
    ack_completed: u64,
    ack_confirmed: u64,
}

/// Per-connection state owned by the poll thread.
///
/// Field order matters for teardown: handlers holding buffers come before
/// `socket` so buffers are released before the QP can be destroyed.
struct ConnState {
    /// Generation of the occupied slot; completions tagged with another
    /// generation belonged to a previous occupant and are dropped.
    generation: u8,
    recv: RecvStats,
    send: SendStats,
    last_ack_timestamp: Instant,
    flow: FlowConfig,
    /// Window-blocked framed sends in FIFO order.
    pending_sends: VecDeque<Buffer>,
    pending_receiver: tokio::sync::mpsc::Receiver<Buffer>,
    /// Next send-queue id that has not been swept yet (see `sweep_sq`).
    sq_swept: u64,
    /// Negotiated receive buffer size (`max_msg_size`).
    recv_buf_size: usize,
    /// Whether to aggregate window-blocked sends.
    msg_aggregation: bool,
    /// Recycled receive buffers: pure-ACK completions and copied-out small
    /// messages return their buffer here so the repost skips the shared
    /// pool.
    recv_buf_cache: Vec<Buffer>,
    /// Receive work requests that could not be reposted (transient buffer
    /// pool exhaustion); retried on subsequent iterations instead of
    /// failing the connection.
    recv_deficit: u64,
    socket: Arc<RdmaSocket>,
    state: Arc<State>,
    _budget: BudgetGuard,
    _supervisor_guard: TaskSupervisorGuard,
    _ring_reservation: RingReservation,
}

impl ConnState {
    fn new(reg: RegisterConn, generation: u8, budget: BudgetGuard) -> Self {
        Self {
            generation,
            recv: RecvStats {
                submitted: reg.recv_submitted,
                ..Default::default()
            },
            send: SendStats::default(),
            last_ack_timestamp: Instant::now(),
            flow: FlowConfig::for_window(reg.send_window),
            pending_sends: VecDeque::new(),
            pending_receiver: reg.pending_receiver,
            sq_swept: 0,
            recv_buf_size: reg.recv_buf_size,
            msg_aggregation: reg.msg_aggregation,
            recv_buf_cache: Vec::new(),
            recv_deficit: 0,
            socket: reg.socket,
            state: reg.state,
            _budget: budget,
            _supervisor_guard: reg.supervisor_guard,
            _ring_reservation: reg.ring_reservation,
        }
    }

    /// Handles one work completion for this connection.
    fn handle_wc(&mut self, wc: &ibv_wc, batch: &mut DispatchBatch) {
        if !wc.is_recv() {
            // Sweep unsignaled data sends completed before this SQ
            // completion (RC SQs complete in post order): reclaim their
            // buffers and count each as one completed data WR. Only plain
            // data sends are ever unsignaled, so every swept buffer is a
            // data WR.
            let id = wc.wr_id.get_id();
            for swept in self.sq_swept..id {
                if self.socket.queue_pair.take_send_buffer(swept).is_some() {
                    self.send.data_completed += 1;
                }
            }
            self.sq_swept = self.sq_swept.max(id + 1);
        }

        let buffer = self.socket.queue_pair.take_buffer(&wc.wr_id);
        let result = if wc.is_recv() {
            // Receive WRs always post a single buffer.
            self.handle_recv_completion(wc, buffer.and_then(WrBuffers::into_single), batch)
        } else {
            self.handle_send_completion(wc, buffer)
        };
        if result.is_err() {
            self.socket.set_error();
        }
    }

    fn handle_recv_completion(
        &mut self,
        wc: &ibv_wc,
        buffer: Option<Buffer>,
        batch: &mut DispatchBatch,
    ) -> Result<()> {
        self.recv.completed += 1;

        if !wc.succ() {
            self.socket.set_error();
            return Err(Error::new(
                ErrorKind::RdmaRecvFailed,
                format!(
                    "recv completion error: {:?}, {:?}",
                    wc.status, wc.vendor_err
                ),
            ));
        }

        // Immediate data (ACK credit counters) can arrive standalone or
        // piggybacked on a data send.
        if let Some(ack) = wc.imm() {
            self.send.data_confirmed += u64::from(ack & 0xFFFF);
            self.send.ack_confirmed += u64::from(ack >> 16);
        }

        if let Some(mut buf) = buffer {
            buf.set_len(wc.byte_len as usize);
            if buf.is_empty() {
                // Standalone ACK: the buffer is untouched, recycle it.
                self.recv.imm_received += 1;
                self.cache_recv_buf(buf);
            } else {
                // One receive completion = one flow control credit, no
                // matter how many frames the buffer carries: the credit
                // stands for the receive-ring buffer, which is consumed
                // exactly once per WC. The frames are walked and parsed by
                // the dispatch workers, never here.
                self.recv.data_received += 1;
                let frames = if buf.len() <= SMALL_MSG_COPY_MAX {
                    // Copy small buffers out and recycle the receive
                    // buffer; see `SMALL_MSG_COPY_MAX` for why.
                    let copied = Bytes::copy_from_slice(&buf);
                    self.cache_recv_buf(buf);
                    copied
                } else {
                    Bytes::from_owner(buf)
                };
                batch.push((self.state.clone(), Socket::from(&self.socket), frames));
            }
        } else if wc.imm().is_some() {
            self.recv.imm_received += 1;
        } else {
            self.recv.data_received += 1;
        }

        // Post a new recv buffer to replace the consumed one, preferring a
        // recycled buffer over a shared pool round trip.
        let new_buf = match self.recv_buf_cache.pop() {
            Some(buf) => buf,
            None => match self.socket.rdmabuf_pool.allocate(self.recv_buf_size) {
                Ok(buf) => buf,
                Err(e) => {
                    // Transient pool exhaustion: don't fail the connection,
                    // retry the repost during housekeeping. The ring shrank
                    // by one in the meantime.
                    if self.recv_deficit == 0 {
                        tracing::warn!("recv repost allocation failed (will retry): {e}");
                    }
                    self.recv_deficit += 1;
                    return Ok(());
                }
            },
        };
        self.socket
            .queue_pair
            .recv(new_buf)
            .map_err(|e| Error::new(ErrorKind::RdmaRecvFailed, e.to_string()))?;
        self.recv.submitted += 1;
        Ok(())
    }

    /// Retries receive reposts that previously failed on allocation.
    fn retry_recv_deficit(&mut self) {
        while self.recv_deficit > 0 {
            let buf = match self.recv_buf_cache.pop() {
                Some(buf) => buf,
                None => match self.socket.rdmabuf_pool.allocate(self.recv_buf_size) {
                    Ok(buf) => buf,
                    Err(_) => return,
                },
            };
            match self.socket.queue_pair.recv(buf) {
                Ok(()) => {
                    self.recv.submitted += 1;
                    self.recv_deficit -= 1;
                }
                Err(e) => {
                    tracing::error!("recv repost failed: {e}");
                    self.socket.set_error();
                    return;
                }
            }
        }
    }

    /// Keeps a bounded number of receive-sized buffers for repost reuse.
    fn cache_recv_buf(&mut self, buf: Buffer) {
        const MAX_CACHED: usize = 8;
        if buf.capacity() >= self.recv_buf_size && self.recv_buf_cache.len() < MAX_CACHED {
            self.recv_buf_cache.push(buf);
        }
    }

    fn handle_send_completion(&mut self, wc: &ibv_wc, buffer: Option<WrBuffers>) -> Result<()> {
        match wc.wr_id.get_type() {
            // RDMA one-sided operation: hand the buffer back to the
            // caller. Reads consume no peer receive buffer, so they take
            // part in no flow control accounting.
            WRType::Read => {
                if let Some((_, sender)) = self.socket.rdma_completions.remove(&wc.wr_id) {
                    // Read WRs always post a single buffer.
                    let _ = sender.send((wc.status, buffer.and_then(WrBuffers::into_single)));
                    return Ok(());
                }
            }
            // A buffer-less immediate send is a standalone ACK; one with a
            // buffer is a data send with a piggybacked ACK, which lives in
            // the data ledger.
            WRType::SendImm if buffer.is_none() => self.send.ack_completed += 1,
            _ => self.send.data_completed += 1,
        }

        if wc.succ() {
            Ok(())
        } else {
            tracing::error!("send completion error: {wc:?}");
            Err(Error::new(
                ErrorKind::RdmaSendFailed,
                format!("send completion error: {wc:?}"),
            ))
        }
    }

    /// Moves window-blocked sends from the mpsc channel into the FIFO.
    fn drain_pending(&mut self) {
        while let Ok(buf) = self.pending_receiver.try_recv() {
            self.pending_sends.push_back(buf);
        }
    }

    /// Updates flow control state, flushes window-unblocked pending sends
    /// and emits acknowledgments when thresholds are reached.
    fn update_flow_control(&mut self) -> Result<()> {
        if !self.socket.state.is_ok() {
            // Pending sends never acquired a credit; just drop them.
            self.pending_sends.clear();
            return Ok(());
        }

        // One credit per data WR, returned once the WR completed locally
        // (buffer reclaimed) *and* the peer acknowledged the matching
        // receive completion.
        let finished = std::cmp::min(self.send.data_completed, self.send.data_confirmed);

        // Liveness diagnostics: a pending send that stays window-blocked
        // for seconds indicates a flow control stall (peer ACKs missing or
        // completion accounting gone wrong).
        if !self.pending_sends.is_empty() && self.last_ack_timestamp.elapsed().as_secs() >= 2 {
            tracing::warn!(
                "flow stall: qp={} pending={} finished={finished} ok={} send={:?} recv={:?}",
                self.socket.queue_pair.qp_num(),
                self.pending_sends.len(),
                self.socket.state.is_ok(),
                self.send,
                self.recv,
            );
        }
        // An acknowledgment overdue for seconds means the standalone-ACK
        // path is starved (the peer's send window may be stalling on it).
        if self.recv.data_received - self.recv.data_acked >= u64::from(self.flow.ack_threshold)
            && self.last_ack_timestamp.elapsed().as_secs() >= 2
        {
            tracing::warn!(
                "ack starvation: qp={} ok={} send={:?} recv={:?}",
                self.socket.queue_pair.qp_num(),
                self.socket.state.is_ok(),
                self.send,
                self.recv,
            );
        }

        // Decide whether an ACK is due *before* flushing pending sends so it
        // can piggyback on one of them (saving a standalone WR + CQE + a
        // recv buffer cycle on the peer).
        let mut ack = self.due_ack();

        // Flush pending sends against the *unpublished* finished value:
        // the backlog spends freshly freed credits before
        // `update_send_finished` makes them visible to direct senders, so
        // pending traffic cannot be starved by new sends.
        let flush_result = self.flush_pending(finished, &mut ack);
        self.socket.state.update_send_finished(finished);

        // Send the standalone ACK even when the flush failed (e.g. a
        // transient allocation error): the peer's send window depends on our
        // ACKs, so skipping them would deadlock both sides.
        if let Some(imm) = ack {
            // Cap the number of outstanding standalone ACK work requests.
            let ack_done = std::cmp::min(self.send.ack_completed, self.send.ack_confirmed);
            if self.send.ack_submitted < ack_done + u64::from(self.flow.ack_max_limit) {
                self.submit_ack(imm)?;
                self.mark_acked();
            }
        }

        flush_result
    }

    /// Returns the ACK immediate value if an acknowledgment is due.
    fn due_ack(&self) -> Option<u32> {
        let pending_data = u32::try_from(self.recv.data_received - self.recv.data_acked).unwrap();
        let pending_imm = u32::try_from(self.recv.imm_received - self.recv.imm_acked).unwrap();
        // The 5s timer also acts as a keepalive: on a connection whose peer
        // is gone, the ACK send fails at the transport level and triggers
        // teardown of the stale connection.
        if pending_data >= self.flow.ack_threshold
            || pending_imm >= self.flow.ack_max_limit / 2
            || self.last_ack_timestamp.elapsed().as_secs() >= 5
        {
            Some((pending_imm << 16) + pending_data)
        } else {
            None
        }
    }
    /// Records that all received completions have been acknowledged.
    fn mark_acked(&mut self) {
        self.recv.data_acked = self.recv.data_received;
        self.recv.imm_acked = self.recv.imm_received;
        self.last_ack_timestamp = Instant::now();
    }

    /// Flushes pending sends in FIFO order while credits are available,
    /// attaching the due ACK (if any) to the first posted send as
    /// immediate data.
    ///
    /// This is the opportunistic aggregation point: messages only queue
    /// here when the send window was full, so packing whatever is *already
    /// waiting* into one RDMA send amortizes per-WR costs (doorbell, CQE,
    /// recv buffer + credit on the peer) without adding any latency on the
    /// uncontended fast path, which posts directly from the sender task.
    /// Messages are framed, so an aggregate is plain concatenation — and
    /// since credits are per WR, an aggregate consumes a *single* credit,
    /// making aggregation actively relieve the window pressure that caused
    /// the queueing.
    ///
    /// Aggregation needs a fallible pool allocation for the scratch
    /// buffer; when the pool is exhausted the flush falls back to a
    /// zero-allocation *gather-list* send of the same run, keeping the
    /// aggregation (and its per-WR credit savings) intact under memory
    /// pressure.
    fn flush_pending(&mut self, finished: u64, ack: &mut Option<u32>) -> Result<()> {
        let agg_cap = self.recv_buf_size.min(MAX_AGG_BYTES);
        while !self.pending_sends.is_empty() {
            match self.socket.state.try_acquire_at(finished) {
                // Pending flushes are always posted signaled (see
                // `post_data`), so the tail flag needs no handling here.
                SendPermit::Granted { .. } => {}
                SendPermit::Full => break,
                SendPermit::Error => {
                    self.pending_sends.clear();
                    break;
                }
            }

            // Determine the FIFO run that fits one aggregate. The first
            // message always counts, even when it alone exceeds `agg_cap`
            // (oversized messages are posted unaggregated below).
            let mut count = 1;
            let mut total = self.pending_sends[0].len();
            if self.msg_aggregation {
                while count < self.pending_sends.len() {
                    let framed = self.pending_sends[count].len();
                    if total + framed > agg_cap {
                        break;
                    }
                    total += framed;
                    count += 1;
                }
            }

            if count < 2 {
                // No aggregation (disabled, oversized, or a single pending
                // message): post directly, no allocation needed.
                let buf = self.pending_sends.pop_front().unwrap();
                self.post_data(buf, ack)?;
                continue;
            }

            // Copy the run into one contiguous send: for the typically
            // small frames queueing here, the sub-µs memcpy on this
            // dedicated thread is measurably cheaper than the NIC-side
            // cost of a many-SGE gather WQE (~10% peak QPS on 1 KiB
            // echo). Under pool exhaustion, degrade to a zero-allocation
            // gather-list send instead of per-message WRs, so aggregation
            // (and the credits it saves) survives memory pressure.
            match self.socket.rdmabuf_pool.allocate(total) {
                Ok(mut agg) => {
                    agg.set_len(0);
                    for _ in 0..count {
                        let frame = self.pending_sends.pop_front().unwrap();
                        agg.extend_from_slice(&frame)?;
                    }
                    tracing::trace!("aggregating {count} pending messages into one {total}B send");
                    self.post_data(agg, ack)?;
                }
                Err(e) => {
                    // The gather list is capped by the QP's SGE limit; any
                    // remainder of the run is handled on the next loop
                    // iteration.
                    let take = count.min(self.socket.queue_pair.gather_limit());
                    if take < 2 {
                        let buf = self.pending_sends.pop_front().unwrap();
                        self.post_data(buf, ack)?;
                        continue;
                    }
                    tracing::debug!("aggregate allocation failed ({e}); posting a gather list");
                    let frames: Box<[Buffer]> = self.pending_sends.drain(..take).collect();
                    self.post_gather(frames, ack)?;
                }
            }
        }
        Ok(())
    }

    /// Posts a pending data send (one WR = one credit), piggybacking the
    /// due ACK as immediate data when present.
    ///
    /// Always signaled, bypassing the selective signaling interval: pending
    /// flushes only happen when the send window was full, so a flushed send
    /// (especially an aggregate) can be the connection's *last* data WR for
    /// a while. If it were unsignaled, its credit would stay stranded until
    /// an unrelated signaled WR sweeps it — when that happens on both peers
    /// simultaneously, neither side can send, neither receives (so no
    /// ACK-threshold ACKs are posted), and the connection deadlocks until
    /// the 5s keepalive ACK completion finally sweeps the SQ.
    fn post_data(&mut self, buf: Buffer, ack: &mut Option<u32>) -> Result<()> {
        let result = match ack.take() {
            Some(imm) => {
                let posted =
                    self.socket
                        .queue_pair
                        .send_imm(buf, imm, ibv_send_flags::IBV_SEND_SIGNALED);
                if posted.is_ok() {
                    self.mark_acked();
                }
                posted.map(|_| ())
            }
            None => self
                .socket
                .queue_pair
                .send_signaled(buf, ibv_send_flags::IBV_SEND_SIGNALED)
                .map(|_| ()),
        };
        result.map_err(|e| {
            tracing::error!("failed to send pending buffer: {e}");
            self.socket.set_error();
            Error::new(
                ErrorKind::RdmaSendFailed,
                format!("failed to send pending buffer: {e}"),
            )
        })
    }

    /// Posts an aggregated pending send as one gather-list WR (one WR =
    /// one credit), piggybacking the due ACK as immediate data when
    /// present. Always signaled, for the same reason as [`post_data`].
    ///
    /// [`post_data`]: Self::post_data
    fn post_gather(&mut self, frames: Box<[Buffer]>, ack: &mut Option<u32>) -> Result<()> {
        let imm = ack.take();
        let had_ack = imm.is_some();
        match self.socket.queue_pair.send_gather(frames, imm) {
            Ok(_) => {
                if had_ack {
                    self.mark_acked();
                }
                Ok(())
            }
            Err(e) => {
                tracing::error!("failed to send gathered pending buffers: {e}");
                self.socket.set_error();
                Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    format!("failed to send gathered pending buffers: {e}"),
                ))
            }
        }
    }

    fn submit_ack(&mut self, imm_data: u32) -> Result<()> {
        let ret = self
            .socket
            .queue_pair
            .send_imm_only(imm_data, ibv_send_flags::IBV_SEND_SIGNALED);
        self.send.ack_submitted += 1;
        match ret {
            Ok(()) => Ok(()),
            Err(err) => {
                self.send.ack_completed += 1;
                self.send.ack_confirmed += 1;
                tracing::error!("submit ack error: {err}");
                self.socket.set_error();
                Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    format!("failed to post ack: {err}"),
                ))
            }
        }
    }

    /// Whether this connection can be torn down.
    ///
    /// Only true after the socket entered the error state (QP moved to ERR):
    /// every outstanding work request then produces a flush CQE, so waiting
    /// for the ACK and recv counters to settle guarantees the QP finished
    /// flushing. Buffers of successfully-completed unsignaled sends never
    /// produce a CQE and are reclaimed explicitly before removal.
    fn ready_to_remove(&mut self) -> bool {
        if self.socket.state.is_ok()
            || self.send.ack_submitted != self.send.ack_completed
            || self.recv.submitted != self.recv.completed
            || !self.pending_sends.is_empty()
        {
            return false;
        }
        self.socket.queue_pair.reclaim_send_buffers();
        true
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

    fn run(mut self) {
        let mut wcs = [ibv_wc::default(); 64];
        let mut batch: DispatchBatch = Vec::new();
        let mut spin_until = Instant::now();
        let mut next_housekeeping = Instant::now();
        let mut last_dump = Instant::now();

        loop {
            // 1. Drain the completion queue.
            let mut progressed = false;
            loop {
                let n = match self.cq.poll(&mut wcs) {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::error!("CQ poll failed, stopping RDMA poll thread: {e}");
                        self.shutdown_cleanup();
                        return;
                    }
                };
                for wc in &wcs[..n] {
                    self.dispatch(wc, &mut batch);
                }
                if batch.len() >= MAX_DISPATCH_BATCH {
                    self.dispatcher.flush(&mut batch);
                }
                progressed |= n > 0;
                if n < wcs.len() {
                    break;
                }
            }
            self.dispatcher.flush(&mut batch);

            // 2. Registration inbox and per-connection housekeeping:
            //    pending sends, flow control, teardown. This pass is
            //    O(connections) including clock reads, so during the spin
            //    window it only runs when a completion was processed or the
            //    periodic interval elapsed — not on every idle spin
            //    iteration. `register()` wakes the thread, so a registration
            //    is picked up after at most one housekeeping interval.
            let now = Instant::now();
            if progressed || now >= next_housekeeping {
                next_housekeeping = now + Self::HOUSEKEEPING_INTERVAL;

                if self.shared.shutdown.load(Ordering::Acquire) {
                    break;
                }
                self.drain_incoming();

                if tracing::enabled!(tracing::Level::DEBUG)
                    && last_dump.elapsed() >= Self::DUMP_INTERVAL
                {
                    last_dump = Instant::now();
                    for conn in self.conns.iter().flatten() {
                        tracing::debug!(
                            "conn dump: qp={} ok={} pending={} send={:?} recv={:?}",
                            conn.socket.queue_pair.qp_num(),
                            conn.socket.state.is_ok(),
                            conn.pending_sends.len(),
                            conn.send,
                            conn.recv,
                        );
                    }
                }

                for slot in 0..self.conns.len() {
                    let Some(conn) = self.conns[slot].as_mut() else {
                        continue;
                    };
                    conn.drain_pending();
                    if conn.recv_deficit > 0 {
                        conn.retry_recv_deficit();
                    }
                    if let Err(e) = conn.update_flow_control() {
                        tracing::error!("flow control update error: {e}");
                    }
                    if conn.ready_to_remove() {
                        // Drop the connection before recycling its slot so
                        // no new occupant can race its teardown.
                        self.conns[slot] = None;
                        release_slot(&self.shared, slot as u16);
                    }
                }
            }

            if progressed {
                spin_until = now + self.spin;
                continue;
            }

            // 3. Busy-poll window after the last completion.
            if now < spin_until {
                std::hint::spin_loop();
                continue;
            }

            // 4. Idle: arm the CQ notification, close the race with one more
            //    poll, then sleep on the completion channel + wake pipe.
            if let Err(e) = self.cq.req_notify(false) {
                tracing::error!("req_notify failed, stopping RDMA poll thread: {e}");
                self.shutdown_cleanup();
                return;
            }
            match self.cq.poll(&mut wcs) {
                Ok(0) => {}
                Ok(n) => {
                    for wc in &wcs[..n] {
                        self.dispatch(wc, &mut batch);
                    }
                    self.dispatcher.flush(&mut batch);
                    spin_until = Instant::now() + self.spin;
                    continue;
                }
                Err(e) => {
                    tracing::error!("CQ poll failed, stopping RDMA poll thread: {e}");
                    self.shutdown_cleanup();
                    return;
                }
            }

            match poll_readable2(
                self.comp_channel.fd().as_raw_fd(),
                self.wake_rx.as_raw_fd(),
                Self::IDLE_TIMEOUT_MS,
            ) {
                Ok((cq_ready, wake_ready)) => {
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
                    if cq_ready || wake_ready {
                        spin_until = Instant::now() + self.spin;
                    }
                }
                Err(e) => {
                    tracing::error!("poll(2) failed, stopping RDMA poll thread: {e}");
                    self.shutdown_cleanup();
                    return;
                }
            }
        }

        // Shutdown: connections are dropped here; their buffers return to
        // the pool when the QPs are destroyed.
        self.shutdown_cleanup();
    }

    /// Moves newly registered connections from the shared inbox into their
    /// slots.
    fn drain_incoming(&mut self) {
        if !self.shared.has_incoming.load(Ordering::Acquire) {
            return;
        }
        let drained = {
            let mut inner = self.shared.inner.lock().unwrap();
            self.shared.has_incoming.store(false, Ordering::Release);
            std::mem::take(&mut inner.incoming)
        };
        for incoming in drained {
            let slot = incoming.slot as usize;
            if self.conns.len() <= slot {
                self.conns.resize_with(slot + 1, || None);
            }
            debug_assert!(self.conns[slot].is_none(), "poller slot {slot} occupied");
            self.conns[slot] = Some(ConnState::new(
                *incoming.conn,
                incoming.generation,
                incoming.budget,
            ));
        }
    }

    fn dispatch(&mut self, wc: &ibv_wc, batch: &mut DispatchBatch) {
        let (slot, generation) = split_tag(wc.wr_id.get_tag());
        if let Some(Some(conn)) = self.conns.get_mut(slot)
            && conn.generation == generation
        {
            conn.handle_wc(wc, batch);
            return;
        }
        // The completion may have raced its connection's registration (the
        // receive ring is posted before the connection reaches the inbox):
        // pull the inbox and retry.
        self.drain_incoming();
        if let Some(Some(conn)) = self.conns.get_mut(slot)
            && conn.generation == generation
        {
            conn.handle_wc(wc, batch);
        } else {
            tracing::warn!(
                "dropping completion for unknown connection {slot}:{generation}: {wc:?}"
            );
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
        }
        drop(drained);
        for conn in self.conns.iter().flatten() {
            conn.socket.set_error();
        }
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
        let shard_count = shard_count.max(1) as usize;
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
mod tests {
    use ruapc_rdma::WRID;

    use super::*;

    /// The shared CQ length must be clamped to the device's `max_cqe`:
    /// drivers reject larger requests with EINVAL (e.g. the rxe soft-RoCE
    /// driver caps `max_cqe` at 32767, below the default `device_cq_len`).
    #[tokio::test]
    async fn test_cq_len_clamped_to_device_max() {
        let device = crate::rdma::test_utils::open_rdma_device();
        let config = PollerConfig {
            cq_len: u32::MAX,
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
    fn test_conn_tag_roundtrip() {
        for (slot, generation) in [(0u16, 0u8), (1, 255), (MAX_SLOTS as u16 - 1, 42)] {
            let tag = conn_tag(slot, generation);
            assert!(tag <= WRID::TAG_MAX);
            assert_eq!(split_tag(tag), (slot as usize, generation));
        }
    }

    #[test]
    fn test_for_each_frame_walks_all_frames() {
        let mut buf = Vec::new();
        let frames: [&[u8]; 3] = [b"first", b"", b"third-frame"];
        for frame in frames {
            buf.extend_from_slice(&u32::try_from(frame.len()).unwrap().to_be_bytes());
            buf.extend_from_slice(frame);
        }
        let mut seen = Vec::new();
        for_each_frame(&Bytes::from(buf), |frame| seen.push(frame));
        assert_eq!(seen, frames.map(Bytes::from_static).to_vec());
    }

    #[test]
    fn test_for_each_frame_stops_on_truncation() {
        // Header claims 100 bytes but only 3 follow.
        let mut buf = 100u32.to_be_bytes().to_vec();
        buf.extend_from_slice(b"abc");
        let mut count = 0;
        for_each_frame(&Bytes::from(buf), |_| count += 1);
        assert_eq!(count, 0);

        // One valid frame, then a truncated header.
        let mut buf = 1u32.to_be_bytes().to_vec();
        buf.extend_from_slice(b"x");
        buf.extend_from_slice(&[0u8, 0]);
        let mut count = 0;
        for_each_frame(&Bytes::from(buf), |_| count += 1);
        assert_eq!(count, 1);
    }
}
