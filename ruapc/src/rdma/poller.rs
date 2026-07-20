//! Dedicated per-device RDMA completion poll thread
//!
//! Replaces the previous per-connection async event loop. Each RDMA device
//! gets one shared completion queue (send + recv for every connection on
//! that device) and one dedicated OS thread that polls it:
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
//! Completions are routed to per-connection state by `ibv_wc::qp_num`.
//! Response dispatch (`Waiter::post`) executes directly on the poll thread
//! (a single oneshot wake to the awaiting task); request dispatch spawns
//! onto the tokio runtime captured at poller creation.

use std::{
    collections::{BTreeMap, HashMap},
    io::{Read as _, Write as _},
    os::unix::io::AsRawFd as _,
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
        mpsc::{Receiver, Sender, TryRecvError},
    },
    time::{Duration, Instant},
};

use foldhash::fast::RandomState;
use ruapc_rdma::{CompChannel, CompletionQueue, WRType, ibv_send_flags, ibv_wc, poll_readable2};

use super::RdmaSocket;
use crate::{Buffer, Error, ErrorKind, Message, Result, Socket, State, task::TaskSupervisorGuard};

/// Magic prefix of an aggregated RDMA send containing multiple messages as
/// `[4B big-endian frame length][message bytes]` frames. A plain message
/// starts with its 4-byte metadata length, which can never equal this value.
const AGG_MAGIC: [u8; 4] = *b"RUAG";
/// Per-frame header size (big-endian u32 frame length).
const AGG_FRAME_HEADER: usize = 4;
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
/// and their volume is bounded by the window.
const SMALL_MSG_COPY_MAX: usize = 1024;

/// A send that could not be posted immediately because the send window was
/// full; forwarded to the poll thread for ordered flushing.
pub struct SendMsg {
    pub id: u64,
    pub buf: Buffer,
}

/// Messages parsed from one completion batch, dispatched together.
///
/// Dispatching (request spawn / response oneshot wake) from the poll thread
/// goes through tokio's remote-injection path, whose shared lock becomes the
/// global throughput ceiling. Every `tokio::spawn` from the poll thread —
/// even one per batch — takes that lock and a cross-thread wakeup.
///
/// Batches are therefore handed to a long-lived per-shard *dispatcher task*
/// over an unbounded channel: the send is a lock-free enqueue, wakeups
/// coalesce while the dispatcher is busy, and the per-request handler spawns
/// happen on a runtime worker where they use the lock-free local queue (and
/// are then work-stealable by other workers).
type DispatchBatch = Vec<(Arc<State>, Socket, Message)>;

/// Flush threshold for a dispatch batch (bounds latency and memory).
const MAX_DISPATCH_BATCH: usize = 256;

/// Forwards the accumulated messages to the shard's dispatcher task.
fn flush_dispatch(
    tx: &tokio::sync::mpsc::UnboundedSender<DispatchBatch>,
    batch: &mut DispatchBatch,
) {
    if batch.is_empty() {
        return;
    }
    if let Err(e) = tx.send(std::mem::take(batch)) {
        // Dispatcher gone: the runtime is shutting down; drop the messages.
        tracing::debug!("dispatcher task gone; dropping {} message(s)", e.0.len());
    }
}

/// Messages processed per dispatcher chunk. Handling a message wakes or
/// spawns a task on the *current* worker's local queue; processing a large
/// batch inline would pile them all onto one worker. Spawning the tail
/// chunks as separate tasks keeps them work-stealable, so idle workers can
/// spread the resulting wakes across the runtime.
const DISPATCH_CHUNK: usize = 64;

/// Handles one chunk of dispatched messages.
fn run_dispatch_chunk(chunk: Vec<(Arc<State>, Socket, Message)>) {
    for (state, socket, msg) in chunk {
        if let Err(e) = state.handle_recv(&socket, msg) {
            tracing::error!("Failed to handle message: {e}");
        }
    }
}

/// Runs a shard's dispatcher: unpacks batches from the poll thread and
/// hands each message to the router (requests) or waiter (responses) from a
/// runtime worker thread.
async fn run_dispatcher(mut rx: tokio::sync::mpsc::UnboundedReceiver<DispatchBatch>) {
    while let Some(mut batch) = rx.recv().await {
        while batch.len() > DISPATCH_CHUNK {
            let chunk = batch.split_off(batch.len() - DISPATCH_CHUNK);
            tokio::spawn(async move { run_dispatch_chunk(chunk) });
        }
        run_dispatch_chunk(batch);
    }
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

/// Everything the poll thread needs to manage one connection.
pub struct RegisterConn {
    pub socket: Arc<RdmaSocket>,
    pub state: Arc<State>,
    pub pending_receiver: tokio::sync::mpsc::Receiver<SendMsg>,
    /// Number of receive work requests already posted by the registrar.
    pub recv_submitted: u64,
    /// Negotiated receive buffer size (`max_msg_size`).
    pub recv_buf_size: usize,
    /// Whether to aggregate window-blocked sends (local send-side toggle;
    /// receivers always understand aggregated frames).
    pub msg_aggregation: bool,
    /// Keeps `SocketPool::join` waiting until this connection is torn down.
    pub supervisor_guard: TaskSupervisorGuard,
}

enum PollerCmd {
    Register(Box<RegisterConn>, BudgetGuard),
}

/// Handle to a per-device poll thread.
///
/// Dropping the handle disconnects the command channel, wakes the thread and
/// joins it (unless the drop happens on the poll thread itself, which can
/// occur when the last `Arc<State>` is released during connection teardown).
pub struct DevicePoller {
    cq: Arc<CompletionQueue>,
    cmd_tx: Option<Sender<PollerCmd>>,
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

        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let handle = tokio::runtime::Handle::current();

        // Long-lived dispatcher task for this shard; exits when the poll
        // thread drops the sender.
        let (dispatch_tx, dispatch_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(run_dispatcher(dispatch_rx));

        let thread = {
            let cq = cq.clone();
            let comp_channel = comp_channel.clone();
            std::thread::Builder::new()
                .name(format!("ruapc-rdma-poll-{device_name}"))
                .spawn(move || {
                    let _rt = handle.enter();
                    PollLoop {
                        cq,
                        comp_channel,
                        wake_rx,
                        cmd_rx,
                        dispatch_tx,
                        spin: Duration::from_micros(config.spin_us),
                        conns: HashMap::default(),
                        orphans: Vec::new(),
                        unack_cq_events: 0,
                    }
                    .run();
                })
                .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?
        };

        Ok(Self {
            cq,
            cmd_tx: Some(cmd_tx),
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

    /// Registers a connection with the poll thread.
    ///
    /// `qp_depth` is the connection's total queue depth (send + recv work
    /// requests); registration fails if the shared CQ cannot absorb it.
    pub fn register(&self, conn: RegisterConn, qp_depth: u32) -> Result<()> {
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

        // The budget guard travels with the command and lives inside the
        // connection state; dropping it (send failure or teardown) releases
        // the budget.
        let guard = BudgetGuard {
            budget,
            depth: qp_depth,
        };
        let sent = self
            .cmd_tx
            .as_ref()
            .map(|tx| tx.send(PollerCmd::Register(Box::new(conn), guard)).is_ok())
            .unwrap_or(false);
        if !sent {
            return Err(Error::new(
                ErrorKind::RdmaSendFailed,
                "RDMA poll thread is not running".into(),
            ));
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
        self.cmd_tx.take();
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
    /// Number of unacknowledged messages before triggering an acknowledgment.
    ack_threshold: u32,
    /// Maximum number of unacknowledged messages allowed.
    ack_max_limit: u32,
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            ack_threshold: 16,
            ack_max_limit: 32,
        }
    }
}

/// Receive-side statistics and handling for one connection.
#[derive(Debug, Default)]
struct RecvStats {
    submitted: u64,
    completed: u64,
    imm_received: u64,
    imm_acked: u64,
    data_received: u64,
    data_acked: u64,
}

/// Send-side statistics for one connection.
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
    recv: RecvStats,
    send: SendStats,
    last_ack_timestamp: Instant,
    flow: FlowConfig,
    pending_sends: BTreeMap<u64, Buffer>,
    pending_receiver: tokio::sync::mpsc::Receiver<SendMsg>,
    /// Next send-queue id that has not been swept yet (see `sweep_sq`).
    sq_swept: u64,
    /// Negotiated receive buffer size (`max_msg_size`).
    recv_buf_size: usize,
    /// Whether to aggregate window-blocked sends.
    msg_aggregation: bool,
    /// Recycled receive buffers: pure-ACK completions and split aggregates
    /// return their buffer here so the repost skips the shared pool.
    recv_buf_cache: Vec<Buffer>,
    /// Receive work requests that could not be reposted (transient buffer
    /// pool exhaustion); retried on subsequent iterations instead of
    /// failing the connection.
    recv_deficit: u64,
    /// Logical message count per aggregate SQ id: an aggregate send carries
    /// several window-consuming messages, so its (swept) completion must
    /// advance `data_completed` by the frame count.
    agg_counts: BTreeMap<u64, u64>,
    socket: Arc<RdmaSocket>,
    state: Arc<State>,
    _budget: BudgetGuard,
    _supervisor_guard: TaskSupervisorGuard,
}

impl ConnState {
    fn new(reg: RegisterConn, budget: BudgetGuard) -> Self {
        Self {
            recv: RecvStats {
                submitted: reg.recv_submitted,
                ..Default::default()
            },
            send: SendStats::default(),
            last_ack_timestamp: Instant::now(),
            flow: FlowConfig::default(),
            pending_sends: BTreeMap::new(),
            pending_receiver: reg.pending_receiver,
            sq_swept: 0,
            recv_buf_size: reg.recv_buf_size,
            msg_aggregation: reg.msg_aggregation,
            recv_buf_cache: Vec::new(),
            recv_deficit: 0,
            agg_counts: BTreeMap::new(),
            socket: reg.socket,
            state: reg.state,
            _budget: budget,
            _supervisor_guard: reg.supervisor_guard,
        }
    }

    /// Handles one work completion for this connection.
    fn handle_wc(&mut self, wc: &ibv_wc, batch: &mut DispatchBatch) {
        if !wc.is_recv() {
            // Sweep unsignaled data sends completed before this SQ
            // completion (RC SQs complete in post order): reclaim their
            // buffers and count them as completed. Aggregate sends count as
            // their logical message count.
            let id = wc.wr_id.get_id();
            for swept in self.sq_swept..id {
                if self.socket.queue_pair.take_send_buffer(swept).is_some() {
                    self.send.data_completed += self.agg_counts.remove(&swept).unwrap_or(1);
                }
            }
            self.sq_swept = self.sq_swept.max(id + 1);
        }

        let buffer = self.socket.queue_pair.take_buffer(&wc.wr_id);
        let result = if wc.is_recv() {
            self.handle_recv_completion(wc, buffer, batch)
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

        // Immediate data (ACK counters) can arrive standalone or piggybacked
        // on a data send.
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
            } else if buf.len() >= AGG_MAGIC.len() && buf[..AGG_MAGIC.len()] == AGG_MAGIC {
                self.handle_aggregate(buf, batch);
            } else {
                self.recv.data_received += 1;
                // Copy small messages out and recycle the receive buffer;
                // see `SMALL_MSG_COPY_MAX` for why.
                let parsed = if buf.len() <= SMALL_MSG_COPY_MAX {
                    let copied = bytes::Bytes::copy_from_slice(&buf);
                    self.cache_recv_buf(buf);
                    Message::parse(copied)
                } else {
                    Message::parse(buf)
                };
                match parsed {
                    Ok(msg) => batch.push((self.state.clone(), Socket::from(&self.socket), msg)),
                    Err(e) => tracing::error!("Failed to parse message: {e}"),
                }
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

    /// Parses an aggregate buffer (`AGG_MAGIC` + repeated `[4B len][msg]`
    /// frames) and dispatches every contained message.
    ///
    /// Frames are zero-copy slices into the refcounted receive buffer; the
    /// buffer returns to the pool once the last frame is dropped.
    fn handle_aggregate(&mut self, buf: Buffer, batch: &mut DispatchBatch) {
        let owner = bytes::Bytes::from_owner(buf);
        let mut offset = AGG_MAGIC.len();
        while offset < owner.len() {
            let Some(header) = owner.get(offset..offset + 4) else {
                tracing::error!("truncated aggregate frame header at {offset}");
                break;
            };
            let frame_len = u32::from_be_bytes(header.try_into().unwrap()) as usize;
            if offset + 4 + frame_len > owner.len() {
                tracing::error!("truncated aggregate frame ({frame_len}B) at {offset}");
                break;
            }
            let frame = owner.slice(offset + 4..offset + 4 + frame_len);
            self.recv.data_received += 1;
            match Message::parse(frame) {
                Ok(msg) => batch.push((self.state.clone(), Socket::from(&self.socket), msg)),
                Err(e) => tracing::error!("Failed to parse aggregated message: {e}"),
            }
            offset += 4 + frame_len;
        }
    }

    fn handle_send_completion(&mut self, wc: &ibv_wc, buffer: Option<Buffer>) -> Result<()> {
        // RDMA one-sided operation: hand the buffer back to the caller.
        if let Some((_, sender)) = self.socket.rdma_completions.remove(&wc.wr_id) {
            let _ = sender.send((wc.status, buffer));
            return Ok(());
        }

        let id = wc.wr_id.get_id();
        match wc.wr_id.get_type() {
            WRType::SendImm => {
                if let Some(count) = self.agg_counts.remove(&id) {
                    // A data send carrying a piggybacked ACK; it lives
                    // outside the standalone-ACK ledger.
                    self.send.data_completed += count;
                } else {
                    self.send.ack_completed += 1;
                }
            }
            _ => {
                self.send.data_completed += self.agg_counts.remove(&id).unwrap_or(1);
            }
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

    /// Moves window-blocked sends from the mpsc channel into the ordered map.
    fn drain_pending(&mut self) {
        while let Ok(msg) = self.pending_receiver.try_recv() {
            self.pending_sends.insert(msg.id, msg.buf);
        }
    }

    /// Updates flow control state, flushes window-unblocked pending sends
    /// and emits acknowledgments when thresholds are reached.
    fn update_flow_control(&mut self) -> Result<()> {
        if !self.socket.state.is_ok() {
            while let Some(_pending) = self.pending_sends.pop_first() {
                self.send.data_completed += 1;
            }
            return Ok(());
        }

        let completed_sends = std::cmp::min(self.send.data_completed, self.send.data_confirmed);
        let sendable_bound = self.socket.state.update_send_finished(completed_sends);

        // Liveness diagnostics: a pending send that stays window-blocked for
        // seconds indicates a flow control stall (peer ACKs missing or
        // completion accounting gone wrong).
        if let Some((first, _)) = self.pending_sends.first_key_value()
            && *first >= sendable_bound
            && self.last_ack_timestamp.elapsed().as_secs() >= 2
        {
            tracing::warn!(
                "flow stall: qp={} pending={} first_key={first} bound={sendable_bound} ok={} send={:?} recv={:?}",
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
        let flush_result = self.flush_pending(sendable_bound, &mut ack);

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
    /// Records that all received messages have been acknowledged.
    fn mark_acked(&mut self) {
        self.recv.data_acked = self.recv.data_received;
        self.recv.imm_acked = self.recv.imm_received;
        self.last_ack_timestamp = Instant::now();
    }

    /// Flushes window-unblocked pending sends in send-index order, attaching the
    /// due ACK (if any) to the first posted send as immediate data.
    ///
    /// This is the opportunistic aggregation point: messages only queue here
    /// when the send window was full, so packing whatever is *already
    /// waiting* into one RDMA send amortizes per-message costs (doorbell,
    /// CQE, recv processing on the peer) without adding any latency on the
    /// uncontended fast path, which posts directly from the sender task.
    ///
    /// Messages are only removed from `pending_sends` once their posting no
    /// longer needs a fallible allocation: a dropped pending message would
    /// permanently shrink the send window (its index was already consumed,
    /// but no completion will ever account for it), and 32 cumulative drops
    /// deadlock the connection. The aggregate buffer is therefore allocated
    /// from a *pre-scan* of the eligible run, before any message is popped;
    /// if the pool is exhausted, the flush degrades to posting messages
    /// unaggregated (which needs no allocation).
    fn flush_pending(&mut self, sendable_bound: u64, ack: &mut Option<u32>) -> Result<()> {
        let agg_cap = self.recv_buf_size.min(MAX_AGG_BYTES);
        while let Some((&first_key, _)) = self.pending_sends.first_key_value()
            && first_key < sendable_bound
        {
            // Pre-scan the eligible run: how many messages fit in one
            // aggregate and their packed size. The first message is always
            // counted, even when it alone exceeds `agg_cap` (oversized
            // messages are posted unaggregated below).
            let mut total = AGG_MAGIC.len();
            let mut count = 0usize;
            if self.msg_aggregation {
                for (&key, buf) in self.pending_sends.iter() {
                    let framed = AGG_FRAME_HEADER + buf.len();
                    if key >= sendable_bound || (count > 0 && total + framed > agg_cap) {
                        break;
                    }
                    total += framed;
                    count += 1;
                }
            }

            if count < 2 {
                // No aggregation (disabled, oversized, or a single eligible
                // message): post directly, no allocation needed.
                let (_, buf) = self.pending_sends.pop_first().unwrap();
                self.post_data(buf, 1, ack)?;
                continue;
            }

            let mut agg = match self.socket.rdmabuf_pool.allocate(total) {
                Ok(buf) => buf,
                Err(e) => {
                    // Transient pool exhaustion: keep traffic flowing by
                    // posting the first message unaggregated; the rest of
                    // the run is retried on the next loop iteration.
                    tracing::debug!("aggregate allocation failed ({e}); posting unaggregated");
                    let (_, buf) = self.pending_sends.pop_first().unwrap();
                    self.post_data(buf, 1, ack)?;
                    continue;
                }
            };
            agg.set_len(0);
            agg.extend_from_slice(&AGG_MAGIC)?;
            for _ in 0..count {
                let (_, frame) = self.pending_sends.pop_first().unwrap();
                agg.extend_from_slice(&u32::try_from(frame.len())?.to_be_bytes())?;
                agg.extend_from_slice(&frame)?;
            }
            tracing::debug!("aggregating {count} pending messages into one {total}B send");
            self.post_data(agg, count as u64, ack)?;
        }
        Ok(())
    }

    /// Posts a pending data send carrying `count` logical messages,
    /// piggybacking the due ACK as immediate data when present.
    ///
    /// Always signaled, bypassing the selective signaling interval: pending
    /// flushes only happen when the send window was full, so a flushed send
    /// (especially an aggregate carrying a whole window's worth of
    /// messages) can be the connection's *last* data WR for a while. If it
    /// were unsignaled, its window slots would stay stranded until an
    /// unrelated signaled WR sweeps them — when the aggregate covers the
    /// entire window on both peers simultaneously, neither side can send,
    /// neither receives (so no ACK-threshold ACKs are posted), and the
    /// connection deadlocks until the 5s keepalive ACK completion finally
    /// sweeps the SQ. That was the root cause of the sporadic 5s request
    /// timeouts and the "flow stall" storms under deep backlog.
    fn post_data(&mut self, buf: Buffer, count: u64, ack: &mut Option<u32>) -> Result<()> {
        let piggyback = ack.is_some();
        let result = match ack.take() {
            Some(imm) => {
                let posted =
                    self.socket
                        .queue_pair
                        .send_imm(buf, imm, ibv_send_flags::IBV_SEND_SIGNALED);
                if posted.is_ok() {
                    self.mark_acked();
                }
                posted
            }
            None => self
                .socket
                .queue_pair
                .send_signaled(buf, ibv_send_flags::IBV_SEND_SIGNALED),
        };
        match result {
            Ok(sq_id) => {
                // Record the logical message count for aggregates, and for
                // every piggybacked send: SendImm completions consult
                // `agg_counts` to tell piggybacked data sends (outside the
                // standalone-ACK ledger) apart from standalone ACKs.
                if piggyback || count > 1 {
                    self.agg_counts.insert(sq_id, count);
                }
                Ok(())
            }
            Err(e) => {
                tracing::error!("failed to send pending buffer: {e}");
                self.socket.set_error();
                Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    format!("failed to send pending buffer: {e}"),
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
    cmd_rx: Receiver<PollerCmd>,
    /// Hands parsed message batches to this shard's dispatcher task.
    dispatch_tx: tokio::sync::mpsc::UnboundedSender<DispatchBatch>,
    spin: Duration,
    conns: HashMap<u32, ConnState, RandomState>,
    /// Completions whose qp_num had no registered connection yet (the
    /// registration command may still be in flight); retried briefly.
    orphans: Vec<(Instant, ibv_wc)>,
    unack_cq_events: u32,
}

impl PollLoop {
    /// Idle sleep timeout; bounds the latency of periodic housekeeping
    /// (5s ACK timer, orphan expiry) when no completions arrive.
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
            // 1. Retry completions that raced connection registration.
            self.retry_orphans(&mut batch);

            // 2. Drain the completion queue.
            let mut progressed = false;
            loop {
                let n = match self.cq.poll(&mut wcs) {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::error!("CQ poll failed, stopping RDMA poll thread: {e}");
                        self.fail_all_conns();
                        return;
                    }
                };
                for wc in &wcs[..n] {
                    self.dispatch(wc, &mut batch);
                }
                if batch.len() >= MAX_DISPATCH_BATCH {
                    flush_dispatch(&self.dispatch_tx, &mut batch);
                }
                progressed |= n > 0;
                if n < wcs.len() {
                    break;
                }
            }
            flush_dispatch(&self.dispatch_tx, &mut batch);

            // 3. Registration commands and per-connection housekeeping:
            //    pending sends, flow control, teardown. This pass is
            //    O(connections) including clock reads (and the command
            //    channel's try_recv is not free either), so during the spin
            //    window it only runs when a completion was processed or the
            //    periodic interval elapsed — not on every idle spin
            //    iteration. `register()` wakes the thread, so a registration
            //    is picked up after at most one housekeeping interval.
            let now = Instant::now();
            if progressed || now >= next_housekeeping {
                next_housekeeping = now + Self::HOUSEKEEPING_INTERVAL;

                // A disconnected command channel means the owning pool is
                // being dropped.
                match self.drain_cmds() {
                    Ok(()) => {}
                    Err(_disconnected) => break,
                }

                if tracing::enabled!(tracing::Level::DEBUG)
                    && last_dump.elapsed() >= Self::DUMP_INTERVAL
                {
                    last_dump = Instant::now();
                    for (qp, conn) in &self.conns {
                        tracing::debug!(
                            "conn dump: qp={qp} ok={} pending={} send={:?} recv={:?}",
                            conn.socket.state.is_ok(),
                            conn.pending_sends.len(),
                            conn.send,
                            conn.recv,
                        );
                    }
                }

                self.conns.retain(|_, conn| {
                    conn.drain_pending();
                    if conn.recv_deficit > 0 {
                        conn.retry_recv_deficit();
                    }
                    if let Err(e) = conn.update_flow_control() {
                        tracing::error!("flow control update error: {e}");
                    }
                    !conn.ready_to_remove()
                });
            }

            if progressed {
                spin_until = now + self.spin;
                continue;
            }

            // 4. Busy-poll window after the last completion.
            if now < spin_until {
                std::hint::spin_loop();
                continue;
            }

            // 5. Idle: arm the CQ notification, close the race with one more
            //    poll, then sleep on the completion channel + wake pipe.
            if let Err(e) = self.cq.req_notify(false) {
                tracing::error!("req_notify failed, stopping RDMA poll thread: {e}");
                self.fail_all_conns();
                return;
            }
            match self.cq.poll(&mut wcs) {
                Ok(0) => {}
                Ok(n) => {
                    for wc in &wcs[..n] {
                        self.dispatch(wc, &mut batch);
                    }
                    flush_dispatch(&self.dispatch_tx, &mut batch);
                    spin_until = Instant::now() + self.spin;
                    continue;
                }
                Err(e) => {
                    tracing::error!("CQ poll failed, stopping RDMA poll thread: {e}");
                    self.fail_all_conns();
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
                    self.fail_all_conns();
                    return;
                }
            }
        }

        // Shutdown: connections are dropped here; their buffers return to
        // the pool when the QPs are destroyed.
        self.fail_all_conns();
    }

    fn drain_cmds(&mut self) -> std::result::Result<(), ()> {
        loop {
            match self.cmd_rx.try_recv() {
                Ok(PollerCmd::Register(reg, budget)) => {
                    let qp_num = reg.socket.queue_pair.qp_num();
                    let conn = ConnState::new(*reg, budget);
                    if self.conns.insert(qp_num, conn).is_some() {
                        tracing::error!("duplicate RDMA qp_num {qp_num} registered");
                    }
                }
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => return Err(()),
            }
        }
    }

    fn dispatch(&mut self, wc: &ibv_wc, batch: &mut DispatchBatch) {
        if let Some(conn) = self.conns.get_mut(&wc.qp_num) {
            conn.handle_wc(wc, batch);
        } else {
            self.orphans.push((Instant::now(), *wc));
        }
    }

    fn retry_orphans(&mut self, batch: &mut DispatchBatch) {
        if self.orphans.is_empty() {
            return;
        }
        let orphans = std::mem::take(&mut self.orphans);
        for (seen, wc) in orphans {
            if let Some(conn) = self.conns.get_mut(&wc.qp_num) {
                conn.handle_wc(&wc, batch);
            } else if seen.elapsed() < Duration::from_secs(1) {
                self.orphans.push((seen, wc));
            } else {
                tracing::warn!(
                    "dropping completion for unknown qp_num {}: {:?}",
                    wc.qp_num,
                    wc
                );
            }
        }
    }

    fn fail_all_conns(&mut self) {
        for conn in self.conns.values() {
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
/// cores.
#[derive(Default)]
pub struct DevicePollers(Mutex<HashMap<String, DeviceShards, RandomState>>);

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
        let mut map = self.0.lock().unwrap();
        let entry = map.entry(name.clone()).or_default();
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
        };
        let poller = DevicePoller::start(device.context(), "cq-clamp-test", config)
            .expect("CQ creation must succeed with a clamped length");
        let max_cqe = device.context().query_device().unwrap().max_cqe;
        assert!(poller.cq_capacity <= u32::try_from(max_cqe.max(1)).unwrap_or(u32::MAX));
        assert!(poller.cq_capacity > 0);
    }
}
