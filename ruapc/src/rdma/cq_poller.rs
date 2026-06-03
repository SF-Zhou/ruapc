//! Shared completion-queue poller.
//!
//! Instead of one CQ + one event-loop task per RDMA connection (which does not
//! scale: a busy-poll loop per connection burns O(connections) cores), this
//! module multiplexes many queue pairs onto a small, fixed number of shared
//! completion queues. Each shared CQ is drained by a single dedicated OS thread
//! running an adaptive busy-poll loop. The number of CQs/pollers is therefore
//! O(cores), independent of the connection count.
//!
//! # Work-request demultiplexing
//!
//! A shared CQ receives completions for every QP attached to it, so a
//! completion must be routed back to its originating connection and operation.
//! Each work request is posted under a process-wide unique `wr_id` and
//! registered in the CQ's [`WrRegistry`] together with a [`WrContext`] carrying:
//!   - the operation kind ([`WrOp`]),
//!   - the buffer it owns (so completion returns it to the pool / caller),
//!   - a `Weak` handle to the owning [`RdmaSocket`] (so the poller can drive
//!     that connection's per-connection flow control).
//!
//! The poller reaps a completion, removes its `WrContext` from the registry,
//! and dispatches based on `WrOp`.
//!
//! # Threading model
//!
//! All completions for a given connection land on the same CQ (a connection is
//! pinned to one CQ for its lifetime), so each connection's [`ConnState`]
//! (recv/send counters, ACK state) is touched **only** by that CQ's single
//! poller thread and needs no locking. The send-side fast path runs on caller
//! (tokio) threads and only touches the lock-free [`RdmaState`] credit counters
//! and the lock-free registry; queued (flow-controlled) sends are handed to the
//! poller through an mpsc channel.

use std::{
    collections::HashMap,
    os::fd::AsRawFd,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use ruapc_rdma::{CompChannel, CompletionQueue, ibv_wc};

use crate::{Buffer, Error, ErrorKind, Message, Result, Socket, State, rdma::RdmaSocket};

/// Process-wide monotonic work-request id allocator.
///
/// The `wr_id` is carried on the wire and echoed back in the completion; it
/// only needs to be unique among work requests that may be in flight on the
/// same CQ at the same time. A global counter trivially satisfies that. The
/// full 64-bit range is usable (the `WRID` type no longer reserves any bits).
static NEXT_WR_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates a new globally-unique work-request id.
pub(crate) fn next_wr_id() -> u64 {
    NEXT_WR_ID.fetch_add(1, Ordering::Relaxed)
}

/// Oneshot completion delivered to an awaiting RDMA-read caller.
pub(crate) type RdmaCompletion = (ruapc_rdma::ibv_wc_status, Option<Buffer>);

/// The kind of operation a work request represents.
pub(crate) enum WrOp {
    /// A pre-posted receive buffer.
    Recv,
    /// An outbound data SEND.
    DataSend,
    /// An outbound flow-control ACK (zero-length SEND_WITH_IMM). Carries no
    /// buffer.
    Ack,
    /// An RDMA READ; on completion the buffer + status are delivered to the
    /// awaiting caller via this oneshot sender.
    RdmaRead(tokio::sync::oneshot::Sender<RdmaCompletion>),
}

/// Per-work-request bookkeeping stored in a CQ's [`WrRegistry`].
pub(crate) struct WrContext {
    /// The connection that posted this work request. `Weak` so a dropped
    /// connection does not keep the socket (and its QP) alive; the poller
    /// simply discards completions whose socket has gone away.
    pub(crate) socket: Weak<RdmaSocket>,
    /// Buffer owned by this work request (None for ACKs).
    pub(crate) buf: Option<Buffer>,
    /// Operation kind.
    pub(crate) op: WrOp,
}

/// A per-CQ table mapping `wr_id` -> [`WrContext`].
///
/// Written by send-side caller threads (on post) and by the poller thread (on
/// completion). Guarded by a `Mutex`; contention is naturally partitioned
/// across the N shared CQs since each connection only ever registers into its
/// own CQ's registry.
#[derive(Default)]
pub(crate) struct WrRegistry {
    map: Mutex<HashMap<u64, WrContext>>,
}

impl std::fmt::Debug for WrRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrRegistry").finish()
    }
}

impl WrRegistry {
    /// Registers a work request and posts it, atomically with respect to the
    /// poller.
    ///
    /// The buffer's ownership is moved into the registry first (so it stays
    /// pinned for the NIC). `post` is then invoked with the assigned `wr_id`
    /// and a borrow of the just-stored buffer; on `Err` the entry is rolled
    /// back. The registry lock is held across `post`, which is safe and cheap:
    /// the only other accessor is the poller, and a completion for this `wr_id`
    /// cannot arrive before `post` has even submitted it.
    pub(crate) fn register_and_post<F>(&self, ctx: WrContext, post: F) -> Result<()>
    where
        F: FnOnce(u64, Option<&Buffer>) -> Result<()>,
    {
        let id = next_wr_id();
        let mut map = self.map.lock().unwrap();
        // Move the work-request context (and the buffer it owns) into the
        // registry first so the buffer stays pinned for the NIC, then post
        // using a borrow taken from the map itself.
        let entry = map.entry(id).or_insert(ctx);
        let buf_ref = entry.buf.as_ref();
        match post(id, buf_ref) {
            Ok(()) => Ok(()),
            Err(e) => {
                map.remove(&id);
                Err(e)
            }
        }
    }

    /// Removes and returns the context for `id`, if present.
    pub(crate) fn take(&self, id: u64) -> Option<WrContext> {
        self.map.lock().unwrap().remove(&id)
    }
}

/// A shared completion queue plus its completion channel and work-request
/// registry. Cloned (via `Arc`) into every connection assigned to this CQ and
/// into the poller that drains it.
#[derive(Clone)]
pub(crate) struct SharedCq {
    pub(crate) cq: Arc<CompletionQueue>,
    pub(crate) comp_channel: Arc<CompChannel>,
    pub(crate) registry: Arc<WrRegistry>,
}

impl std::fmt::Debug for SharedCq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedCq").finish()
    }
}

/// Flow-control thresholds (per connection).
struct FlowConfig {
    ack_threshold: u32,
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

/// Per-connection state driven exclusively by the owning CQ's poller thread.
///
/// This is the old per-connection `EventLoop` state, minus the CQ/poller
/// infrastructure (which is now shared). It is registered with the poller when
/// the connection is created and removed when the connection drains.
pub(crate) struct ConnState {
    socket: Arc<RdmaSocket>,
    state: Arc<State>,
    /// Receiver half of the flow-controlled (queued) send channel.
    pending_receiver: tokio::sync::mpsc::Receiver<SendMsg>,
    /// Buffers whose send was deferred because the credit window was full,
    /// keyed by their send index so they are released in order.
    pending_sends: std::collections::BTreeMap<u64, Buffer>,

    // Recv-side counters.
    recv_buffer_size: usize,
    recv_submitted: u64,
    recv_completed: u64,
    imm_received: u64,
    imm_acked: u64,
    data_received: u64,
    data_acked: u64,

    // Send-side counters.
    data_completed: u64,
    data_confirmed: u64,
    ack_submitted: u64,
    ack_completed: u64,
    ack_confirmed: u64,

    last_ack_timestamp: Instant,
    flow_config: FlowConfig,
}

/// A flow-controlled send handed from a caller thread to the poller.
pub struct SendMsg {
    pub id: u64,
    pub buf: Buffer,
}

impl ConnState {
    pub(crate) fn new(
        socket: Arc<RdmaSocket>,
        state: Arc<State>,
        pending_receiver: tokio::sync::mpsc::Receiver<SendMsg>,
        recv_buffer_size: usize,
    ) -> Self {
        Self {
            socket,
            state,
            pending_receiver,
            pending_sends: std::collections::BTreeMap::new(),
            recv_buffer_size,
            recv_submitted: 0,
            recv_completed: 0,
            imm_received: 0,
            imm_acked: 0,
            data_received: 0,
            data_acked: 0,
            data_completed: 0,
            data_confirmed: 0,
            ack_submitted: 0,
            ack_completed: 0,
            ack_confirmed: 0,
            last_ack_timestamp: Instant::now(),
            flow_config: FlowConfig::default(),
        }
    }

    /// Pre-posts `count` receive buffers of `recv_buffer_size`.
    pub(crate) fn submit_recv_tasks(&mut self, count: usize) -> Result<()> {
        for _ in 0..count {
            let buf = self.socket.rdmabuf_pool.allocate(self.recv_buffer_size)?;
            self.post_recv(buf)?;
        }
        Ok(())
    }

    fn post_recv(&mut self, buf: Buffer) -> Result<()> {
        self.socket.post_recv(buf)?;
        self.recv_submitted += 1;
        Ok(())
    }

    /// Handles a single completion belonging to this connection.
    fn handle_completion(&mut self, wc: &ibv_wc, ctx: WrContext) {
        match ctx.op {
            WrOp::Recv => self.handle_recv_completion(wc, ctx.buf),
            WrOp::DataSend => {
                self.data_completed += 1;
                if !wc.succ() {
                    tracing::error!("send completion error: {wc:?}");
                    self.socket.set_error();
                }
                // ctx.buf dropped here -> returned to the pool.
            }
            WrOp::Ack => {
                self.ack_completed += 1;
                if !wc.succ() {
                    tracing::error!("ack completion error: {wc:?}");
                    self.socket.set_error();
                }
            }
            WrOp::RdmaRead(sender) => {
                // Deliver buffer + status back to the awaiting caller.
                let _ = sender.send((wc.status, ctx.buf));
            }
        }
    }

    fn handle_recv_completion(&mut self, wc: &ibv_wc, buffer: Option<Buffer>) {
        self.recv_completed += 1;

        if !wc.succ() {
            tracing::error!(
                "recv completion error: {:?}, {:?}",
                wc.status,
                wc.vendor_err
            );
            self.socket.set_error();
            return;
        } else if let Some(ack) = wc.imm() {
            // Flow-control ACK from the peer.
            self.imm_received += 1;
            self.data_confirmed += u64::from(ack & 0xFFFF);
            self.ack_confirmed += u64::from(ack >> 16);
        } else {
            self.data_received += 1;
            if let Some(mut buf) = buffer {
                buf.set_len(wc.byte_len as usize);
                if let Ok(msg) = Message::parse(buf)
                    && let Err(e) = self.state.handle_recv(&Socket::from(&self.socket), msg)
                {
                    tracing::error!("Failed to handle message: {e}");
                }
            }
        }

        // Re-post a fresh recv buffer to keep the receive queue full.
        match self.socket.rdmabuf_pool.allocate(self.recv_buffer_size) {
            Ok(new_buf) => {
                if let Err(e) = self.post_recv(new_buf) {
                    tracing::error!("failed to re-post recv buffer: {e}");
                    self.socket.set_error();
                }
            }
            Err(e) => {
                tracing::error!("failed to allocate recv buffer: {e}");
                self.socket.set_error();
            }
        }
    }

    /// Returns true if there are queued sends waiting for credit.
    fn has_pending_sends(&self) -> bool {
        !self.pending_sends.is_empty()
    }

    /// Advances flow control: releases queued sends as credit frees up and
    /// emits an ACK to the peer when enough receives have accumulated.
    fn update_flow_control(&mut self) {
        // Drain newly-queued sends from the caller channel.
        while let Ok(msg) = self.pending_receiver.try_recv() {
            self.pending_sends.insert(msg.id, msg.buf);
        }

        if !self.socket.state.is_ok() {
            // Connection failed: drop queued buffers, counting them as completed
            // so the drain check can make progress.
            while self.pending_sends.pop_first().is_some() {
                self.data_completed += 1;
            }
            return;
        }

        let completed_sends = std::cmp::min(self.data_completed, self.data_confirmed);
        let sendable_bound = self.socket.state.update_send_finished(completed_sends);
        while let Some(entry) = self.pending_sends.first_entry()
            && *entry.key() < sendable_bound
        {
            let buf = entry.remove();
            if let Err(e) = self.socket.post_data_send(buf) {
                tracing::error!("failed to send pending buffer: {e}");
                self.socket.set_error();
                return;
            }
        }

        let ack_done = std::cmp::min(self.ack_completed, self.ack_confirmed);
        if self.ack_submitted >= ack_done + u64::from(self.flow_config.ack_max_limit) {
            return;
        }

        let pending_data = u32::try_from(self.data_received - self.data_acked).unwrap_or(u32::MAX);
        let pending_imm = u32::try_from(self.imm_received - self.imm_acked).unwrap_or(u32::MAX);

        if pending_data >= self.flow_config.ack_threshold
            || pending_imm >= self.flow_config.ack_max_limit / 2
            || self.last_ack_timestamp.elapsed().as_secs() >= 5
        {
            self.submit_ack(pending_imm, pending_data);
            self.data_acked = self.data_received;
            self.imm_acked = self.imm_received;
            self.last_ack_timestamp = Instant::now();
        }
    }

    fn submit_ack(&mut self, pending_imm: u32, pending_data: u32) {
        let imm_data = (pending_imm << 16) + pending_data;
        match self.socket.post_ack(imm_data) {
            Ok(()) => self.ack_submitted += 1,
            Err(err) => {
                // Account for the never-completing ack so drain can finish.
                self.ack_submitted += 1;
                self.ack_completed += 1;
                self.ack_confirmed += 1;
                tracing::error!("submit ack error: {err}");
                self.socket.set_error();
            }
        }
    }

    /// True when the connection has fully drained and can be removed.
    fn ready_to_remove(&self) -> bool {
        self.socket.state.ready_to_remove(self.data_completed)
            && self.ack_submitted == self.ack_completed
            && self.recv_submitted == self.recv_completed
    }
}

/// Registry of the connections currently driven by one poller, keyed by QP num.
type ConnMap = Mutex<HashMap<u32, ConnState>>;

/// A dedicated OS thread that drains one shared CQ with adaptive busy-polling.
pub(crate) struct CqPoller {
    shared: SharedCq,
    conns: Arc<ConnMap>,
    stop: Arc<AtomicBool>,
}

impl CqPoller {
    pub(crate) fn new(shared: SharedCq, conns: Arc<ConnMap>, stop: Arc<AtomicBool>) -> Self {
        Self {
            shared,
            conns,
            stop,
        }
    }

    /// Runs the poll loop until `stop` is set and all connections have drained.
    pub(crate) fn run(self) {
        // Number of consecutive idle iterations to busy-poll before parking on
        // the completion-channel fd. Keeps latency low under load while letting
        // a fully idle CQ sleep instead of burning the core.
        const SPIN_BUDGET: u32 = 512;
        let mut wcs = [ibv_wc::default(); 64];
        let mut unacked_events: u32 = 0;
        let mut idle_spins: u32 = 0;

        loop {
            let stopping = self.stop.load(Ordering::Acquire);

            let processed = self.drain(&mut wcs);

            // Drive per-connection flow control and reap fully-drained
            // connections. Track whether any connection still has queued sends
            // waiting for credit (those need us to keep spinning).
            let mut any_pending = false;
            {
                let mut conns = self.conns.lock().unwrap();
                conns.retain(|_, conn| {
                    // On shutdown, force the connection into error so its WRs
                    // drain and it becomes removable.
                    if stopping {
                        conn.socket.set_error();
                    }
                    conn.update_flow_control();
                    if conn.ready_to_remove() {
                        return false;
                    }
                    any_pending |= conn.has_pending_sends();
                    true
                });
                if conns.is_empty() && stopping {
                    break;
                }
            }

            if processed > 0 || any_pending {
                idle_spins = 0;
                continue;
            }

            if stopping {
                // Draining for shutdown: yield until all connections are
                // reaped; do not park.
                std::thread::yield_now();
                continue;
            }

            idle_spins += 1;
            if idle_spins < SPIN_BUDGET {
                // Busy-poll window. Yield to the OS scheduler rather than a hard
                // spin so that, if pollers are oversubscribed relative to cores
                // (e.g. many connection pools in one process), they share CPU
                // fairly instead of livelocking. Under normal operation
                // (pollers <= cores) this still returns to poll the CQ almost
                // immediately, keeping latency low.
                std::thread::yield_now();
                continue;
            }
            idle_spins = 0;

            // Idle: arm notification and park on the comp-channel fd.
            if self.shared.cq.req_notify(false).is_err() {
                continue;
            }
            // Re-check once after arming to avoid a lost wakeup.
            if self.drain(&mut wcs) > 0 {
                continue;
            }
            self.park(&mut unacked_events);
        }

        if unacked_events > 0 {
            self.shared.cq.ack_events(unacked_events);
        }
    }

    /// Polls and processes all currently-available completions. Returns the
    /// number handled.
    fn drain(&self, wcs: &mut [ibv_wc]) -> usize {
        let mut total = 0;
        loop {
            let n = match self.shared.cq.poll(wcs) {
                Ok(n) => n,
                Err(e) => {
                    tracing::error!("cq poll error: {e}");
                    return total;
                }
            };
            for wc in wcs[..n].iter() {
                let id = wc.wr_id.get_id();
                let Some(ctx) = self.shared.registry.take(id) else {
                    // No context: e.g. a stale completion after the connection
                    // was torn down. Nothing to do.
                    continue;
                };
                // Route to the owning connection's state. We must drop the
                // strong ref before re-locking conns for flow control, so scope
                // the upgrade here.
                if let Some(socket) = ctx.socket.upgrade() {
                    let qp_num = socket.queue_pair.qp_num();
                    if let Some(conn) = self.conns.lock().unwrap().get_mut(&qp_num) {
                        conn.handle_completion(wc, ctx);
                    }
                    // else: connection already removed; ctx (and its buffer)
                    // drops here, returning the buffer to the pool.
                }
            }
            total += n;
            if n < wcs.len() {
                break;
            }
        }
        total
    }

    /// Blocks until the completion channel signals an event (or a short
    /// timeout), then consumes the event so the CQ can be re-armed next round.
    #[allow(unsafe_code)]
    fn park(&self, unacked_events: &mut u32) {
        let raw = self.shared.comp_channel.fd().as_raw_fd();
        let mut pfd = libc::pollfd {
            fd: raw,
            events: libc::POLLIN,
            revents: 0,
        };
        // 200 ms timeout bounds shutdown latency and serves as a periodic
        // wakeup for time-based ACK flushing.
        let ret = unsafe { libc::poll(&mut pfd, 1, 200) };
        if ret > 0 && (pfd.revents & libc::POLLIN) != 0 {
            // Consume the event (channel is non-blocking). Each successful
            // get_event must eventually be balanced by ack_events.
            match self.shared.comp_channel.get_event() {
                Ok(_) => *unacked_events += 1,
                Err(e) => {
                    if e.kind != ruapc_rdma::ErrorKind::IBGetCompQueueEventFail {
                        tracing::error!("get_cq_event error: {e}");
                    }
                }
            }
        }
    }
}

/// One shared CQ slot: the CQ/registry plus the set of connections it drives.
struct CqSlot {
    shared: SharedCq,
    conns: Arc<ConnMap>,
}

/// A fixed pool of shared completion queues, partitioned per RDMA device.
///
/// At construction it creates `cq_count` CQs (and dedicated poller threads) for
/// each device. Connections are assigned (randomly) to one CQ on their device
/// for their whole lifetime. The pool owns the poller `JoinHandle`s and the
/// shared stop flag; [`shutdown`](Self::shutdown) signals all pollers and joins
/// the threads, which must happen before the QPs/CQs are destroyed.
pub(crate) struct CqPool {
    /// `slots[device_index]` holds the CQ slots for that device.
    slots: Vec<Vec<CqSlot>>,
    stop: Arc<AtomicBool>,
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl CqPool {
    /// Builds `cq_count` shared CQs + pollers for each device.
    ///
    /// `make_cq(device_index) -> (CompletionQueue, CompChannel)` is provided by
    /// the caller since CQ creation needs the device's verbs context.
    pub(crate) fn new<F>(
        device_count: usize,
        cq_count: usize,
        runtime: tokio::runtime::Handle,
        mut make_cq: F,
    ) -> Result<Self>
    where
        F: FnMut(usize) -> Result<(Arc<CompletionQueue>, Arc<CompChannel>)>,
    {
        let cq_count = cq_count.max(1);
        let stop = Arc::new(AtomicBool::new(false));
        let mut slots = Vec::with_capacity(device_count);
        let mut handles = Vec::with_capacity(device_count * cq_count);

        for dev in 0..device_count {
            let mut dev_slots = Vec::with_capacity(cq_count);
            for _ in 0..cq_count {
                let (cq, comp_channel) = make_cq(dev)?;
                comp_channel
                    .set_nonblock()
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
                let shared = SharedCq {
                    cq,
                    comp_channel,
                    registry: Arc::new(WrRegistry::default()),
                };
                let conns: Arc<ConnMap> = Arc::new(Mutex::new(HashMap::new()));

                let poller = CqPoller::new(shared.clone(), conns.clone(), stop.clone());
                let rt = runtime.clone();
                let handle = std::thread::Builder::new()
                    .name(format!("ruapc-cq-{dev}"))
                    .spawn(move || {
                        // Enter the tokio runtime so the poller can spawn request
                        // handlers via the captured Handle.
                        let _guard = rt.enter();
                        poller.run();
                    })
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
                handles.push(handle);
                dev_slots.push(CqSlot { shared, conns });
            }
            slots.push(dev_slots);
        }

        Ok(Self {
            slots,
            stop,
            handles: Mutex::new(handles),
        })
    }

    /// Picks a shared CQ on `device_index` for a new connection, round-robin /
    /// pseudo-random via `selector`.
    pub(crate) fn pick(&self, device_index: usize, selector: usize) -> SharedCq {
        let dev_slots = &self.slots[device_index];
        dev_slots[selector % dev_slots.len()].shared.clone()
    }

    /// Registers a fully-established connection's state with the poller that
    /// owns `shared`, so the poller starts driving its completions and flow
    /// control.
    pub(crate) fn register_conn(&self, shared: &SharedCq, qp_num: u32, conn: ConnState) {
        for dev_slots in &self.slots {
            for slot in dev_slots {
                if Arc::ptr_eq(&slot.shared.registry, &shared.registry) {
                    slot.conns.lock().unwrap().insert(qp_num, conn);
                    return;
                }
            }
        }
    }

    /// Signals all pollers to stop and joins their threads. Idempotent.
    pub(crate) fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl std::fmt::Debug for CqPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CqPool")
            .field("devices", &self.slots.len())
            .finish()
    }
}

impl Drop for CqPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}
