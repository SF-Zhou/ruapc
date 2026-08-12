//! Per-connection state owned by the poll thread: flow control, pending
//! sends, receive-ring accounting and teardown readiness.

use std::{collections::VecDeque, sync::Arc, time::Instant};

use bytes::Bytes;
use ruapc_rdma::{WRType, WrBuffers, ibv_send_flags, ibv_wc};

use super::{BudgetGuard, RegisterConn, RingReservation, dispatch::DispatchBatch};
use crate::{
    Buffer, Error, ErrorKind, Result, Socket, State,
    rdma::{RdmaSocket, SendPermit},
    task::TaskSupervisorGuard,
};

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
pub(super) struct RecvStats {
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
pub(super) struct SendStats {
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
pub(super) struct ConnState {
    /// Generation of the occupied slot; completions tagged with another
    /// generation belonged to a previous occupant and are dropped.
    pub(super) generation: u8,
    pub(super) recv: RecvStats,
    pub(super) send: SendStats,
    last_ack_timestamp: Instant,
    flow: FlowConfig,
    /// Window-blocked framed sends in FIFO order.
    pub(super) pending_sends: VecDeque<Buffer>,
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
    pub(super) recv_deficit: u64,
    pub(super) socket: Arc<RdmaSocket>,
    pub(super) state: Arc<State>,
    _budget: BudgetGuard,
    _supervisor_guard: TaskSupervisorGuard,
    _ring_reservation: RingReservation,
    _conn_count_guard: super::super::ConnCountGuard,
}

impl ConnState {
    pub(super) fn new(reg: RegisterConn, generation: u8, budget: BudgetGuard) -> Self {
        reg.state.metrics.connection_opened("RDMA");
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
            _conn_count_guard: reg.conn_count_guard,
        }
    }

    /// Handles one work completion for this connection.
    pub(super) fn handle_wc(&mut self, wc: &ibv_wc, batch: &mut DispatchBatch) {
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
        if let Some(connection_id) = self.socket.take_accept_lease() {
            let _ = self
                .state
                .socket_pool
                .rdma_receive_observed(connection_id, &self.socket);
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
    pub(super) fn retry_recv_deficit(&mut self) {
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
            // RDMA one-sided operation: account the completion on its
            // batch (the batch owns the memory, not the WR slot table).
            // Reads consume no peer receive buffer, so they take part in
            // no flow control accounting.
            WRType::Read => {
                debug_assert!(buffer.is_none(), "read WRs store no slot buffer");
                // Return the in-flight-read permits (per-NIC + per-SQ)
                // taken at post time.
                self.socket.read_permits.add_permits(1);
                self.socket.sq_read_permits.add_permits(1);
                if let Some((_, batch)) = self.socket.rdma_completions.remove(&wc.wr_id) {
                    batch.complete_one(wc.succ());
                }
                if wc.succ() {
                    return Ok(());
                }
                // Fall through to the error return below (which moves the
                // connection to the error state).
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
    pub(super) fn drain_pending(&mut self) {
        while let Ok(buf) = self.pending_receiver.try_recv() {
            self.pending_sends.push_back(buf);
        }
    }

    /// Updates flow control state, flushes window-unblocked pending sends
    /// and emits acknowledgments when thresholds are reached.
    pub(super) fn update_flow_control(&mut self) -> Result<()> {
        if !self.socket.state.is_ok() {
            // Pending sends never acquired a credit; just drop them.
            self.pending_sends.clear();
            return Ok(());
        }
        if self.socket.take_activation_request()
            && let Err(err) = self.submit_ack(0)
        {
            self.socket.request_activation();
            return Err(err);
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

    /// Fails read batches that exceeded their deadline; a NIC stuck on an
    /// RDMA READ must not park the caller forever. Failing the connection
    /// moves the QP to the error state, so the outstanding reads
    /// eventually surface as flush completions — which is what releases
    /// their memory holds safely.
    pub(super) fn sweep_read_timeouts(&self, now: Instant) {
        if self.socket.rdma_completions.is_empty() {
            return;
        }
        let mut fired = false;
        for entry in self.socket.rdma_completions.iter() {
            let batch = entry.value();
            if batch.expired(now)
                && batch.fail(Error::new(
                    ErrorKind::RdmaReadTimeout,
                    "RDMA READ did not complete within rdma.read_timeout_ms; \
                     failing the connection to flush it"
                        .into(),
                ))
            {
                fired = true;
            }
        }
        if fired {
            tracing::error!(
                "RDMA READ timeout on qp={}, moving connection to error state",
                self.socket.queue_pair.qp_num()
            );
            self.socket.set_error();
        }
    }

    /// Whether this connection can be torn down.
    ///
    /// Only true after the socket entered the error state (QP moved to ERR):
    /// every outstanding work request then produces a flush CQE, so waiting
    /// for the ACK and recv counters to settle guarantees the QP finished
    /// flushing. Buffers of successfully-completed unsignaled sends never
    /// produce a CQE and are reclaimed explicitly before removal.
    /// Outstanding RDMA READ batches also block removal: their memory
    /// holds may only be released once their (flush) completions arrived.
    pub(super) fn ready_to_remove(&mut self) -> bool {
        if self.socket.state.is_ok()
            || self.send.ack_submitted != self.send.ack_completed
            || self.recv.submitted != self.recv.completed
            || !self.pending_sends.is_empty()
            || !self.socket.rdma_completions.is_empty()
        {
            return false;
        }
        self.socket.queue_pair.reclaim_send_buffers();
        true
    }
}
