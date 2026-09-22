//! Per-connection state owned by the poll thread: flow control, pending
//! sends, receive-ring accounting and teardown readiness.

use std::{collections::VecDeque, sync::Arc, time::Instant};

use bytes::Bytes;
use ruapc_rdma::{
    Completion, CompletionCursor, CompletionIdentity, WRType, WrBuffers, ibv_send_flags, ibv_wc,
};

use super::{RegisterConn, RingReservation, dispatch::DispatchBatch, flow::FlowControl};
use crate::{
    Buffer, Error, ErrorKind, Result, Socket, State,
    rdma::{RdmaSocket, SendPermit},
    task::TaskSupervisorGuard,
};

/// Upper bound for one aggregated send; packing more rarely helps and would
/// only add head-of-line latency for the packed messages.
const MAX_AGG_BYTES: usize = 64 * 1024;

/// Copy received frame batches up to this size and cache the registered
/// buffer for reposting. This keeps small messages from holding receive-ring
/// memory while handlers run. Larger batches retain the zero-copy path and
/// require another pool buffer for reposting; pool pressure can shrink the ring.
const SMALL_MSG_COPY_MAX: usize = 1024;

/// Owns the open-connection accounting and failure notification. There is one
/// guard per registered poller connection, so normal removal and poller failure
/// settle the same lifecycle exactly once. The guard owns the State reference
/// the connection already needed; it adds no allocation or shared ownership.
struct RegisteredConnection {
    state: Arc<State>,
    conn_id: u64,
}

impl RegisteredConnection {
    fn new(state: Arc<State>, conn_id: u64) -> Self {
        state.metrics.connection_opened("RDMA");
        Self { state, conn_id }
    }
}

impl Drop for RegisteredConnection {
    fn drop(&mut self) {
        self.state.metrics.connection_closed("RDMA");
        self.state.connection_closed(
            self.conn_id,
            &Error::new(ErrorKind::ConnectionClosed, "rdma connection closed".into()),
        );
    }
}

/// Per-connection state owned by the poll thread.
///
/// Field order matters for teardown: handlers holding buffers come before
/// `socket` so buffers are released before the QP can be destroyed.
pub(super) struct ConnState {
    /// Listed once in the poller's active maintenance queue. Never shared
    /// with posting tasks and never used to authorize completion ownership.
    pub(super) dirty: bool,
    /// CQ-assigned QP identity and sequence floor; stale completions are discarded
    /// before they can affect a replacement connection's flow control.
    pub(super) identity: CompletionIdentity,
    pub(super) flow: FlowControl,
    /// Window-blocked framed sends in FIFO order.
    pub(super) pending_sends: VecDeque<Buffer>,
    pending_receiver: tokio::sync::mpsc::Receiver<Buffer>,
    /// Progress of the QP-owned selective SEND reclamation.
    completion_cursor: CompletionCursor,
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
    registration: RegisteredConnection,
    _supervisor_guard: TaskSupervisorGuard,
    _ring_reservation: RingReservation,
    _conn_count_guard: super::super::ConnCountGuard,
}

impl ConnState {
    pub(super) fn new(reg: RegisterConn) -> Self {
        let registration = RegisteredConnection::new(reg.state, reg.socket.conn_id);
        Self {
            dirty: false,
            identity: reg.socket.queue_pair.send_identity(),
            flow: FlowControl::new(reg.send_window, reg.recv_submitted, Instant::now()),
            pending_sends: VecDeque::new(),
            pending_receiver: reg.pending_receiver,
            completion_cursor: CompletionCursor::default(),
            recv_buf_size: reg.recv_buf_size,
            msg_aggregation: reg.msg_aggregation,
            recv_buf_cache: Vec::new(),
            recv_deficit: 0,
            socket: reg.socket,
            registration,
            _supervisor_guard: reg.supervisor_guard,
            _ring_reservation: reg.ring_reservation,
            _conn_count_guard: reg.conn_count_guard,
        }
    }

    /// Handles one work completion for this connection.
    pub(super) fn handle_wc(&mut self, completion: Completion<'_>, batch: &mut DispatchBatch) {
        let completed = match self
            .socket
            .queue_pair
            .complete(completion, &mut self.completion_cursor)
        {
            Ok(completed) => completed,
            Err(error) => {
                tracing::error!(%error, "completion does not belong to the routed RDMA connection");
                self.socket.set_error();
                return;
            }
        };
        for _ in 0..completed.swept_sends {
            self.flow.data_completed();
        }
        let wc = completed.wc;
        let buffer = completed.buffer;
        let result = if wc.is_recv() {
            // Receive WRs always post a single buffer.
            self.handle_recv_completion(wc, buffer.and_then(WrBuffers::into_single), batch)
        } else {
            self.handle_send_completion(wc, buffer)
        };
        if let Err(err) = result {
            // QP setup only programs the NIC; an unreachable path often first
            // fails on the activation SEND. Preserve that completion error,
            // while suppressing the ensuing flush-completion noise.
            if self.socket.state.is_ok() {
                tracing::warn!(
                    conn_id = self.socket.conn_id,
                    local_qp = self.socket.queue_pair.qp_num(),
                    path = ?self.socket.path,
                    wr_id = ?wc.wr_id,
                    status = ?wc.status,
                    vendor_err = wc.vendor_err,
                    %err,
                    "RDMA work completion failed; closing connection"
                );
            }
            self.socket.set_error();
        }
    }

    fn handle_recv_completion(
        &mut self,
        wc: &ibv_wc,
        buffer: Option<Buffer>,
        batch: &mut DispatchBatch,
    ) -> Result<()> {
        self.flow.receive_completed();

        if !wc.succ() {
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
                .registration
                .state
                .socket_pool
                .rdma_receive_observed(connection_id, &self.socket);
        }

        // Immediate data (ACK credit counters) can arrive standalone or
        // piggybacked on a data send.
        if let Some(ack) = wc.imm() {
            self.flow.peer_ack(ack);
        }

        if let Some(mut buf) = buffer {
            buf.set_len(wc.byte_len as usize);
            if buf.is_empty() {
                // Standalone ACK: the buffer is untouched, recycle it.
                self.flow.received_ack();
                self.cache_recv_buf(buf);
            } else {
                // One receive completion = one flow control credit, no
                // matter how many frames the buffer carries: the credit
                // stands for the receive-ring buffer, which is consumed
                // exactly once per WC. The frames are walked and parsed by
                // the dispatch workers, never here.
                self.flow.received_data();
                let frames = if buf.len() <= SMALL_MSG_COPY_MAX {
                    // Copy small buffers out and recycle the receive
                    // buffer; see `SMALL_MSG_COPY_MAX` for why.
                    let copied = Bytes::copy_from_slice(&buf);
                    self.cache_recv_buf(buf);
                    copied
                } else {
                    Bytes::from_owner(buf)
                };
                batch.push((
                    self.registration.state.clone(),
                    Socket::from(&self.socket),
                    frames,
                ));
            }
        } else if wc.imm().is_some() {
            self.flow.received_ack();
        } else {
            self.flow.received_data();
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
        self.flow.receive_posted();
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
                    self.flow.receive_posted();
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
                self.socket.read_credits.complete();
                if wc.succ() {
                    return Ok(());
                }
                // Fall through to the error return below (which moves the
                // connection to the error state).
            }
            // A buffer-less immediate send is a standalone ACK; one with a
            // buffer is a data send with a piggybacked ACK, which lives in
            // the data ledger.
            WRType::SendImm if buffer.is_none() => self.flow.ack_completed(),
            _ => self.flow.data_completed(),
        }

        if wc.succ() {
            Ok(())
        } else {
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
        // Activation consumes the same bounded ACK capacity as keepalives.
        // Leave the request pending when the limit is full.
        if self.flow.can_submit_ack()
            && self.socket.take_activation_request()
            && let Err(err) = self.submit_ack(0)
        {
            self.socket.request_activation();
            return Err(err);
        }

        // One credit per data WR, returned once the WR completed locally
        // (buffer reclaimed) *and* the peer acknowledged the matching
        // receive completion.
        let finished = self.flow.finished_data();
        let now = Instant::now();

        // Liveness diagnostics: a pending send that stays window-blocked
        // for seconds indicates a flow control stall (peer ACKs missing or
        // completion accounting gone wrong).
        if !self.pending_sends.is_empty() && self.flow.stalled(now) {
            tracing::warn!(
                "flow stall: qp={} pending={} finished={finished} ok={} flow={:?}",
                self.socket.queue_pair.qp_num(),
                self.pending_sends.len(),
                self.socket.state.is_ok(),
                self.flow,
            );
        }
        // An acknowledgment overdue for seconds means the standalone-ACK
        // path is starved (the peer's send window may be stalling on it).
        if self.flow.ack_starved(now) {
            tracing::warn!(
                "ack starvation: qp={} ok={} flow={:?}",
                self.socket.queue_pair.qp_num(),
                self.socket.state.is_ok(),
                self.flow,
            );
        }

        // Decide whether an ACK is due *before* flushing pending sends so it
        // can piggyback on one of them (saving a standalone WR + CQE + a
        // recv buffer cycle on the peer).
        let mut ack = self.flow.due_ack(now);

        // Flush pending sends against the *unpublished* finished value:
        // the backlog spends freshly freed credits before
        // `update_send_finished` makes them visible to direct senders, so
        // pending traffic cannot be starved by new sends.
        let flush_result = self.flush_pending(finished, &mut ack);
        self.socket.state.update_send_finished(finished);

        // Send the standalone ACK even when the flush failed (e.g. a
        // transient allocation error): the peer's send window depends on our
        // ACKs, so skipping them would deadlock both sides.
        if let Some(imm) = ack
            && self.flow.can_submit_ack()
        {
            self.submit_ack(imm)?;
            self.flow.mark_acked(imm, Instant::now());
        }

        flush_result
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
    /// pool-allocation-free *gather-list* send of the same run, keeping the
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

            // Prefer one contiguous SGE. If the pool cannot allocate it,
            // gather existing buffers to preserve aggregation's credit savings.
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
                    self.flow.mark_acked(imm, Instant::now());
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
        match self.socket.queue_pair.send_gather(frames, imm) {
            Ok(_) => {
                if let Some(imm) = imm {
                    self.flow.mark_acked(imm, Instant::now());
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
        self.flow.ack_posted(ret.is_ok());
        match ret {
            Ok(()) => Ok(()),
            Err(err) => {
                tracing::error!("submit ack error: {err}");
                self.socket.set_error();
                Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    format!("failed to post ack: {err}"),
                ))
            }
        }
    }

    /// Fails expired READ waiters and moves the QP to ERR. Posted memory and
    /// permits stay held until completion processing or successful QP destruction.
    pub(super) fn sweep_read_timeouts(&self, now: Instant) {
        // Avoid walking every shard of an empty READ map for every idle QP.
        // A READ starting after this check is covered by the next sweep; a
        // posted READ retains its SQ permit until completion processing.
        if self.socket.read_credits.is_idle() {
            return;
        }
        if self.socket.queue_pair.expire_reads(now) {
            tracing::error!(
                "RDMA READ timeout on qp={}, moving connection to error state",
                self.socket.queue_pair.qp_num()
            );
            self.socket.set_error();
        }
    }

    /// Whether this connection can be torn down.
    ///
    /// Requires error state, settled ACK/receive counters, no pending sends,
    /// and closed, idle READ admission with no pending batches. Waiting for
    /// every SQ permit covers posters paused before registering their batch;
    /// earlier removal would lose their completions and NIC permits.
    /// Successfully completed unsignaled SENDs may still own buffers until
    /// the QP is destroyed.
    pub(super) fn ready_to_remove(&mut self) -> bool {
        !self.socket.state.is_ok()
            && self.flow.flushed()
            && self.pending_sends.is_empty()
            && self.socket.read_credits.is_closed()
            && self.socket.read_credits.is_idle()
            && !self.socket.queue_pair.has_pending_reads()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::time::Duration;

    #[tokio::test]
    async fn registered_connection_drop_fails_its_waiters_and_settles_metrics() {
        let config = crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        };
        let ctx = crate::Context::create(&config).unwrap();
        let state = &ctx.state;
        let (id, receiver) = state.waiter.alloc(Duration::from_secs(30));
        state.waiter.bind_connection(id, 42);
        let (other_id, other_receiver) = state.waiter.alloc(Duration::from_secs(30));
        state.waiter.bind_connection(other_id, 43);

        let recorder = DebuggingRecorder::new();
        let snapshot = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let registration = RegisteredConnection::new(state.clone(), 42);
            assert_eq!(state.waiter.pending_count(), 2);
            // The poller owns this guard; clearing its connection registry drops
            // it on both ordinary removal and any provider-error exit.
            drop(registration);
        });

        assert_eq!(
            receiver.recv().await.unwrap_err().kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(
            state.waiter.pending_count(),
            1,
            "other connections remain pending"
        );
        let metrics = snapshot.snapshot().into_vec();
        let (_, _, _, value) = metrics
            .iter()
            .find(|(key, ..)| key.key().name() == "ruapc_connections")
            .unwrap();
        assert!(matches!(value, DebugValue::Gauge(value) if value.0 == 0.0));
        drop(other_receiver);
    }
}
