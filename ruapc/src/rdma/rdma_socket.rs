use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_rdma::{QueuePair, ReadSge, WRID, ibv_send_flags};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::{RdmaPathInfo, RdmaState, SendPermit};
use crate::{
    Buffer, BufferPool, Context, CopyOp, Error, RemoteIoError, RemoteSpace, SocketTrait, State,
    core::{
        WriteTarget,
        scatter::{self, SpaceLayout},
    },
    error::{ErrorKind, Result},
    msg::MsgMeta,
    rdma::poller::{FRAME_HEADER, PollerWaker},
    services::{MemoryService, MetaService},
};

/// Serializes a message as one wire frame: `[4B frame_len][4B meta_len]
/// [meta][payload]`.
///
/// Every RDMA send is a sequence of such frames (usually one). The frame
/// header makes messages self-delimiting, so the poll thread can aggregate
/// window-blocked sends by plain concatenation and the receive side always
/// walks the same frame loop — no aggregation magic, no special cases.
struct FramedBuffer<'a>(&'a mut Buffer);

impl crate::msg::SendMsg for FramedBuffer<'_> {
    fn size(&self) -> usize {
        self.0.len()
    }

    fn prepare(&mut self) -> Result<()> {
        self.0.set_len(0);
        // Reserve the frame length header; patched in `finish`.
        self.0.extend_from_slice(&0u32.to_be_bytes())?;
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        let meta_len = u32::try_from(payload_offset - meta_offset - FRAME_HEADER)?;
        self.0[meta_offset..meta_offset + FRAME_HEADER].copy_from_slice(&meta_len.to_be_bytes());
        let frame_len = u32::try_from(self.0.len() - FRAME_HEADER)?;
        self.0[..FRAME_HEADER].copy_from_slice(&frame_len.to_be_bytes());
        Ok(())
    }

    fn writer(&mut self) -> impl std::io::Write {
        #[repr(transparent)]
        struct Writer<'a>(&'a mut Buffer);

        impl std::io::Write for Writer<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                self.0.extend_from_slice(buf).map_err(std::io::Error::other)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        Writer(self.0)
    }
}

/// The memory kept alive for one in-flight RDMA READ batch.
///
/// Whatever variant it is, the underlying registered memory must stay
/// owned here until *every* work completion of the batch (success, error
/// or flush) has been observed — only then is the NIC guaranteed to no
/// longer DMA into it.
#[derive(Debug)]
pub(crate) enum ReadHold {
    /// Server-side `remote_read`: the local destination buffers.
    Buffers(Vec<Buffer>),
    /// Client-side `pull`: the request's pinned write target. Never read
    /// back — held purely for ownership until the batch settles.
    Target(#[allow(dead_code)] Arc<WriteTarget>),
}

/// Shared completion state of one batch of RDMA READ work requests.
///
/// Every WR of the batch maps (via `rdma_completions`) to the same
/// `Arc<ReadBatch>`. The poll thread decrements `remaining` per work
/// completion; the last one resolves the waiter and releases the hold.
/// The background timeout sweep can resolve the waiter *early* (with an
/// error) — the hold then stays inside the batch until the flush
/// completions arrive, so timed-out reads never recycle memory the NIC
/// may still write to.
#[derive(Debug)]
pub(crate) struct ReadBatch {
    /// Work completions still outstanding.
    remaining: AtomicUsize,
    /// Whether any completion carried an error status (or a post failed).
    failed: AtomicBool,
    /// Timeout deadline (`None` when `rdma.read_timeout_ms` is 0).
    deadline: Option<Instant>,
    inner: Mutex<ReadBatchInner>,
}

#[derive(Debug)]
struct ReadBatchInner {
    hold: Option<ReadHold>,
    tx: Option<tokio::sync::oneshot::Sender<std::result::Result<ReadHold, Error>>>,
}

impl ReadBatch {
    fn new(
        count: usize,
        hold: ReadHold,
        tx: tokio::sync::oneshot::Sender<std::result::Result<ReadHold, Error>>,
        deadline: Option<Instant>,
    ) -> Arc<Self> {
        Arc::new(Self {
            remaining: AtomicUsize::new(count),
            failed: AtomicBool::new(false),
            deadline,
            inner: Mutex::new(ReadBatchInner {
                hold: Some(hold),
                tx: Some(tx),
            }),
        })
    }

    /// Records one work completion (called from the poll thread); the
    /// last one resolves the batch.
    pub(crate) fn complete_one(&self, ok: bool) {
        if !ok {
            self.failed.store(true, Ordering::Release);
        }
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.finish();
        }
    }

    /// Accounts `unposted` work requests that never reached the hardware
    /// after a mid-batch post failure.
    fn abort_unposted(&self, unposted: usize) {
        self.failed.store(true, Ordering::Release);
        if self.remaining.fetch_sub(unposted, Ordering::AcqRel) == unposted {
            self.finish();
        }
    }

    /// All completions arrived: release the hold and resolve the waiter
    /// (unless the timeout sweep already did).
    fn finish(&self) {
        let mut inner = self.inner.lock().unwrap();
        // Taking the hold out releases the memory when it drops below —
        // safe now that no work request references it anymore.
        let hold = inner.hold.take();
        let Some(tx) = inner.tx.take() else {
            return;
        };
        let result = if self.failed.load(Ordering::Acquire) {
            Err(Error::new(
                ErrorKind::RdmaSendFailed,
                "RDMA READ failed (work completion error)".into(),
            ))
        } else {
            hold.ok_or_else(|| {
                Error::new(ErrorKind::RdmaSendFailed, "RDMA READ hold missing".into())
            })
        };
        let _ = tx.send(result);
    }

    /// Resolves the waiter with `err` without releasing the hold (used by
    /// the timeout sweep and poller shutdown). Returns whether this call
    /// resolved it.
    pub(crate) fn fail(&self, err: Error) -> bool {
        let mut inner = self.inner.lock().unwrap();
        match inner.tx.take() {
            Some(tx) => {
                let _ = tx.send(Err(err));
                true
            }
            None => false,
        }
    }

    /// Whether the batch exceeded its deadline.
    pub(crate) fn expired(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|deadline| deadline <= now)
    }

    /// Abandons a batch none of whose work requests were posted,
    /// recovering the hold synchronously.
    fn cancel(&self) -> Option<ReadHold> {
        let mut inner = self.inner.lock().unwrap();
        inner.tx.take();
        inner.hold.take()
    }
}

/// One planned RDMA READ work request: a contiguous remote range
/// scattered into up to `max_send_sge` local segments.
#[derive(Debug)]
struct PlannedRead {
    remote_addr: u64,
    rkey: u32,
    sges: Vec<ReadSge>,
}

/// Translates the chunk plan of a validated op batch into concrete work
/// requests, resolving remote regions to `(addr, rkey)` and local
/// segments (given as per-segment `(base address, lkey)`) to scatter
/// entries.
fn build_planned_reads(
    regions: &[RemoteBufferInfo],
    src_layout: &SpaceLayout,
    dst_layout: &SpaceLayout,
    dst_bases: &[(u64, u32)],
    ops: &[CopyOp],
    max_sge: usize,
) -> Result<Vec<PlannedRead>> {
    scatter::plan_chunks(src_layout, dst_layout, ops, max_sge.max(1))
        .into_iter()
        .map(|chunk| {
            let region = &regions[chunk.seg];
            let sges = chunk
                .dst
                .iter()
                .map(|slice| {
                    let (base, lkey) = dst_bases[slice.seg];
                    Ok(ReadSge {
                        addr: base + slice.off,
                        len: u32::try_from(slice.len).map_err(|_| {
                            Error::new(
                                ErrorKind::InvalidCopyOp,
                                "scatter slice exceeds u32::MAX bytes".into(),
                            )
                        })?,
                        lkey,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(PlannedRead {
                remote_addr: region.addr + chunk.off,
                rkey: region.key.rkey,
                sges,
            })
        })
        .collect()
}

#[derive(Debug)]
pub struct RdmaSocket {
    /// Declared before `rdma_completions` deliberately: fields drop in
    /// declaration order, and the QP must be destroyed first
    /// (`ibv_destroy_qp` returning guarantees no further DMA) before any
    /// [`ReadHold`] parked in `rdma_completions` releases its memory back
    /// to the pool.
    pub(crate) queue_pair: QueuePair,
    /// In-flight RDMA READ work requests, each mapping to its batch.
    pub(crate) rdma_completions: dashmap::DashMap<WRID, Arc<ReadBatch>>,
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    pub(crate) state: RdmaState,
    /// Window-blocked framed sends, flushed by the poll thread once
    /// credits free up.
    pub(crate) pending_sender: Sender<Buffer>,
    /// Wakes the device poll thread (pending sends, error teardown).
    pub(crate) poller_waker: PollerWaker,
    /// Negotiated maximum serialized message size (= the peer's receive
    /// buffer size).
    pub(crate) max_msg_size: usize,
    /// The (local NIC, remote NIC) pair this connection runs on.
    pub(crate) path: RdmaPathInfo,
    /// Process-wide unique connection id (see [`crate::task::next_conn_id`]).
    pub(crate) conn_id: u64,
    /// Aggregate health of the outbound peer this stripe belongs to.
    peer_health: std::sync::OnceLock<std::sync::Weak<super::RdmaPeerHealth>>,
    /// Initiator asks the poll thread to send one accounted immediate-only
    /// SEND after control-plane confirmation.
    activation_requested: AtomicBool,
    /// Server-side accept lease notified by the first successful receive.
    accept_lease_id: AtomicU64,
    /// Bounds in-flight RDMA READ work requests per *local NIC*: shared
    /// by every connection of the pool on this device
    /// (`rdma.max_inflight_read_wrs`) — the congestion control knob for
    /// read traffic, covering both server-side `remote_read` and
    /// client-side `pull`. Permits are forgotten on post and re-added by
    /// the poll thread per completion.
    pub(crate) read_permits: Arc<tokio::sync::Semaphore>,
    /// Per-connection safety cap (`qp.max_send_wr / 2`, not a policy
    /// knob): the send queue is shared with regular sends, and the
    /// device-wide read budget landing on a single QP must not overflow
    /// it. Accounted exactly like `read_permits`.
    pub(crate) sq_read_permits: tokio::sync::Semaphore,
    /// Software deadline for RDMA READ completions; `None` disables the
    /// timeout. Enforced by the poll thread's periodic sweep, not by
    /// per-operation timers.
    read_timeout: Option<Duration>,
}

impl RdmaSocket {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        queue_pair: QueuePair,
        rdmabuf_pool: Arc<BufferPool>,
        pending_sender: Sender<Buffer>,
        poller_waker: PollerWaker,
        max_msg_size: usize,
        send_window: u32,
        path: RdmaPathInfo,
        read_timeout: Option<Duration>,
        read_permits: Arc<tokio::sync::Semaphore>,
        sq_read_cap: u32,
    ) -> Self {
        Self {
            queue_pair,
            rdma_completions: dashmap::DashMap::default(),
            rdmabuf_pool,
            state: RdmaState::new(send_window.max(1)),
            pending_sender,
            poller_waker,
            max_msg_size,
            path,
            conn_id: crate::task::next_conn_id(),
            peer_health: std::sync::OnceLock::new(),
            activation_requested: AtomicBool::new(false),
            accept_lease_id: AtomicU64::new(0),
            read_permits,
            sq_read_permits: tokio::sync::Semaphore::new(sq_read_cap.max(1) as usize),
            read_timeout,
        }
    }

    pub(crate) fn set_peer_health(&self, health: &Arc<super::RdmaPeerHealth>) {
        let _ = self.peer_health.set(Arc::downgrade(health));
    }

    pub(crate) fn peer_health(&self) -> Option<std::sync::Weak<super::RdmaPeerHealth>> {
        self.peer_health.get().cloned()
    }

    pub(crate) fn request_activation(&self) {
        self.activation_requested.store(true, Ordering::Release);
        self.poller_waker.wake();
    }

    pub(crate) fn take_activation_request(&self) -> bool {
        self.activation_requested.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn set_accept_lease(&self, connection_id: u64) {
        debug_assert_ne!(connection_id, 0);
        self.accept_lease_id.store(connection_id, Ordering::Release);
    }

    pub(crate) fn take_accept_lease(&self) -> Option<u64> {
        if self.accept_lease_id.load(Ordering::Acquire) == 0 {
            return None;
        }
        match self.accept_lease_id.swap(0, Ordering::AcqRel) {
            0 => None,
            connection_id => Some(connection_id),
        }
    }

    /// Serializes a message into a right-sized framed buffer.
    ///
    /// The serialized size is unknown upfront, so try increasingly larger
    /// buffers (4 KiB → 64 KiB → 256 KiB → negotiated `max_msg_size`).
    /// Typical RPC messages fit the first rung; larger payloads should use
    /// the remote read/write paths.
    fn serialize_msg<P: Serialize>(&self, meta: &MsgMeta, payload: &P) -> Result<Buffer> {
        let mut last_err = None;
        for size in [4 * 1024, 64 * 1024, 256 * 1024, self.max_msg_size] {
            let size = size.min(self.max_msg_size);
            let mut buf = self.rdmabuf_pool.allocate(size)?;
            match meta.serialize_to(payload, &mut FramedBuffer(&mut buf)) {
                Ok(()) => return Ok(buf),
                Err(e) => last_err = Some(e),
            }
            if size == self.max_msg_size {
                break;
            }
        }
        Err(last_err.unwrap_or_else(|| {
            Error::new(
                ErrorKind::SerializeFailed,
                format!(
                    "message exceeds negotiated max_msg_size ({}); use remote read/write for large payloads",
                    self.max_msg_size
                ),
            )
        }))
    }

    pub fn set_error(&self) {
        self.state.set_error();
        let mut attr = ruapc_rdma::ibv_qp_attr {
            qp_state: ruapc_rdma::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        let mask = ruapc_rdma::ibv_qp_attr_mask::IBV_QP_STATE;
        let _ = self.queue_pair.modify(&mut attr, mask.0 as _);
        // Ensure the poll thread notices the error even when the QP had no
        // outstanding work requests to flush.
        self.poller_waker.wake();
    }

    /// Posts the planned reads and waits for the batch to complete.
    ///
    /// On success the hold is handed back. On failure the second element
    /// carries the hold only when *nothing* reached the hardware; once a
    /// work request is in flight the memory stays parked in the batch
    /// until its (possibly flush) completion arrives.
    async fn execute_reads(
        &self,
        reads: &[PlannedRead],
        hold: ReadHold,
    ) -> std::result::Result<ReadHold, (Error, Option<ReadHold>)> {
        debug_assert!(!reads.is_empty());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let deadline = self.read_timeout.map(|timeout| Instant::now() + timeout);
        let batch = ReadBatch::new(reads.len(), hold, tx, deadline);

        let mut posted = 0usize;
        let mut post_err: Option<Error> = None;
        for read in reads {
            // Bound in-flight READ work requests; permits come back from
            // the poll thread as completions arrive. The per-connection
            // SQ guard is taken first so a batch never sits on scarce
            // per-NIC permits while blocked on its own send queue.
            let sq_permit = match self.sq_read_permits.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    post_err = Some(Error::new(
                        ErrorKind::RdmaSendFailed,
                        "RDMA read permits closed".into(),
                    ));
                    break;
                }
            };
            let device_permit = match self.read_permits.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    post_err = Some(Error::new(
                        ErrorKind::RdmaSendFailed,
                        "RDMA read permits closed".into(),
                    ));
                    break;
                }
            };
            let result = self.queue_pair.read_sges(
                &read.sges,
                read.remote_addr,
                read.rkey,
                // Registered under the SQ post lock *before* the WR is
                // posted, so its completion can never miss the batch.
                |wr_id| {
                    self.rdma_completions.insert(wr_id, batch.clone());
                },
                |wr_id| {
                    self.rdma_completions.remove(&wr_id);
                },
            );
            match result {
                Ok(_) => {
                    // Both released by the poll thread on completion.
                    sq_permit.forget();
                    device_permit.forget();
                    posted += 1;
                }
                Err(e) => {
                    drop(device_permit);
                    drop(sq_permit);
                    post_err = Some(e.into());
                    break;
                }
            }
        }

        if let Some(err) = post_err {
            if posted == 0 {
                // Nothing reached the hardware: recover the hold now.
                return Err((err, batch.cancel()));
            }
            // Some reads are in flight: fail the connection so they flush,
            // then wait for the batch to settle (flush completions or the
            // poller's teardown/timeout paths resolve it).
            batch.abort_unposted(reads.len() - posted);
            self.set_error();
            let _ = rx.await;
            return Err((err, None));
        }

        match rx.await {
            Ok(Ok(hold)) => Ok(hold),
            Ok(Err(e)) => Err((e, None)),
            Err(_) => Err((
                Error::new(
                    ErrorKind::RdmaSendFailed,
                    "RDMA read batch abandoned (connection torn down)".into(),
                ),
                None,
            )),
        }
    }

    /// Executes the client side of a `MemoryService/pull`: RDMA READs from
    /// the peer's regions into the pinned write target. The target `Arc`
    /// keeps the destination memory alive for as long as any read is in
    /// flight, so no post-transfer liveness verification is needed.
    pub(crate) async fn pull_into_target(
        &self,
        regions: &[RemoteBufferInfo],
        src_layout: &SpaceLayout,
        ops: &[CopyOp],
        target: Arc<WriteTarget>,
    ) -> Result<()> {
        let device = &self.queue_pair.device_index;
        let bases = target.export_sge_bases(device)?;
        let planned = build_planned_reads(
            regions,
            src_layout,
            target.layout(),
            &bases,
            ops,
            self.queue_pair.gather_limit(),
        )?;
        if planned.is_empty() {
            return Ok(());
        }
        match self.execute_reads(&planned, ReadHold::Target(target)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => Err(e),
        }
    }
}

impl SocketTrait for RdmaSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
        let buf = self.serialize_msg(meta, payload)?;

        // Bind the pending request to this connection so it fails eagerly
        // if the connection dies before the response arrives.
        if meta.is_req() {
            state.waiter.bind_connection(meta.msgid, self.conn_id);
        }

        match self.state.try_acquire() {
            SendPermit::Granted { window_tail } => {
                // Invariant: a fully consumed send window must contain at
                // least one signaled WR, otherwise its slots stay stranded
                // (unsignaled completions are only swept by later signaled
                // ones) and the connection stalls until the 5s keepalive.
                // Direct sends within the window make the window-tail send
                // signaled; pending flushes (the other way credits get
                // consumed) are always signaled by the poll thread.
                let posted = if window_tail {
                    self.queue_pair
                        .send_signaled(buf, ibv_send_flags::IBV_SEND_SIGNALED)
                } else {
                    self.queue_pair.send(buf, ibv_send_flags::IBV_SEND_SIGNALED)
                };
                posted.map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
                Ok(())
            }
            SendPermit::Full => {
                // Window exhausted: hand the framed message to the poll
                // thread, which flushes (and opportunistically aggregates)
                // pending sends as credits free up.
                self.pending_sender
                    .send(buf)
                    .await
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
                // The poll thread may be sleeping; enqueueing a pending send
                // produces no completion event, so wake it explicitly.
                self.poller_waker.wake();
                Ok(())
            }
            SendPermit::Error => Err(ErrorKind::RdmaSendFailed.into()),
        }
    }

    async fn remote_read(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
        remote: &RemoteSpace<'_>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        let device = &self.queue_pair.device_index;
        let bases = local
            .iter()
            .map(|buf| {
                let key = buf
                    .memory_key(device)
                    .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
                Ok((buf.as_ptr() as u64, key.lkey))
            })
            .collect::<Result<Vec<(u64, u32)>>>();
        let bases = match bases {
            Ok(bases) => bases,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        let dst_layout = match SpaceLayout::from_lens(local.iter().map(|b| b.len() as u64)) {
            Ok(layout) => layout,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        let planned = match build_planned_reads(
            remote.regions(),
            remote.layout(),
            &dst_layout,
            &bases,
            ops,
            self.queue_pair.gather_limit(),
        ) {
            Ok(planned) => planned,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if planned.is_empty() {
            return Ok(local);
        }

        let local = match self.execute_reads(&planned, ReadHold::Buffers(local)).await {
            Ok(ReadHold::Buffers(local)) => local,
            Ok(ReadHold::Target(_)) => unreachable!("remote_read holds buffers"),
            Err((e, hold)) => {
                let buffers = match hold {
                    Some(ReadHold::Buffers(buffers)) => Some(buffers),
                    _ => None,
                };
                return Err(RemoteIoError::new(e, buffers));
            }
        };

        // After the RDMA READs complete, verify the client's original
        // request is still alive. RDMA READ is one-sided — the client
        // cannot know its memory was read — and once its request times
        // out, the read buffers may have been reclaimed and refilled, so
        // the data would be garbage.
        let msgid = ctx.msg_meta.msgid;
        let client = crate::Client::default();
        let still_waiting: bool = match client.is_message_waiting(ctx, &msgid).await {
            Ok(w) => w,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if !still_waiting {
            return Err(RemoteIoError::new(
                Error::new(
                    ErrorKind::Timeout,
                    "RDMA read completed but client request has already timed out".into(),
                ),
                Some(local),
            ));
        }

        Ok(local)
    }

    async fn remote_write(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        // No one-sided RDMA WRITE (unsafe against client buffer lifetime):
        // send a reverse `pull` RPC advertising our source buffers as read
        // regions; the client executes RDMA READs into its pinned write
        // target. This future holds `local` across the await, so the
        // advertised regions stay valid for the whole transfer.
        let req = crate::services::MemoryPullReq {
            msgid: ctx.msg_meta.msgid,
            ops: ops.to_vec(),
        };
        let client = crate::Client::default();
        match client.with_read_buffers(&local).pull(ctx, &req).await {
            Ok(()) => Ok(local),
            Err(e) => Err(RemoteIoError::new(e, Some(local))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Devices;

    fn pool() -> Arc<BufferPool> {
        let devices = Arc::new(Devices::default());
        ruapc_bufpool::BufferPoolBuilder::new(devices).build()
    }

    fn make_hold(pool: &Arc<BufferPool>) -> ReadHold {
        let mut buf = pool.allocate(64 * 1024).unwrap();
        buf.set_len(16);
        ReadHold::Buffers(vec![buf])
    }

    /// The happy path: the last completion resolves the batch with its
    /// hold.
    #[tokio::test]
    async fn test_read_batch_completes_with_hold() {
        let pool = pool();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch = ReadBatch::new(2, make_hold(&pool), tx, None);
        batch.complete_one(true);
        batch.complete_one(true);
        match rx.await.unwrap() {
            Ok(ReadHold::Buffers(bufs)) => assert_eq!(bufs.len(), 1),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    /// Any errored completion fails the whole batch; the hold is released
    /// (not returned) because every WR has settled by then.
    #[tokio::test]
    async fn test_read_batch_error_completion_fails_batch() {
        let pool = pool();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch = ReadBatch::new(2, make_hold(&pool), tx, None);
        batch.complete_one(false);
        batch.complete_one(true);
        let err = rx.await.unwrap().unwrap_err();
        assert_eq!(err.kind, ErrorKind::RdmaSendFailed);
    }

    /// The timeout sweep resolves the waiter early but must *not* release
    /// the hold: the NIC may still DMA into the memory until the flush
    /// completions arrive.
    #[tokio::test]
    async fn test_read_batch_timeout_keeps_hold_until_flush() {
        let pool = pool();

        // Observe the hold's lifetime through a pinned write target.
        let mut buf = pool.allocate(64 * 1024).unwrap();
        buf.set_len(16);
        let target = WriteTarget::new(vec![buf]).unwrap();
        let observer = target.clone();

        let (tx, rx) = tokio::sync::oneshot::channel();
        let deadline = Some(Instant::now() - Duration::from_millis(1));
        let batch = ReadBatch::new(2, ReadHold::Target(target), tx, deadline);

        // The sweep fires: the waiter resolves with RdmaReadTimeout...
        assert!(batch.expired(Instant::now()));
        assert!(batch.fail(Error::kind(ErrorKind::RdmaReadTimeout)));
        // ... only once ...
        assert!(!batch.fail(Error::kind(ErrorKind::RdmaReadTimeout)));
        let err = rx.await.unwrap().unwrap_err();
        assert_eq!(err.kind, ErrorKind::RdmaReadTimeout);

        // ... and the memory stays pinned until the flush completions.
        assert!(
            WriteTarget::try_into_buffers(observer.clone()).is_none(),
            "hold must stay pinned while completions are outstanding"
        );
        batch.complete_one(false);
        batch.complete_one(false);
        assert!(
            WriteTarget::try_into_buffers(observer).is_some(),
            "hold must be released once every completion arrived"
        );
    }

    /// A batch without a deadline never expires.
    #[tokio::test]
    async fn test_read_batch_no_deadline_never_expires() {
        let pool = pool();
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let batch = ReadBatch::new(1, make_hold(&pool), tx, None);
        assert!(!batch.expired(Instant::now() + Duration::from_secs(3600)));
        batch.complete_one(true);
    }

    /// `cancel` recovers the hold synchronously (used when no WR was
    /// posted at all).
    #[tokio::test]
    async fn test_read_batch_cancel_recovers_hold() {
        let pool = pool();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch = ReadBatch::new(1, make_hold(&pool), tx, None);
        assert!(matches!(batch.cancel(), Some(ReadHold::Buffers(_))));
        // The waiter observes a closed channel, not a stray result.
        assert!(rx.await.is_err());
    }
}
