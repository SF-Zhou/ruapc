//! Owned RDMA READ plans. Posted destinations stay held through completion
//! or successful QP destruction; failure notification does not release them.

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use ruapc_bufpool::Buffer;
use tokio::sync::oneshot;

use super::super::wr_slots::SlotTable;
use super::{QueuePair, ReadSge};
use crate::{Error, ErrorKind, Result, WRID};

static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

pub(super) fn next_owner() -> u64 {
    NEXT_OWNER
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .expect("QP read identity exhausted")
}

/// Connection-local READ bookkeeping stays inline in the QP so posting and
/// completion do not follow another allocation to reach their ownership table.
///
/// The common path is a power-of-two slot table indexed by SQ sequence: one
/// compare-exchange per posted READ and one per completion, with no hashing,
/// locking or per-shard state. A sequence whose slot is still occupied by an
/// older in-flight READ spills into a small locked map instead of failing the
/// READ. That needs an older READ to stay in flight across at least one full
/// table of later SQ sequences (sends included), so the fallback is off the
/// common path; [`ReadState::spills`] counts every use for diagnosis.
///
/// `ReadBatch::remaining` covers every WR in the plan, including a suffix still
/// waiting for admission. `ReadBatch::accepted` counts WRs the provider took,
/// and `ReadBatch::aborted` counts the suffix the cursor settled without
/// posting, so a deadline sweep derives in-flight work as
/// `accepted + aborted - (total - remaining)` without another atomic on the
/// completion path. It never closes a healthy connection for work that has not
/// reached the NIC.
///
/// Only batches that have posted at least one WR are tracked for deadline and
/// failure notification. That list holds `Weak` references and is pruned on a
/// bounded registration cadence, so a normally completed batch is not retained
/// indefinitely when the poll thread skips idle sweeps.
pub(super) struct ReadState {
    batches: SlotTable<Arc<ReadBatch>>,
    pending: Mutex<PendingBatches>,
    overflow: Mutex<HashMap<u64, Arc<ReadBatch>>>,
    overflow_len: AtomicUsize,
    /// READs that used `overflow` because an older in-flight READ still
    /// occupied their slot. Per-QP and monotonic; a diagnostic only.
    spills: AtomicUsize,
    owner: u64,
}

/// Registrations between bounded cleanups of the pending weak list.
///
/// A dead entry retains only its `Arc` allocation, but an unbounded list would
/// leak one such allocation per completed batch when no deadline sweep runs.
const PENDING_PRUNE_INTERVAL: usize = 64;

#[derive(Default)]
struct PendingBatches {
    entries: Vec<Weak<ReadBatch>>,
    /// Scratch list reused across deadline scans so the poll path does not
    /// allocate for every sweep. It is cached empty: a retained batch would
    /// outlive the scan and defer its destruction to the poll thread.
    scan: Vec<Arc<ReadBatch>>,
    /// Registrations since the last bounded cleanup.
    registrations: usize,
}

impl ReadState {
    pub(super) fn new(depth: u32) -> Self {
        Self {
            batches: SlotTable::new(depth),
            pending: Mutex::new(PendingBatches::default()),
            overflow: Mutex::new(HashMap::new()),
            overflow_len: AtomicUsize::new(0),
            spills: AtomicUsize::new(0),
            owner: next_owner(),
        }
    }

    fn lock_pending(&self) -> std::sync::MutexGuard<'_, PendingBatches> {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_overflow(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Arc<ReadBatch>>> {
        self.overflow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records a posted READ and reports whether its sequence was vacant.
    ///
    /// A slot still occupied by an older in-flight READ spills into the
    /// fallback map instead of rejecting the READ; [`Self::spills`] counts it.
    #[inline]
    fn insert(&self, sequence: u64, batch: Arc<ReadBatch>) -> bool {
        if let Err(batch) = self.batches.insert(sequence, batch) {
            self.spills.fetch_add(1, Ordering::Relaxed);
            let mut overflow = self.lock_overflow();
            debug_assert!(
                !overflow.contains_key(&sequence),
                "READ sequence {sequence} recorded twice"
            );
            let vacant = overflow.insert(sequence, batch).is_none();
            if vacant {
                self.overflow_len.fetch_add(1, Ordering::Release);
            }
            return vacant;
        }
        true
    }

    /// Settles one recorded READ, or rolls back a failed post.
    #[inline]
    fn remove(&self, sequence: u64) -> Option<Arc<ReadBatch>> {
        if let Some(batch) = self.batches.take(sequence) {
            return Some(batch);
        }
        if self.overflow_len.load(Ordering::Acquire) != 0 {
            let mut overflow = self.lock_overflow();
            if let Some(batch) = overflow.remove(&sequence) {
                self.overflow_len.fetch_sub(1, Ordering::Release);
                return Some(batch);
            }
        }
        None
    }

    fn len(&self) -> usize {
        self.batches.len() + self.overflow_len.load(Ordering::Acquire)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of READs that used the fallback map instead of a vacant slot.
    fn spills(&self) -> usize {
        self.spills.load(Ordering::Relaxed)
    }

    /// Makes a batch visible to deadline and failure scans after its first
    /// successful post. Registration happens after the provider call, so an
    /// unposted plan can never be mistaken for an outstanding READ.
    fn register_pending(&self, batch: &Arc<ReadBatch>) {
        let mut pending = self.lock_pending();
        pending.registrations += 1;
        if pending.registrations >= PENDING_PRUNE_INTERVAL {
            pending.registrations = 0;
            pending.entries.retain(Self::retain_pending);
        }
        pending.entries.push(Arc::downgrade(batch));
    }

    /// Keeps only batches that can still post or settle work.
    fn retain_pending(weak: &Weak<ReadBatch>) -> bool {
        weak.upgrade()
            .is_some_and(|batch| batch.remaining.load(Ordering::Acquire) != 0)
    }

    /// Collects live batches while pruning dead and settled entries. The
    /// returned list is scanned without holding the pending lock, so a waker
    /// (or a panicking waker) runs outside it.
    fn pending_batches(&self) -> Vec<Arc<ReadBatch>> {
        let mut pending = self.lock_pending();
        let mut scan = std::mem::take(&mut pending.scan);
        debug_assert!(scan.is_empty(), "cached scan list retained batches");
        pending.entries.retain(|weak| {
            let Some(batch) = weak.upgrade() else {
                return false;
            };
            if batch.remaining.load(Ordering::Acquire) == 0 {
                return false;
            }
            scan.push(batch);
            true
        });
        scan
    }

    /// Returns the scratch list after a scan, keeping only its capacity.
    ///
    /// Clearing before taking the lock drops the scanned references on the
    /// scanning thread, so a cached batch cannot outlive the sweep or move its
    /// destruction to the poll thread.
    fn restore_scan(&self, mut scan: Vec<Arc<ReadBatch>>) {
        scan.clear();
        if scan.capacity() == 0 {
            return;
        }
        let mut pending = self.lock_pending();
        if scan.capacity() > pending.scan.capacity() {
            pending.scan = scan;
        }
    }

    fn fail_pending(&self) {
        let batches = self.pending_batches();
        for batch in &batches {
            batch.fail(ReadFailure::ConnectionClosed);
        }
        self.restore_scan(batches);
    }

    fn expire_pending(&self, now: Instant) -> bool {
        let batches = self.pending_batches();
        let mut expired = false;
        for batch in &batches {
            // `remaining` also counts an unposted suffix waiting for admission.
            // Only work the NIC can still access may expire the watchdog.
            if batch.has_in_flight() {
                expired |= batch.expire(now);
            }
        }
        self.restore_scan(batches);
        expired
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.lock_pending().entries.len()
    }

    #[cfg(test)]
    fn scan_capacity(&self) -> usize {
        self.lock_pending().scan.capacity()
    }
}

/// A destination slice addressed by buffer index, never by a caller's pointer.
#[derive(Clone, Copy, Debug)]
pub struct ReadSegment {
    pub buffer: usize,
    pub offset: usize,
    pub len: usize,
}

/// A contiguous remote range scattered into owned local buffers.
#[derive(Debug)]
pub struct ReadRequest {
    pub remote_addr: u64,
    pub rkey: u32,
    pub segments: Vec<ReadSegment>,
}

#[derive(Debug)]
struct PreparedRead {
    remote_addr: u64,
    rkey: u32,
    /// Half-open range of this request's descriptors in the plan's shared
    /// scatter-list allocation.
    sge_start: usize,
    sge_end: usize,
}

/// Reason a posted READ batch did not produce usable destination buffers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadFailure {
    Completion,
    /// The READ watchdog expired; the connection must be closed.
    Timeout,
    /// The caller's request deadline expired without proving a connection fault.
    RequestTimeout,
    ConnectionClosed,
    Cancelled,
}

/// Returns buffers after every READ succeeds, or a failure without buffers.
/// Failure notification may precede completion; the QP retains DMA ownership.
pub type ReadReceiver = oneshot::Receiver<std::result::Result<Vec<Buffer>, ReadFailure>>;

/// A validated batch's exclusive posting cursor. Each request can be posted
/// once, to its preparing QP. Dropping the cursor accounts its unposted suffix;
/// buffers already visible to the NIC remain owned by that QP.
///
/// Leaking the cursor skips that accounting: the batch and its buffers are
/// never settled, and a batch that had already posted stays in the QP's
/// deadline scan for the QP's lifetime. Do not `mem::forget` it.
#[derive(Debug)]
pub struct ReadPosting {
    owner: u64,
    reads: Vec<PreparedRead>,
    /// All requests' descriptors in one allocation; each [`PreparedRead`]
    /// selects a contiguous range.
    sges: Vec<ReadSge>,
    next: usize,
    batch: Arc<ReadBatch>,
}

impl ReadPosting {
    pub fn remaining(&self) -> usize {
        self.reads.len() - self.next
    }

    /// Recovers buffers only if no request reached the NIC. Otherwise the QP
    /// retains them through completion or destruction, even without a receiver.
    pub fn cancel(mut self) -> Option<Vec<Buffer>> {
        if self.next == 0 {
            self.next = self.reads.len();
            self.batch.cancel_unposted()
        } else {
            None
        }
    }
}

impl Drop for ReadPosting {
    fn drop(&mut self) {
        let unposted = self.remaining();
        if unposted != 0 {
            self.batch.abort_unposted(unposted);
        }
    }
}

/// READ batch state.
///
/// Field order is part of the design: `repr(C)` puts `accepted` exactly one
/// cache line after `remaining`, so the cursor's per-WR acceptance store cannot
/// invalidate the line a completion updates. `total`, the deadlines and the
/// rarely written flags fill that line instead. See
/// `posting_accounting_stays_off_the_completion_line`.
#[derive(Debug)]
#[repr(C)]
pub(super) struct ReadBatch {
    /// WRs not yet settled by completion or unposted cancellation. Written by
    /// the poll thread for every completion.
    remaining: AtomicUsize,
    /// Total WRs in the plan; immutable after construction.
    total: usize,
    failed: AtomicBool,
    read_timeout_reported: AtomicBool,
    read_deadline: Option<Instant>,
    request_deadline: Option<Instant>,
    /// WRs that cursor settled without posting after an earlier WR had already
    /// been accepted. Written only by that cursor's cancellation path.
    aborted: AtomicUsize,
    /// WRs the provider accepted. Written only by the plan's exclusive posting
    /// cursor, so it never races [`Self::aborted`].
    accepted: AtomicUsize,
    inner: Mutex<ReadBatchInner>,
}

#[derive(Debug)]
struct ReadBatchInner {
    buffers: Option<Vec<Buffer>>,
    tx: Option<oneshot::Sender<std::result::Result<Vec<Buffer>, ReadFailure>>>,
}

impl ReadBatch {
    fn new(
        count: usize,
        buffers: Vec<Buffer>,
        read_deadline: Option<Instant>,
        request_deadline: Option<Instant>,
    ) -> (Arc<Self>, ReadReceiver) {
        let (tx, rx) = oneshot::channel();
        let batch = Arc::new(Self {
            total: count,
            remaining: AtomicUsize::new(count),
            accepted: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            read_deadline,
            request_deadline,
            read_timeout_reported: AtomicBool::new(false),
            inner: Mutex::new(ReadBatchInner {
                buffers: Some(buffers),
                tx: Some(tx),
            }),
        });
        if count == 0 {
            batch.finish();
        }
        (batch, rx)
    }

    #[inline]
    fn complete_one(&self, success: bool) {
        if !success {
            self.failed.store(true, Ordering::Release);
        }
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.finish();
        }
    }

    fn abort_unposted(&self, count: usize) {
        self.failed.store(true, Ordering::Release);
        if self.remaining.fetch_sub(count, Ordering::AcqRel) == count {
            self.finish();
        }
        // Settle the suffix for the in-flight derivation. `remaining` is
        // decremented first, so a concurrent scan undercounts instead of
        // reporting never-posted work as DMA.
        self.aborted.fetch_add(count, Ordering::AcqRel);
    }

    /// Records that the provider accepted `accepted` WRs of this plan.
    ///
    /// The field sits one cache line after `remaining`, so this store does not
    /// invalidate the line a completion updates.
    #[inline]
    fn mark_accepted(&self, accepted: usize) {
        self.accepted.store(accepted, Ordering::Release);
    }

    /// Whether a provider-accepted WR has not settled yet.
    ///
    /// In-flight work is `accepted + aborted - (total - remaining)`: accepted
    /// WRs plus the unposted suffix the cursor already settled, minus every
    /// settled WR. Both accounting fields are loaded before `remaining`, so a
    /// concurrent abort or completion can only make the derived count lower
    /// (a sweep that misses this round) rather than report an unposted suffix
    /// as DMA still in flight; the next sweep observes the final value.
    #[inline]
    fn has_in_flight(&self) -> bool {
        let accounted = self
            .accepted
            .load(Ordering::Acquire)
            .saturating_add(self.aborted.load(Ordering::Acquire));
        let settled = self
            .total
            .saturating_sub(self.remaining.load(Ordering::Acquire));
        accounted > settled
    }

    fn finish(&self) {
        let (buffers, tx) = {
            let mut inner = self.inner.lock().unwrap();
            (inner.buffers.take(), inner.tx.take())
        };
        let result = if self.failed.load(Ordering::Acquire) {
            drop(buffers);
            Err(ReadFailure::Completion)
        } else {
            buffers.ok_or(ReadFailure::Cancelled)
        };
        if let Some(tx) = tx {
            let _ = tx.send(result);
        }
    }

    fn fail(&self, reason: ReadFailure) -> bool {
        let tx = self.inner.lock().unwrap().tx.take();
        if let Some(tx) = tx {
            let _ = tx.send(Err(reason));
            true
        } else {
            false
        }
    }

    /// Reports the earlier expired deadline without releasing DMA ownership.
    /// The independent READ watchdog remains armed after request expiry or
    /// receiver cancellation, and requests connection closure at most once.
    fn expire(&self, now: Instant) -> bool {
        let read_expired = self.read_deadline.is_some_and(|deadline| deadline <= now);
        if self.request_deadline.is_some_and(|request| {
            request <= now && self.read_deadline.is_none_or(|read| request <= read)
        }) {
            self.fail(ReadFailure::RequestTimeout);
        } else if read_expired {
            self.fail(ReadFailure::Timeout);
        }
        read_expired && !self.read_timeout_reported.swap(true, Ordering::Relaxed)
    }

    fn cancel_unposted(&self) -> Option<Vec<Buffer>> {
        let (buffers, tx) = {
            let mut inner = self.inner.lock().unwrap();
            (inner.buffers.take(), inner.tx.take())
        };
        drop(tx);
        buffers
    }
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidReadPlan, message.into())
}

fn prepare(
    buffers: &[Buffer],
    requests: Vec<ReadRequest>,
    gather_limit: usize,
    mut lkey: impl FnMut(&Buffer) -> Result<u32>,
) -> Result<(Vec<PreparedRead>, Vec<ReadSge>)> {
    if buffers.len() > u32::MAX as usize {
        return Err(invalid("READ destination buffer count overflows"));
    }
    let keys = buffers.iter().map(&mut lkey).collect::<Result<Vec<_>>>()?;
    // A single destination cannot overlap another one. Keep its bounds/key
    // checks but avoid allocating an interval list for this common case.
    let check_overlap = requests.len() > 1
        || requests
            .first()
            .is_some_and(|request| request.segments.len() > 1);
    // Pack (buffer index, offset) into one sort key. Buffer capacities keep
    // destinations below u32; this makes the overlap sort compare u64 keys
    // instead of 24-byte interval tuples.
    let mut destinations: Vec<(u64, u32)> = Vec::new();
    // Descriptors of every request share one allocation; a per-request Vec
    // would add an allocation and copy for each RDMA READ of a batch.
    let segments = requests
        .iter()
        .map(|request| request.segments.len())
        .sum::<usize>();
    let mut sges = Vec::with_capacity(segments);
    let mut reads = Vec::with_capacity(requests.len());
    for request in requests {
        if request.segments.is_empty() || request.segments.len() > gather_limit {
            return Err(invalid("invalid READ scatter count"));
        }
        let sge_start = sges.len();
        let mut total = 0u64;
        for segment in request.segments {
            let buffer = buffers
                .get(segment.buffer)
                .ok_or_else(|| invalid("READ buffer index out of bounds"))?;
            let end = segment
                .offset
                .checked_add(segment.len)
                .ok_or_else(|| invalid("READ destination overflows"))?;
            if segment.len == 0 || end > buffer.len() {
                return Err(invalid("READ destination exceeds buffer length"));
            }
            let len =
                u32::try_from(segment.len).map_err(|_| invalid("READ segment exceeds u32::MAX"))?;
            total = total
                .checked_add(u64::from(len))
                .ok_or_else(|| invalid("READ length overflows"))?;
            if check_overlap {
                let offset = u32::try_from(segment.offset)
                    .map_err(|_| invalid("READ destination exceeds u32::MAX"))?;
                destinations.push((
                    (u64::try_from(segment.buffer)
                        .map_err(|_| invalid("READ destination buffer index overflows"))?
                        << 32)
                        | u64::from(offset),
                    len,
                ));
            }
            sges.push(ReadSge {
                addr: buffer.as_ptr() as u64 + segment.offset as u64,
                len,
                lkey: keys[segment.buffer],
            });
        }
        request
            .remote_addr
            .checked_add(total)
            .ok_or_else(|| invalid("READ remote address overflows"))?;
        reads.push(PreparedRead {
            remote_addr: request.remote_addr,
            rkey: request.rkey,
            sge_start,
            sge_end: sges.len(),
        });
    }
    destinations.sort_unstable_by_key(|destination| destination.0);
    if destinations.windows(2).any(|pair| {
        let [previous, next] = pair else {
            unreachable!("windows yields two elements");
        };
        let previous_start = (previous.0 & 0xffff_ffff) as u32;
        previous.0 >> 32 == next.0 >> 32
            && u64::from(previous_start) + u64::from(previous.1)
                > u64::from((next.0 & 0xffff_ffff) as u32)
    }) {
        return Err(invalid("READ destinations overlap"));
    }
    Ok((reads, sges))
}

impl QueuePair {
    /// Validates all destination bounds/overlaps and resolves local keys from
    /// owned buffers. Invalid plans return every buffer without posting.
    pub fn prepare_reads(
        &self,
        buffers: Vec<Buffer>,
        requests: Vec<ReadRequest>,
        deadline: Option<Instant>,
    ) -> std::result::Result<(ReadPosting, ReadReceiver), (Error, Vec<Buffer>)> {
        self.prepare_reads_with_deadlines(buffers, requests, deadline, None)
    }

    /// Prepares owned READs with independent connection and request deadlines.
    ///
    /// Request expiry only notifies the receiver; READ-watchdog expiry also
    /// requires closing the connection, even if the receiver has already timed
    /// out or been dropped. Neither deadline permits early buffer recovery.
    /// Callers must separately bound admission before posting: only WRs the
    /// provider accepted are visible to [`Self::expire_reads`].
    pub fn prepare_reads_with_deadlines(
        &self,
        buffers: Vec<Buffer>,
        requests: Vec<ReadRequest>,
        read_deadline: Option<Instant>,
        request_deadline: Option<Instant>,
    ) -> std::result::Result<(ReadPosting, ReadReceiver), (Error, Vec<Buffer>)> {
        let (reads, sges) = match prepare(&buffers, requests, self.gather_limit(), |buffer| {
            self.lkey(buffer)
        }) {
            Ok(prepared) => prepared,
            Err(error) => return Err((error, buffers)),
        };
        let (batch, receiver) =
            ReadBatch::new(reads.len(), buffers, read_deadline, request_deadline);
        Ok((
            ReadPosting {
                owner: self.read_state.owner,
                reads,
                sges,
                next: 0,
                batch,
            },
            receiver,
        ))
    }

    /// Posts the next request once. The QP installs ownership before exposing
    /// its addresses to hardware; a failed post rolls that ownership back.
    pub fn post_read(&self, posting: &mut ReadPosting) -> Result<()> {
        if posting.owner != self.read_state.owner {
            return Err(invalid("READ plan belongs to another QP"));
        }
        let read = posting
            .reads
            .get(posting.next)
            .ok_or_else(|| invalid("READ plan already fully posted"))?;
        // SAFETY: prepare validated every range against the owned buffers and
        // resolved keys on this QP. The exclusive cursor posts each range once.
        // This QP keeps the batch through an authentic completion or destruction.
        let sges = &posting.sges[read.sge_start..read.sge_end];
        unsafe {
            self.read_sges(
                sges,
                read.remote_addr,
                read.rkey,
                |wr_id| {
                    self.read_state
                        .insert(wr_id.sequence(), posting.batch.clone());
                },
                |wr_id| {
                    self.read_state.remove(wr_id.sequence());
                },
            )
        }?;
        // The provider has accepted this WR. Account it before any operation
        // that can unwind, or Drop would treat posted DMA as unposted.
        let accepted = posting.next + 1;
        posting.batch.mark_accepted(accepted);
        posting.next = accepted;
        if posting.next == 1 {
            self.read_state.register_pending(&posting.batch);
        }
        Ok(())
    }

    /// Fails waiters without returning buffers still referenced by the NIC.
    pub fn fail_pending_reads(&self) {
        self.read_state.fail_pending();
    }

    /// Fails expired waiters while preserving their buffers. Returns true only
    /// for a newly expired READ watchdog, requiring the caller to move the QP
    /// to ERR. Request expiry alone does not prove a connection fault, and
    /// does not disarm the READ watchdog after notifying the receiver.
    pub fn expire_reads(&self, now: Instant) -> bool {
        self.read_state.expire_pending(now)
    }

    /// Whether a recorded READ still awaits completion or rollback.
    ///
    /// An unposted suffix is not recorded here: closing READ admission fails
    /// its waiter, which then settles the batch, and teardown waits for that
    /// admission to become closed and idle.
    pub fn has_pending_reads(&self) -> bool {
        !self.read_state.is_empty()
    }

    /// Number of posted READ WRs whose completions have not been processed.
    ///
    /// Concurrent posting and completion make this a diagnostic snapshot.
    /// Accounting must exclude both operations before using the count, and
    /// destroy the QP before returning credits for its unpolled WRs. The count
    /// itself is not completion evidence and does not release DMA ownership.
    /// It does not observe an unposted suffix, which owns no ledger entry.
    pub fn pending_read_count(&self) -> usize {
        self.read_state.len()
    }

    /// Number of READs this QP recorded in the fallback map because an older
    /// in-flight READ still occupied their sequence-indexed slot.
    ///
    /// A sequence collides only when an older READ stays in flight across a
    /// full table of later SQ sequences (sends included). The fallback changes
    /// bookkeeping cost only: it never rejects, reorders or corrupts a READ.
    /// The value is monotonic per QP and is a diagnostic snapshot, not
    /// completion evidence.
    pub fn read_slot_spills(&self) -> usize {
        self.read_state.spills()
    }

    /// Called only after the completion path verifies CQ/QP ownership.
    pub(super) fn complete_read(&self, wr_id: WRID, success: bool) {
        if let Some(batch) = self.read_state.remove(wr_id.sequence()) {
            batch.complete_one(success);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruapc_bufpool::{BufferPool, BufferPoolBuilder, EmptyDevices};
    use std::time::Duration;

    fn buffers() -> (Vec<Buffer>, std::sync::Weak<BufferPool>) {
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices)).build();
        let mut buffer = pool.allocate(1024 * 1024).unwrap();
        buffer.set_len(16);
        (vec![buffer], Arc::downgrade(&pool))
    }

    fn request(buffer: usize, offset: usize, len: usize) -> ReadRequest {
        ReadRequest {
            remote_addr: 1024,
            rkey: 7,
            segments: vec![ReadSegment {
                buffer,
                offset,
                len,
            }],
        }
    }

    #[test]
    fn validates_bounds_overflow_and_cross_request_overlap() {
        let (buffers, _) = buffers();
        for requests in [
            vec![request(1, 0, 1)],
            vec![request(0, 16, 1)],
            vec![request(0, usize::MAX, 1)],
            vec![request(0, 0, 0)],
            vec![request(0, 0, 8), request(0, 7, 2)],
            vec![ReadRequest {
                segments: vec![
                    ReadSegment {
                        buffer: 0,
                        offset: 0,
                        len: 8,
                    },
                    ReadSegment {
                        buffer: 0,
                        offset: 7,
                        len: 2,
                    },
                ],
                ..request(0, 0, 1)
            }],
            vec![ReadRequest {
                remote_addr: u64::MAX,
                ..request(0, 0, 1)
            }],
        ] {
            assert_eq!(
                prepare(&buffers, requests, 32, |_| Ok(0)).unwrap_err().kind,
                ErrorKind::InvalidReadPlan
            );
        }
        let (plan, sges) = prepare(
            &buffers,
            vec![request(0, 0, 8), request(0, 8, 8)],
            32,
            |_| Ok(42),
        )
        .unwrap();
        assert_eq!(sges[plan[1].sge_start].addr, buffers[0].as_ptr() as u64 + 8);
        assert_eq!(sges[plan[1].sge_start].lkey, 42);
        assert!(prepare(&buffers, vec![request(0, 0, 8)], 0, |_| Ok(0)).is_err());
    }

    #[test]
    fn only_last_completion_returns_owned_memory() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(2, buffers, None, None);
        batch.complete_one(true);
        assert!(rx.try_recv().is_err());
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        let returned = rx.try_recv().unwrap().unwrap();
        assert_eq!(returned[0].len(), 16);
        drop(returned);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn timeout_notifies_once_and_holds_memory_through_flush() {
        let (buffers, lifetime) = buffers();
        let now = Instant::now();
        let (batch, mut rx) =
            ReadBatch::new(2, buffers, Some(now - Duration::from_millis(1)), None);
        assert!(batch.expire(now));
        assert!(!batch.expire(now));
        assert!(!batch.fail(ReadFailure::ConnectionClosed));
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Timeout);
        batch.complete_one(false);
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(false);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn request_timeout_keeps_dma_owned_and_leaves_read_watchdog_armed() {
        let (buffers, lifetime) = buffers();
        let request_deadline = Instant::now();
        let read_deadline = request_deadline + Duration::from_secs(1);
        let (batch, mut rx) =
            ReadBatch::new(2, buffers, Some(read_deadline), Some(request_deadline));

        assert!(!batch.expire(request_deadline));
        assert_eq!(
            rx.try_recv().unwrap().unwrap_err(),
            ReadFailure::RequestTimeout
        );
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        assert!(lifetime.upgrade().is_some());
        assert!(batch.expire(read_deadline));
        assert!(!batch.expire(read_deadline));
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(false);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn request_timeout_without_read_watchdog_never_requests_connection_closure() {
        let (buffers, lifetime) = buffers();
        let deadline = Instant::now();
        let (batch, mut rx) = ReadBatch::new(1, buffers, None, Some(deadline));
        assert!(!batch.expire(deadline));
        assert_eq!(
            rx.try_recv().unwrap().unwrap_err(),
            ReadFailure::RequestTimeout
        );
        assert!(!batch.expire(deadline + Duration::from_secs(60)));
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn delayed_sweep_reports_earlier_deadline_and_still_requests_read_timeout_closure() {
        let now = Instant::now();
        for (read_deadline, request_deadline, expected) in [
            (
                now + Duration::from_secs(1),
                now,
                ReadFailure::RequestTimeout,
            ),
            (now, now + Duration::from_secs(1), ReadFailure::Timeout),
            (now, now, ReadFailure::RequestTimeout),
        ] {
            let (buffers, lifetime) = buffers();
            let (batch, mut rx) =
                ReadBatch::new(1, buffers, Some(read_deadline), Some(request_deadline));
            let sweep_at = now + Duration::from_secs(2);
            assert!(batch.expire(sweep_at));
            assert_eq!(rx.try_recv().unwrap().unwrap_err(), expected);
            assert!(!batch.expire(sweep_at));
            assert!(lifetime.upgrade().is_some());
            batch.complete_one(false);
            assert!(lifetime.upgrade().is_none());
        }
    }

    #[test]
    fn cancelled_receiver_does_not_disarm_read_watchdog() {
        let (buffers, lifetime) = buffers();
        let deadline = Instant::now();
        let (batch, rx) = ReadBatch::new(1, buffers, Some(deadline), None);
        drop(rx);
        assert!(batch.expire(deadline));
        assert!(!batch.expire(deadline));
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn cancelled_poster_accounts_the_unposted_suffix() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(3, buffers, None, None);
        let reads = (0..3)
            .map(|_| PreparedRead {
                remote_addr: 0,
                rkey: 0,
                sge_start: 0,
                sge_end: 0,
            })
            .collect();
        let posting = ReadPosting {
            owner: 1,
            reads,
            sges: Vec::new(),
            next: 1,
            batch: batch.clone(),
        };
        drop(posting);
        assert!(rx.try_recv().is_err());
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Completion);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn partial_failure_after_early_completions_settles_once() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(3, buffers, None, None);
        batch.complete_one(true);
        batch.complete_one(true);
        assert!(rx.try_recv().is_err());
        batch.abort_unposted(1);
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Completion);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn dropped_receiver_cannot_release_inflight_memory() {
        let (buffers, lifetime) = buffers();
        let (batch, rx) = ReadBatch::new(1, buffers, None, None);
        drop(rx);
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn ledger_spills_colliding_sequences_without_losing_ownership() {
        let (buffers_a, lifetime_a) = buffers();
        let (buffers_b, lifetime_b) = buffers();
        let (batch_a, _rx_a) = ReadBatch::new(1, buffers_a, None, None);
        let (batch_b, _rx_b) = ReadBatch::new(1, buffers_b, None, None);
        let state = ReadState::new(64);
        let capacity = SlotTable::<Arc<ReadBatch>>::new(64).capacity() as u64;
        assert!(state.insert(0, batch_a.clone()));
        assert_eq!(state.spills(), 0);
        assert!(state.insert(capacity, batch_b.clone()));
        // The second sequence reuses the first slot index while its READ is
        // still recorded, so it must be counted as a fallback use.
        assert_eq!(state.spills(), 1);
        assert_eq!(state.len(), 2);
        assert!(Arc::ptr_eq(&state.remove(0).unwrap(), &batch_a));
        assert!(Arc::ptr_eq(&state.remove(capacity).unwrap(), &batch_b));
        assert!(state.is_empty());
        assert_eq!(state.spills(), 1);
        drop(batch_a);
        drop(batch_b);
        assert!(lifetime_a.upgrade().is_none());
        assert!(lifetime_b.upgrade().is_none());
    }

    #[test]
    fn ledger_scans_only_posted_batches_and_prunes_settled_entries() {
        let (buffers, lifetime) = buffers();
        let deadline = Instant::now() - Duration::from_millis(1);
        let (batch, mut rx) = ReadBatch::new(1, buffers, Some(deadline), None);
        let state = ReadState::new(64);
        // A prepared but unposted batch is not an outstanding READ.
        assert!(!state.expire_pending(Instant::now()));
        assert!(rx.try_recv().is_err());
        state.register_pending(&batch);
        // Registration alone is not enough: the WR must be installed before
        // the watchdog can observe it.
        assert!(!state.expire_pending(Instant::now()));
        assert!(state.insert(0, batch.clone()));
        batch.mark_accepted(1);
        assert!(state.expire_pending(Instant::now()));
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Timeout);
        // Settlement drops the batch from the scan without returning its
        // buffers, and cannot request connection closure again.
        assert!(lifetime.upgrade().is_some());
        assert!(state.remove(0).is_some());
        batch.complete_one(false);
        assert!(!state.expire_pending(Instant::now()));
        assert!(lifetime.upgrade().is_none());
        drop(batch);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn ledger_fails_every_posted_batch_without_releasing_dma_holders() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(1, buffers, None, None);
        let state = ReadState::new(64);
        state.register_pending(&batch);
        state.fail_pending();
        assert_eq!(
            rx.try_recv().unwrap().unwrap_err(),
            ReadFailure::ConnectionClosed
        );
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(false);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn unposted_read_suffix_does_not_trigger_watchdog_closure() {
        let (buffers, lifetime) = buffers();
        let deadline = Instant::now() - Duration::from_millis(1);
        let (batch, mut rx) = ReadBatch::new(2, buffers, Some(deadline), None);
        let state = ReadState::new(64);
        state.register_pending(&batch);

        // Only the first WR reached the NIC and completed; the second is
        // still waiting for admission and owns no DMA.
        assert!(state.insert(0, batch.clone()));
        batch.mark_accepted(1);
        assert!(state.remove(0).is_some());
        batch.complete_one(true);
        assert!(!state.expire_pending(Instant::now()));
        assert!(rx.try_recv().is_err());
        assert!(lifetime.upgrade().is_some());

        // Posting the suffix arms the expired watchdog for real DMA.
        assert!(state.insert(1, batch.clone()));
        batch.mark_accepted(2);
        assert!(state.expire_pending(Instant::now()));
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Timeout);
        assert!(state.remove(1).is_some());
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(false);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn completed_batches_are_pruned_without_deadline_scans() {
        let state = ReadState::new(64);
        for sequence in 0..10_000u64 {
            let (buffers, lifetime) = buffers();
            let (batch, rx) = ReadBatch::new(1, buffers, None, None);
            assert!(state.insert(sequence, batch.clone()));
            state.register_pending(&batch);
            assert!(state.remove(sequence).is_some());
            drop(rx);
            batch.complete_one(true);
            assert!(lifetime.upgrade().is_none());
        }
        assert!(
            state.pending_len() <= PENDING_PRUNE_INTERVAL,
            "completed batches accumulated: {}",
            state.pending_len()
        );
        assert!(state.is_empty());
    }

    #[test]
    fn scan_cache_keeps_capacity_without_retaining_batches() {
        let state = ReadState::new(64);
        let mut batches = Vec::new();
        let mut lifetimes = Vec::new();
        for sequence in 0..4u64 {
            let (buffers, lifetime) = buffers();
            let (batch, _rx) = ReadBatch::new(1, buffers, None, None);
            state.register_pending(&batch);
            assert!(state.insert(sequence, batch.clone()));
            batches.push(batch);
            lifetimes.push(lifetime);
        }
        let scan = state.pending_batches();
        assert_eq!(scan.len(), 4);
        state.restore_scan(scan);
        // Settlement and completion drop the only strong references. A scan
        // cache that retained its batches would keep them alive past the sweep.
        drop(batches);
        for sequence in 0..4u64 {
            assert!(state.remove(sequence).is_some());
        }
        for lifetime in lifetimes {
            assert!(
                lifetime.upgrade().is_none(),
                "cached scan list retained a batch"
            );
        }
        // The next scan reuses the cached capacity and prunes dead entries.
        let scan = state.pending_batches();
        assert!(scan.capacity() >= 4);
        assert!(scan.is_empty());
        state.restore_scan(scan);
        assert!(state.is_empty());
        assert!(state.scan_capacity() >= 4);
    }

    /// The posting cursor's per-WR store must never land on the line a
    /// completion updates. Any nonzero multiple of the line size guarantees
    /// that for every allocation address; the field order is chosen so the
    /// gap costs no extra allocation space.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn posting_accounting_stays_off_the_completion_line() {
        let gap =
            std::mem::offset_of!(ReadBatch, accepted) - std::mem::offset_of!(ReadBatch, remaining);
        assert_eq!(
            gap, 64,
            "`accepted` must be one cache line after `remaining`; keep the repr(C) field order"
        );
    }

    #[test]
    fn poisoned_pending_lock_does_not_block_registration_or_failure() {
        let state = Arc::new(ReadState::new(64));
        let (buffers, _) = buffers();
        let (batch, mut rx) = ReadBatch::new(1, buffers, None, None);
        let poisoned = Arc::clone(&state);
        std::thread::spawn(move || {
            let _guard = poisoned.pending.lock().unwrap();
            panic!("poison the pending list");
        })
        .join()
        .unwrap_err();
        // A provider post must never turn lock poisoning into an unwind that
        // Drop would mistake for an unposted WR.
        state.register_pending(&batch);
        state.fail_pending();
        assert_eq!(
            rx.try_recv().unwrap().unwrap_err(),
            ReadFailure::ConnectionClosed
        );
    }

    #[test]
    fn unposted_cancel_recovers_memory_and_closes_receiver() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(1, buffers, None, None);
        let posting = ReadPosting {
            owner: 1,
            reads: vec![PreparedRead {
                remote_addr: 0,
                rkey: 0,
                sge_start: 0,
                sge_end: 0,
            }],
            sges: Vec::new(),
            next: 0,
            batch,
        };
        let returned = posting.cancel().unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        drop(returned);
        assert!(lifetime.upgrade().is_none());
    }
}
