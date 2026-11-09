//! RDMA READ plans owning destination capabilities through completion or QP
//! destruction. Owned capabilities retain their allocations; unsafe borrowed
//! capabilities rely on their constructor's lifetime and exclusive-access contract.

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;

use std::{
    collections::{HashMap, hash_map::Entry},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

#[cfg(any(test, feature = "bench-internals"))]
use ruapc_bufpool::Buffer;
use ruapc_bufpool::WriteBuffer;
use ruapc_bufpool::completion as oneshot;

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
pub(super) struct ReadState {
    batches: SlotTable<Arc<ReadBatch>>,
    overflow: Mutex<HashMap<u64, Arc<ReadBatch>>>,
    overflow_len: AtomicUsize,
    /// READs that used `overflow` because an older in-flight READ still
    /// occupied their slot. Per-QP and monotonic; a diagnostic only.
    spills: AtomicUsize,
    owner: u64,
}

impl ReadState {
    pub(super) fn new(depth: u32) -> Self {
        Self {
            batches: SlotTable::new(depth),
            overflow: Mutex::new(HashMap::new()),
            overflow_len: AtomicUsize::new(0),
            spills: AtomicUsize::new(0),
            owner: next_owner(),
        }
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
            match overflow.entry(sequence) {
                Entry::Vacant(entry) => {
                    entry.insert(batch);
                    self.overflow_len.fetch_add(1, Ordering::Release);
                    return true;
                }
                // Preserve the earlier WR's DMA hold. The posting caller must
                // reject this invariant violation before exposing another WR.
                Entry::Occupied(_) => return false,
            }
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

    /// The provider QP has been destroyed, so every recorded WR can settle.
    fn finish_destroyed(&mut self) {
        for batch in self.batches.drain() {
            batch.fail(ReadFailure::ConnectionClosed);
            batch.complete_one(false);
        }
        for (_, batch) in self
            .overflow
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
        {
            batch.fail(ReadFailure::ConnectionClosed);
            batch.complete_one(false);
        }
        *self.overflow_len.get_mut() = 0;
    }
}

/// A destination slice addressed by buffer index, never by a caller's pointer.
#[derive(Clone, Copy, Debug)]
pub struct ReadSegment {
    pub buffer: usize,
    pub offset: usize,
    pub len: usize,
}

/// A contiguous remote range scattered into exclusive destination capabilities.
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

/// Reason a READ batch did not produce usable destination buffers.
///
/// Completion failure and abandonment are detected by this crate. Timeout and
/// connection failures are labels supplied by higher-layer policy through
/// [`ReadHandle::fail`]; this crate neither runs timers nor closes connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ReadFailure {
    Completion = 1,
    /// The READ watchdog expired; the runtime's settlement policy decides
    /// when outstanding work requires closing the connection.
    Timeout,
    /// The caller's request deadline expired without proving a connection fault.
    RequestTimeout,
    ConnectionClosed,
    Cancelled,
}

impl ReadFailure {
    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => None,
            1 => Some(Self::Completion),
            2 => Some(Self::Timeout),
            3 => Some(Self::RequestTimeout),
            4 => Some(Self::ConnectionClosed),
            5 => Some(Self::Cancelled),
            _ => unreachable!("invalid READ failure code"),
        }
    }
}

/// A settled READ plan and its original destination capabilities.
///
/// Both success and failure return every original capability in order. Borrowed
/// capabilities do not own their allocation: their unsafe construction contract
/// must remain satisfied. Failure does not establish a valid transfer result.
#[derive(Debug)]
pub struct ReadOutcome {
    pub result: std::result::Result<(), ReadFailure>,
    pub buffers: Vec<WriteBuffer>,
}

/// Runtime-independent completion of a READ plan owning write capabilities.
///
/// A result requires every posted READ to complete or its provider QP to be
/// destroyed, plus accounting for the unposted suffix. Dropping the future
/// records cancellation but does not release owned memory while DMA can continue.
/// Borrowed memory remains governed by the capability's unsafe contract.
#[derive(Debug)]
pub struct ReadCompletion {
    receiver: oneshot::Receiver<ReadOutcome>,
    batch: Option<Weak<ReadBatch>>,
}

impl ReadCompletion {
    /// Takes a ready result without waiting. `None` means it is still pending.
    ///
    /// # Panics
    ///
    /// Panics if the outcome has already been taken.
    pub fn try_recv(&mut self) -> Option<ReadOutcome> {
        let outcome = self
            .receiver
            .try_recv()
            .expect("READ completion consumed or producer lost its buffers");
        if outcome.is_some() {
            self.batch = None;
        }
        outcome
    }
}

impl Future for ReadCompletion {
    type Output = ReadOutcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let outcome = Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|result| result.expect("READ completion consumed or producer lost its buffers"));
        if outcome.is_ready() {
            self.batch = None;
        }
        outcome
    }
}

impl Drop for ReadCompletion {
    fn drop(&mut self) {
        if let Some(batch) = self.batch.as_ref().and_then(Weak::upgrade) {
            batch.fail(ReadFailure::Cancelled);
        }
    }
}

/// A weak observer and failure notifier for one READ plan.
///
/// The handle neither owns destination buffers nor grants completion authority.
/// It allows a higher layer to implement deadlines and connection failure
/// policy without retaining a completed batch. The posting cursor and QP own
/// the batch; an observation can temporarily extend that lifetime. Recording
/// failure cannot authorize memory recovery.
#[derive(Clone, Debug)]
pub struct ReadHandle(Weak<ReadBatch>);

impl ReadHandle {
    /// Upgrades the weak handle while the batch still has an owner.
    ///
    /// The observation must be dropped outside locks used by the receiving
    /// task: it can become the last owner and close the completion sender.
    pub fn observe(&self) -> Option<ReadObservation> {
        self.0.upgrade().map(ReadObservation)
    }

    /// Whether any work, including an unposted suffix, remains unsettled.
    /// Like [`Self::observe`], this may temporarily become the final owner;
    /// query an observation when holding a lock needed by the receiver.
    pub fn is_pending(&self) -> bool {
        self.observe().is_some_and(|batch| batch.is_pending())
    }

    /// Whether provider-accepted work remains unsettled.
    ///
    /// A concurrent posting or completion may make this observation stale.
    /// It is suitable for a watchdog sweep, never for recovering memory.
    pub fn has_in_flight(&self) -> bool {
        self.observe().is_some_and(|batch| batch.has_in_flight())
    }

    /// Records the first failure without completing the result early.
    /// Returns whether this call recorded the batch's first failure.
    pub fn fail(&self, reason: ReadFailure) -> bool {
        self.observe().is_some_and(|batch| batch.fail(reason))
    }

    /// Returns the first recorded failure while the batch remains observable.
    pub fn failure(&self) -> Option<ReadFailure> {
        self.observe().and_then(|batch| batch.failure())
    }
}

/// A temporary strong observation of a READ batch, without completion authority.
///
/// Keeping it alive can extend the batch's lifetime. It cannot access buffers,
/// settle work or authorize recovery. Drop it outside application locks because
/// dropping the final owner can notify a waiting completion future.
#[derive(Debug)]
pub struct ReadObservation(Arc<ReadBatch>);

impl ReadObservation {
    /// Whether work, including an unposted suffix, remains unsettled.
    pub fn is_pending(&self) -> bool {
        self.0.remaining.load(Ordering::Acquire) != 0
    }

    /// Whether provider-accepted work remains unsettled; not completion evidence.
    pub fn has_in_flight(&self) -> bool {
        self.0.has_in_flight()
    }

    /// Records a failure without releasing destination ownership.
    pub fn fail(&self, reason: ReadFailure) -> bool {
        self.0.fail(reason)
    }

    /// Returns the first recorded failure, independently of DMA settlement.
    pub fn failure(&self) -> Option<ReadFailure> {
        self.0.failure()
    }
}

/// A validated batch's exclusive posting cursor. Each request can be posted
/// once, to its preparing QP. Dropping the cursor accounts its unposted suffix;
/// buffers already visible to the NIC remain owned by that QP.
///
/// Leaking the cursor skips that accounting: the batch and its buffers are
/// never settled. An observer can continue reporting it as pending, even after
/// all posted work completed. Do not `mem::forget` it.
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
    /// Creates a weak observer without transferring ownership or completion authority.
    pub fn handle(&self) -> ReadHandle {
        ReadHandle(Arc::downgrade(&self.batch))
    }

    pub fn remaining(&self) -> usize {
        self.reads.len() - self.next
    }

    /// Records cancellation and accounts for the unposted suffix.
    ///
    /// Recover the original storage through the accompanying
    /// [`ReadCompletion`]. Posted work still has to settle before it is ready.
    pub fn cancel(self) {
        self.batch.fail(ReadFailure::Cancelled);
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
/// cache line after `remaining`. Posting never invalidates the counter updated
/// by every completion. The result mutex and rarely written fields fill that
/// first line; see `posting_accounting_stays_off_the_completion_line`.
#[derive(Debug)]
#[repr(C)]
pub(super) struct ReadBatch {
    /// WRs not yet settled by completion or unposted cancellation.
    remaining: AtomicUsize,
    total: usize,
    failure: AtomicU8,
    inner: Mutex<ReadBatchInner>,
    /// WRs the provider accepted. Written only by the posting cursor.
    accepted: AtomicUsize,
    /// Unposted suffix settled by the cursor, never concurrently with acceptance.
    aborted: AtomicUsize,
}

#[derive(Debug)]
struct ReadBatchInner {
    buffers: Option<Vec<WriteBuffer>>,
    tx: Option<oneshot::Sender<ReadOutcome>>,
}

impl ReadBatch {
    fn new(count: usize, buffers: Vec<WriteBuffer>) -> (Arc<Self>, ReadCompletion) {
        let (tx, receiver) = oneshot::channel();
        let batch = Arc::new(Self {
            total: count,
            remaining: AtomicUsize::new(count),
            accepted: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
            failure: AtomicU8::new(0),
            inner: Mutex::new(ReadBatchInner {
                buffers: Some(buffers),
                tx: Some(tx),
            }),
        });
        if count == 0 {
            batch.finish();
        }
        let completion = ReadCompletion {
            receiver,
            batch: Some(Arc::downgrade(&batch)),
        };
        (batch, completion)
    }

    #[cfg(test)]
    fn new_owned(count: usize, buffers: Vec<Buffer>) -> (Arc<Self>, ReadCompletion) {
        let buffers = buffers.into_iter().map(WriteBuffer::from).collect();
        Self::new(count, buffers)
    }

    #[inline]
    fn complete_one(&self, success: bool) {
        if !success {
            self.record_failure(ReadFailure::Completion);
        }
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.finish();
        }
    }

    fn abort_unposted(&self, count: usize) {
        self.record_failure(ReadFailure::Cancelled);
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
        let (outcome, tx) = {
            let mut inner = self.inner.lock().unwrap();
            let Some(buffers) = inner.buffers.take() else {
                return;
            };
            (
                ReadOutcome {
                    result: self.failure().map_or(Ok(()), Err),
                    buffers,
                },
                inner.tx.take(),
            )
        };
        if let Some(tx) = tx {
            let _ = tx.send(outcome);
        }
    }

    fn fail(&self, reason: ReadFailure) -> bool {
        // Serialize policy failures with publication of the final outcome.
        let inner = self.inner.lock().unwrap();
        inner.buffers.is_some() && self.record_failure(reason)
    }

    fn record_failure(&self, reason: ReadFailure) -> bool {
        self.failure
            .compare_exchange(0, reason as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn failure(&self) -> Option<ReadFailure> {
        ReadFailure::from_code(self.failure.load(Ordering::Acquire))
    }
}

impl Drop for ReadBatch {
    fn drop(&mut self) {
        // A posted WR retains this batch in its QP ledger. The last owner can
        // disappear with buffers left only before posting or after provider QP
        // destruction. Preserve recovery even if a producer abandons its plan.
        let inner = self.inner.get_mut().unwrap();
        if let (Some(buffers), Some(tx)) = (inner.buffers.take(), inner.tx.take()) {
            let reason =
                ReadFailure::from_code(*self.failure.get_mut()).unwrap_or(ReadFailure::Cancelled);
            let _ = tx.send(ReadOutcome {
                result: Err(reason),
                buffers,
            });
        }
    }
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidReadPlan, message.into())
}

trait ReadDestination {
    fn len(&self) -> usize;
    fn as_ptr(&self) -> *const u8;
}

impl ReadDestination for WriteBuffer {
    fn len(&self) -> usize {
        self.len()
    }

    fn as_ptr(&self) -> *const u8 {
        self.as_ptr()
    }
}

#[cfg(any(test, feature = "bench-internals"))]
impl ReadDestination for Buffer {
    fn len(&self) -> usize {
        self.len()
    }

    fn as_ptr(&self) -> *const u8 {
        self.as_ptr()
    }
}

fn prepare<B: ReadDestination>(
    buffers: &[B],
    requests: Vec<ReadRequest>,
    gather_limit: usize,
    mut lkey: impl FnMut(&B) -> Result<u32>,
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
    /// Called only after provider QP destruction has succeeded.
    pub(super) fn finish_destroyed_reads(&mut self) {
        self.read_state.finish_destroyed();
    }

    /// Prepares READs using owned or unsafely borrowed destination capabilities.
    /// An invalid plan returns every capability without posting.
    ///
    /// Owned capabilities retain allocations independently of the caller.
    /// Borrowed capabilities require their constructor's lifetime and access
    /// obligations to remain satisfied, including across cancellation. Posting
    /// and completion are independent of the async runtime. Use
    /// [`ReadPosting::handle`] to observe progress and record policy failures;
    /// recording failure never completes outstanding DMA early.
    pub fn prepare_reads(
        &self,
        buffers: Vec<WriteBuffer>,
        requests: Vec<ReadRequest>,
    ) -> std::result::Result<(ReadPosting, ReadCompletion), (Error, Vec<WriteBuffer>)> {
        let (reads, sges) = match prepare(&buffers, requests, self.gather_limit(), |buffer| {
            buffer
                .memory_key(self.device_index)
                .map(|key| key.lkey)
                .map_err(|error| Error::new(ErrorKind::IBRegMemoryRegionFail, error.to_string()))
        }) {
            Ok(prepared) => prepared,
            Err(error) => return Err((error, buffers)),
        };
        let (batch, receiver) = ReadBatch::new(reads.len(), buffers);
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
        // SAFETY: prepare validated every range against its write capability and
        // resolved keys on this QP. The exclusive cursor posts each range once.
        // This QP keeps the batch through an authentic completion or destruction;
        // owned capabilities retain memory, while borrowed constructors require
        // the caller to keep their allocation and registration alive throughout.
        let sges = &posting.sges[read.sge_start..read.sge_end];
        unsafe {
            self.read_sges(
                sges,
                read.remote_addr,
                read.rkey,
                |wr_id| {
                    // Registration precedes provider posting. Keep this check
                    // active in release builds: a duplicate must never submit
                    // another WR or replace the original batch's DMA hold.
                    assert!(
                        self.read_state
                            .insert(wr_id.sequence(), posting.batch.clone()),
                        "READ sequence {} recorded twice",
                        wr_id.sequence()
                    );
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
        Ok(())
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
    use ruapc_bufpool::{BufferPool, BufferPoolBuilder};

    fn buffers() -> (Vec<Buffer>, std::sync::Weak<BufferPool>) {
        let pool = BufferPoolBuilder::new(Vec::new()).build();
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
        let original_bytes = buffers[0].as_ptr();
        let (batch, mut rx) = ReadBatch::new_owned(2, buffers);
        batch.complete_one(true);
        assert!(rx.try_recv().is_none());
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        let outcome = rx.try_recv().unwrap();
        assert_eq!(outcome.result, Ok(()));
        assert_eq!(outcome.buffers[0].as_ptr(), original_bytes);
        assert_eq!(outcome.buffers[0].len(), 16);
        drop(outcome);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn failure_records_once_and_returns_original_memory_after_flush() {
        for reason in [
            ReadFailure::Timeout,
            ReadFailure::RequestTimeout,
            ReadFailure::ConnectionClosed,
            ReadFailure::Cancelled,
        ] {
            let (buffers, lifetime) = buffers();
            let original_bytes = buffers[0].as_ptr();
            let (batch, mut rx) = ReadBatch::new_owned(2, buffers);
            let handle = ReadHandle(Arc::downgrade(&batch));
            assert!(handle.fail(reason));
            assert!(!handle.fail(ReadFailure::ConnectionClosed));
            assert_eq!(handle.failure(), Some(reason));
            assert!(rx.try_recv().is_none());
            batch.complete_one(false);
            assert!(rx.try_recv().is_none());
            assert!(lifetime.upgrade().is_some());
            batch.complete_one(false);
            let outcome = rx.try_recv().unwrap();
            assert_eq!(outcome.result, Err(reason));
            assert_eq!(outcome.buffers[0].as_ptr(), original_bytes);
            assert!(!handle.is_pending());
            assert!(!handle.fail(ReadFailure::Timeout));
            drop(outcome);
            assert!(lifetime.upgrade().is_none());
        }
    }

    #[test]
    fn failed_completion_returns_memory_after_every_other_completion() {
        let (buffers, lifetime) = buffers();
        let original_bytes = buffers[0].as_ptr();
        let (batch, mut rx) = ReadBatch::new_owned(2, buffers);
        batch.complete_one(false);
        assert!(rx.try_recv().is_none());
        assert!(!batch.fail(ReadFailure::Timeout));
        batch.complete_one(true);
        let outcome = rx.try_recv().unwrap();
        assert_eq!(outcome.result, Err(ReadFailure::Completion));
        assert_eq!(outcome.buffers[0].as_ptr(), original_bytes);
        drop(outcome);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn cancelled_poster_accounts_the_unposted_suffix() {
        let (buffers, lifetime) = buffers();
        let original_bytes = buffers[0].as_ptr();
        let (batch, mut rx) = ReadBatch::new_owned(3, buffers);
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
        batch.mark_accepted(1);
        let handle = posting.handle();
        drop(posting);
        assert!(handle.has_in_flight());
        assert!(rx.try_recv().is_none());
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        let outcome = rx.try_recv().unwrap();
        assert_eq!(outcome.result, Err(ReadFailure::Cancelled));
        assert_eq!(outcome.buffers[0].as_ptr(), original_bytes);
        drop(outcome);
        assert!(lifetime.upgrade().is_none());
        assert!(!handle.has_in_flight());
        assert!(!handle.is_pending());
    }

    #[test]
    fn partial_failure_after_early_completions_settles_once() {
        let (buffers, lifetime) = buffers();
        let original_bytes = buffers[0].as_ptr();
        let (batch, mut rx) = ReadBatch::new_owned(3, buffers);
        batch.complete_one(true);
        batch.complete_one(true);
        assert!(rx.try_recv().is_none());
        batch.abort_unposted(1);
        let outcome = rx.try_recv().unwrap();
        assert_eq!(outcome.result, Err(ReadFailure::Cancelled));
        assert_eq!(outcome.buffers[0].as_ptr(), original_bytes);
        drop(outcome);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn dropped_receiver_cannot_release_inflight_memory() {
        let (buffers, lifetime) = buffers();
        let (batch, rx) = ReadBatch::new_owned(1, buffers);
        drop(rx);
        assert_eq!(batch.failure(), Some(ReadFailure::Cancelled));
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn omitted_receiver_drop_cannot_release_inflight_memory() {
        let (buffers, lifetime) = buffers();
        let (batch, rx) = ReadBatch::new_owned(1, buffers);
        // Model a forgotten consumer without leaking its allocation from this
        // test. Neither posting nor completion may rely on its destructor.
        let receiver = std::mem::ManuallyDrop::new(rx);
        assert!(lifetime.upgrade().is_some());
        assert_eq!(batch.failure(), None);
        batch.complete_one(true);
        assert!(lifetime.upgrade().is_some());
        drop(std::mem::ManuallyDrop::into_inner(receiver));
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn destroyed_qp_settles_ledger_and_spills_but_waits_for_unposted_suffix() {
        let (buffers_a, lifetime_a) = buffers();
        let (buffers_b, lifetime_b) = buffers();
        let original_a = buffers_a[0].as_ptr();
        let original_b = buffers_b[0].as_ptr();
        let (batch_a, mut rx_a) = ReadBatch::new_owned(3, buffers_a);
        let (batch_b, mut rx_b) = ReadBatch::new_owned(1, buffers_b);
        let mut state = ReadState::new(1);
        let capacity = SlotTable::<Arc<ReadBatch>>::new(1).capacity() as u64;
        assert!(state.insert(0, batch_a.clone()));
        assert!(state.insert(1, batch_a.clone()));
        assert!(state.insert(capacity, batch_b.clone()));
        batch_a.mark_accepted(2);
        batch_b.mark_accepted(1);
        assert!(batch_a.fail(ReadFailure::Timeout));
        state.finish_destroyed();
        assert!(state.is_empty());
        assert!(rx_a.try_recv().is_none());
        assert!(!batch_a.has_in_flight());
        assert_eq!(batch_a.remaining.load(Ordering::Acquire), 1);
        let outcome_b = rx_b.try_recv().unwrap();
        assert_eq!(outcome_b.result, Err(ReadFailure::ConnectionClosed));
        assert_eq!(outcome_b.buffers[0].as_ptr(), original_b);
        batch_a.abort_unposted(1);
        let outcome_a = rx_a.try_recv().unwrap();
        assert_eq!(outcome_a.result, Err(ReadFailure::Timeout));
        assert_eq!(outcome_a.buffers[0].as_ptr(), original_a);
        drop(outcome_a);
        drop(outcome_b);
        assert!(lifetime_a.upgrade().is_none());
        assert!(lifetime_b.upgrade().is_none());
    }

    #[test]
    fn ledger_spills_colliding_sequences_without_losing_ownership() {
        let (buffers_a, lifetime_a) = buffers();
        let (buffers_b, lifetime_b) = buffers();
        let (batch_a, rx_a) = ReadBatch::new_owned(1, buffers_a);
        let (batch_b, rx_b) = ReadBatch::new_owned(1, buffers_b);
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
        drop(rx_a);
        drop(rx_b);
        assert!(lifetime_a.upgrade().is_none());
        assert!(lifetime_b.upgrade().is_none());
    }

    #[test]
    fn duplicate_overflow_sequence_preserves_original_completion_owner() {
        let (buffers_a, _) = buffers();
        let (buffers_b, lifetime_b) = buffers();
        let (buffers_c, _) = buffers();
        let original_b = buffers_b[0].as_ptr();
        let original_c = buffers_c[0].as_ptr();
        let (batch_a, mut rx_a) = ReadBatch::new_owned(1, buffers_a);
        let (batch_b, mut rx_b) = ReadBatch::new_owned(1, buffers_b);
        let (batch_c, mut rx_c) = ReadBatch::new_owned(1, buffers_c);
        let state = ReadState::new(1);
        let capacity = SlotTable::<Arc<ReadBatch>>::new(1).capacity() as u64;
        assert!(state.insert(0, batch_a));
        assert!(state.insert(capacity, batch_b));

        // Model an invalid reused sequence before provider posting. The old
        // batch has no other strong owner: replacing its entry would publish
        // its buffers without completion evidence.
        assert!(!state.insert(capacity, batch_c.clone()));
        assert_eq!(state.len(), 2);
        assert_eq!(state.overflow_len.load(Ordering::Acquire), 1);
        assert!(rx_b.try_recv().is_none());
        assert!(rx_c.try_recv().is_none());
        assert!(lifetime_b.upgrade().is_some());

        state.remove(capacity).unwrap().complete_one(true);
        let original = rx_b.try_recv().unwrap();
        assert_eq!(original.result, Ok(()));
        assert_eq!(original.buffers[0].as_ptr(), original_b);
        assert!(rx_c.try_recv().is_none());
        // Rejected work never reached the provider; only its unposted
        // accounting can return the new batch's buffers.
        batch_c.abort_unposted(1);
        let rejected = rx_c.try_recv().unwrap();
        assert_eq!(rejected.result, Err(ReadFailure::Cancelled));
        assert_eq!(rejected.buffers[0].as_ptr(), original_c);
        state.remove(0).unwrap().complete_one(true);
        assert_eq!(rx_a.try_recv().unwrap().result, Ok(()));
        assert!(state.is_empty());
        drop(original);
        assert!(lifetime_b.upgrade().is_none());
    }

    #[test]
    fn destroyed_qp_recovers_poisoned_overflow_without_losing_buffers() {
        let (buffers_a, _) = buffers();
        let (buffers_b, lifetime_b) = buffers();
        let original_b = buffers_b[0].as_ptr();
        let (batch_a, mut rx_a) = ReadBatch::new_owned(1, buffers_a);
        let (batch_b, mut rx_b) = ReadBatch::new_owned(1, buffers_b);
        let mut state = ReadState::new(1);
        let capacity = SlotTable::<Arc<ReadBatch>>::new(1).capacity() as u64;
        assert!(state.insert(0, batch_a));
        assert!(state.insert(capacity, batch_b));
        let overflow = &state.overflow;
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = overflow.lock().unwrap();
                panic!("inject READ overflow lock poisoning");
            })
            .is_err()
        );
        assert!(state.overflow.is_poisoned());
        assert!(rx_b.try_recv().is_none());

        // Successful provider destruction is already established at this
        // boundary. Poisoning must not skip settlement or lose the originals.
        state.finish_destroyed();
        assert!(state.is_empty());
        assert_eq!(
            rx_a.try_recv().unwrap().result,
            Err(ReadFailure::ConnectionClosed)
        );
        let outcome = rx_b.try_recv().unwrap();
        assert_eq!(outcome.result, Err(ReadFailure::ConnectionClosed));
        assert_eq!(outcome.buffers[0].as_ptr(), original_b);
        drop(outcome);
        assert!(lifetime_b.upgrade().is_none());
    }

    #[test]
    fn handle_observes_only_provider_accepted_work_as_in_flight() {
        let (buffers, _) = buffers();
        let (batch, _rx) = ReadBatch::new_owned(2, buffers);
        let handle = ReadHandle(Arc::downgrade(&batch));
        assert!(handle.is_pending());
        assert!(!handle.has_in_flight());
        // A CQE may beat the posting cursor's acceptance store.
        batch.complete_one(true);
        assert!(!handle.has_in_flight());
        batch.mark_accepted(1);
        assert!(!handle.has_in_flight());
        batch.mark_accepted(2);
        assert!(handle.has_in_flight());
        batch.complete_one(true);
        assert!(!handle.has_in_flight());
        assert!(!handle.is_pending());
    }

    #[test]
    fn producer_disappearance_wakes_completion_without_a_runtime() {
        use std::task::{Wake, Waker};

        struct WakeCounter(AtomicUsize);
        impl Wake for WakeCounter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let (buffers, lifetime) = buffers();
        let original_bytes = buffers[0].as_ptr();
        let (batch, mut rx) = ReadBatch::new_owned(1, buffers);
        let handle = ReadHandle(Arc::downgrade(&batch));
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut rx).poll(&mut cx).is_pending());
        drop(batch);
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert!(lifetime.upgrade().is_some());
        assert!(!handle.is_pending());
        assert!(!handle.has_in_flight());
        assert!(!handle.fail(ReadFailure::ConnectionClosed));
        let Poll::Ready(outcome) = Pin::new(&mut rx).poll(&mut cx) else {
            panic!("producer disappearance did not settle its buffers");
        };
        assert_eq!(outcome.result, Err(ReadFailure::Cancelled));
        assert_eq!(outcome.buffers[0].as_ptr(), original_bytes);
        drop(outcome);
        assert!(lifetime.upgrade().is_none());
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
    fn unposted_cancel_returns_original_memory_through_completion() {
        let (buffers, lifetime) = buffers();
        let original_bytes = buffers[0].as_ptr();
        let (batch, mut rx) = ReadBatch::new_owned(1, buffers);
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
        let observation = posting.handle().observe().unwrap();
        posting.cancel();
        assert!(!observation.is_pending());
        assert!(!observation.has_in_flight());
        let outcome = rx.try_recv().unwrap();
        assert_eq!(outcome.result, Err(ReadFailure::Cancelled));
        assert_eq!(outcome.buffers[0].as_ptr(), original_bytes);
        drop(outcome);
        assert!(lifetime.upgrade().is_none());
    }
}
