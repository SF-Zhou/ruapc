//! Owned RDMA READ plans. Posted destinations stay held through completion
//! or successful QP destruction; failure notification does not release them.

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use ruapc_bufpool::Buffer;
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

/// Reason a READ batch did not produce usable destination buffers.
///
/// Completion failure and abandonment are detected by this crate. Timeout and
/// connection failures are labels supplied by higher-layer policy through
/// [`ReadHandle::fail`]; this crate neither runs timers nor closes connections.
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

/// Runtime-independent result of an owned READ plan.
///
/// Successful buffers are returned only after every READ has completed.
/// Failure notification may precede completion; the QP retains DMA ownership.
/// Dropping this future does not release posted buffers. Producer disappearance
/// (including cancellation before posting) resolves to [`ReadFailure::Cancelled`].
#[derive(Debug)]
pub struct ReadCompletion {
    receiver: oneshot::Receiver<std::result::Result<Vec<Buffer>, ReadFailure>>,
}

impl ReadCompletion {
    /// Takes a ready result without waiting. `None` means it is still pending.
    pub fn try_recv(&mut self) -> Option<std::result::Result<Vec<Buffer>, ReadFailure>> {
        match self.receiver.try_recv() {
            Ok(result) => result,
            Err(_) => Some(Err(ReadFailure::Cancelled)),
        }
    }
}

impl Future for ReadCompletion {
    type Output = std::result::Result<Vec<Buffer>, ReadFailure>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|result| result.unwrap_or(Err(ReadFailure::Cancelled)))
    }
}

/// A weak observer and failure notifier for one READ plan.
///
/// The handle neither owns destination buffers nor grants completion authority.
/// It allows a higher layer to implement deadlines and connection failure
/// policy without retaining a completed batch. The posting cursor and QP own
/// the batch; an observation can temporarily extend that lifetime. Failure
/// notification cannot authorize memory recovery.
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

    /// Sends a caller-selected failure if the result is not yet published.
    /// Buffers remain held until completion or successful QP destruction.
    /// Returns whether this call took the still-open result sender.
    pub fn fail(&self, reason: ReadFailure) -> bool {
        self.observe().is_some_and(|batch| batch.fail(reason))
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

    /// Notifies a failure without releasing destination ownership.
    pub fn fail(&self, reason: ReadFailure) -> bool {
        self.0.fail(reason)
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
/// cache line after `remaining`. Posting never invalidates the counter updated
/// by every completion. The result mutex and rarely written fields fill that
/// first line; see `posting_accounting_stays_off_the_completion_line`.
#[derive(Debug)]
#[repr(C)]
pub(super) struct ReadBatch {
    /// WRs not yet settled by completion or unposted cancellation.
    remaining: AtomicUsize,
    total: usize,
    failed: AtomicBool,
    inner: Mutex<ReadBatchInner>,
    /// WRs the provider accepted. Written only by the posting cursor.
    accepted: AtomicUsize,
    /// Unposted suffix settled by the cursor, never concurrently with acceptance.
    aborted: AtomicUsize,
}

#[derive(Debug)]
struct ReadBatchInner {
    buffers: Option<Vec<Buffer>>,
    tx: Option<oneshot::Sender<std::result::Result<Vec<Buffer>, ReadFailure>>>,
}

impl ReadBatch {
    fn new(count: usize, buffers: Vec<Buffer>) -> (Arc<Self>, ReadCompletion) {
        let (tx, receiver) = oneshot::channel();
        let batch = Arc::new(Self {
            total: count,
            remaining: AtomicUsize::new(count),
            accepted: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            inner: Mutex::new(ReadBatchInner {
                buffers: Some(buffers),
                tx: Some(tx),
            }),
        });
        if count == 0 {
            batch.finish();
        }
        (batch, ReadCompletion { receiver })
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

    fn cancel_unposted(&self) -> Option<Vec<Buffer>> {
        // The cursor proves no WR reached the provider, so no completion can
        // race this settlement. Existing observers must also see cancellation.
        self.remaining.store(0, Ordering::Release);
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
    ///
    /// The returned completion is independent of any async runtime. The caller
    /// may use [`ReadPosting::handle`] to observe progress and notify failures;
    /// request deadlines and connection watchdogs belong to that higher layer.
    pub fn prepare_reads(
        &self,
        buffers: Vec<Buffer>,
        requests: Vec<ReadRequest>,
    ) -> std::result::Result<(ReadPosting, ReadCompletion), (Error, Vec<Buffer>)> {
        let (reads, sges) = match prepare(&buffers, requests, self.gather_limit(), |buffer| {
            self.lkey(buffer)
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
    use ruapc_bufpool::{BufferPool, BufferPoolBuilder, EmptyDevices};

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
        let (batch, mut rx) = ReadBatch::new(2, buffers);
        batch.complete_one(true);
        assert!(rx.try_recv().is_none());
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        let returned = rx.try_recv().unwrap().unwrap();
        assert_eq!(returned[0].len(), 16);
        drop(returned);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn failure_notifies_once_and_holds_memory_through_flush() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(2, buffers);
        let handle = ReadHandle(Arc::downgrade(&batch));
        assert!(handle.fail(ReadFailure::Timeout));
        assert!(!handle.fail(ReadFailure::ConnectionClosed));
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Timeout);
        batch.complete_one(false);
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(false);
        assert!(lifetime.upgrade().is_none());
        assert!(!handle.is_pending());
    }

    #[test]
    fn cancelled_poster_accounts_the_unposted_suffix() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(3, buffers);
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
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Completion);
        assert!(lifetime.upgrade().is_none());
        assert!(!handle.has_in_flight());
        assert!(!handle.is_pending());
    }

    #[test]
    fn partial_failure_after_early_completions_settles_once() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(3, buffers);
        batch.complete_one(true);
        batch.complete_one(true);
        assert!(rx.try_recv().is_none());
        batch.abort_unposted(1);
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Completion);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn dropped_receiver_cannot_release_inflight_memory() {
        let (buffers, lifetime) = buffers();
        let (batch, rx) = ReadBatch::new(1, buffers);
        drop(rx);
        assert!(lifetime.upgrade().is_some());
        batch.complete_one(true);
        assert!(lifetime.upgrade().is_none());
    }

    #[test]
    fn ledger_spills_colliding_sequences_without_losing_ownership() {
        let (buffers_a, lifetime_a) = buffers();
        let (buffers_b, lifetime_b) = buffers();
        let (batch_a, _rx_a) = ReadBatch::new(1, buffers_a);
        let (batch_b, _rx_b) = ReadBatch::new(1, buffers_b);
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
    fn handle_observes_only_provider_accepted_work_as_in_flight() {
        let (buffers, _) = buffers();
        let (batch, _rx) = ReadBatch::new(2, buffers);
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
        let (batch, mut rx) = ReadBatch::new(1, buffers);
        let handle = ReadHandle(Arc::downgrade(&batch));
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut rx).poll(&mut cx).is_pending());
        drop(batch);
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert!(lifetime.upgrade().is_none());
        assert!(!handle.is_pending());
        assert!(!handle.has_in_flight());
        assert!(!handle.fail(ReadFailure::ConnectionClosed));
        assert!(matches!(
            Pin::new(&mut rx).poll(&mut cx),
            Poll::Ready(Err(ReadFailure::Cancelled))
        ));
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
    fn unposted_cancel_recovers_memory_and_closes_receiver() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(1, buffers);
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
        let returned = posting.cancel().unwrap();
        assert!(!observation.is_pending());
        assert!(!observation.has_in_flight());
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), ReadFailure::Cancelled);
        drop(returned);
        assert!(lifetime.upgrade().is_none());
    }
}
