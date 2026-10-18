//! Owned RDMA READ plans. Posted destinations stay held through completion
//! or successful QP destruction; failure notification does not release them.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Instant;

use ruapc_bufpool::Buffer;
use tokio::sync::oneshot;

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
pub(super) struct ReadState {
    batches: dashmap::DashMap<WRID, Arc<ReadBatch>>,
    owner: u64,
}

impl ReadState {
    pub(super) fn new() -> Self {
        Self {
            // A fixed shard count bounds idle per-QP storage independently
            // of host CPU count; SQ posting is already serialized.
            batches: dashmap::DashMap::with_shard_amount(4),
            owner: next_owner(),
        }
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
    sges: Vec<ReadSge>,
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
#[derive(Debug)]
pub struct ReadPosting {
    owner: u64,
    reads: Vec<PreparedRead>,
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

#[derive(Debug)]
pub(super) struct ReadBatch {
    remaining: AtomicUsize,
    failed: AtomicBool,
    read_deadline: Option<Instant>,
    request_deadline: Option<Instant>,
    read_timeout_reported: AtomicBool,
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
            remaining: AtomicUsize::new(count),
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
) -> Result<Vec<PreparedRead>> {
    let keys = buffers.iter().map(&mut lkey).collect::<Result<Vec<_>>>()?;
    // A single destination cannot overlap another one. Keep its bounds/key
    // checks but avoid allocating an interval list for this common case.
    let check_overlap = requests.len() > 1
        || requests
            .first()
            .is_some_and(|request| request.segments.len() > 1);
    let mut destinations = Vec::new();
    // Consume the plan so Vec's collecting iterator can reuse its allocation
    // for the prepared descriptors, including each request's scatter list.
    let reads = requests
        .into_iter()
        .map(|request| {
            if request.segments.is_empty() || request.segments.len() > gather_limit {
                return Err(invalid("invalid READ scatter count"));
            }
            let mut total = 0u64;
            let sges = request
                .segments
                .into_iter()
                .map(|segment| {
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
                    let len = u32::try_from(segment.len)
                        .map_err(|_| invalid("READ segment exceeds u32::MAX"))?;
                    total = total
                        .checked_add(u64::from(len))
                        .ok_or_else(|| invalid("READ length overflows"))?;
                    if check_overlap {
                        destinations.push((segment.buffer, segment.offset, end));
                    }
                    Ok(ReadSge {
                        addr: buffer.as_ptr() as u64 + segment.offset as u64,
                        len,
                        lkey: keys[segment.buffer],
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            request
                .remote_addr
                .checked_add(total)
                .ok_or_else(|| invalid("READ remote address overflows"))?;
            Ok(PreparedRead {
                remote_addr: request.remote_addr,
                rkey: request.rkey,
                sges,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    destinations.sort_unstable();
    if destinations
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0 && pair[0].2 > pair[1].1)
    {
        return Err(invalid("READ destinations overlap"));
    }
    Ok(reads)
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
    /// Callers must separately bound admission before posting: only posted WRs
    /// are visible to [`Self::expire_reads`].
    pub fn prepare_reads_with_deadlines(
        &self,
        buffers: Vec<Buffer>,
        requests: Vec<ReadRequest>,
        read_deadline: Option<Instant>,
        request_deadline: Option<Instant>,
    ) -> std::result::Result<(ReadPosting, ReadReceiver), (Error, Vec<Buffer>)> {
        let reads = match prepare(&buffers, requests, self.gather_limit(), |buffer| {
            self.lkey(buffer)
        }) {
            Ok(reads) => reads,
            Err(error) => return Err((error, buffers)),
        };
        let (batch, receiver) =
            ReadBatch::new(reads.len(), buffers, read_deadline, request_deadline);
        Ok((
            ReadPosting {
                owner: self.read_state.owner,
                reads,
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
        unsafe {
            self.read_sges(
                &read.sges,
                read.remote_addr,
                read.rkey,
                |wr_id| {
                    self.read_state.batches.insert(wr_id, posting.batch.clone());
                },
                |wr_id| {
                    self.read_state.batches.remove(&wr_id);
                },
            )
        }?;
        posting.next += 1;
        Ok(())
    }

    /// Fails waiters without returning buffers still referenced by the NIC.
    pub fn fail_pending_reads(&self) {
        for entry in &self.read_state.batches {
            entry.value().fail(ReadFailure::ConnectionClosed);
        }
    }

    /// Fails expired waiters while preserving their buffers. Returns true only
    /// for a newly expired READ watchdog, requiring the caller to move the QP
    /// to ERR. Request expiry alone does not prove a connection fault, and
    /// does not disarm the READ watchdog after notifying the receiver.
    pub fn expire_reads(&self, now: Instant) -> bool {
        let mut expired = false;
        for entry in &self.read_state.batches {
            expired |= entry.value().expire(now);
        }
        expired
    }

    pub fn has_pending_reads(&self) -> bool {
        !self.read_state.batches.is_empty()
    }

    /// Number of posted READ WRs whose completions have not been processed.
    ///
    /// Concurrent posting and completion make this a diagnostic snapshot.
    /// Accounting must exclude both operations before using the count, and
    /// destroy the QP before returning credits for its unpolled WRs. The count
    /// itself is not completion evidence and does not release DMA ownership.
    pub fn pending_read_count(&self) -> usize {
        self.read_state.batches.len()
    }

    /// Called only after the completion path verifies CQ/QP ownership.
    pub(super) fn complete_read(&self, wr_id: WRID, success: bool) {
        if let Some((_, batch)) = self.read_state.batches.remove(&wr_id) {
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
        let plan = prepare(
            &buffers,
            vec![request(0, 0, 8), request(0, 8, 8)],
            32,
            |_| Ok(42),
        )
        .unwrap();
        assert_eq!(plan[1].sges[0].addr, buffers[0].as_ptr() as u64 + 8);
        assert_eq!(plan[1].sges[0].lkey, 42);
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
                sges: Vec::new(),
            })
            .collect();
        let posting = ReadPosting {
            owner: 1,
            reads,
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
    fn unposted_cancel_recovers_memory_and_closes_receiver() {
        let (buffers, lifetime) = buffers();
        let (batch, mut rx) = ReadBatch::new(1, buffers, None, None);
        let posting = ReadPosting {
            owner: 1,
            reads: vec![PreparedRead {
                remote_addr: 0,
                rkey: 0,
                sges: Vec::new(),
            }],
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
