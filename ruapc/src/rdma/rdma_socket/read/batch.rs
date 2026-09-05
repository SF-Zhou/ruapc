//! Completion accounting and the registered-memory hold of one READ batch.
//! This state machine is independent of QP posting and can be tested without
//! an RDMA device, including timeout and partial-post interleavings.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use crate::{Buffer, Error, ErrorKind, Result, remote_memory::WriteTarget};

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
    /// Client-side `read_into_target`: the request's pinned write target. Never read
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
    /// Timeout deadline (`None` when `rdma.remote_memory.read_timeout_ms` is 0).
    deadline: Option<Instant>,
    inner: Mutex<ReadBatchInner>,
}

#[derive(Debug)]
struct ReadBatchInner {
    hold: Option<ReadHold>,
    tx: Option<tokio::sync::oneshot::Sender<Result<ReadHold>>>,
}

impl ReadBatch {
    pub(super) fn new(
        count: usize,
        hold: ReadHold,
        tx: tokio::sync::oneshot::Sender<Result<ReadHold>>,
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
    pub(super) fn abort_unposted(&self, unposted: usize) {
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
    pub(super) fn cancel(&self) -> Option<ReadHold> {
        let mut inner = self.inner.lock().unwrap();
        inner.tx.take();
        inner.hold.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BufferPool, Devices};
    use std::time::Duration;

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

    /// Completions may arrive while the posting task still owns an unposted
    /// suffix. A failed post must account that suffix exactly once and keep
    /// registered memory pinned until the last actual WR finishes.
    #[tokio::test]
    async fn partial_post_failure_keeps_memory_until_the_last_completion() {
        let pool = pool();
        let mut buf = pool.allocate(64 * 1024).unwrap();
        buf.set_len(16);
        let target = WriteTarget::new(vec![buf]).unwrap();
        let observer = target.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch = ReadBatch::new(3, ReadHold::Target(target), tx, None);
        batch.complete_one(true);
        batch.abort_unposted(1);
        assert!(WriteTarget::try_into_buffers(observer.clone()).is_none());
        batch.complete_one(false);
        assert_eq!(
            rx.await.unwrap().unwrap_err().kind,
            ErrorKind::RdmaSendFailed
        );
        assert!(WriteTarget::try_into_buffers(observer).is_some());
    }

    #[tokio::test]
    async fn partial_post_failure_resolves_after_earlier_completions_already_arrived() {
        let pool = pool();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let batch = ReadBatch::new(3, make_hold(&pool), tx, None);
        batch.complete_one(true);
        batch.complete_one(true);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        batch.abort_unposted(1);
        assert_eq!(
            rx.await.unwrap().unwrap_err().kind,
            ErrorKind::RdmaSendFailed
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
