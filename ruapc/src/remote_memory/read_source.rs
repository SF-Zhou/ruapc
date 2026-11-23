//! Route source access to the request future that still borrows its buffers.
//!
//! A live mailbox is not evidence that its source remains borrowed: safe code
//! can forget the driving future. Only that future's active polling can answer
//! a post-READ probe or gather inline bytes. Queued requests contain no borrowed
//! source memory, and every reply belongs to one operation.

use std::{
    future::Future,
    sync::{Arc, Mutex, Weak},
    time::Instant,
};

use bytes::BytesMut;
use ruapc_memory::{RangeCursor, ReadSource};
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc, oneshot},
    time::timeout_at,
};

use crate::{Error, ErrorKind, Result, msg::frame::MAX_DATA_LEN};

const SOURCE_REQUEST_CAPACITY: usize = 32;

/// Most bytes the owner copies for one request: the raw bytes of one data
/// frame. Source ranges may overlap, so without this bound a peer could make
/// one poll of the borrowing future copy far more than the source holds.
const MAX_GATHER_BYTES: u64 = MAX_DATA_LEN as u64;

type ReservedFrame = (BytesMut, OwnedSemaphorePermit);

/// The handler owns the slot until its source claims the frame. A stale mailbox
/// carries only a weak handle; an active copy owns both storage and permits.
#[derive(Debug)]
struct GatherWork {
    frame: Mutex<Option<ReservedFrame>>,
}

#[derive(Debug)]
enum SourceRequest {
    Validate {
        deadline: Instant,
        reply: oneshot::Sender<()>,
    },
    Gather {
        deadline: Instant,
        ranges: RangeCursor,
        work: Weak<GatherWork>,
        reply: oneshot::Sender<Result<ReservedFrame>>,
    },
}

impl SourceRequest {
    fn handle(self, source: &ReadSource<'_>) {
        match self {
            Self::Validate { deadline, reply } => {
                if Instant::now() < deadline {
                    let _ = reply.send(());
                }
            }
            Self::Gather {
                deadline,
                ranges,
                work,
                reply,
            } => {
                if Instant::now() < deadline
                    && !reply.is_closed()
                    && let Some((frame, permit)) = work
                        .upgrade()
                        .and_then(|work| work.frame.lock().unwrap().take())
                {
                    let result = append(source, ranges, frame).map(|bytes| (bytes, permit));
                    let _ = reply.send(result);
                }
            }
        }
    }
}

/// Fill the already-reserved frame while the source owner is active.
fn append(
    source: &ReadSource<'_>,
    mut ranges: RangeCursor,
    mut into: BytesMut,
) -> Result<BytesMut> {
    while let Some((offset, len)) = ranges.next_span(u64::MAX) {
        source.for_each_slice(offset, len, |slice| into.extend_from_slice(slice))?;
    }
    Ok(into)
}

/// A background handler can enqueue work, but cannot access the borrowed source.
#[derive(Clone, Debug)]
pub(crate) struct ReadSourceSender {
    requests: mpsc::Sender<SourceRequest>,
    /// Logical length of the source, which its borrow keeps fixed.
    source_len: u64,
}

/// The borrowed source of one request attempt together with its mailbox.
/// Only the original request future drives it, so source requests are answered
/// exactly while that future is polled with its borrow.
pub(crate) struct ReadSourceService<'a> {
    source: &'a ReadSource<'a>,
    requests: mpsc::Receiver<SourceRequest>,
}

/// Opens the route to `source` for one request attempt.
pub(crate) fn serve_read_source<'a>(
    source: &'a ReadSource<'a>,
) -> (ReadSourceSender, ReadSourceService<'a>) {
    let (sender, requests) = mpsc::channel(SOURCE_REQUEST_CAPACITY);
    (
        ReadSourceSender {
            requests: sender,
            source_len: source.layout().total(),
        },
        ReadSourceService { source, requests },
    )
}

fn source_unavailable() -> Error {
    Error::new(
        ErrorKind::Timeout,
        "read source request expired or its owner stopped polling".into(),
    )
}

impl ReadSourceSender {
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.requests.is_closed()
    }

    /// A fresh reply validates only READs completed before this probe was sent.
    pub(crate) async fn validate(&self, deadline: Instant) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.request(
            SourceRequest::Validate { deadline, reply },
            response,
            deadline,
        )
        .await
    }

    /// Logical length of the source space. Lets a handler reject a batch
    /// before it asks the owner for any byte.
    pub(crate) fn source_len(&self) -> u64 {
        self.source_len
    }

    /// Copies a checked piece while the owner is actively polling its source.
    /// Queued requests hold a weak handle to the budgeted frame.
    pub(crate) async fn gather_reserved(
        &self,
        ranges: RangeCursor,
        into: BytesMut,
        deadline: Instant,
        permit: OwnedSemaphorePermit,
    ) -> Result<ReservedFrame> {
        if ranges.remaining() > MAX_GATHER_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!("inline read piece exceeds the {MAX_GATHER_BYTES} bytes of a data frame"),
            ));
        }
        let size = usize::try_from(ranges.remaining()).expect("one bounded data frame");
        let mut frame = BytesMut::with_capacity(into.len() + size);
        frame.extend_from_slice(&into);
        let work = Arc::new(GatherWork {
            frame: Mutex::new(Some((frame, permit))),
        });
        let (reply, response) = oneshot::channel();
        self.request(
            SourceRequest::Gather {
                deadline,
                ranges,
                work: Arc::downgrade(&work),
                reply,
            },
            response,
            deadline,
        )
        .await?
    }

    #[cfg(test)]
    pub(crate) async fn gather(
        &self,
        ranges: RangeCursor,
        into: BytesMut,
        deadline: Instant,
    ) -> Result<BytesMut> {
        let capacity = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = capacity.try_acquire_owned().unwrap();
        let (bytes, _) = self.gather_reserved(ranges, into, deadline, permit).await?;
        Ok(bytes)
    }

    async fn request<T>(
        &self,
        request: SourceRequest,
        response: oneshot::Receiver<T>,
        deadline: Instant,
    ) -> Result<T> {
        if Instant::now() >= deadline {
            return Err(source_unavailable());
        }
        timeout_at(deadline.into(), async {
            self.requests
                .send(request)
                .await
                .map_err(|_| source_unavailable())?;
            response.await.map_err(|_| source_unavailable())
        })
        .await
        .map_err(|_| source_unavailable())?
    }
}

impl ReadSourceService<'_> {
    /// Drives both the business operation and source requests without allowing
    /// references to escape into independently running tasks. The same service
    /// may cover successive send and response phases of one immutable source.
    pub(crate) async fn drive<F: Future>(&mut self, future: F) -> F::Output {
        tokio::pin!(future);
        loop {
            // Poll business completion first, and yield between bounded batches
            // so a busy source mailbox cannot monopolize the executor.
            for _ in 0..SOURCE_REQUEST_CAPACITY {
                tokio::select! {
                    biased;
                    output = &mut future => return output,
                    request = self.requests.recv() => match request {
                        Some(request) => request.handle(self.source),
                        None => return future.await,
                    },
                }
            }
            tokio::task::yield_now().await;
        }
    }

    /// Awaits `future`, answering source requests if the attempt has a source.
    pub(crate) async fn drive_optional<F: Future>(
        service: Option<&mut Self>,
        future: F,
    ) -> F::Output {
        match service {
            Some(service) => service.drive(future).await,
            None => future.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::{pending, ready},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use crate::{Buffer, BufferPool};

    use super::*;

    fn buffers() -> Vec<Buffer> {
        let pool = BufferPool::new(Vec::new());
        [b"abcd", b"efgh"]
            .into_iter()
            .map(|bytes| {
                let mut buffer = pool.allocate(bytes.len()).unwrap();
                buffer.set_len(bytes.len());
                buffer.copy_from_slice(bytes);
                buffer
            })
            .collect()
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(1)
    }

    #[tokio::test]
    async fn forgetting_the_driver_allows_source_reuse_but_no_new_confirmation() {
        let mut buffers = buffers();
        let completed_read = buffers[0].to_vec();
        let sender;
        let completed_probe = {
            let source = ReadSource::new(&buffers);
            let mut service;
            (sender, service) = serve_read_source(&source);
            // The request future owns its service.
            let mut driver = Box::pin(async move { service.drive(pending::<()>()).await });

            // Model a completed READ followed by its own fresh probe.
            let mut probe = Box::pin(sender.validate(deadline()));
            assert!(futures_util::poll!(probe.as_mut()).is_pending());
            assert!(futures_util::poll!(driver.as_mut()).is_pending());

            // Forget skips cleanup and leaves the mailbox open, but Rust
            // permits ending the borrow and changing the buffers afterward.
            std::mem::forget(driver);
            probe
        };
        buffers[0].fill(b'x');
        assert_eq!(completed_read, b"abcd");
        completed_probe.await.unwrap();

        let error = sender
            .validate(Instant::now() + Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(!sender.requests.is_closed());
    }

    #[tokio::test]
    async fn gather_appends_owned_bytes_and_checks_ranges_first() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, mut service) = serve_read_source(&source);
        assert_eq!(sender.source_len(), 8);
        let work = async {
            // The bytes follow whatever the storage already holds.
            let frame = sender
                .gather(
                    RangeCursor::new(vec![(2, 4)], 8).unwrap(),
                    BytesMut::from("head:"),
                    deadline(),
                )
                .await
                .unwrap();
            assert_eq!(&frame[..], b"head:cdef");
            // Source ranges may overlap; the storage gets each operation's bytes.
            let frame = sender
                .gather(
                    RangeCursor::new(vec![(0, 4), (0, 4)], 8).unwrap(),
                    BytesMut::new(),
                    deadline(),
                )
                .await
                .unwrap();
            assert_eq!(&frame[..], b"abcdabcd");
            // Even a piece prepared for a larger space is checked against
            // the actual borrowed buffers before it can return bytes.
            let piece = RangeCursor::new(vec![(0, 4), (8, 1)], 9).unwrap();
            let error = sender
                .gather(piece, BytesMut::new(), deadline())
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
        };
        service.drive(work).await;
    }

    #[tokio::test]
    async fn only_an_active_copy_or_its_result_keeps_reserved_bytes_after_cancellation() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, mut service) = serve_read_source(&source);
        let capacity = Arc::new(tokio::sync::Semaphore::new(4));

        for poll_source in [false, true] {
            let held = capacity.clone().try_acquire_many_owned(4).unwrap();
            let mut copying = Box::pin(sender.gather_reserved(
                RangeCursor::new(vec![(0, 4)], 8).unwrap(),
                BytesMut::new(),
                deadline(),
                held,
            ));
            assert!(futures_util::poll!(copying.as_mut()).is_pending());
            if poll_source {
                service.requests.try_recv().unwrap().handle(&source);
            }
            assert_eq!(capacity.available_permits(), 0);
            drop(copying);
            assert_eq!(capacity.available_permits(), 4);
            if !poll_source {
                service.requests.try_recv().unwrap().handle(&source);
            }
        }

        let held = capacity.clone().try_acquire_many_owned(4).unwrap();
        let mut copying = Box::pin(sender.gather_reserved(
            RangeCursor::new(vec![(0, 4)], 8).unwrap(),
            BytesMut::new(),
            deadline(),
            held,
        ));
        assert!(futures_util::poll!(copying.as_mut()).is_pending());
        let SourceRequest::Gather { work, .. } = service.requests.try_recv().unwrap() else {
            panic!("expected gather request")
        };
        let frame = work
            .upgrade()
            .unwrap()
            .frame
            .lock()
            .unwrap()
            .take()
            .unwrap();
        drop(copying);
        assert_eq!(capacity.available_permits(), 0);
        drop(frame);
        assert_eq!(capacity.available_permits(), 4);
    }

    #[tokio::test]
    async fn oversized_gather_work_is_rejected_before_reaching_the_owner() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, service) = serve_read_source(&source);
        // Nobody drives the service: every rejection precedes the mailbox.
        let ranges =
            RangeCursor::new(vec![(0, MAX_GATHER_BYTES), (0, 1)], MAX_GATHER_BYTES).unwrap();
        let result = sender.gather(ranges, BytesMut::new(), deadline()).await;
        assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidCopyOp);
        assert_eq!(service.requests.len(), 0);
    }

    #[tokio::test]
    async fn business_completion_stops_confirmation_until_the_next_phase() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, mut service) = serve_read_source(&source);
        let mut probe = Box::pin(sender.validate(deadline()));
        assert!(futures_util::poll!(probe.as_mut()).is_pending());

        assert_eq!(service.drive(ready(17)).await, 17);
        assert!(futures_util::poll!(probe.as_mut()).is_pending());

        // A send phase may end while a probe waits for the response phase.
        service.drive(probe).await.unwrap();
    }

    #[tokio::test]
    async fn dropping_the_service_fails_queued_and_new_requests() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, service) = serve_read_source(&source);
        let mut queued = Box::pin(sender.validate(deadline()));
        assert!(futures_util::poll!(queued.as_mut()).is_pending());
        drop(service);
        assert_eq!(queued.await.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(
            sender.validate(deadline()).await.unwrap_err().kind,
            ErrorKind::Timeout
        );
        assert_eq!(
            sender
                .gather(RangeCursor::default(), BytesMut::new(), deadline())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
    }

    #[tokio::test]
    async fn queue_admission_and_unpolled_replies_share_the_deadline() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, service) = serve_read_source(&source);
        let mut queued = Vec::new();
        for _ in 0..SOURCE_REQUEST_CAPACITY {
            let mut probe = Box::pin(sender.validate(deadline()));
            assert!(futures_util::poll!(probe.as_mut()).is_pending());
            queued.push(probe);
        }
        assert_eq!(service.requests.len(), SOURCE_REQUEST_CAPACITY);
        assert_eq!(
            sender
                .validate(Instant::now() + Duration::from_millis(10))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
        assert_eq!(service.requests.len(), SOURCE_REQUEST_CAPACITY);
    }

    #[tokio::test]
    async fn expired_queued_work_cannot_be_answered_when_polling_resumes() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, mut service) = serve_read_source(&source);
        let expired = Instant::now();
        let (validate_reply, validate_response) = oneshot::channel();
        let (inline_reply, inline_response) = oneshot::channel();
        sender
            .requests
            .try_send(SourceRequest::Validate {
                deadline: expired,
                reply: validate_reply,
            })
            .unwrap();
        sender
            .requests
            .try_send(SourceRequest::Gather {
                work: Weak::new(),
                deadline: expired,
                ranges: RangeCursor::new(vec![(0, 4)], 8).unwrap(),
                reply: inline_reply,
            })
            .unwrap();
        service
            .drive(async {
                assert!(validate_response.await.is_err());
                assert!(inline_response.await.is_err());
            })
            .await;
    }

    #[tokio::test]
    async fn an_idle_or_closed_mailbox_does_not_spin_or_wake_itself() {
        struct WakeCount(AtomicUsize);
        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        for closed in [false, true] {
            let (sender, mut service) = serve_read_source(&source);
            let sender = (!closed).then_some(sender);
            let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
            let waker = Waker::from(wake_count.clone());
            let mut driver = Box::pin(service.drive(pending::<()>()));
            assert!(matches!(
                driver.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Pending
            ));
            assert_eq!(wake_count.0.load(Ordering::Relaxed), 0);
            drop(sender);
        }
    }
}
