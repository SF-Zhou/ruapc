//! Route source access to the request future that still borrows its buffers.
//!
//! A live mailbox is not evidence that its source remains borrowed: safe code
//! can forget the driving future. Only that future's active polling can answer
//! a post-READ probe or gather inline bytes. Queued requests contain no borrowed
//! source memory, and every reply belongs to one operation.

use std::{future::Future, time::Instant};

use ruapc_memory::{CopyBatch, ReadSource};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout_at,
};

use crate::{CopyOp, Error, ErrorKind, MAX_COPY_OPS, Result, msg::frame::MAX_MSG_SIZE};

const SOURCE_REQUEST_CAPACITY: usize = 32;

/// A reply of this many bytes could never be framed. Source ranges may overlap,
/// so without this bound a peer could request far more than the source holds.
const MAX_INLINE_BYTES: u64 = MAX_MSG_SIZE as u64;

#[derive(Debug)]
enum SourceRequest {
    Validate {
        deadline: Instant,
        reply: oneshot::Sender<()>,
    },
    Inline {
        deadline: Instant,
        ops: Vec<CopyOp>,
        reply: oneshot::Sender<Result<Vec<u8>>>,
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
            Self::Inline {
                deadline,
                ops,
                reply,
            } => {
                if Instant::now() < deadline && !reply.is_closed() {
                    let result = CopyBatch::new(&ops)
                        .and_then(|batch| source.gather(&batch))
                        .map_err(Into::into);
                    let _ = reply.send(result);
                }
            }
        }
    }
}

/// A background handler can enqueue work, but cannot access the borrowed source.
#[derive(Clone, Debug)]
pub(crate) struct ReadSourceSender {
    requests: mpsc::Sender<SourceRequest>,
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
        ReadSourceSender { requests: sender },
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

    /// Copies inline bytes inside the source owner's polling context.
    ///
    /// Bounds the peer-supplied work before queueing or cloning it: the
    /// operation count, and the bytes the owner would allocate and copy in its
    /// poll. Full validation and byte access remain in the borrowing future.
    pub(crate) async fn gather(&self, ops: &[CopyOp], deadline: Instant) -> Result<Vec<u8>> {
        if ops.len() > MAX_COPY_OPS {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                "too many inline read operations".into(),
            ));
        }
        let bytes = ops
            .iter()
            .try_fold(0u64, |total, op| total.checked_add(op.len));
        if bytes.is_none_or(|bytes| bytes >= MAX_INLINE_BYTES) {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!("inline read exceeds the {MAX_INLINE_BYTES}-byte message limit"),
            ));
        }
        let (reply, response) = oneshot::channel();
        self.request(
            SourceRequest::Inline {
                deadline,
                ops: ops.to_vec(),
                reply,
            },
            response,
            deadline,
        )
        .await?
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
    async fn inline_gather_returns_owned_bytes_and_checks_ranges() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, mut service) = serve_read_source(&source);
        let work = async {
            assert_eq!(
                sender
                    .gather(&[CopyOp::new(2, 0, 4)], deadline())
                    .await
                    .unwrap(),
                b"cdef"
            );
            // Source ranges may overlap; the reply holds each operation's bytes.
            assert_eq!(
                sender
                    .gather(&[CopyOp::new(0, 0, 4), CopyOp::new(0, 4, 4)], deadline())
                    .await
                    .unwrap(),
                b"abcdabcd"
            );
            let error = sender
                .gather(&[CopyOp::new(7, 0, 2)], deadline())
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
        };
        service.drive(work).await;
    }

    #[tokio::test]
    async fn oversized_inline_work_is_rejected_before_reaching_the_owner() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, service) = serve_read_source(&source);
        // Nobody drives the service: every rejection precedes the mailbox.
        for ops in [
            vec![CopyOp::new(0, 0, 0); MAX_COPY_OPS + 1],
            vec![CopyOp::new(0, 0, MAX_INLINE_BYTES)],
            // Overlapping source ranges amplify a small source.
            (0..MAX_COPY_OPS as u64)
                .map(|index| {
                    let len = MAX_INLINE_BYTES.div_ceil(MAX_COPY_OPS as u64);
                    CopyOp::new(0, index * len, len)
                })
                .collect(),
            vec![CopyOp::new(0, 0, u64::MAX), CopyOp::new(0, 0, 1)],
        ] {
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                sender.gather(&ops, Instant::now() + Duration::from_secs(30)),
            )
            .await
            .expect("oversized work must fail without waiting for the owner");
            assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidCopyOp);
        }
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
            sender.gather(&[], deadline()).await.unwrap_err().kind,
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
            .try_send(SourceRequest::Inline {
                deadline: expired,
                ops: vec![CopyOp::new(0, 0, 4)],
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
