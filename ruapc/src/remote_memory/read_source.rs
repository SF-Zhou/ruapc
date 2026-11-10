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

use crate::{CopyOp, Error, ErrorKind, Result};

const SOURCE_REQUEST_CAPACITY: usize = 32;

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

/// Driven only by the original request future while it borrows the source.
pub(crate) struct ReadSourceReceiver {
    requests: mpsc::Receiver<SourceRequest>,
}

pub(crate) fn read_source_channel() -> (ReadSourceSender, ReadSourceReceiver) {
    let (sender, receiver) = mpsc::channel(SOURCE_REQUEST_CAPACITY);
    (
        ReadSourceSender { requests: sender },
        ReadSourceReceiver { requests: receiver },
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
    pub(crate) async fn gather(&self, ops: Vec<CopyOp>, deadline: Instant) -> Result<Vec<u8>> {
        let (reply, response) = oneshot::channel();
        self.request(
            SourceRequest::Inline {
                deadline,
                ops,
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

impl ReadSourceReceiver {
    /// Drives both the business operation and source requests without allowing
    /// references to escape into independently running tasks. The same receiver
    /// may cover successive send and response phases of one immutable source.
    pub(crate) async fn drive<F: Future>(
        &mut self,
        source: &ReadSource<'_>,
        future: F,
    ) -> F::Output {
        tokio::pin!(future);
        loop {
            // Poll business completion first, and yield between bounded batches
            // so a busy source mailbox cannot monopolize the executor.
            for _ in 0..SOURCE_REQUEST_CAPACITY {
                tokio::select! {
                    biased;
                    output = &mut future => return output,
                    request = self.requests.recv() => match request {
                        Some(request) => request.handle(source),
                        None => return future.await,
                    },
                }
            }
            tokio::task::yield_now().await;
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
        let (sender, mut receiver) = read_source_channel();
        let completed_read = buffers[0].to_vec();
        let completed_probe = {
            let source = ReadSource::new(&buffers);
            let mut driver = Box::pin(receiver.drive(&source, pending::<()>()));

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
        let (sender, mut receiver) = read_source_channel();
        let work = async {
            assert_eq!(
                sender
                    .gather(vec![CopyOp::new(2, 0, 4)], deadline())
                    .await
                    .unwrap(),
                b"cdef"
            );
            let error = sender
                .gather(vec![CopyOp::new(7, 0, 2)], deadline())
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
        };
        receiver.drive(&source, work).await;
    }

    #[tokio::test]
    async fn business_completion_stops_confirmation_until_the_next_phase() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, mut receiver) = read_source_channel();
        let mut probe = Box::pin(sender.validate(deadline()));
        assert!(futures_util::poll!(probe.as_mut()).is_pending());

        assert_eq!(receiver.drive(&source, ready(17)).await, 17);
        assert!(futures_util::poll!(probe.as_mut()).is_pending());

        // A send phase may end while a probe waits for the response phase.
        receiver.drive(&source, probe).await.unwrap();
    }

    #[tokio::test]
    async fn dropping_the_receiver_fails_queued_and_new_requests() {
        let (sender, receiver) = read_source_channel();
        let mut queued = Box::pin(sender.validate(deadline()));
        assert!(futures_util::poll!(queued.as_mut()).is_pending());
        drop(receiver);
        assert_eq!(queued.await.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(
            sender.validate(deadline()).await.unwrap_err().kind,
            ErrorKind::Timeout
        );
        assert_eq!(
            sender
                .gather(Vec::new(), deadline())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
    }

    #[tokio::test]
    async fn queue_admission_and_unpolled_replies_share_the_deadline() {
        let (sender, receiver) = read_source_channel();
        let mut queued = Vec::new();
        for _ in 0..SOURCE_REQUEST_CAPACITY {
            let mut probe = Box::pin(sender.validate(deadline()));
            assert!(futures_util::poll!(probe.as_mut()).is_pending());
            queued.push(probe);
        }
        assert_eq!(receiver.requests.len(), SOURCE_REQUEST_CAPACITY);
        assert_eq!(
            sender
                .validate(Instant::now() + Duration::from_millis(10))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
        assert_eq!(receiver.requests.len(), SOURCE_REQUEST_CAPACITY);
    }

    #[tokio::test]
    async fn expired_queued_work_cannot_be_answered_when_polling_resumes() {
        let buffers = buffers();
        let source = ReadSource::new(&buffers);
        let (sender, mut receiver) = read_source_channel();
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
        receiver
            .drive(&source, async {
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
            let (sender, mut receiver) = read_source_channel();
            let sender = (!closed).then_some(sender);
            let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
            let waker = Waker::from(wake_count.clone());
            let mut driver = Box::pin(receiver.drive(&source, pending::<()>()));
            assert!(matches!(
                driver.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Pending
            ));
            assert_eq!(wake_count.0.load(Ordering::Relaxed), 0);
            drop(sender);
        }
    }
}
