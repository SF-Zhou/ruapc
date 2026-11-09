//! Per-request source commands, serviced only by the future borrowing the source.

use std::time::Instant;

use ruapc_memory::{BorrowedSource, CopyBatch};
use tokio::sync::{mpsc, oneshot};

use crate::{Client, CopyOp, Error, ErrorKind, Result, Waiter};

/// A waiter may retain this sender, but never a source pointer or byte accessor.
#[derive(Clone, Debug)]
pub(crate) struct SourceSender {
    commands: mpsc::Sender<SourceCommand>,
    confirm: bool,
}

/// Owned by the request future. Forgetting that future cannot answer a source
/// validation or perform a copy, even while its original waiter remains present.
pub(crate) struct SourceRequests(mpsc::Receiver<SourceCommand>);

enum SourceCommand {
    Gather {
        ops: Vec<CopyOp>,
        reply: oneshot::Sender<Result<Vec<u8>>>,
    },
    Validate {
        reply: oneshot::Sender<Result<bool>>,
    },
}

impl SourceSender {
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.commands.is_closed()
    }

    #[cfg(test)]
    pub(crate) fn channel() -> (Self, SourceRequests) {
        Self::channel_with_confirmation(true)
    }

    pub(crate) fn channel_with_confirmation(confirm: bool) -> (Self, SourceRequests) {
        // Bound commands retained by a forgotten or temporarily unpolled call.
        let (sender, receiver) = mpsc::channel(1);
        (
            Self {
                commands: sender,
                confirm,
            },
            SourceRequests(receiver),
        )
    }

    pub(crate) async fn gather(
        &self,
        ops: &[CopyOp],
        deadline: Option<Instant>,
    ) -> Result<Vec<u8>> {
        self.request(deadline, |reply| SourceCommand::Gather {
            ops: ops.to_vec(),
            reply,
        })
        .await
    }

    pub(crate) async fn validate(
        &self,
        waiter: &Waiter,
        msgid: u64,
        deadline: Option<Instant>,
    ) -> Result<bool> {
        if !self.confirm {
            // The unsafe source capability supplies immutability while this
            // original request is pending. Recheck membership here: a cloned
            // sender must not validate after response delivery or cancellation.
            return Ok(waiter.contains_message_id(msgid));
        }
        self.request(deadline, |reply| SourceCommand::Validate { reply })
            .await
    }

    async fn request<T>(
        &self,
        deadline: Option<Instant>,
        command: impl FnOnce(oneshot::Sender<Result<T>>) -> SourceCommand,
    ) -> Result<T> {
        // This is the reverse request's budget, not the original source's
        // nominal expiry: successful validation remains a membership check.
        let deadline = deadline.unwrap_or_else(|| Instant::now() + Client::default().timeout);
        let (reply, result) = oneshot::channel();
        tokio::time::timeout_at(deadline.into(), async {
            self.commands
                .send(command(reply))
                .await
                .map_err(|_| source_gone())?;
            result.await.map_err(|_| source_gone())?
        })
        .await
        .map_err(|_| Error::kind(ErrorKind::Timeout))?
    }
}

impl SourceRequests {
    /// One synchronous read or validation under the original future's Rust
    /// borrow. The caller polls its RPC response first, so completed requests
    /// cannot service queued commands before returning their result.
    pub(crate) async fn serve_next(
        &mut self,
        source: &BorrowedSource<'_>,
        waiter: &Waiter,
        msgid: u64,
    ) -> bool {
        let Some(command) = self.0.recv().await else {
            return false;
        };
        let pending = waiter.contains_message_id(msgid);
        match command {
            SourceCommand::Gather { ops, reply } => {
                if !reply.is_closed() {
                    let result = if pending {
                        CopyBatch::new(&ops)
                            .and_then(|batch| source.gather(&batch))
                            .map_err(Into::into)
                    } else {
                        Err(source_gone())
                    };
                    let _ = reply.send(result);
                }
            }
            SourceCommand::Validate { reply } => {
                let _ = reply.send(Ok(pending));
            }
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

fn source_gone() -> Error {
    Error::new(
        ErrorKind::Timeout,
        "original request no longer serves its source".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Buffer, BufferPool};
    use std::time::Duration;

    fn buffers() -> Vec<Buffer> {
        let pool = BufferPool::new(Vec::new());
        let mut buffer = pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"data");
        vec![buffer]
    }

    #[tokio::test]
    async fn validation_requires_owner_progress_and_checks_membership_not_original_deadline() {
        let waiter = Waiter::default();
        let (msgid, response) = waiter.alloc_until(Instant::now() - Duration::from_secs(1));
        let (sender, mut requests) = SourceSender::channel();
        waiter.bind_read_source(msgid, sender.clone());
        let buffers = buffers();
        let source = BorrowedSource::new(&buffers).unwrap();
        let mut validation = Box::pin(sender.validate(
            &waiter,
            msgid,
            Some(Instant::now() + Duration::from_secs(1)),
        ));
        assert!(futures_util::poll!(validation.as_mut()).is_pending());
        assert!(waiter.contains_message_id(msgid));
        assert!(futures_util::poll!(validation.as_mut()).is_pending());
        assert!(requests.serve_next(&source, &waiter, msgid).await);
        assert!(validation.await.unwrap());
        drop(response);
    }

    #[tokio::test]
    async fn old_commands_cannot_validate_a_new_request() {
        let waiter = Waiter::default();
        let (old_id, old_response) = waiter.alloc(Duration::from_secs(1));
        let (sender, mut requests) = SourceSender::channel();
        waiter.bind_read_source(old_id, sender.clone());
        let buffers = buffers();
        let source = BorrowedSource::new(&buffers).unwrap();
        let mut validation = Box::pin(sender.validate(&waiter, old_id, None));
        assert!(futures_util::poll!(validation.as_mut()).is_pending());
        drop(old_response);
        let (new_id, new_response) = waiter.alloc(Duration::from_secs(1));
        let (new_sender, _new_requests) = SourceSender::channel();
        waiter.bind_read_source(new_id, new_sender);
        assert_ne!(old_id, new_id);
        assert!(requests.serve_next(&source, &waiter, old_id).await);
        assert!(!validation.await.unwrap());
        let ops = [CopyOp::new(0, 0, 4)];
        let mut gather = Box::pin(sender.gather(&ops, None));
        assert!(futures_util::poll!(gather.as_mut()).is_pending());
        assert!(requests.serve_next(&source, &waiter, old_id).await);
        assert_eq!(gather.await.unwrap_err().kind, ErrorKind::Timeout);
        assert!(waiter.contains_message_id(new_id));
        drop(new_response);
    }

    #[tokio::test]
    async fn reverse_deadline_bounds_both_a_full_mailbox_and_an_unanswered_command() {
        let (sender, _requests) = SourceSender::channel();
        let waiter = Waiter::default();
        let deadline = Some(Instant::now() + Duration::from_millis(10));
        let first = sender.validate(&waiter, 0, deadline);
        let second = sender.validate(&waiter, 0, deadline);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(second.unwrap_err().kind, ErrorKind::Timeout);
    }

    #[tokio::test]
    async fn caller_validation_needs_no_owner_poll_and_rechecks_revocation() {
        for completion in 0..3 {
            let waiter = Waiter::default();
            // Validation is a membership check, even after nominal expiry.
            let deadline = Instant::now() - Duration::from_secs(1);
            let (msgid, response) = waiter.alloc_until(deadline);
            let (sender, mut requests) = SourceSender::channel_with_confirmation(false);
            waiter.bind_read_source(msgid, sender.clone());
            let buffers = buffers();
            let _borrow = BorrowedSource::new(&buffers).unwrap();
            let mut validation = Box::pin(sender.validate(&waiter, msgid, None));
            assert!(matches!(
                futures_util::poll!(validation.as_mut()),
                std::task::Poll::Ready(Ok(true))
            ));
            assert!(requests.0.try_recv().is_err(), "no command was queued");
            match completion {
                0 => drop(response),
                1 => {
                    waiter.post(msgid, crate::Message::default());
                    drop(response);
                }
                _ => {
                    waiter.expire(Instant::now());
                    drop(response);
                }
            }
            let (next, _response) = waiter.alloc(Duration::from_secs(1));
            assert_ne!(next, msgid);
            assert!(!sender.validate(&waiter, msgid, None).await.unwrap());
        }
    }

    #[tokio::test]
    async fn caller_validation_does_not_bypass_the_source_borrow_for_cpu_copies() {
        let waiter = Waiter::default();
        let (msgid, response) = waiter.alloc(Duration::from_secs(1));
        let (sender, mut requests) = SourceSender::channel_with_confirmation(false);
        waiter.bind_read_source(msgid, sender.clone());
        let buffers = buffers();
        let source = BorrowedSource::new(&buffers).unwrap();
        let ops = [CopyOp::new(0, 0, 4)];
        let mut copy = Box::pin(sender.gather(&ops, None));
        assert!(futures_util::poll!(copy.as_mut()).is_pending());
        assert!(sender.validate(&waiter, msgid, None).await.unwrap());
        assert!(futures_util::poll!(copy.as_mut()).is_pending());
        requests.serve_next(&source, &waiter, msgid).await;
        assert_eq!(copy.await.unwrap(), b"data");

        let mut late_copy = Box::pin(sender.gather(&ops, None));
        assert!(futures_util::poll!(late_copy.as_mut()).is_pending());
        drop(response);
        requests.serve_next(&source, &waiter, msgid).await;
        assert_eq!(late_copy.await.unwrap_err().kind, ErrorKind::Timeout);
    }
}
