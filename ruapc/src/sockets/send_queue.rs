//! Deadline-aware admission before a transport accepts a message.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use tokio::sync::mpsc;

use crate::{Error, ErrorKind, MsgMeta, Result, Waiter};

/// The local waiter and absolute budget of an outgoing request. Responses
/// carry the peer's message ID and must never consult our waiter table.
pub(crate) struct SendRequest<'a> {
    waiter: &'a Waiter,
    conn_id: u64,
    request: Option<(u64, Instant)>,
}

impl<'a> SendRequest<'a> {
    pub(crate) fn bind(meta: &mut MsgMeta, waiter: &'a Waiter, conn_id: u64) -> Result<Self> {
        let request = if meta.is_req() {
            // Bind before checking closure so teardown cannot miss the waiter.
            let deadline = waiter
                .bind_connection_deadline(meta.msgid, conn_id)
                .ok_or_else(|| Error::kind(ErrorKind::Timeout))?;
            // The binding lookup just established liveness. Prepare the wire
            // budget here instead of looking up the same entry again.
            meta.timeout_ms = Self::wire_timeout_ms(Self::remaining_until(deadline)?);
            Some((meta.msgid, deadline))
        } else {
            None
        };
        Ok(Self {
            waiter,
            conn_id,
            request,
        })
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.request.map(|(_, deadline)| deadline)
    }

    /// Check the clock even when the periodic waiter sweep has not run yet.
    pub(crate) fn check(&self) -> Result<()> {
        self.remaining().map(|_| ())
    }

    /// Account for queue admission time before encoding the wire metadata.
    pub(crate) fn refresh(&self, meta: &mut MsgMeta) -> Result<()> {
        if let Some(remaining) = self.remaining()? {
            meta.timeout_ms = Self::wire_timeout_ms(remaining);
        }
        Ok(())
    }

    fn remaining(&self) -> Result<Option<Duration>> {
        let Some((msgid, deadline)) = self.request else {
            return Ok(None);
        };
        if self.waiter.deadline_on_connection(msgid, self.conn_id) != Some(deadline) {
            return Err(ErrorKind::Timeout.into());
        }
        Self::remaining_until(deadline).map(Some)
    }

    fn remaining_until(deadline: Instant) -> Result<Duration> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ErrorKind::Timeout.into());
        }
        Ok(remaining)
    }

    fn wire_timeout_ms(remaining: Duration) -> u32 {
        u32::try_from(remaining.as_nanos().div_ceil(1_000_000)).unwrap_or(u32::MAX)
    }
}

/// Reserve capacity without accepting the message. Only a full queue creates
/// a timer; callers recheck connection/request state before synchronous commit.
pub(crate) async fn reserve_send<T, F: Future<Output = ()>>(
    sender: &mpsc::Sender<T>,
    deadline: Option<Instant>,
    closed: impl FnOnce() -> F,
) -> Result<mpsc::Permit<'_, T>> {
    if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
        return Err(ErrorKind::Timeout.into());
    }
    match sender.try_reserve() {
        Ok(permit) => Ok(permit),
        Err(mpsc::error::TrySendError::Closed(())) => Err(ErrorKind::ConnectionClosed.into()),
        Err(mpsc::error::TrySendError::Full(())) => {
            // Keep timer and notification state out of every send's future.
            // This concrete future uses static dispatch and allocates only
            // when the bounded queue is full.
            Box::pin(wait_for_slot(sender, deadline, closed())).await
        }
    }
}

#[cold]
async fn wait_for_slot<T>(
    sender: &mpsc::Sender<T>,
    deadline: Option<Instant>,
    closed: impl Future<Output = ()>,
) -> Result<mpsc::Permit<'_, T>> {
    tokio::select! {
        biased;
        () = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                None => std::future::pending().await,
            }
        } => Err(ErrorKind::Timeout.into()),
        () = closed => Err(ErrorKind::ConnectionClosed.into()),
        permit = sender.reserve() => permit.map_err(|_| ErrorKind::ConnectionClosed.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::{future::pending, time::Duration};

    use super::*;

    #[tokio::test]
    async fn expired_deadline_rejects_an_available_slot() {
        let (sender, mut receiver) = mpsc::channel::<()>(1);
        let error = reserve_send(&sender, Some(Instant::now()), pending)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(sender.capacity(), 1);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_full_queue_times_out_without_consuming_capacity() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(1).await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(20);
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            reserve_send(&sender, Some(deadline), pending),
        )
        .await
        .expect("queue admission must observe its deadline")
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(receiver.recv().await, Some(1));
        assert!(receiver.try_recv().is_err());
        assert_eq!(sender.capacity(), 1);
    }

    #[tokio::test]
    async fn expiry_is_terminal_when_closure_and_capacity_are_also_ready() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.try_send(()).unwrap();
        let closed = tokio_util::sync::CancellationToken::new();
        let deadline = Instant::now() + Duration::from_millis(20);
        let reservation = reserve_send(&sender, Some(deadline), || closed.cancelled());
        tokio::pin!(reservation);
        assert!(futures_util::poll!(reservation.as_mut()).is_pending());

        tokio::time::sleep_until((deadline + Duration::from_millis(1)).into()).await;
        closed.cancel();
        receiver.try_recv().unwrap();
        assert_eq!(reservation.await.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(sender.capacity(), 1);
    }
}
