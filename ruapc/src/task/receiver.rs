use tokio::sync::oneshot;

use crate::{
    Error, ErrorKind, Result, WaiterCleaner,
    task::waiter::{WaiterResponse, WaiterResult},
};

/// Internal receiver for RPC response messages.
///
/// The receiver is used internally to wait for responses from remote services.
/// It wraps a oneshot channel and handles automatic cleanup of waiter entries.
pub(crate) struct Receiver<'a> {
    response: oneshot::Receiver<WaiterResult>,
    cleanup: WaiterCleaner<'a>,
}

impl<'a> Receiver<'a> {
    pub(crate) fn new(
        response: oneshot::Receiver<WaiterResult>,
        cleanup: WaiterCleaner<'a>,
    ) -> Self {
        Self { response, cleanup }
    }

    /// Receives a response message along with any write buffer.
    ///
    /// This method waits for a response to arrive through the channel.
    /// Once notified, the cleanup guard releases its waiter owner without
    /// removing the entry again; the posting or failure path already removed it.
    ///
    /// # Returns
    ///
    /// A tuple of (Message, Option<Arc<WriteTarget>>). The target is
    /// present if the request attached write buffers; the server may have
    /// written into it during the request.
    pub(crate) async fn recv(self) -> Result<WaiterResponse> {
        let Self { response, cleanup } = self;
        // The entry has already been removed by post, eager failure or the
        // expiry sweep. Cancellation before this await resolves instead
        // drops `cleanup` and removes the still-pending entry.
        let result = response
            .await
            .unwrap_or_else(|_| Err(Error::kind(ErrorKind::Timeout)));
        cleanup.disarm();
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::{Message, Waiter};

    #[cfg(feature = "rdma")]
    use crate::{Error, ErrorKind};
    #[cfg(feature = "rdma")]
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    #[tokio::test]
    async fn test_receiver_receives_message() {
        let waiter = std::sync::Arc::new(Waiter::default());
        let (msgid, rx) = waiter.alloc(std::time::Duration::from_secs(30));

        let w = waiter.clone();
        tokio::spawn(async move {
            let mut msg = Message::default();
            msg.meta.method = "ping".into();
            w.post(msgid, msg);
        });

        let (msg, _write_buf) = rx.recv().await.unwrap();
        assert_eq!(msg.meta.method, "ping");
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn owned_receiver_can_move_to_a_task_and_releases_its_owner_on_success() {
        let waiter = Arc::new(Waiter::default());
        let (msgid, receiver) = waiter.alloc_owned_until(Instant::now() + Duration::from_secs(30));
        assert_eq!(Arc::strong_count(&waiter), 2);
        let task = tokio::spawn(receiver.recv());
        let message = Message::default();
        waiter.post(msgid, message);
        task.await.unwrap().unwrap();
        assert_eq!(waiter.pending_count(), 0);
        assert_eq!(Arc::strong_count(&waiter), 1);
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn owned_receiver_failure_and_expiry_release_the_waiter_owner() {
        for expires in [false, true] {
            let waiter = Arc::new(Waiter::default());
            let deadline = Instant::now() + Duration::from_secs(30);
            let (msgid, receiver) = waiter.alloc_owned_until(deadline);
            assert_eq!(Arc::strong_count(&waiter), 2);
            waiter.bind_connection(msgid, 1);
            let expected = if expires {
                waiter.expire(deadline);
                ErrorKind::Timeout
            } else {
                waiter.fail_connection(1, &Error::kind(ErrorKind::ConnectionClosed));
                ErrorKind::ConnectionClosed
            };
            assert_eq!(receiver.recv().await.unwrap_err().kind, expected);
            assert_eq!(waiter.pending_count(), 0);
            assert_eq!(Arc::strong_count(&waiter), 1);
        }
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn dropping_owned_receivers_before_or_during_poll_cleans_the_entry_and_owner() {
        let waiter = Arc::new(Waiter::default());
        let (msgid, receiver) = waiter.alloc_owned_until(Instant::now() + Duration::from_secs(30));
        assert!(waiter.contains_message_id(msgid));
        assert_eq!(Arc::strong_count(&waiter), 2);
        drop(receiver);
        assert_eq!(waiter.pending_count(), 0);
        assert_eq!(Arc::strong_count(&waiter), 1);

        let (msgid, receiver) = waiter.alloc_owned_until(Instant::now() + Duration::from_secs(30));
        let mut receiving = Box::pin(receiver.recv());
        assert!(futures_util::poll!(receiving.as_mut()).is_pending());
        assert!(waiter.contains_message_id(msgid));
        assert_eq!(Arc::strong_count(&waiter), 2);
        drop(receiving);
        assert_eq!(waiter.pending_count(), 0);
        assert_eq!(Arc::strong_count(&waiter), 1);
    }
}
