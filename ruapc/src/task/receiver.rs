use tokio::sync::oneshot;

use crate::{Error, ErrorKind, Result, WaiterCleaner, task::waiter::WaiterResponse};

/// Internal receiver for RPC response messages.
///
/// The receiver is used internally to wait for responses from remote services.
/// It wraps a oneshot channel and handles automatic cleanup of waiter entries.
pub enum Receiver<'a> {
    /// No receiver (placeholder state).
    None,
    /// Active oneshot receiver with cleanup guard.
    OneShotRx(oneshot::Receiver<WaiterResponse>, WaiterCleaner<'a>),
}

impl Receiver<'_> {
    /// Receives a response message along with any write buffer.
    ///
    /// This method waits for a response to arrive through the channel.
    /// If received successfully, the cleanup guard is forgotten (the entry
    /// was already removed by `Waiter::post`).
    ///
    /// # Returns
    ///
    /// A tuple of (Message, Option<Buffer>). The buffer is present if the
    /// server stored one via `store_write_buffer` during the request.
    pub async fn recv(self) -> Result<WaiterResponse> {
        match self {
            Receiver::None => Err(Error::kind(ErrorKind::InvalidArgument)),
            Receiver::OneShotRx(rx, cleaner) => {
                // A dropped sender means the waiter entry vanished without a
                // response — most commonly the coarse expiry sweep.
                let result = rx.await.map_err(|_| Error::kind(ErrorKind::Timeout));
                std::mem::forget(cleaner);
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, Waiter};

    #[tokio::test]
    async fn test_receiver_none_returns_error() {
        let result = Receiver::None.recv().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
    }

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
}
