use tokio::sync::oneshot;

use crate::{
    Error, ErrorKind, Result, WaiterCleaner,
    task::waiter::{WaiterResponse, WaiterResult},
};

/// Internal receiver for RPC response messages.
///
/// The receiver is used internally to wait for responses from remote services.
/// It wraps a oneshot channel and handles automatic cleanup of waiter entries.
pub(crate) enum Receiver<'a> {
    /// Active oneshot receiver with cleanup guard.
    OneShotRx(oneshot::Receiver<WaiterResult>, WaiterCleaner<'a>),
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
    /// A tuple of (Message, Option<Arc<WriteTarget>>). The target is
    /// present if the request attached write buffers; the server may have
    /// written into it during the request.
    pub(crate) async fn recv(self) -> Result<WaiterResponse> {
        match self {
            Receiver::OneShotRx(rx, cleaner) => {
                // A dropped sender means the waiter entry vanished without a
                // response — most commonly the coarse expiry sweep. An
                // explicit `Err` is an eager failure (e.g. the connection
                // carrying the request was closed).
                let result = match rx.await {
                    Ok(waiter_result) => waiter_result,
                    Err(_) => Err(Error::kind(ErrorKind::Timeout)),
                };
                std::mem::forget(cleaner);
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Message, Waiter};

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
