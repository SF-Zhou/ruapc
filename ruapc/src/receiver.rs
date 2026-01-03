use tokio::sync::oneshot;

use crate::{Error, ErrorKind, Message, Result, WaiterCleaner};

/// Internal receiver for RPC response messages.
///
/// The receiver is used internally to wait for responses from remote services.
/// It wraps a oneshot channel and handles automatic cleanup of waiter entries.
pub(crate) enum Receiver<'a> {
    /// No receiver (placeholder state).
    None,
    /// Active oneshot receiver with cleanup guard.
    OneShotRx(oneshot::Receiver<Message>, WaiterCleaner<'a>),
}

impl Receiver<'_> {
    /// Receives a response message.
    ///
    /// This method waits for a response message to arrive through the channel.
    /// If the message is successfully received, the cleanup guard is forgotten
    /// to keep the waiter entry until the message is processed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The receiver is in the `None` state
    /// - The channel is closed before receiving a message
    pub async fn recv(self) -> Result<Message> {
        match self {
            Receiver::None => Err(Error::kind(ErrorKind::InvalidArgument)),
            Receiver::OneShotRx(rx, cleaner) => {
                let result = rx
                    .await
                    .map_err(|e| Error::new(ErrorKind::TcpRecvMsgFailed, e.to_string()));
                std::mem::forget(cleaner); // do not call cleaner's Drop if we successfully received the message.
                result
            }
        }
    }
}
