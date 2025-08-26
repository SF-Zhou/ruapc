use tokio::sync::oneshot;

use crate::{Error, ErrorKind, Message, Result, WaiterCleaner};

pub enum Receiver<'a> {
    None,
    OneShotRx(oneshot::Receiver<Message>, WaiterCleaner<'a>),
}

impl Receiver<'_> {
    /// # Errors
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
