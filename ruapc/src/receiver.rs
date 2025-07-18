use tokio::sync::oneshot;

use crate::{Error, ErrorKind, RecvMsg, Result};

#[derive(Debug)]
pub enum Receiver {
    None,
    OneShotRx(oneshot::Receiver<RecvMsg>),
}

impl Receiver {
    /// # Errors
    pub async fn recv(self) -> Result<RecvMsg> {
        match self {
            Receiver::None => Err(Error::kind(ErrorKind::InvalidArgument)),
            Receiver::OneShotRx(rx) => rx
                .await
                .map_err(|e| Error::new(ErrorKind::TcpRecvMsgFailed, e.to_string())),
        }
    }
}
