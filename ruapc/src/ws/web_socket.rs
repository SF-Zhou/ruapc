use bytes::{Bytes, BytesMut};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    MsgFlags, Receiver, Waiter,
    error::{Error, ErrorKind, Result},
    msg::MsgMeta,
};

#[derive(Debug, Clone)]
pub struct WebSocket {
    stream: mpsc::Sender<Bytes>,
}

impl WebSocket {
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self { stream }
    }

    pub async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        waiter: &Waiter,
    ) -> Result<Receiver> {
        let receiver = if meta.flags.contains(MsgFlags::IsReq) {
            let (msgid, rx) = waiter.alloc();
            meta.msgid = msgid;
            Receiver::OneShotRx(rx)
        } else {
            Receiver::None
        };

        let mut bytes = BytesMut::with_capacity(512);
        meta.serialize_to(payload, &mut bytes)?;

        self.stream
            .send(bytes.into())
            .await
            .map_err(|e| Error::new(ErrorKind::WebSocketSendFailed, e.to_string()))?;

        Ok(receiver)
    }
}
