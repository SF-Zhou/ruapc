use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    Receiver, State,
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

    pub async fn send<'a, P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &'a Arc<State>,
    ) -> Result<Receiver<'a>> {
        let receiver = if meta.is_req() {
            let (msgid, rx) = state.waiter.alloc();
            meta.msgid = msgid;
            rx
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
