use std::any::Any;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::{BoxFuture, Error, ErrorKind, MsgMeta, Result, RuapcSocket, SocketType};

/// WebSocket for sending messages.
///
/// This socket implementation sends messages through an MPSC channel,
/// which is connected to a background task that handles the actual WebSocket I/O.
#[derive(Debug, Clone)]
pub struct WebSocket {
    stream: mpsc::Sender<Bytes>,
}

impl WebSocket {
    /// Creates a new WebSocket with the given channel sender.
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self { stream }
    }

    /// Internal method to serialize and send a message.
    pub async fn send_internal(&self, meta: MsgMeta, payload: Bytes) -> Result<()> {
        let mut bytes = BytesMut::with_capacity(512);
        meta.serialize_to_bytes(payload, &mut bytes)?;

        self.stream
            .send(bytes.into())
            .await
            .map_err(|e| Error::new(ErrorKind::WebSocketSendFailed, e.to_string()))?;

        Ok(())
    }
}

impl RuapcSocket for WebSocket {
    fn send_bytes(&self, meta: MsgMeta, payload: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.send_internal(meta, payload))
    }

    fn socket_type(&self) -> SocketType {
        SocketType::WS
    }

    fn clone_boxed(&self) -> Box<dyn RuapcSocket> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
