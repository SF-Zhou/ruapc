use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    SocketTrait, State,
    error::{Error, ErrorKind, Result},
    msg::MsgMeta,
};

#[derive(Debug, Clone)]
pub struct WebSocket {
    stream: mpsc::Sender<Bytes>,
    conn_id: u64,
}

impl WebSocket {
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self {
            stream,
            conn_id: crate::task::next_conn_id(),
        }
    }

    /// Unique id of the underlying connection.
    pub(crate) fn conn_id(&self) -> u64 {
        self.conn_id
    }

    /// Whether `other` refers to the same underlying connection.
    pub(crate) fn same_socket(&self, other: &Self) -> bool {
        self.conn_id == other.conn_id
    }
}

impl SocketTrait for WebSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
        let mut bytes = BytesMut::with_capacity(512);
        meta.serialize_to(payload, &mut bytes)?;

        // Bind the pending request to this connection so it fails eagerly
        // if the connection dies before the response arrives.
        if meta.is_req() {
            state.waiter.bind_connection(meta.msgid, self.conn_id);
        }

        self.stream
            .send(bytes.into())
            .await
            .map_err(|e| Error::new(ErrorKind::WebSocketSendFailed, e.to_string()))?;

        Ok(())
    }
}
