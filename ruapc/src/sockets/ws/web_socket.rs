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
    inner: Arc<WebSocketInner>,
}

#[derive(Debug)]
pub(crate) struct WebSocketInner {
    stream: mpsc::Sender<Bytes>,
    lifecycle: crate::sockets::ConnectionLifecycle,
}

impl WebSocketInner {
    pub(crate) fn is_closed(&self) -> bool {
        self.lifecycle.is_closed() || self.stream.is_closed()
    }
}

impl WebSocket {
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self {
            inner: Arc::new(WebSocketInner {
                stream,
                lifecycle: crate::sockets::ConnectionLifecycle::new(),
            }),
        }
    }

    /// Unique id of the underlying connection.
    pub(crate) fn conn_id(&self) -> u64 {
        self.inner.lifecycle.conn_id()
    }

    /// Whether `other` refers to the same underlying connection.
    pub(crate) fn same_socket(&self, other: &Self) -> bool {
        self.conn_id() == other.conn_id()
    }

    /// Marks the connection closed; returns `true` exactly once (the send
    /// and recv loops both report failures — teardown must run once).
    pub(crate) fn mark_closed(&self) -> bool {
        self.inner.lifecycle.close_once()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub(crate) fn health(&self) -> std::sync::Weak<WebSocketInner> {
        Arc::downgrade(&self.inner)
    }
}

impl crate::sockets::PoolConnection for WebSocket {
    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn same_connection(&self, other: &Self) -> bool {
        self.same_socket(other)
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
            state.waiter.bind_connection(meta.msgid, self.conn_id());
        }

        if self.is_closed() {
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                "WebSocket connection is closed".into(),
            ));
        }

        self.inner
            .stream
            .send(bytes.into())
            .await
            .map_err(|e| Error::new(ErrorKind::WebSocketSendFailed, e.to_string()))?;

        Ok(())
    }
}
