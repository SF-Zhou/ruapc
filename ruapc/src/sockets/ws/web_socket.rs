use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    SocketTrait, State,
    error::Result,
    msg::{
        MsgMeta,
        frame::{SendFrame, encode, encode_raw},
    },
    sockets::{ChannelConnection, PoolConnection},
};

#[derive(Debug, Clone)]
pub struct WebSocket {
    inner: Arc<ChannelConnection>,
}

impl WebSocket {
    pub fn new(stream: mpsc::Sender<SendFrame>) -> Self {
        Self {
            inner: Arc::new(ChannelConnection::new(stream)),
        }
    }

    pub(crate) async fn send_raw<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        control: &P,
        parts: &[Bytes],
        state: &Arc<State>,
    ) -> Result<()> {
        self.inner
            .send(meta, &state.waiter, "WebSocket", |meta| {
                encode_raw(meta, control, parts).map_err(Into::into)
            })
            .await
    }

    /// Unique id of the underlying connection.
    pub(crate) fn conn_id(&self) -> u64 {
        self.inner.conn_id()
    }

    /// Whether `other` refers to the same underlying connection.
    pub(crate) fn same_socket(&self, other: &Self) -> bool {
        self.conn_id() == other.conn_id()
    }

    /// Marks the connection closed; returns `true` exactly once (the send
    /// and recv loops both report failures — teardown must run once).
    pub(crate) fn mark_closed(&self) -> bool {
        self.inner.close_once()
    }

    pub(crate) async fn closed(&self) {
        self.inner.closed().await;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl PoolConnection for WebSocket {
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
        self.inner
            .send(meta, &state.waiter, "WebSocket", |meta| {
                encode(meta, payload).map(Into::into).map_err(Into::into)
            })
            .await
    }
}
