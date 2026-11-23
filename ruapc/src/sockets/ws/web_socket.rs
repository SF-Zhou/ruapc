use std::sync::Arc;

use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    State,
    error::Result,
    msg::{MsgMeta, frame::encode},
    sockets::{ChannelConnection, Outbound, PoolConnection},
};

#[derive(Debug, Clone)]
pub struct WebSocket {
    inner: Arc<ChannelConnection>,
}

impl WebSocket {
    pub(crate) fn new(stream: mpsc::Sender<Outbound>) -> Self {
        Self {
            inner: Arc::new(ChannelConnection::new(stream, "WebSocket")),
        }
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

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Completes once teardown marked this connection closed.
    pub(crate) async fn closed(&self) {
        self.inner.closed().await;
    }

    /// The connection's send queue, which also carries data frames.
    pub(crate) fn channel(&self) -> &ChannelConnection {
        &self.inner
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

impl WebSocket {
    pub(crate) async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
        self.inner
            .send(meta, &state.waiter, |meta| {
                encode(meta, payload).map_err(Into::into)
            })
            .await
    }
}
