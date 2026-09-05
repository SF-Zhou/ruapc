use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    SocketTrait, State,
    error::{ErrorKind, Result},
    msg::MsgMeta,
};

#[derive(Debug, Clone)]
pub struct TcpSocket {
    inner: Arc<crate::sockets::ChannelConnection>,
}

impl TcpSocket {
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self {
            inner: Arc::new(crate::sockets::ChannelConnection::new(stream)),
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

    pub(crate) fn health(&self) -> std::sync::Weak<crate::sockets::ChannelConnection> {
        Arc::downgrade(&self.inner)
    }
}

impl crate::sockets::PoolConnection for TcpSocket {
    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn same_connection(&self, other: &Self) -> bool {
        self.same_socket(other)
    }
}

impl SocketTrait for TcpSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
        let bytes = crate::msg::frame::encode(meta, payload)?;

        let sender = self.inner.prepare_send(meta, state, "TCP")?;
        sender
            .send(bytes)
            .await
            .map_err(|error| crate::Error::new(ErrorKind::TcpSendMsgFailed, error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{Context, MsgFlags, SocketPoolConfig};

    #[tokio::test]
    async fn closed_socket_rejects_request_after_binding_waiter() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let socket = TcpSocket::new(sender);
        assert!(socket.mark_closed());
        let (msgid, _waiter) = ctx.state.waiter.alloc(Duration::from_secs(1));
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq,
            ..Default::default()
        };

        let err = socket.send(&mut meta, &(), &ctx.state).await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::ConnectionClosed);
    }
}
