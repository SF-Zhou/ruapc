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
    inner: Arc<TcpConnection>,
}

#[derive(Debug)]
pub(crate) struct TcpConnection {
    channel: crate::sockets::ChannelConnection,
    // Keep cancellation under the socket's Arc so per-request clones do not
    // clone and release a token, which takes its internal mutex.
    closed: tokio_util::sync::CancellationToken,
}

impl TcpConnection {
    pub(crate) fn is_closed(&self) -> bool {
        self.channel.is_closed()
    }
}

impl TcpSocket {
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self {
            inner: Arc::new(TcpConnection {
                channel: crate::sockets::ChannelConnection::new(stream),
                closed: tokio_util::sync::CancellationToken::new(),
            }),
        }
    }

    pub(crate) async fn closed(&self) {
        self.inner.closed.cancelled().await;
    }

    /// Unique id of the underlying connection.
    pub(crate) fn conn_id(&self) -> u64 {
        self.inner.channel.conn_id()
    }

    /// Whether `other` refers to the same underlying connection.
    pub(crate) fn same_socket(&self, other: &Self) -> bool {
        self.conn_id() == other.conn_id()
    }

    /// Marks the connection closed; returns `true` exactly once (the send
    /// and recv loops both report failures — teardown must run once).
    pub(crate) fn mark_closed(&self) -> bool {
        let first = self.inner.channel.close_once();
        self.inner.closed.cancel();
        first
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub(crate) fn health(&self) -> std::sync::Weak<TcpConnection> {
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

        let sender = self.inner.channel.prepare_send(meta, state, "TCP")?;
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

    #[test]
    fn health_observer_does_not_keep_connection_alive() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let socket = crate::Socket::TCP(TcpSocket::new(sender));
        let health = socket.health().unwrap();
        let cloned = socket.clone();
        assert!(health.same_scope(&cloned.health().unwrap()));
        assert!(health.is_connected());

        drop(socket);
        assert!(health.is_connected());
        drop(cloned);
        assert!(!health.is_connected());
        assert!(receiver.is_closed());
    }

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
