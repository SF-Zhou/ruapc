use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
use hyper::body::Frame;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_util::sync::WaitForCancellationFutureOwned;

use crate::{
    Error, ErrorKind, Message, MsgMeta, Result, SocketTrait, State,
    msg::{
        SendMsg,
        frame::{SendFrame, encode, encode_raw},
    },
    sockets::{ChannelConnection, ConnectionCloseGuard, ConnectionLifecycle, PoolConnection},
};

#[derive(Clone, Debug)]
pub enum HttpSocket {
    ForResponse(u64),
    Stream(StreamSocket),
}

/// Sender half of an HTTP/2 `/_rpc` bidirectional stream.
#[derive(Clone, Debug)]
pub struct StreamSocket {
    inner: Arc<ChannelConnection>,
}

impl StreamSocket {
    pub(crate) fn new(sender: mpsc::Sender<SendFrame>) -> Self {
        Self {
            inner: Arc::new(ChannelConnection::new(sender)),
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

    /// Marks the connection closed; returns `true` exactly once.
    pub(crate) fn mark_closed(&self) -> bool {
        self.inner.close_once()
    }

    pub(crate) async fn closed(&self) {
        self.inner.closed().await;
    }

    pub(crate) fn close_handle(&self) -> Arc<ConnectionLifecycle> {
        self.inner.close_handle()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl PoolConnection for HttpSocket {
    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn same_connection(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Stream(left), Self::Stream(right)) => left.same_socket(right),
            _ => false,
        }
    }
}

impl HttpSocket {
    pub(crate) async fn send_raw<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        control: &P,
        parts: &[Bytes],
        state: &Arc<State>,
    ) -> Result<()> {
        match self {
            Self::Stream(socket) => {
                socket
                    .inner
                    .send(meta, &state.waiter, "HTTP", |meta| {
                        encode_raw(meta, control, parts).map_err(Into::into)
                    })
                    .await
            }
            Self::ForResponse(_) => Err(Error::new(
                ErrorKind::InvalidArgument,
                "segmented payloads require a bidirectional stream".into(),
            )),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Self::ForResponse(_) => false,
            Self::Stream(socket) => socket.is_closed(),
        }
    }
}

/// A streaming body backed by an mpsc channel.
///
/// Implements `http_body::Body` so it can be used as both
/// request and response body for HTTP/2 bidirectional streaming.
pub struct ChannelBody {
    rx: Option<mpsc::Receiver<SendFrame>>,
    pending: std::vec::IntoIter<Bytes>,
    closed: Pin<Box<WaitForCancellationFutureOwned>>,
    _close_on_drop: ConnectionCloseGuard,
}

impl ChannelBody {
    pub(super) fn new(rx: mpsc::Receiver<SendFrame>, close: Arc<ConnectionLifecycle>) -> Self {
        Self {
            rx: Some(rx),
            pending: Vec::new().into_iter(),
            closed: Box::pin(close.closed_owned()),
            _close_on_drop: close.close_guard(),
        }
    }
}

impl hyper::body::Body for ChannelBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
        if self.rx.is_none() {
            return Poll::Ready(None);
        }
        // Use an error rather than a normal end-of-stream so Hyper resets this
        // stream and discards bytes already queued behind HTTP/2 flow control.
        if self.closed.as_mut().poll(cx).is_ready() {
            self.rx.take();
            self.pending = Vec::new().into_iter();
            return Poll::Ready(Some(Err(Error::new(
                ErrorKind::ConnectionClosed,
                "HTTP RPC stream closed before its body completed".into(),
            ))));
        }
        if let Some(data) = self.pending.next() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        let Some(rx) = self.rx.as_mut() else {
            return Poll::Ready(None);
        };
        match rx.poll_recv(cx) {
            Poll::Ready(Some(frame)) => {
                let (prefix, parts) = frame.into_parts();
                self.pending = parts.into_iter();
                Poll::Ready(Some(Ok(Frame::data(prefix))))
            }
            Poll::Ready(None) => {
                self.rx.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl SocketTrait for HttpSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
        match self {
            HttpSocket::ForResponse(msgid) => {
                let mut bytes = BytesMut::new();
                let writer = SendMsg::writer(&mut bytes);
                serde_json::to_writer(writer, payload)?;

                if meta.is_rsp() {
                    let msg = Message {
                        meta: meta.clone(),
                        payload: bytes.into(),
                    };
                    state.waiter.post(*msgid, msg);
                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!("invalid msg type {:?}", meta),
                    ))
                }
            }
            HttpSocket::Stream(stream_socket) => {
                stream_socket
                    .inner
                    .send(meta, &state.waiter, "HTTP", |meta| {
                        encode(meta, payload).map(Into::into).map_err(Into::into)
                    })
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::frame::encode_parts;
    use http_body_util::BodyExt;
    use std::time::Duration;

    #[tokio::test]
    async fn connection_close_wakes_pending_body_and_ends_it() {
        let (sender, receiver) = mpsc::channel(1);
        let socket = StreamSocket::new(sender);
        let mut body = ChannelBody::new(receiver, socket.close_handle());
        let reader = tokio::spawn(async move { body.frame().await });
        tokio::task::yield_now().await;
        assert!(socket.mark_closed());
        let error = tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert_eq!(
            error.msg,
            "HTTP RPC stream closed before its body completed"
        );
    }

    #[tokio::test]
    async fn connection_close_drops_pending_and_queued_body_owners() {
        struct Owner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let source = Arc::new(vec![7; 16]);
        let (sender, receiver) = mpsc::channel(2);
        for _ in 0..2 {
            sender
                .try_send(
                    encode_parts(
                        &MsgMeta::default(),
                        &[Bytes::from_owner(Owner(source.clone()))],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let socket = StreamSocket::new(sender);
        let mut body = ChannelBody::new(receiver, socket.close_handle());
        drop(body.frame().await.unwrap().unwrap()); // First frame's metadata.
        assert_eq!(Arc::strong_count(&source), 3);
        socket.mark_closed();
        let error = body.frame().await.unwrap().unwrap_err();
        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert_eq!(Arc::strong_count(&source), 1);
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn dropping_body_requests_close_without_claiming_teardown() {
        let (sender, receiver) = mpsc::channel(1);
        let socket = StreamSocket::new(sender);
        let body = ChannelBody::new(receiver, socket.close_handle());
        drop(body);
        assert!(socket.is_closed());
        socket.closed().await;
        // The receive task still owns close-once metrics, eviction and waiter
        // failure after the body's DropGuard has requested shutdown.
        assert!(socket.mark_closed());
        assert!(!socket.mark_closed());
    }

    #[tokio::test]
    async fn body_yields_owned_chunks_without_copying_or_interleaving_frames() {
        let parts = [Bytes::from_static(b"first"), Bytes::from_static(b"second")];
        let frame = encode_parts(&MsgMeta::default(), &parts).unwrap();
        let prefix = frame.chunks().next().unwrap().clone();
        let next = Bytes::from_static(b"next frame");
        let (sender, receiver) = mpsc::channel(2);
        sender.try_send(frame).unwrap();
        sender.try_send(next.clone().into()).unwrap();
        drop(sender);
        let close = Arc::new(ConnectionLifecycle::new());
        let mut body = ChannelBody::new(receiver, close.clone());
        for expected in [prefix, parts[0].clone(), parts[1].clone(), next] {
            let actual = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(actual.as_ptr(), expected.as_ptr());
            assert_eq!(actual, expected);
        }
        assert!(body.frame().await.is_none());
        close.request_close();
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn body_releases_emitted_chunks_and_drops_unsent_owners_on_cancellation() {
        struct Owner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let first = Arc::new(vec![1; 16]);
        let second = Arc::new(vec![2; 16]);
        let frame = encode_parts(
            &MsgMeta::default(),
            &[
                Bytes::from_owner(Owner(first.clone())),
                Bytes::from_owner(Owner(second.clone())),
            ],
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(1);
        sender.try_send(frame).unwrap();
        let mut body = ChannelBody::new(receiver, Arc::new(ConnectionLifecycle::new()));
        drop(body.frame().await.unwrap().unwrap()); // Metadata prefix.
        let emitted = body.frame().await.unwrap().unwrap();
        assert_eq!(Arc::strong_count(&first), 2);
        drop(emitted);
        assert_eq!(Arc::strong_count(&first), 1);
        assert_eq!(Arc::strong_count(&second), 2);
        drop(body);
        assert_eq!(Arc::strong_count(&second), 1);
        assert!(sender.is_closed());
    }
}
