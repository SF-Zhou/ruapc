use std::{
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
use hyper::body::Frame;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{Error, ErrorKind, Message, MsgMeta, Result, SocketTrait, State, msg::SendMsg};

#[derive(Clone, Debug)]
pub enum HttpSocket {
    ForResponse(u64),
    Stream(StreamSocket),
}

/// Sender half of an HTTP/2 `/_rpc` bidirectional stream.
#[derive(Clone, Debug)]
pub struct StreamSocket {
    inner: Arc<crate::sockets::ChannelConnection>,
}

impl StreamSocket {
    pub(crate) fn new(sender: mpsc::Sender<Bytes>) -> Self {
        Self {
            inner: Arc::new(crate::sockets::ChannelConnection::new(sender)),
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

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl crate::sockets::PoolConnection for HttpSocket {
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
    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Self::ForResponse(_) => false,
            Self::Stream(socket) => socket.is_closed(),
        }
    }

    pub(crate) fn health(&self) -> Option<std::sync::Weak<crate::sockets::ChannelConnection>> {
        match self {
            Self::ForResponse(_) => None,
            Self::Stream(socket) => Some(Arc::downgrade(&socket.inner)),
        }
    }
}

/// A streaming body backed by an mpsc channel.
///
/// Implements `http_body::Body` so it can be used as both
/// request and response body for HTTP/2 bidirectional streaming.
pub struct ChannelBody {
    rx: mpsc::Receiver<Bytes>,
}

impl ChannelBody {
    pub fn new(rx: mpsc::Receiver<Bytes>) -> Self {
        Self { rx }
    }
}

impl hyper::body::Body for ChannelBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(data)) => Poll::Ready(Some(Ok(Frame::data(data)))),
            Poll::Ready(None) => Poll::Ready(None),
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
                let bytes = crate::msg::frame::encode(meta, payload)?;

                let sender = stream_socket.inner.prepare_send(meta, state, "HTTP")?;
                sender.send(bytes).await.map_err(|error| {
                    crate::Error::new(ErrorKind::HttpSendReqFailed, error.to_string())
                })
            }
        }
    }
}
