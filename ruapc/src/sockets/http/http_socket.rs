use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
use hyper::body::Frame;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    Error, ErrorKind, Message, MsgMeta, Result, State,
    msg::{SendMsg, frame::encode},
    sockets::{ChannelConnection, Outbound, PoolConnection, Segment},
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
    pub(crate) fn new(sender: mpsc::Sender<Outbound>) -> Self {
        Self {
            inner: Arc::new(ChannelConnection::new(sender, "HTTP")),
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

    /// The stream's send queue, which also carries data frames.
    pub(crate) fn channel(&self) -> &ChannelConnection {
        &self.inner
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
/// Each segment of a queued frame becomes one body chunk, so the buffers a
/// data frame shares with its sender reach the connection without a copy.
pub struct ChannelBody {
    rx: mpsc::Receiver<Outbound>,
    /// Segments of the frame being sent that have not been yielded yet.
    pending: VecDeque<Segment>,
}

impl ChannelBody {
    pub(crate) fn new(rx: mpsc::Receiver<Outbound>) -> Self {
        Self {
            rx,
            pending: VecDeque::new(),
        }
    }
}

impl hyper::body::Body for ChannelBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            if let Some(segment) = self.pending.pop_front() {
                return Poll::Ready(Some(Ok(Frame::data(segment.into_bytes()))));
            }
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(frame)) => self.pending.extend(frame.segments()),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl HttpSocket {
    pub(crate) async fn send<P: Serialize>(
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
                    state.waiter.post(*msgid, None, msg);
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
                    .send(meta, &state.waiter, |meta| {
                        encode(meta, payload).map_err(Into::into)
                    })
                    .await
            }
        }
    }
}
