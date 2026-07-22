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
    sender: mpsc::Sender<Bytes>,
    conn_id: u64,
    closed: Arc<std::sync::atomic::AtomicBool>,
}

impl StreamSocket {
    pub(crate) fn new(sender: mpsc::Sender<Bytes>) -> Self {
        Self {
            sender,
            conn_id: crate::task::next_conn_id(),
            closed: Arc::default(),
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

    /// Marks the connection closed; returns `true` exactly once.
    pub(crate) fn mark_closed(&self) -> bool {
        !self.closed.swap(true, std::sync::atomic::Ordering::SeqCst)
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
                let _ = serde_json::to_writer(writer, payload);

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
                // Use TCP-style framing: magic + len + meta_len + meta + payload.
                use crate::sockets::tcp::MAGIC_NUM;

                struct StreamBytes(BytesMut);

                impl SendMsg for StreamBytes {
                    fn size(&self) -> usize {
                        self.0.size()
                    }

                    fn prepare(&mut self) -> Result<()> {
                        self.0.extend_from_slice(&MAGIC_NUM.to_be_bytes());
                        self.0.extend_from_slice(&0u32.to_be_bytes());
                        self.0.prepare()
                    }

                    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
                        const S: usize = std::mem::size_of::<u32>();
                        if meta_offset < S {
                            return Err(Error::new(
                                ErrorKind::SerializeFailed,
                                format!("invalid meta offset: {meta_offset}"),
                            ));
                        }
                        self.0.finish(meta_offset, payload_offset)?;
                        let total_len = u32::try_from(self.size() - meta_offset)?;
                        self.0[meta_offset - S..meta_offset]
                            .copy_from_slice(&total_len.to_be_bytes());
                        Ok(())
                    }

                    fn writer(&mut self) -> impl std::io::Write {
                        self.0.writer()
                    }
                }

                let mut bytes = StreamBytes(BytesMut::with_capacity(512));
                meta.serialize_to(payload, &mut bytes)?;

                // Bind the pending request to this connection so it fails
                // eagerly if the connection dies before the response arrives.
                if meta.is_req() {
                    state
                        .waiter
                        .bind_connection(meta.msgid, stream_socket.conn_id);
                }

                stream_socket
                    .sender
                    .send(bytes.0.into())
                    .await
                    .map_err(|e| Error::new(ErrorKind::HttpSendReqFailed, e.to_string()))?;

                Ok(())
            }
        }
    }
}
