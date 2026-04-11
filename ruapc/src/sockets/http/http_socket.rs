use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper::body::Frame;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Serialize;
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc},
};

use crate::{Error, ErrorKind, Message, MsgMeta, Result, SocketTrait, State, msg::SendMsg};

#[derive(Clone, Debug)]
pub enum HttpSocket {
    ForRequest(Arc<Connections>),
    ForResponse(u64),
    Stream(mpsc::Sender<Bytes>),
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

impl HttpSocket {
    async fn send_request(
        method: &str,
        bytes: Bytes,
        connections: &Arc<Connections>,
    ) -> Result<Bytes> {
        // 1. acquire or establish HTTP/2 connection.
        let mut sender = {
            let mut guard = connections.sender.lock().await;
            if let Some(sender) = guard.as_ref() {
                sender.clone()
            } else {
                let stream = TcpStream::connect(connections.addr)
                    .await
                    .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;

                let (sender, conn) = hyper::client::conn::http2::handshake(
                    TokioExecutor::new(),
                    TokioIo::new(stream),
                )
                .await
                .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;
                tokio::spawn(conn);

                *guard = Some(sender.clone());
                sender
            }
        };

        // 2. build request.
        let req = hyper::Request::builder()
            .uri(format!("http://{}/{}", connections.addr, method))
            .header("Content-Type", "application/json")
            .method(hyper::Method::POST)
            .body(Full::new(bytes))
            .map_err(|e| Error::new(ErrorKind::HttpBuildReqFailed, e.to_string()))?;

        // 3. send request.
        let rsp = sender
            .send_request(req)
            .await
            .map_err(|e| Error::new(ErrorKind::HttpSendReqFailed, e.to_string()))?;

        // 4. collect body bytes.
        let body_bytes = rsp
            .into_body()
            .collect()
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?
            .to_bytes();

        Ok(body_bytes)
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
            HttpSocket::ForRequest(connections) => {
                let mut bytes = BytesMut::new();
                let writer = SendMsg::writer(&mut bytes);
                let _ = serde_json::to_writer(writer, payload);

                if meta.is_req() {
                    let msgid = meta.msgid;
                    let method = meta.method.clone();
                    let bytes = bytes.freeze();
                    let waiter = state.waiter.clone();
                    let connections = connections.clone();
                    tokio::spawn(async move {
                        let results = Self::send_request(&method, bytes, &connections).await;
                        let bytes = match results {
                            Ok(bytes) => bytes,
                            Err(err) => serde_json::to_vec(&Result::<()>::Err(err))
                                .unwrap_or_default()
                                .into(),
                        };
                        let msg = Message::new(MsgMeta::default(), bytes.into());
                        waiter.post(msgid, msg);
                    });

                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!("invalid msg type {:?}", meta),
                    ))
                }
            }
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
            HttpSocket::Stream(sender) => {
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

                sender
                    .send(bytes.0.into())
                    .await
                    .map_err(|e| Error::new(ErrorKind::HttpSendReqFailed, e.to_string()))?;

                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct Connections {
    pub addr: SocketAddr,
    pub sender: Mutex<Option<SendRequest<Full<Bytes>>>>,
}
