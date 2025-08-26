use std::{net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::{net::TcpStream, sync::Mutex};

use crate::{Error, ErrorKind, Message, MsgMeta, Receiver, Result, State, msg::SendMsg};

#[derive(Clone, Debug)]
pub enum HttpSocket {
    ForRequest(Arc<Connections>),
    ForResponse(u64),
}

impl HttpSocket {
    pub fn send<'a, P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &'a Arc<State>,
    ) -> Result<Receiver<'a>> {
        let mut bytes = BytesMut::new();
        let writer = SendMsg::writer(&mut bytes);
        let _ = serde_json::to_writer(writer, payload);

        match self {
            HttpSocket::ForRequest(connections) => {
                if meta.is_req() {
                    let (msgid, rx) = state.waiter.alloc();
                    meta.msgid = msgid;

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

                    Ok(rx)
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "invalid send response".to_string(),
                    ))
                }
            }
            HttpSocket::ForResponse(msgid) => {
                if meta.is_req() {
                    Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "invalid send request".to_string(),
                    ))
                } else {
                    let msg = Message {
                        meta: meta.clone(),
                        payload: bytes.into(),
                    };
                    state.waiter.post(*msgid, msg);
                    Ok(Receiver::None)
                }
            }
        }
    }

    async fn send_request(
        method: &str,
        bytes: Bytes,
        connections: &Arc<Connections>,
    ) -> Result<Bytes> {
        // 1. acquire connection.
        let mut sender = if let Some(sender) = connections.vec.lock().await.pop() {
            sender
        } else {
            // estabilish new connection.
            let stream = TcpStream::connect(connections.addr)
                .await
                .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;

            let (sender, conn) = hyper::client::conn::http1::handshake::<TokioIo<_>, Full<Bytes>>(
                TokioIo::new(stream),
            )
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;
            tokio::spawn(conn);

            sender
        };

        // 2. build request.
        let req = hyper::Request::builder()
            .uri(format!("http://{}/{}", connections.addr, method))
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

        // 5. restore connection.
        connections.vec.lock().await.push(sender);

        Ok(body_bytes)
    }
}

#[derive(Debug)]
pub struct Connections {
    pub addr: SocketAddr,
    pub vec: Mutex<Vec<SendRequest<Full<Bytes>>>>,
}
