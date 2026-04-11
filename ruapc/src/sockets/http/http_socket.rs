use std::{net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Serialize;
use tokio::{net::TcpStream, sync::Mutex};

use crate::{Error, ErrorKind, Message, MsgMeta, Result, SocketTrait, State, msg::SendMsg};

#[derive(Clone, Debug)]
pub enum HttpSocket {
    ForRequest(Arc<Connections>),
    ForResponse(u64),
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
        let mut bytes = BytesMut::new();
        let writer = SendMsg::writer(&mut bytes);
        let _ = serde_json::to_writer(writer, payload);

        match self {
            HttpSocket::ForRequest(connections) => {
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
        }
    }
}

#[derive(Debug)]
pub struct Connections {
    pub addr: SocketAddr,
    pub sender: Mutex<Option<SendRequest<Full<Bytes>>>>,
}
