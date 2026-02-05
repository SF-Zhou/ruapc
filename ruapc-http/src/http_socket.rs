use std::{any::Any, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::{net::TcpStream, sync::Mutex};

use crate::{BoxFuture, Error, ErrorKind, MsgMeta, Result, RuapcSocket, SocketType};

/// HTTP socket for sending messages.
///
/// This socket has two modes:
/// - `ForRequest`: Used by clients to send requests
/// - `ForResponse`: Used by servers to send responses
#[derive(Clone, Debug)]
pub enum HttpSocket {
    /// Socket for making HTTP requests (client mode).
    ForRequest(Arc<Connections>),
    /// Socket for sending HTTP responses (server mode).
    ForResponse(u64),
}

/// HTTP client connection pool.
#[derive(Debug)]
pub struct Connections {
    /// Target server address.
    pub addr: SocketAddr,
    /// Pool of reusable HTTP connections.
    pub vec: Mutex<Vec<SendRequest<Full<Bytes>>>>,
}

impl HttpSocket {
    /// Sends a message using this HTTP socket.
    ///
    /// For requests: sends an HTTP POST to the server and awaits the response.
    /// Note: The response bytes are received but discarded here because the
    /// actual response handling is done through the waiter mechanism at the
    /// higher level (ruapc crate). The response is delivered via the waiter
    /// channel, not as a return value from this method.
    ///
    /// For responses: This is a server-side socket used to mark responses.
    /// The actual response sending is handled by the HttpSocketPool's request
    /// handler which posts to the waiter channel. This variant exists to allow
    /// the socket to be passed through the RPC framework's message handling.
    pub async fn send_internal(&self, meta: MsgMeta, payload: Bytes) -> Result<()> {
        match self {
            HttpSocket::ForRequest(connections) => {
                if meta.is_req() {
                    let method = meta.method.clone();
                    // Send the request and await the response.
                    // The response bytes are intentionally discarded here because
                    // the response is handled through the waiter mechanism at a higher level.
                    Self::send_request(&method, payload, connections).await.map(|_| ())
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!("invalid msg type {:?}", meta),
                    ))
                }
            }
            HttpSocket::ForResponse(_msgid) => {
                if meta.is_rsp() {
                    // Server-side response handling.
                    // The actual response is sent via HttpSocketPool.handle_request
                    // which posts to the waiter channel. This socket variant exists
                    // to allow the socket to be passed through message handling.
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

    /// Sends an HTTP request to the server.
    pub async fn send_request(
        method: &str,
        bytes: Bytes,
        connections: &Arc<Connections>,
    ) -> Result<Bytes> {
        // 1. acquire connection.
        let mut sender = if let Some(sender) = connections.vec.lock().await.pop() {
            sender
        } else {
            // establish new connection.
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

        // 5. restore connection.
        connections.vec.lock().await.push(sender);

        Ok(body_bytes)
    }
}

impl RuapcSocket for HttpSocket {
    fn send_bytes(&self, meta: MsgMeta, payload: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.send_internal(meta, payload))
    }

    fn socket_type(&self) -> SocketType {
        SocketType::HTTP
    }

    fn clone_boxed(&self) -> Box<dyn RuapcSocket> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
