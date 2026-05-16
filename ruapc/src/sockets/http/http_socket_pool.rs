use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use foldhash::fast::RandomState;
use http_body_util::{BodyExt, Either, Full};
use hyper::{Request, Response, body::Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::DropGuard;

use super::http_socket::{ChannelBody, HttpSocket};
use crate::{
    Error, ErrorKind, Message, MsgFlags, MsgMeta, RawStream, Result, Socket, SocketPoolConfig,
    SocketPoolTrait, SocketType, State, TaskSupervisor, sockets::tcp,
};

pub struct HttpSocketPool {
    socket_map: RwLock<HashMap<SocketAddr, HttpSocket, RandomState>>,
    http: Builder<TokioExecutor>,
    task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for HttpSocketPool {
    fn create(
        _config: &SocketPoolConfig,
        _devices: &std::sync::Arc<crate::Devices>,
        _buffer_pool: &std::sync::Arc<crate::BufferPool>,
    ) -> Result<Self> {
        let mut http = Builder::new(TokioExecutor::new());
        http.http1().keep_alive(true);
        Ok(Self {
            socket_map: RwLock::default(),
            http,
            task_supervisor: TaskSupervisor::create(),
        })
    }

    async fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        self.handle_new_stream(state, stream, addr)
    }

    fn stop(&self) {
        self.task_supervisor.stop();
    }

    fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

    async fn acquire(
        &self,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<Socket> {
        if socket_type != SocketType::HTTP {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("invalid socket type {socket_type} for HttpSocketPool"),
            ));
        }

        // Check if the socket is already in the socket map.
        if let Ok(socket_map) = self.socket_map.try_read()
            && let Some(socket) = socket_map.get(addr)
        {
            return Ok(socket.into());
        }

        // If not, establish an HTTP/2 streaming connection.
        let mut socket_map = self.socket_map.write().await;
        if let Some(socket) = socket_map.get(addr) {
            return Ok(socket.into());
        }

        let socket = Self::connect_stream(addr, state).await?;
        socket_map.insert(*addr, socket.clone());
        Ok(socket.into())
    }
}

impl HttpSocketPool {
    fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        let RawStream::TCP(tcp_stream) = stream else {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "invalid socket type".into(),
            ));
        };

        let state = state.clone();
        let http = self.http.clone();

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            let connection = http.serve_connection_with_upgrades(
                TokioIo::new(tcp_stream),
                hyper::service::service_fn(move |req: Request<Incoming>| {
                    Self::handle_request(req, state.clone(), addr)
                }),
            );
            tokio::select! {
                () = task_supervisor.stopped() => {},
                r = connection => {
                    if let Err(e) = r {
                        tracing::error!("recv loop for {addr} failed: {e}");
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn handle_request(
        mut req: Request<Incoming>,
        state: Arc<State>,
        addr: SocketAddr,
    ) -> Result<Response<Either<Full<Bytes>, ChannelBody>>> {
        if hyper_tungstenite::is_upgrade_request(&req) {
            let (response, websocket) = hyper_tungstenite::upgrade(&mut req, None)
                .map_err(|e| Error::new(ErrorKind::HttpUpgradeFailed, e.to_string()))?;

            let state = state.clone();
            tokio::spawn(async move {
                let websocket = match websocket.await {
                    Ok(socket) => socket,
                    Err(err) => {
                        tracing::error!("upgrade HTTP to WebSocket failed: {err}");
                        return;
                    }
                };
                state
                    .handle_new_stream(RawStream::WS(Box::new(websocket)), addr)
                    .await;
            });

            return Ok(response.map(Either::Left));
        }

        // Handle /_rpc: bidirectional streaming for reverse RPC.
        if req.method() == hyper::Method::POST && req.uri().path() == "/_rpc" {
            return Self::handle_rpc_stream(req, state, addr).await;
        }

        if req.method() == hyper::Method::GET {
            match req.uri().path() {
                "/openapi.json" => {
                    let openapi_json = serde_json::to_string_pretty(&state.router.openapi)?;
                    return Ok(Response::builder()
                        .header("Content-Type", "application/json")
                        .body(Either::Left(Full::new(Bytes::from(openapi_json))))
                        .unwrap());
                }
                "/rapidoc/rapidoc-min.js" => {
                    return Ok(Response::builder()
                        .header("Content-Type", "application/javascript")
                        .body(Either::Left(Full::new(Bytes::from(include_str!(
                            "rapidoc/rapidoc-min.js"
                        )))))
                        .unwrap());
                }
                "/rapidoc" | "/rapidoc/" | "/rapidoc/index.html" => {
                    let html = include_str!("rapidoc/index.html");
                    return Ok(Response::builder()
                        .header("Content-Type", "text/html; charset=utf-8")
                        .body(Either::Left(Full::new(Bytes::from(html))))
                        .unwrap());
                }
                _ => {
                    return Ok(Response::builder()
                        .status(404)
                        .body(Either::Left(Full::new(Bytes::from("Not Found"))))
                        .unwrap());
                }
            }
        }

        let (msgid, rx) = state.waiter.alloc();
        let meta = MsgMeta {
            method: req.uri().path().trim_start_matches('/').to_string(),
            flags: MsgFlags::IsReq,
            msgid,
            buffer_info: None,
        };
        let bytes = match req.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => {
                return Ok(Response::builder()
                    .status(500)
                    .body(Either::Left(Full::new(Bytes::from(
                        "Internal Server Error",
                    ))))
                    .unwrap());
            }
        };
        let msg = Message::new(meta, bytes.into());

        let socket = Socket::HTTP(HttpSocket::ForResponse(msgid));
        state.handle_recv(&socket, msg)?;

        let msg = rx
            .recv()
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;

        Ok(Response::builder()
            .header("Content-Type", "application/json")
            .body(Either::Left(Full::new(msg.payload.into())))
            .unwrap())
    }

    /// Handle a `POST /_rpc` request for bidirectional streaming.
    ///
    /// Creates a pair of channels:
    /// - Request body recv loop: reads framed messages from client → `state.handle_recv()`
    /// - Response body send channel: server sends framed messages back to client via `ChannelBody`
    async fn handle_rpc_stream(
        req: Request<Incoming>,
        state: Arc<State>,
        addr: SocketAddr,
    ) -> Result<Response<Either<Full<Bytes>, ChannelBody>>> {
        // Create the send channel for server → client messages.
        let (tx, rx) = mpsc::channel::<Bytes>(1024);
        let socket = HttpSocket::Stream(tx);
        let socket_for_recv = Socket::HTTP(socket);

        // Spawn recv loop: read framed messages from the request body.
        tokio::spawn({
            let state = state.clone();
            let socket_for_recv = socket_for_recv.clone();
            async move {
                if let Err(e) = Self::recv_loop(req.into_body(), &socket_for_recv, &state).await {
                    tracing::error!("http rpc recv loop for {addr} failed: {e}");
                }
            }
        });

        // Return streaming response.
        Ok(Response::builder()
            .header("Content-Type", "application/octet-stream")
            .body(Either::Right(ChannelBody::new(rx)))
            .unwrap())
    }

    /// Read framed messages from an HTTP body stream.
    ///
    /// Uses the same wire format as TCP: `[magic][len][body]`.
    async fn recv_loop(mut body: Incoming, socket: &Socket, state: &Arc<State>) -> Result<()> {
        let mut buffer = BytesMut::with_capacity(1 << 20);
        loop {
            // Try to parse complete messages from the buffer.
            while let Some(bytes) = tcp::parse_message(&mut buffer)? {
                let msg = Message::parse(bytes)?;
                state.handle_recv(socket, msg)?;
            }

            // Read more data from the body.
            match body.frame().await {
                Some(Ok(frame)) => {
                    if let Some(data) = frame.data_ref() {
                        buffer.extend_from_slice(data);
                    }
                }
                Some(Err(e)) => {
                    return Err(Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()));
                }
                None => return Ok(()), // Body stream ended.
            }
        }
    }

    /// Client-side: establish an HTTP/2 streaming connection to `/_rpc`.
    ///
    /// Sends a POST request with a streaming body and starts a recv loop
    /// on the response body. Returns an `HttpSocket::Stream` for sending.
    async fn connect_stream(addr: &SocketAddr, state: &Arc<State>) -> Result<HttpSocket> {
        use hyper::client::conn::http2;

        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;

        let (mut sender, conn) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;
        tokio::spawn(conn);

        // Create send channel for client → server messages (request body).
        let (req_tx, req_rx) = mpsc::channel::<Bytes>(1024);

        let req = Request::builder()
            .uri(format!("http://{addr}/_rpc"))
            .method(hyper::Method::POST)
            .body(ChannelBody::new(req_rx))
            .map_err(|e| Error::new(ErrorKind::HttpBuildReqFailed, e.to_string()))?;

        let rsp = sender
            .send_request(req)
            .await
            .map_err(|e| Error::new(ErrorKind::HttpSendReqFailed, e.to_string()))?;

        // Create the socket for sending messages.
        let socket = HttpSocket::Stream(req_tx);

        // Spawn recv loop on the response body.
        let socket_for_recv = Socket::HTTP(socket.clone());
        let state = state.clone();
        let addr = *addr;
        tokio::spawn(async move {
            if let Err(e) = Self::recv_loop(rsp.into_body(), &socket_for_recv, &state).await {
                tracing::error!("http rpc client recv loop for {addr} failed: {e}");
            }
        });

        Ok(socket)
    }
}

impl std::fmt::Debug for HttpSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSocketPool").finish()
    }
}
