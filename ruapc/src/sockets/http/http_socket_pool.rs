use std::{net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Either, Full};
use hyper::{Request, Response, body::Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::sync::mpsc;
use tokio_util::sync::DropGuard;

use super::http_socket::{ChannelBody, HttpSocket, StreamSocket};
use crate::{
    ConnectionMap, Error, ErrorKind, Message, MsgFlags, MsgMeta, RawStream, Result, Socket,
    SocketPoolConfig, SocketPoolTrait, State, TaskSupervisor, TaskSupervisorHandle, sockets::tcp,
};

type HttpSocketMap = ConnectionMap<HttpSocket>;

pub struct HttpSocketPool {
    socket_map: HttpSocketMap,
    connect_locks: crate::sockets::connect::ConnectLocks,
    base_path: Arc<str>,
    http: Builder<TokioExecutor>,
    task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for HttpSocketPool {
    fn create(
        config: &SocketPoolConfig,
        _devices: &std::sync::Arc<crate::Devices>,
        _buffer_pool: &std::sync::Arc<crate::BufferPool>,
    ) -> Result<Self> {
        let mut http = Builder::new(TokioExecutor::new());
        http.http1().keep_alive(true);
        Ok(Self {
            socket_map: ConnectionMap::default(),
            connect_locks: Default::default(),
            base_path: Arc::from(config.normalized_http_base_path()?),
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

    async fn acquire(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket> {
        // Check if the socket is already in the socket map.
        if let Some(socket) = self.socket_map.try_get_live(addr) {
            return Ok(socket.into());
        }

        let _connect = self.connect_locks.lock(*addr).await;
        if let Some(socket) = self.socket_map.get_live(addr).await {
            return Ok(socket.into());
        }

        let supervisor = self.task_supervisor.handle();
        let socket =
            Self::connect_stream(addr, state, &self.socket_map, &supervisor, &self.base_path)
                .await?;
        Ok(socket.into())
    }
}

impl HttpSocketPool {
    pub(crate) fn try_acquire(&self, addr: &SocketAddr) -> Option<Socket> {
        self.socket_map.try_get_live(addr).map(Into::into)
    }

    pub(crate) async fn acquire_existing(&self, addr: &SocketAddr) -> Option<Socket> {
        self.socket_map.get_live(addr).await.map(Into::into)
    }

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
        let base_path = self.base_path.clone();
        let supervisor = self.task_supervisor.handle();

        let task_supervisor = self
            .task_supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "HTTP pool stopped".into()))?;
        tokio::spawn(async move {
            let connection = http.serve_connection_with_upgrades(
                TokioIo::new(tcp_stream),
                hyper::service::service_fn(move |req: Request<Incoming>| {
                    Self::handle_request(
                        req,
                        state.clone(),
                        addr,
                        supervisor.clone(),
                        base_path.clone(),
                    )
                }),
            );
            tokio::select! {
                () = task_supervisor.stopped() => {},
                r = connection => {
                    if let Err(e) = r {
                        tracing::debug!(%addr, error = %e, "HTTP connection ended");
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
        supervisor: TaskSupervisorHandle,
        base_path: Arc<str>,
    ) -> Result<Response<Either<Full<Bytes>, ChannelBody>>> {
        if hyper_tungstenite::is_upgrade_request(&req) {
            let ws_config = crate::sockets::ws::web_socket_config();
            let (response, websocket) = hyper_tungstenite::upgrade(&mut req, Some(ws_config))
                .map_err(|e| Error::new(ErrorKind::HttpUpgradeFailed, e.to_string()))?;

            let state = state.clone();
            let _ = supervisor.try_spawn(async move {
                match websocket.await {
                    Ok(socket) => {
                        state
                            .handle_new_stream(RawStream::WS(Box::new(socket)), addr)
                            .await;
                    }
                    Err(err) => tracing::error!("upgrade HTTP to WebSocket failed: {err}"),
                }
            });

            return Ok(response.map(Either::Left));
        }

        let Some(path) = Self::strip_base_path(&base_path, req.uri().path()) else {
            return Ok(Self::not_found());
        };

        // Handle /_rpc: bidirectional streaming for reverse RPC.
        if req.method() == hyper::Method::POST && path == "/_rpc" {
            return Self::handle_rpc_stream(req, state, addr, &supervisor).await;
        }

        if req.method() == hyper::Method::GET {
            match path {
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
                    let html =
                        include_str!("rapidoc/index.html").replace("{{BASE_PATH}}", &base_path);
                    return Ok(Response::builder()
                        .header("Content-Type", "text/html; charset=utf-8")
                        .body(Either::Left(Full::new(Bytes::from(html))))
                        .unwrap());
                }
                _ => {
                    return Ok(Self::not_found());
                }
            }
        }

        // Unary HTTP is part of the public RPC surface. Internal control
        // methods remain available to trusted peers over the framed `/_rpc`
        // stream, but must not be callable (or discoverable) as ordinary
        // HTTP endpoints.
        let method = path.trim_start_matches('/');
        if req.method() != hyper::Method::POST || !state.router.is_public_method(method) {
            return Ok(Self::not_found());
        }

        let method = method.to_owned();
        // Cap the request body at the wire-format message limit; an
        // unauthenticated POST must not be able to buffer unbounded data.
        let limited =
            http_body_util::Limited::new(req.into_body(), crate::msg::frame::MAX_MSG_SIZE);
        let bytes = match limited.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) if e.is::<http_body_util::LengthLimitError>() => {
                return Ok(Response::builder()
                    .status(413)
                    .body(Either::Left(Full::new(Bytes::from("Payload Too Large"))))
                    .unwrap());
            }
            Err(_) => {
                return Ok(Response::builder()
                    .status(500)
                    .body(Either::Left(Full::new(Bytes::from(
                        "Internal Server Error",
                    ))))
                    .unwrap());
            }
        };
        // A unary request starts its response budget when its complete body
        // arrives, just like framed TCP, WebSocket and HTTP/2 messages. Pending
        // uploads own no waiter entry and cannot expire a future handler's slot.
        const UNARY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        let (msgid, rx) = state.waiter.alloc(UNARY_TIMEOUT);
        let meta = MsgMeta {
            method,
            flags: MsgFlags::IsReq,
            msgid,
            read_regions: Vec::new(),
            write_regions: Vec::new(),
            timeout_ms: u32::try_from(UNARY_TIMEOUT.as_millis()).unwrap_or(u32::MAX),
        };
        let msg = Message::new(meta, bytes.into());

        let socket = Socket::HTTP(HttpSocket::ForResponse(msgid));
        state.handle_recv(&socket, msg)?;

        let (msg, _write_buffer) = rx
            .recv()
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;

        Ok(Response::builder()
            .header("Content-Type", "application/json")
            .body(Either::Left(Full::new(msg.payload.into())))
            .unwrap())
    }

    fn strip_base_path<'a>(base_path: &str, path: &'a str) -> Option<&'a str> {
        if base_path.is_empty() {
            return Some(path);
        }

        let rest = path.strip_prefix(base_path)?;
        if rest.is_empty() {
            Some("/")
        } else if rest.starts_with('/') {
            Some(rest)
        } else {
            None
        }
    }

    fn not_found() -> Response<Either<Full<Bytes>, ChannelBody>> {
        Response::builder()
            .status(404)
            .body(Either::Left(Full::new(Bytes::from("Not Found"))))
            .unwrap()
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
        supervisor: &TaskSupervisorHandle,
    ) -> Result<Response<Either<Full<Bytes>, ChannelBody>>> {
        // Create the send channel for server → client messages.
        let (tx, rx) = mpsc::channel::<Bytes>(1024);
        let stream_socket = StreamSocket::new(tx);
        let socket_for_recv = Socket::HTTP(HttpSocket::Stream(stream_socket.clone()));

        // Spawn recv loop: read framed messages from the request body.
        let task = supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "HTTP pool stopped".into()))?;
        state.metrics.connection_opened("HTTP");
        tokio::spawn({
            let state = state.clone();
            let socket_for_recv = socket_for_recv.clone();
            async move {
                let r = tokio::select! {
                    () = task.stopped() => Err(Error::new(
                        ErrorKind::ConnectionClosed,
                        "HTTP pool stopped".into(),
                    )),
                    r = Self::recv_loop(req.into_body(), &socket_for_recv, &state) => r,
                };
                Self::log_stream_result(addr, "server", &r);
                // The stream ended: eagerly fail pending requests (including
                // reverse RPCs); running handlers retain their own lifetimes.
                if stream_socket.mark_closed() {
                    state.metrics.connection_closed("HTTP");
                    let err = Error::new(
                        ErrorKind::ConnectionClosed,
                        format!("http stream from {addr} closed: {:?}", r.err()),
                    );
                    state.connection_closed(stream_socket.conn_id(), &err);
                }
            }
        });

        // Return streaming response.
        Ok(Response::builder()
            .header("Content-Type", "application/octet-stream")
            .body(Either::Right(ChannelBody::new(rx)))
            .unwrap())
    }

    fn log_stream_result(addr: SocketAddr, side: &'static str, result: &Result<()>) {
        let Err(error) = result else {
            return;
        };
        match error.kind {
            // Local shutdown and a peer closing/resetting its HTTP body are
            // connection lifecycle events. Pending callers receive their own
            // ConnectionClosed errors through the waiter.
            ErrorKind::ConnectionClosed | ErrorKind::HttpWaitRspFailed => {
                tracing::debug!(%addr, side, %error, "HTTP RPC stream ended");
            }
            _ => tracing::warn!(%addr, side, %error, "invalid HTTP RPC stream"),
        }
    }

    /// Read framed messages from an HTTP body stream.
    ///
    /// Uses the same wire format as TCP: `[magic][len][body]`.
    async fn recv_loop(mut body: Incoming, socket: &Socket, state: &Arc<State>) -> Result<()> {
        let mut buffer = BytesMut::with_capacity(1 << 20);
        loop {
            // Try to parse complete messages from the buffer.
            while let Some(bytes) = crate::msg::frame::parse_message(&mut buffer)? {
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
    async fn connect_stream(
        addr: &SocketAddr,
        state: &Arc<State>,
        socket_map: &HttpSocketMap,
        supervisor: &TaskSupervisorHandle,
        base_path: &str,
    ) -> Result<HttpSocket> {
        use hyper::client::conn::http2;

        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;
        tcp::configure_stream(&stream);

        let (mut sender, conn) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;
        let conn_task = supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "HTTP pool stopped".into()))?;
        tokio::spawn(async move {
            tokio::select! {
                () = conn_task.stopped() => {}
                result = conn => {
                    if let Err(err) = result {
                        tracing::debug!("HTTP/2 client connection ended: {err}");
                    }
                }
            }
        });

        // Create send channel for client → server messages (request body).
        let (req_tx, req_rx) = mpsc::channel::<Bytes>(1024);

        let req = Request::builder()
            .uri(format!("http://{addr}{base_path}/_rpc"))
            .method(hyper::Method::POST)
            .body(ChannelBody::new(req_rx))
            .map_err(|e| Error::new(ErrorKind::HttpBuildReqFailed, e.to_string()))?;

        let rsp = sender
            .send_request(req)
            .await
            .map_err(|e| Error::new(ErrorKind::HttpSendReqFailed, e.to_string()))?;
        if !rsp.status().is_success() {
            return Err(Error::new(
                ErrorKind::HttpWaitRspFailed,
                format!("HTTP RPC stream returned {}", rsp.status()),
            ));
        }

        // Create the socket for sending messages.
        let stream_socket = StreamSocket::new(req_tx);
        let socket = HttpSocket::Stream(stream_socket.clone());

        let recv_task = supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "HTTP pool stopped".into()))?;
        // Publish before the receive task can observe EOF and evict it.
        socket_map.publish(*addr, socket.clone()).await;
        // Publish may wait for the map lock and be cancelled by the connect
        // deadline. Start accounting only after that await; from here to spawn
        // there is no cancellation point that could strand an open connection.
        state.metrics.connection_opened("HTTP");

        // Spawn recv loop on the response body. When it exits — error or
        // clean end of stream — evict the socket from the pool and eagerly
        // fail every request still pending on the connection.
        let socket_for_recv = Socket::HTTP(socket.clone());
        let socket_for_map = socket.clone();
        let state = state.clone();
        let socket_map = socket_map.clone();
        let addr = *addr;
        tokio::spawn(async move {
            let r = tokio::select! {
                () = recv_task.stopped() => Err(Error::new(
                    ErrorKind::ConnectionClosed,
                    "HTTP pool stopped".into(),
                )),
                r = Self::recv_loop(rsp.into_body(), &socket_for_recv, &state) => r,
            };
            Self::log_stream_result(addr, "client", &r);
            if !stream_socket.mark_closed() {
                return;
            }
            state.metrics.connection_closed("HTTP");
            socket_map.evict_if_current(&addr, &socket_for_map).await;
            let err = Error::new(
                ErrorKind::ConnectionClosed,
                format!("http connection to {addr} closed: {:?}", r.err()),
            );
            state.connection_closed(stream_socket.conn_id(), &err);
        });

        Ok(socket)
    }
}

impl std::fmt::Debug for HttpSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSocketPool").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn unary_body_collection_precedes_waiter_and_handler_budget() {
        let mut router = crate::Router::default();
        router.add_method::<(), (u32, u64, usize)>(
            "Budget/check",
            Box::new(|mut ctx, payload| {
                tokio::spawn(async move {
                    payload.deserialize::<()>(&ctx.msg_meta).unwrap();
                    let observed = (
                        ctx.msg_meta.timeout_ms,
                        ctx.remaining_time().unwrap().as_millis() as u64,
                        ctx.state.waiter.pending_count(),
                    );
                    ctx.send_rsp::<_, Error>(Ok(observed)).await;
                });
                Ok(())
            }),
        );
        let config = SocketPoolConfig {
            listen_mode: crate::ListenMode::HTTP,
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        };
        let server = crate::Server::create(router, &config).unwrap();
        let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(
            b"POST /Budget/check HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        ).await.unwrap();

        // Hyper sends 100 Continue only when the body is polled. This proves
        // the request reached collection, without sleeps or a 30s timeout.
        let mut interim = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !interim.ends_with(b"\r\n\r\n") {
                interim.push(stream.read_u8().await.unwrap());
            }
        })
        .await
        .unwrap();
        assert!(interim.starts_with(b"HTTP/1.1 100 Continue"));
        assert_eq!(server.state().waiter.pending_count(), 0);

        stream.write_all(b"4\r\nnull\r\n0\r\n\r\n").await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_to_end(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        let body_start = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap()
            + 4;
        let observed: Result<(u32, u64, usize)> =
            serde_json::from_slice(&response[body_start..]).unwrap();
        let (advertised, remaining, pending) = observed.unwrap();
        assert_eq!(advertised, 30_000);
        assert!((25_000..=30_000).contains(&remaining));
        assert_eq!(pending, 1);
        assert_eq!(server.state().waiter.pending_count(), 0);
        server.stop();
        server.join().await;
    }
}
