use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use http_body_util::{BodyExt, Either, Full};
use hyper::{Request, Response, body::Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::sync::mpsc;
use tokio_util::sync::DropGuard;

use super::{
    http_socket::{ChannelBody, HttpSocket, StreamSocket},
    service::HttpService,
};
use crate::{
    ConnectionMap, Endpoint, Error, ErrorKind, Message, RawStream, Result, Socket,
    SocketPoolConfig, SocketPoolTrait, State, TaskSupervisor, TaskSupervisorHandle, sockets::tcp,
};

type HttpSocketMap = ConnectionMap<HttpSocket>;

pub struct HttpSocketPool {
    socket_map: HttpSocketMap,
    connect_groups: crate::sockets::connect::ConnectGroups,
    service: Arc<HttpService>,
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
            connect_groups: Default::default(),
            service: Arc::new(HttpService::new(config.normalized_http_base_path()?)),
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

    async fn acquire(&self, endpoint: &Endpoint, state: &Arc<State>) -> Result<Socket> {
        self.acquire_with_deadline(endpoint, None, state).await
    }
}

impl HttpSocketPool {
    pub(crate) async fn acquire_with_deadline(
        &self,
        endpoint: &Endpoint,
        deadline: Option<std::time::Instant>,
        state: &Arc<State>,
    ) -> Result<Socket> {
        if let Some(socket) = self.try_acquire(endpoint) {
            return Ok(socket);
        }
        let mut group = self.connect_groups.lock(endpoint, deadline).await?;
        if let Some(socket) = self.acquire_existing(endpoint).await {
            return Ok(socket);
        }
        let supervisor = self.task_supervisor.handle();
        let supervisor = &supervisor;
        group
            .connect(deadline, |addr| async move {
                Self::connect_stream(
                    endpoint,
                    &addr,
                    state,
                    &self.socket_map,
                    supervisor,
                    self.service.base_path(),
                )
                .await
                .map(Into::into)
            })
            .await
    }

    pub(crate) fn try_acquire(&self, endpoint: &Endpoint) -> Option<Socket> {
        self.socket_map.try_get_live(endpoint).map(Into::into)
    }

    pub(crate) async fn acquire_existing(&self, endpoint: &Endpoint) -> Option<Socket> {
        self.socket_map.get_live(endpoint).await.map(Into::into)
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
        let service = self.service.clone();
        let supervisor = self.task_supervisor.handle();

        let task_supervisor = self
            .task_supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "HTTP pool stopped".into()))?;
        tokio::spawn(async move {
            let connection = http.serve_connection_with_upgrades(
                TokioIo::new(tcp_stream),
                hyper::service::service_fn(move |req: Request<Incoming>| {
                    service
                        .clone()
                        .handle_request(req, state.clone(), addr, supervisor.clone())
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

    /// Handle a `POST /_rpc` request for bidirectional streaming.
    ///
    /// Creates a pair of channels:
    /// - Request body recv loop: reads framed messages from client → `state.handle_recv()`
    /// - Response body send channel: server sends framed messages back to client via `ChannelBody`
    pub(super) fn handle_rpc_stream(
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
        let mut decoder = crate::msg::frame::FrameDecoder::default();
        while let Some(frame) = body.frame().await {
            let frame =
                frame.map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;
            // Trailers carry no RPC data. Complete frames retain their original
            // Bytes allocation; only frames spanning DATA chunks are buffered.
            if let Ok(mut data) = frame.into_data() {
                while let Some(bytes) = decoder.decode(&mut data)? {
                    let msg = Message::parse(bytes)?;
                    state.handle_recv(socket, msg)?;
                }
            }
        }
        Ok(())
    }

    /// Client-side: establish an HTTP/2 streaming connection to `/_rpc`.
    ///
    /// Sends a POST request with a streaming body and starts a recv loop
    /// on the response body. Returns an `HttpSocket::Stream` for sending.
    async fn connect_stream(
        endpoint: &Endpoint,
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
        socket_map.publish(endpoint.clone(), socket.clone()).await;
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
        let endpoint = endpoint.clone();
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
            socket_map
                .evict_if_current(&endpoint, &socket_for_map)
                .await;
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
