use std::{
    cell::Cell,
    future::{Future, pending},
    net::SocketAddr,
    sync::Arc,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Either, Full};
use hyper::{Request, Response, body::Incoming};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::DropGuard;

use super::{
    http_socket::{ChannelBody, HttpSocket, StreamSocket},
    service::HttpService,
};
use crate::{
    BufferPool, ConnectionMap, Devices, Endpoint, Error, ErrorKind, Message, RawStream, Result,
    Socket, SocketPoolConfig, SocketPoolTrait, State, TaskSupervisor, TaskSupervisorHandle,
    msg::frame::{FrameDecoder, SendFrame},
    sockets::{ConnectionLifecycle, connect::ConnectGroups, tcp},
};

type HttpSocketMap = ConnectionMap<HttpSocket>;

/// Hyper spawns the HTTP/2 connection and body pipes through its executor.
/// Cancelling only its public dispatch future leaves those tasks alive.
#[derive(Clone)]
struct ClientExecutor {
    close: Arc<ConnectionLifecycle>,
    supervisor: TaskSupervisorHandle,
}

impl<F> hyper::rt::Executor<F> for ClientExecutor
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        let Some(task) = self.supervisor.try_start_async_task() else {
            return;
        };
        let close = self.close.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                () = close.closed() => {}
                () = task.stopped() => {}
                _ = future => {}
            }
        });
    }
}

tokio::task_local! {
    // Hyper creates the service future before spawning its HTTP/2 stream task.
    // Registration happens when that future is polled inside this task scope.
    static SERVER_STREAM_CLOSE: Cell<Option<oneshot::Sender<Arc<ConnectionLifecycle>>>>;
}

async fn server_stream_scope<F: Future>(future: F) -> Option<F::Output> {
    let (sender, receiver) = oneshot::channel();
    SERVER_STREAM_CLOSE
        .scope(Cell::new(Some(sender)), async move {
            tokio::select! {
                biased;
                () = async {
                    match receiver.await {
                        Ok(close) => close.closed().await,
                        Err(_) => pending().await,
                    }
                } => None,
                output = future => Some(output),
            }
        })
        .await
}

/// Each Hyper HTTP/2 response pipe has its own cancellation scope. Closing an
/// RPC stream must drop even a pipe blocked on flow control, while preserving
/// the connection and its other streams.
#[derive(Clone)]
struct ServerExecutor {
    supervisor: TaskSupervisorHandle,
}

impl<F> hyper::rt::Executor<F> for ServerExecutor
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        let Some(task) = self.supervisor.try_start_async_task() else {
            return;
        };
        tokio::spawn(async move {
            tokio::select! {
                biased;
                () = task.stopped() => {}
                _ = server_stream_scope(future) => {}
            }
        });
    }
}

pub struct HttpSocketPool {
    socket_map: HttpSocketMap,
    connect_groups: ConnectGroups,
    service: Arc<HttpService>,
    http: Builder<ServerExecutor>,
    task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for HttpSocketPool {
    fn create(
        config: &SocketPoolConfig,
        _devices: &std::sync::Arc<Devices>,
        _buffer_pool: &std::sync::Arc<BufferPool>,
    ) -> Result<Self> {
        let task_supervisor = TaskSupervisor::create();
        let mut http = Builder::new(ServerExecutor {
            supervisor: task_supervisor.handle(),
        });
        http.http1().keep_alive(true);
        Ok(Self {
            socket_map: ConnectionMap::default(),
            connect_groups: Default::default(),
            service: Arc::new(HttpService::new(
                config.normalized_http_base_path()?,
                config.listen_mode,
            )),
            http,
            task_supervisor,
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
        // HTTP/1 services run inline in the connection task, whereas HTTP/2
        // gives each RPC stream its own cancellation and flow-control scope.
        // Reject HTTP/1 before creating stream state or consuming a task-local
        // registration intended for a Hyper HTTP/2 stream task.
        if req.version() != hyper::Version::HTTP_2 {
            return Ok(Response::builder()
                .status(hyper::StatusCode::BAD_REQUEST)
                .body(Either::Left(Full::new(Bytes::from_static(
                    b"RPC streaming requires HTTP/2",
                ))))
                .unwrap());
        }
        // Create the send channel for server → client messages.
        let (tx, rx) = mpsc::channel::<SendFrame>(1024);
        let stream_socket = StreamSocket::new(tx);
        let socket_for_recv = Socket::HTTP(HttpSocket::Stream(stream_socket.clone()));
        let body = ChannelBody::new(rx, stream_socket.close_handle());
        let _ = SERVER_STREAM_CLOSE.try_with(|slot| {
            if let Some(sender) = slot.take() {
                let _ = sender.send(stream_socket.close_handle());
            }
        });

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
                    biased;
                    () = stream_socket.closed() => Err(ErrorKind::ConnectionClosed.into()),
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
            .body(Either::Right(body))
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
        let mut decoder = FrameDecoder::default();
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

        // One dedicated HTTP/2 connection carries this RPC stream. Tie its
        // driver and both bodies to the same close signal, including failed or
        // cancelled setup before the receive task takes ownership.
        let (req_tx, req_rx) = mpsc::channel::<SendFrame>(1024);
        let stream_socket = StreamSocket::new(req_tx);
        let close = stream_socket.close_handle();
        let setup_guard = close.close_guard();
        let (mut sender, conn) = http2::handshake(
            ClientExecutor {
                close: close.clone(),
                supervisor: supervisor.clone(),
            },
            TokioIo::new(stream),
        )
        .await
        .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;
        // Hyper's dispatch future otherwise finishes when the last request
        // handle drops, even while its separately spawned stream is active.
        let connection_sender = sender.clone();
        let conn_task = supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "HTTP pool stopped".into()))?;
        tokio::spawn(async move {
            let _close_on_exit = close.close_guard();
            let _sender = connection_sender;
            tokio::select! {
                biased;
                () = close.closed() => {}
                () = conn_task.stopped() => {}
                result = conn => {
                    if let Err(err) = result {
                        tracing::debug!("HTTP/2 client connection ended: {err}");
                    }
                }
            }
        });

        let req = Request::builder()
            .uri(format!("http://{addr}{base_path}/_rpc"))
            .method(hyper::Method::POST)
            .body(ChannelBody::new(req_rx, stream_socket.close_handle()))
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
                biased;
                () = stream_socket.closed() => Err(ErrorKind::ConnectionClosed.into()),
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

        setup_guard.disarm();
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
    use crate::{
        Context, ListenMode, MsgFlags, MsgMeta, Router, core::ContextEndpoint, msg::frame,
    };
    use hyper::{
        client::conn::{http1, http2},
        server::conn::http2::Builder as Http2Builder,
    };
    use hyper_util::rt::TokioExecutor;
    use std::{convert::Infallible, future::ready, sync::Mutex, time::Duration};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn http1_rpc_stream_is_rejected_without_closing_keep_alive_connection() {
        let config = SocketPoolConfig {
            rdma: None,
            listen_mode: ListenMode::HTTP,
            ..Default::default()
        };
        let base = Context::create(&config).unwrap();
        let pool =
            HttpSocketPool::create(&config, &base.state.devices, &base.state.buffer_pool).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_io = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server_io, addr) = listener.accept().await.unwrap();
        pool.handle_new_stream(&base.state, RawStream::TCP(server_io), addr)
            .unwrap();
        let (mut client, connection) = http1::handshake(TokioIo::new(client_io)).await.unwrap();
        let driver = tokio::spawn(connection);

        tokio::time::timeout(Duration::from_secs(2), async {
            let response = client
                .send_request(
                    Request::builder()
                        .method(hyper::Method::POST)
                        .uri("/_rpc")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), hyper::StatusCode::BAD_REQUEST);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                "RPC streaming requires HTTP/2"
            );

            // Reuse the exact HTTP/1 connection after rejecting the stream.
            // The connection driver must not have acquired a stream close
            // registration from a service future polled inline by Hyper.
            client.ready().await.unwrap();
            let response = client
                .send_request(
                    Request::builder()
                        .uri("/openapi.json")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), hyper::StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let openapi: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(openapi.get("openapi").is_some());
        })
        .await
        .expect("rejecting an HTTP/1 RPC stream must preserve HTTP keep-alive");

        drop(client);
        pool.stop();
        pool.join().await;
        driver.abort();
        let _ = driver.await;
    }

    #[tokio::test]
    async fn request_eof_cancels_blocked_response_without_closing_other_http2_streams() {
        struct Owner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let (contexts_tx, mut contexts_rx) = mpsc::channel(2);
        let mut router = Router::default();
        router.add_handler::<(), (), _, _>("Test/stream", move |ctx, _| {
            contexts_tx.try_send(ctx).unwrap();
            async { Ok(()) }
        });
        let config = SocketPoolConfig {
            rdma: None,
            listen_mode: ListenMode::HTTP,
            ..Default::default()
        };
        let base = Context::create_with_router(router, &config).unwrap();
        let pool =
            HttpSocketPool::create(&config, &base.state.devices, &base.state.buffer_pool).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_io = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server_io, addr) = listener.accept().await.unwrap();
        pool.handle_new_stream(&base.state, RawStream::TCP(server_io), addr)
            .unwrap();
        let (mut client, connection) =
            http2::handshake(TokioExecutor::new(), TokioIo::new(client_io))
                .await
                .unwrap();
        let driver = tokio::spawn(connection);
        let mut requests = Vec::new();
        let mut responses = Vec::new();
        for _ in 0..2 {
            let (tx, rx) = mpsc::channel(1);
            let request = Request::builder()
                .method(hyper::Method::POST)
                .uri("http://localhost/_rpc")
                .body(ChannelBody::new(rx, Arc::new(ConnectionLifecycle::new())))
                .unwrap();
            responses.push(client.send_request(request).await.unwrap().into_body());
            requests.push(tx);
        }
        let mut second_response = responses.pop().unwrap();
        let mut first_response = responses.pop().unwrap();
        let second_request = requests.pop().unwrap();
        let first_request = requests.pop().unwrap();
        let request = |msgid| {
            frame::encode(
                &MsgMeta {
                    method: "Test/stream".into(),
                    flags: MsgFlags::IsReq,
                    msgid,
                    timeout_ms: 10_000,
                    ..Default::default()
                },
                &(),
            )
            .unwrap()
            .into()
        };
        first_request.send(request(1)).await.unwrap();
        let ctx = contexts_rx.recv().await.unwrap();
        let ContextEndpoint::Connected(Socket::HTTP(HttpSocket::Stream(socket))) = &ctx.endpoint
        else {
            panic!("expected HTTP stream context");
        };
        let source = Arc::new(vec![7; 8 << 20]);
        let queued = Arc::new(vec![9; 8 << 20]);
        for owner in [&source, &queued] {
            Socket::HTTP(HttpSocket::Stream(socket.clone()))
                .send_raw(
                    &mut MsgMeta {
                        flags: MsgFlags::IsRsp,
                        msgid: 1,
                        ..Default::default()
                    },
                    &(),
                    &[Bytes::from_owner(Owner(owner.clone()))],
                    &ctx.state,
                )
                .await
                .unwrap();
        }
        // Read one response chunk, then leave both the HTTP/2 stream window
        // and its body pipe blocked with original source owners still held.
        drop(first_response.frame().await.unwrap().unwrap());
        assert_eq!(Arc::strong_count(&source), 2);
        assert_eq!(Arc::strong_count(&queued), 2);
        drop(first_request); // END_STREAM only on the request direction.
        tokio::time::timeout(Duration::from_secs(2), socket.closed())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while Arc::strong_count(&source) != 1 || Arc::strong_count(&queued) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request EOF must release the response pipe and queued raw owners");
        let reset = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(frame) = first_response.frame().await {
                if frame.is_err() {
                    return true;
                }
            }
            false
        })
        .await
        .expect("the peer must observe termination of the closed response stream");
        assert!(
            reset,
            "a cancelled response must reset, not end as a successful body"
        );

        // The second RPC stream was established before the first one closed.
        // It must still dispatch requests and return framed responses.
        second_request.send(request(2)).await.unwrap();
        let mut ctx = tokio::time::timeout(Duration::from_secs(2), contexts_rx.recv())
            .await
            .unwrap()
            .unwrap();
        ctx.send_rsp(Ok::<_, Error>("still open")).await.unwrap();
        let mut decoder = FrameDecoder::default();
        let message = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut data = second_response
                    .frame()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_data()
                    .unwrap();
                if let Some(message) = decoder.decode(&mut data).unwrap() {
                    break Message::parse(message).unwrap();
                }
            }
        })
        .await
        .expect("closing one RPC stream must preserve its siblings");
        assert_eq!(
            message
                .payload
                .deserialize::<Result<String>>(&message.meta)
                .unwrap()
                .unwrap(),
            "still open"
        );
        drop(first_response);
        drop(second_response);
        drop(second_request);
        drop(client);
        pool.stop();
        pool.join().await;
        driver.abort();
        let _ = driver.await;
    }

    #[tokio::test]
    async fn response_eof_cancels_client_driver_and_releases_blocked_raw_source() {
        struct Owner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let ctx = Context::create(&SocketPoolConfig {
            rdma: None,
            ..Default::default()
        })
        .unwrap();
        let weak_state = Arc::downgrade(&ctx.state);
        let pool = HttpSocketPool::create(
            &SocketPoolConfig::default(),
            &ctx.state.devices,
            &ctx.state.buffer_pool,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint: Endpoint = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let (request_tx, mut request_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(1);
        let response_body = Mutex::new(Some(ChannelBody::new(
            response_rx,
            Arc::new(ConnectionLifecycle::new()),
        )));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                request_tx.try_send(request.into_body()).unwrap();
                ready(Ok::<_, Infallible>(Response::new(
                    response_body.lock().unwrap().take().unwrap(),
                )))
            });
            Http2Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
        });
        let socket = pool.acquire(&endpoint, &ctx.state).await.unwrap();
        let source = Arc::new(vec![7; 8 << 20]);
        socket
            .send_raw(
                &mut MsgMeta {
                    flags: MsgFlags::IsRsp,
                    ..Default::default()
                },
                &(),
                &[Bytes::from_owner(Owner(source.clone()))],
                &ctx.state,
            )
            .await
            .unwrap();
        let mut request_body = request_rx.recv().await.unwrap();
        drop(request_body.frame().await.unwrap().unwrap());
        // Stop draining the request, leaving its payload behind HTTP/2 flow
        // control, then finish only the peer's response direction.
        assert_eq!(Arc::strong_count(&source), 2);
        drop(response_tx);
        drop(ctx);
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak_state.strong_count() != 0 || Arc::strong_count(&source) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect(
            "response EOF must release the driver and its pending source without pool shutdown",
        );
        assert!(matches!(&socket, Socket::HTTP(stream) if stream.is_closed()));
        pool.stop();
        pool.join().await;
        server.abort();
        let _ = server.await;
    }
}
