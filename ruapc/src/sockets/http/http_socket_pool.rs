use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use http_body_util::{BodyExt, Either, Full};
use hyper::{Request, Response, body::Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::sync::DropGuard;

use super::{
    http_socket::{ChannelBody, HttpSocket, StreamSocket},
    service::HttpService,
};
use crate::{
    ConnectionMap, Endpoint, Error, ErrorKind, Message, Result, Socket, SocketPoolConfig, State,
    TaskSupervisor, TaskSupervisorHandle,
    msg::frame::{Frame, FrameDecoder},
    remote_memory::IncomingData,
    sockets::{Outbound, connect::ConnectGroups, tcp},
};

type HttpSocketMap = ConnectionMap<HttpSocket>;

pub struct HttpSocketPool {
    socket_map: HttpSocketMap,
    connect_groups: ConnectGroups,
    service: Arc<HttpService>,
    http: Builder<TokioExecutor>,
    task_supervisor: TaskSupervisor,
}

impl HttpSocketPool {
    pub(crate) fn create(config: &SocketPoolConfig) -> Result<Self> {
        let mut http = Builder::new(TokioExecutor::new());
        http.http1().keep_alive(true);
        Ok(Self {
            socket_map: ConnectionMap::default(),
            connect_groups: Default::default(),
            service: Arc::new(HttpService::new(
                config.normalized_http_base_path()?,
                config.listen_mode,
            )),
            http,
            task_supervisor: TaskSupervisor::create(),
        })
    }

    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    pub(crate) async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

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

    pub(crate) fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<()> {
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
                TokioIo::new(stream),
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
        let (tx, rx) = mpsc::channel::<Outbound>(1024);
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
            // ConnectionClosed errors through the waiter. So is closing the
            // stream of a peer that does not read its acknowledgements: its
            // frames were valid, and TCP and WebSocket log it the same way.
            ErrorKind::ConnectionClosed | ErrorKind::HttpWaitRspFailed | ErrorKind::Overloaded => {
                tracing::debug!(%addr, side, %error, "HTTP RPC stream ended");
            }
            _ => tracing::warn!(%addr, side, %error, "invalid HTTP RPC stream"),
        }
    }

    /// Read frames from an HTTP body stream.
    ///
    /// Uses the same wire format as TCP: `[magic][len][body]`.
    async fn recv_loop<B>(mut body: B, socket: &Socket, state: &Arc<State>) -> Result<()>
    where
        B: hyper::body::Body<Data = Bytes> + Unpin,
        B::Error: std::fmt::Display,
    {
        let mut decoder = FrameDecoder::default();
        // The data frame whose raw bytes the stream currently carries.
        let mut receiving: Option<IncomingData> = None;
        while let Some(frame) = body.frame().await {
            let frame =
                frame.map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;
            // Trailers carry no RPC data. Complete frames retain their original
            // Bytes allocation; only frames spanning DATA chunks are buffered.
            let Ok(mut data) = frame.into_data() else {
                continue;
            };
            loop {
                if let Some(incoming) = &mut receiving {
                    // Raw bytes go from each chunk to their destination; the
                    // frame is never assembled.
                    let chunk = data.split_to(incoming.remaining().min(data.len()));
                    incoming.write(&chunk);
                    if incoming.remaining() > 0 {
                        break;
                    }
                    if let Some(incoming) = receiving.take() {
                        incoming.finish(state, socket)?;
                    }
                }
                match decoder.decode_frame(&mut data)? {
                    Some(Frame::Message(bytes)) => {
                        let msg = Message::parse(bytes)?;
                        state.handle_recv(socket, msg)?;
                    }
                    Some(Frame::Data { head, len }) => {
                        receiving = Some(IncomingData::begin(state, socket, head, len));
                    }
                    None => break,
                }
            }
        }
        // The body ended. A clean end cannot interrupt a frame that was
        // already announced; report it instead of treating the stream as done.
        if let Some(incoming) = receiving.take() {
            return Err(Error::new(
                ErrorKind::HttpWaitRspFailed,
                format!(
                    "HTTP RPC stream ended inside a data frame, {} bytes missing",
                    incoming.remaining()
                ),
            ));
        }
        if decoder.has_partial() {
            return Err(Error::new(
                ErrorKind::HttpWaitRspFailed,
                "HTTP RPC stream ended inside a frame".into(),
            ));
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
        let (req_tx, req_rx) = mpsc::channel::<Outbound>(1024);

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

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use http_body_util::StreamBody;
    use hyper::body::Frame as BodyFrame;

    use super::*;
    use crate::sockets::test_util::{data_frame, pending_write_space};

    /// A request or response body that delivers `chunks` and then ends.
    fn body(chunks: &[Bytes]) -> impl hyper::body::Body<Data = Bytes, Error = Infallible> + Unpin {
        let frames: Vec<_> = chunks
            .iter()
            .map(|chunk| Ok(BodyFrame::data(chunk.clone())))
            .collect();
        StreamBody::new(futures_util::stream::iter(frames))
    }

    fn stream() -> (Arc<State>, Socket, mpsc::Receiver<Outbound>) {
        let config = SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        };
        let state = crate::Context::create(&config).unwrap().state;
        let (sender, queue) = mpsc::channel(8);
        let socket = Socket::HTTP(HttpSocket::Stream(StreamSocket::new(sender)));
        (state, socket, queue)
    }

    /// A data frame's bytes reach their destination from whatever body
    /// chunks carry them, without the frame being assembled.
    #[tokio::test]
    async fn data_frames_cross_body_chunks() {
        let (state, socket, _queue) = stream();
        let (target, id, _pending) = pending_write_space(&state, &socket, b"........");
        let frame = data_frame(id, None, &[(6, 1), (1, 3)], b"wxyz");
        for split in 0..=frame.len() {
            let chunks = [frame.slice(..split), frame.slice(split..), frame.clone()];
            HttpSocketPool::recv_loop(body(&chunks), &socket, &state)
                .await
                .unwrap();
        }
        assert_eq!(target.received(), 8 * (frame.len() as u64 + 1));
        assert_eq!(target.seal_now().unwrap()[0].as_slice(), b".xyz..w.");
    }

    /// A body that ends inside a frame did not end cleanly, wherever the
    /// frame was cut: in its header, its head or its raw bytes.
    #[tokio::test]
    async fn a_body_cannot_end_inside_a_frame() {
        let (state, socket, _queue) = stream();
        let (target, id, _pending) = pending_write_space(&state, &socket, b"........");
        let frame = data_frame(id, None, &[(2, 4)], b"wxyz");
        let head = frame.len() - 4;
        // A clean end follows complete frames, or nothing at all.
        for chunks in [&[][..], std::slice::from_ref(&frame)] {
            HttpSocketPool::recv_loop(body(chunks), &socket, &state)
                .await
                .unwrap();
        }
        for cut in 1..frame.len() {
            let chunks = [frame.clone(), frame.slice(..cut)];
            let error = HttpSocketPool::recv_loop(body(&chunks), &socket, &state)
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::HttpWaitRspFailed, "cut {cut}");
            if cut < head {
                assert!(error.msg.contains("inside a frame"), "cut {cut}: {error}");
            } else {
                let missing = format!("{} bytes missing", frame.len() - cut);
                assert!(error.msg.contains(&missing), "cut {cut}: {error}");
            }
        }
        // The same holds for a message frame.
        let message = crate::msg::frame::encode(&crate::MsgMeta::default(), &"x").unwrap();
        let error = HttpSocketPool::recv_loop(body(&[message.slice(..9)]), &socket, &state)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::HttpWaitRspFailed);
        drop(target);
    }
}
