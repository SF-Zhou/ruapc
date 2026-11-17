use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_tungstenite::{
    WebSocketStream, accept_async_with_config, client_async_with_config, tungstenite,
};
use tokio_util::sync::DropGuard;

use super::WebSocket;
use crate::{
    ConnectionMap, Endpoint, Message, RawStream, Socket, State, TaskSupervisor,
    error::{Error, ErrorKind, Result},
    msg::frame::{FrameDecoder, MAX_MSG_SIZE},
    sockets::{connect::ConnectGroups, tcp},
};

/// WebSocket protocol limits aligned with the shared stream frame limit: one
/// binary message carries exactly one stream frame, so both the message and its
/// frame may reach [`MAX_MSG_SIZE`](crate::msg::frame::MAX_MSG_SIZE).
pub(crate) fn web_socket_config() -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(MAX_MSG_SIZE))
        .max_frame_size(Some(MAX_MSG_SIZE))
}

pub struct WebSocketPool {
    socket_map: ConnectionMap<WebSocket>,
    connect_groups: ConnectGroups,
    task_supervisor: TaskSupervisor,
}

impl WebSocketPool {
    pub(crate) fn new() -> Self {
        Self {
            socket_map: ConnectionMap::default(),
            connect_groups: Default::default(),
            task_supervisor: TaskSupervisor::create(),
        }
    }

    pub(crate) async fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        match stream {
            RawStream::TCP(tcp_stream) => {
                let stream = accept_async_with_config(tcp_stream, Some(web_socket_config()))
                    .await
                    .map_err(|e| Error::new(ErrorKind::WebSocketAcceptFailed, e.to_string()))?;
                self.add_socket(None, addr, stream, state)?;
            }
            RawStream::WS(web_socket_stream) => {
                self.add_socket(None, addr, *web_socket_stream, state)?;
            }
        }
        Ok(())
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
        group
            .connect(deadline, |addr| self.connect_address(endpoint, addr, state))
            .await
    }

    async fn connect_address(
        &self,
        endpoint: &Endpoint,
        addr: SocketAddr,
        state: &Arc<State>,
    ) -> Result<Socket> {
        // Connect the TCP stream ourselves so socket options (nodelay,
        // keepalive) can be applied before the WebSocket handshake.
        let tcp_stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::WebSocketConnectFailed, e.to_string()))?;
        tcp::configure_stream(&tcp_stream);
        let (stream, _) = client_async_with_config(
            format!("ws://{addr}"),
            tcp_stream,
            Some(web_socket_config()),
        )
        .await
        .map_err(|e| Error::new(ErrorKind::WebSocketConnectFailed, e.to_string()))?;

        let send_socket = self
            .socket_map
            .try_publish_with(endpoint.clone(), || {
                self.add_socket(Some(endpoint.clone()), addr, stream, state)
            })
            .await?;
        Ok(send_socket.into())
    }

    pub(crate) fn try_acquire(&self, endpoint: &Endpoint) -> Option<Socket> {
        self.socket_map.try_get_live(endpoint).map(Into::into)
    }

    pub(crate) async fn acquire_existing(&self, endpoint: &Endpoint) -> Option<Socket> {
        self.socket_map.get_live(endpoint).await.map(Into::into)
    }

    pub fn add_socket<S>(
        &self,
        endpoint: Option<Endpoint>,
        addr: SocketAddr,
        stream: WebSocketStream<S>,
        state: &Arc<State>,
    ) -> Result<WebSocket>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (send_stream, recv_stream) = stream.split();
        let (sender, receiver) = mpsc::channel(1024);
        let web_socket = WebSocket::new(sender);
        let task_supervisor = self.task_supervisor.try_start_async_task().ok_or_else(|| {
            Error::new(ErrorKind::ConnectionClosed, "WebSocket pool stopped".into())
        })?;
        let recv_task = self.task_supervisor.try_start_async_task().ok_or_else(|| {
            Error::new(ErrorKind::ConnectionClosed, "WebSocket pool stopped".into())
        })?;
        state.metrics.connection_opened("WS");

        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let endpoint = endpoint.clone();
            let web_socket = web_socket.clone();
            let state = state.clone();
            async move {
                let error = tokio::select! {
                    () = task_supervisor.stopped() => {
                        Error::new(ErrorKind::ConnectionClosed, "WebSocket pool stopped".into())
                    }
                    // Teardown from the receive loop must also end this task:
                    // it owns the sink half and keeps the send queue's only
                    // sender alive through `web_socket`, so dropping them is
                    // what closes the underlying stream. Waiting for `recv` to
                    // observe channel closure cannot work while this task
                    // itself keeps the sender alive.
                    () = web_socket.closed() => {
                        Error::new(ErrorKind::ConnectionClosed, "WebSocket connection closed".into())
                    }
                    result = Self::start_send_loop(send_stream, receiver) => result.err()
                        .unwrap_or_else(|| Error::new(
                            ErrorKind::ConnectionClosed,
                            "WebSocket send loop ended".into(),
                        )),
                };
                tracing::debug!("send loop for {addr} ended: {error}");
                Self::evict_socket(
                    &socket_map,
                    endpoint.as_ref(),
                    &addr,
                    &web_socket,
                    &state,
                    &error,
                )
                .await;
            }
        });

        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let endpoint = endpoint.clone();
            let web_socket = web_socket.clone();
            let state = state.clone();
            async move {
                let error = tokio::select! {
                    () = recv_task.stopped() => {
                        Error::new(ErrorKind::ConnectionClosed, "WebSocket pool stopped".into())
                    }
                    // Symmetric teardown: a failed send loop must release the
                    // receive half too instead of leaving the descriptor alive.
                    () = web_socket.closed() => {
                        Error::new(ErrorKind::ConnectionClosed, "WebSocket connection closed".into())
                    }
                    result = Self::start_recv_loop(recv_stream, web_socket.clone(), &state) =>
                        result.err().unwrap_or_else(|| Error::new(
                            ErrorKind::ConnectionClosed,
                            "WebSocket receive loop ended".into(),
                        )),
                };
                tracing::debug!("receive loop for {addr} ended: {error}");
                Self::evict_socket(
                    &socket_map,
                    endpoint.as_ref(),
                    &addr,
                    &web_socket,
                    &state,
                    &error,
                )
                .await;
            }
        });
        Ok(web_socket)
    }

    /// Removes a dead socket from the map (if it is still the mapped one)
    /// and eagerly fails every request pending on the connection.
    async fn evict_socket(
        socket_map: &ConnectionMap<WebSocket>,
        endpoint: Option<&Endpoint>,
        addr: &SocketAddr,
        socket: &WebSocket,
        state: &Arc<State>,
        err: &Error,
    ) {
        // The send and recv loops both report failures; run teardown once.
        if !socket.mark_closed() {
            return;
        }
        state.metrics.connection_closed("WS");
        if let Some(endpoint) = endpoint {
            socket_map.evict_if_current(endpoint, socket).await;
        }
        let err = Error::new(
            ErrorKind::ConnectionClosed,
            format!("connection to {addr} closed: {err}"),
        );
        state.connection_closed(socket.conn_id(), &err);
    }

    async fn start_recv_loop<S>(
        mut recv_stream: SplitStream<WebSocketStream<S>>,
        web_socket: WebSocket,
        state: &Arc<State>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let socket = Socket::WS(web_socket);
        let mut decoder = FrameDecoder::default();
        while let Some(msg) = recv_stream.next().await {
            let msg = msg.map_err(|e| Error::new(ErrorKind::WebSocketRecvFailed, e.to_string()))?;
            match msg {
                tungstenite::Message::Binary(bytes) => {
                    // One WebSocket binary message carries exactly one stream
                    // frame; complete frames are decoded without copying.
                    let mut input = bytes;
                    let body = decoder.decode(&mut input)?.ok_or_else(|| {
                        Error::new(
                            ErrorKind::ParseMsgFailed,
                            "incomplete WebSocket frame".into(),
                        )
                    })?;
                    if !input.is_empty() {
                        return Err(Error::new(
                            ErrorKind::ParseMsgFailed,
                            "trailing bytes after WebSocket frame".into(),
                        ));
                    }
                    let msg = Message::parse(body)?;
                    state.handle_recv(&socket, msg)?;
                }
                tungstenite::Message::Close(_) => {
                    return Err(Error::kind(ErrorKind::WebSocketClosed));
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn start_send_loop<S>(
        mut send_stream: SplitSink<WebSocketStream<S>, tungstenite::Message>,
        mut receiver: mpsc::Receiver<Bytes>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        while let Some(bytes) = receiver.recv().await {
            send_stream
                .send(tungstenite::Message::Binary(bytes))
                .await
                .map_err(|e| Error::new(ErrorKind::WebSocketSendFailed, e.to_string()))?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for WebSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketPool").finish()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::*;
    use crate::{Context, ListenMode, MsgFlags, MsgMeta, RawStream, SocketPoolConfig};
    use tokio_tungstenite::tungstenite::protocol::Role;

    #[tokio::test]
    async fn test_web_socket_pool_debug_format() {
        let pool = WebSocketPool::new();
        let debug = format!("{pool:?}");
        assert!(debug.contains("WebSocketPool"));
        // Exercise shutdown through the guard used by the composite pool.
        drop(pool.drop_guard());
        pool.join().await;
    }

    /// A peer that sends EOF is gone: the server must close the underlying
    /// stream instead of keeping it half open until the pool stops.
    #[tokio::test]
    async fn peer_eof_closes_server_connection() {
        let config = SocketPoolConfig {
            listen_mode: ListenMode::WS,
            ..Default::default()
        };
        let ctx = Context::create(&config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = ctx.state.clone();
        let accept = tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                state
                    .clone()
                    .handle_new_stream(RawStream::TCP(stream), peer)
                    .await;
            }
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "GET / HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\n\
             Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        );
        client.write_all(request.as_bytes()).await.unwrap();
        // Consume the upgrade response: the server has installed its receive
        // loop by the time the handshake completes, so the EOF below is the
        // only end-of-stream it can observe. The timeout keeps a regression
        // that stalls the accept path from hanging the test.
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut response = Vec::new();
            let mut chunk = [0u8; 512];
            while !response.windows(4).any(|window| window == b"\r\n\r\n") {
                let n = client.read(&mut chunk).await.unwrap();
                assert!(n > 0, "connection ended during the WebSocket handshake");
                response.extend_from_slice(&chunk[..n]);
            }
        })
        .await
        .expect("server must answer the WebSocket handshake");
        client.shutdown().await.unwrap();
        let mut buf = [0u8; 1];
        let closed = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf)).await;
        accept.abort();
        let n = closed
            .expect("server must close the WebSocket after peer EOF")
            .expect("client read must succeed");
        assert_eq!(n, 0, "client must observe the server FIN");
    }

    /// The WebSocket mirror of the TCP send-failure regression: when the sink
    /// fails while the peer keeps the stream open, only the connection's
    /// teardown signal can end the pending receive half. Without that arm the
    /// receive task keeps the stream (and its descriptor) alive until the whole
    /// pool stops.
    #[tokio::test]
    async fn send_failure_releases_pending_receive_half() {
        let config = SocketPoolConfig {
            listen_mode: ListenMode::WS,
            ..Default::default()
        };
        let ctx = Context::create(&config).unwrap();
        let pool = WebSocketPool::new();
        let idle_tasks = pool.task_supervisor.running_tasks();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // The test peer owns the connection for the whole test: dropping it would
        // end the receive loop through the EOF path instead.
        let _client = TcpStream::connect(addr).await.unwrap();
        let (mut server_stream, peer) = listener.accept().await.unwrap();

        // Half-close only the write direction before the pool takes the stream:
        // the sink's next send fails, while the receive loop stays blocked
        // because the peer never sends data or a close frame. The stream is
        // wrapped without a handshake, which the pool's framing does not need.
        server_stream.shutdown().await.unwrap();
        let stream = WebSocketStream::from_raw_socket(
            server_stream,
            Role::Server,
            Some(web_socket_config()),
        )
        .await;
        let socket = pool.add_socket(None, peer, stream, &ctx.state).unwrap();

        // One queued request makes the send loop attempt the failing send. The
        // response receiver stays bound: teardown eagerly fails it.
        let (msgid, _response) = ctx.state.waiter.alloc(Duration::from_secs(10));
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq,
            ..Default::default()
        };
        socket.send(&mut meta, &(), &ctx.state).await.unwrap();

        // The receive loop future owned the stream halves and one task
        // registration; returning to the idle count proves both were released.
        // The supervisor keeps one guard for its internal stop observer, so the
        // idle count also shows the pool itself is still running.
        let stopped = tokio::time::timeout(Duration::from_secs(2), async {
            while pool.task_supervisor.running_tasks() != idle_tasks {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            stopped.is_ok(),
            "receive task stayed registered: {} != {idle_tasks}",
            pool.task_supervisor.running_tasks(),
        );
        assert!(socket.is_closed());

        // Only the failed connection may end: the pool keeps accepting new
        // connections and their loops stay registered.
        let _client2 = TcpStream::connect(addr).await.unwrap();
        let (server_stream2, peer2) = listener.accept().await.unwrap();
        let stream2 = WebSocketStream::from_raw_socket(
            server_stream2,
            Role::Server,
            Some(web_socket_config()),
        )
        .await;
        let second = pool
            .add_socket(None, peer2, stream2, &ctx.state)
            .expect("a connection failure must not stop the pool");
        assert!(!second.is_closed());
        assert_eq!(pool.task_supervisor.running_tasks(), idle_tasks + 2);
    }
}
