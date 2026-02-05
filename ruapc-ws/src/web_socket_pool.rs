use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use foldhash::fast::RandomState;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{RwLock, mpsc},
};
use tokio_tungstenite::{WebSocketStream, accept_async, connect_async, tungstenite};
use tokio_util::sync::DropGuard;

use crate::{
    BoxFuture, Error, ErrorKind, Message, MessageHandler, RawStream, Result, RuapcSocket,
    RuapcSocketPool, SocketType, TaskSupervisor, WebSocket,
};

/// Inner state for the WebSocket pool.
struct WebSocketPoolInner {
    socket_map: RwLock<HashMap<SocketAddr, WebSocket, RandomState>>,
    task_supervisor: TaskSupervisor,
}

/// WebSocket pool for managing WebSocket connections.
///
/// This pool manages a collection of WebSockets, handling:
/// - Connection pooling and reuse
/// - Background tasks for sending and receiving
/// - Graceful shutdown
#[derive(Clone)]
pub struct WebSocketPool {
    inner: Arc<WebSocketPoolInner>,
}

impl WebSocketPool {
    /// Creates a new WebSocket pool.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(WebSocketPoolInner {
                socket_map: RwLock::default(),
                task_supervisor: TaskSupervisor::create(),
            }),
        }
    }

    /// Adds a new WebSocket connection.
    pub fn add_socket<S>(
        &self,
        addr: SocketAddr,
        stream: WebSocketStream<S>,
        handler: Arc<dyn MessageHandler>,
    ) -> WebSocket
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (send_stream, recv_stream) = stream.split();
        let (sender, receiver) = mpsc::channel(1024);

        // Start send loop
        let task_guard = self.inner.task_supervisor.start_async_task();
        tokio::spawn(async move {
            tokio::select! {
                () = task_guard.stopped() => {},
                _ = Self::start_send_loop(send_stream, receiver) => {}
            }
        });

        // Start recv loop
        let web_socket = WebSocket::new(sender);
        let task_guard = self.inner.task_supervisor.start_async_task();
        let inner = self.inner.clone();
        let web_socket_clone = web_socket.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = task_guard.stopped() => {},
                r = Self::start_recv_loop(recv_stream, web_socket_clone, handler) => {
                    if let Err(e) = r {
                        tracing::error!("recv loop for {addr} failed: {e}");
                        let mut socket_map = inner.socket_map.write().await;
                        socket_map.remove(&addr);
                    }
                }
            }
        });

        web_socket
    }

    async fn start_recv_loop<S>(
        mut recv_stream: SplitStream<WebSocketStream<S>>,
        web_socket: WebSocket,
        handler: Arc<dyn MessageHandler>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let socket: Box<dyn RuapcSocket> = Box::new(web_socket);
        while let Some(msg) = recv_stream.next().await {
            let msg = msg.map_err(|e| Error::new(ErrorKind::WebSocketRecvFailed, e.to_string()))?;
            match msg {
                tungstenite::Message::Binary(bytes) => {
                    let msg = Message::parse(bytes)?;
                    handler.handle_recv(socket.as_ref(), msg)?;
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

    async fn acquire_internal(
        &self,
        addr: SocketAddr,
        handler: Arc<dyn MessageHandler>,
    ) -> Result<Box<dyn RuapcSocket>> {
        // Check if the socket is already in the socket map.
        if let Ok(socket_map) = self.inner.socket_map.try_read()
            && let Some(socket) = socket_map.get(&addr)
        {
            return Ok(Box::new(socket.clone()));
        }

        // If not, create a new socket and insert it into the socket map.
        let mut socket_map = self.inner.socket_map.write().await;
        if let Some(socket) = socket_map.get(&addr) {
            return Ok(Box::new(socket.clone()));
        }

        let (stream, _) = connect_async(format!("ws://{addr}"))
            .await
            .map_err(|e| Error::new(ErrorKind::WebSocketConnectFailed, e.to_string()))?;

        let send_socket = self.add_socket(addr, stream, handler);
        socket_map.insert(addr, send_socket.clone());
        Ok(Box::new(send_socket))
    }

    async fn handle_new_stream_internal(
        &self,
        handler: Arc<dyn MessageHandler>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        match stream {
            RawStream::TCP(tcp_stream) => {
                let stream = accept_async(tcp_stream)
                    .await
                    .map_err(|e| Error::new(ErrorKind::WebSocketAcceptFailed, e.to_string()))?;
                self.add_socket(addr, stream, handler);
            }
            RawStream::WS(web_socket_stream) => {
                self.add_socket(addr, *web_socket_stream, handler);
            }
        }
        Ok(())
    }
}

impl Default for WebSocketPool {
    fn default() -> Self {
        Self::new()
    }
}

impl RuapcSocketPool for WebSocketPool {
    fn socket_type(&self) -> SocketType {
        SocketType::WS
    }

    fn acquire(
        &self,
        addr: SocketAddr,
        handler: Arc<dyn MessageHandler>,
    ) -> BoxFuture<'_, Result<Box<dyn RuapcSocket>>> {
        Box::pin(self.acquire_internal(addr, handler))
    }

    fn handle_new_stream(
        &self,
        handler: Arc<dyn MessageHandler>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.handle_new_stream_internal(handler, stream, addr))
    }

    fn stop(&self) {
        self.inner.task_supervisor.stop();
    }

    fn drop_guard(&self) -> DropGuard {
        self.inner.task_supervisor.drop_guard()
    }

    fn join(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.inner.task_supervisor.all_stopped())
    }
}

impl std::fmt::Debug for WebSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketPool").finish()
    }
}
