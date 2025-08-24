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

use super::WebSocket;
use crate::{
    Message, RawStream, Socket, SocketType, State, TaskSupervisor,
    error::{Error, ErrorKind, Result},
};

pub struct WebSocketPool {
    socket_map: RwLock<HashMap<SocketAddr, WebSocket, RandomState>>,
    task_supervisor: TaskSupervisor,
}

impl WebSocketPool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            socket_map: RwLock::default(),
            task_supervisor: TaskSupervisor::create(),
        })
    }

    pub async fn handle_new_stream(
        self: &Arc<Self>,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        match stream {
            RawStream::TCP(tcp_stream) => {
                let stream = accept_async(tcp_stream)
                    .await
                    .map_err(|e| Error::new(ErrorKind::WebSocketAcceptFailed, e.to_string()))?;
                self.add_socket(addr, stream, state);
            }
            RawStream::WS(web_socket_stream) => {
                self.add_socket(addr, *web_socket_stream, state);
            }
        }
        Ok(())
    }

    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    pub fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    pub async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

    pub async fn acquire(
        self: &Arc<Self>,
        addr: &SocketAddr,
        socket_type: SocketType,
        state: &Arc<State>,
    ) -> Result<WebSocket> {
        if socket_type != SocketType::WS {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("invalid socket type {socket_type} for WebSocketPool"),
            ));
        }

        // Check if the socket is already in the socket map.
        if let Ok(socket_map) = self.socket_map.try_read()
            && let Some(socket) = socket_map.get(addr)
        {
            return Ok(socket.clone());
        }

        // If not, create a new socket and insert it into the socket map.
        let mut socket_map = self.socket_map.write().await;
        if let Some(socket) = socket_map.get(addr) {
            return Ok(socket.clone());
        }

        let (stream, _) = connect_async(format!("ws://{addr}"))
            .await
            .map_err(|e| Error::new(ErrorKind::WebSocketConnectFailed, e.to_string()))?;

        let send_socket = self.add_socket(*addr, stream, state);
        socket_map.insert(*addr, send_socket.clone());
        Ok(send_socket)
    }

    pub fn add_socket<S>(
        self: &Arc<Self>,
        addr: SocketAddr,
        stream: WebSocketStream<S>,
        state: &Arc<State>,
    ) -> WebSocket
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (send_stream, recv_stream) = stream.split();
        let (sender, receiver) = mpsc::channel(1024);
        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn({
            async move {
                tokio::select! {
                    () = task_supervisor.stopped() => {},
                    _ = Self::start_send_loop(send_stream, receiver) => {}
                }
            }
        });

        let web_socket = WebSocket::new(sender);
        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn({
            let this = self.clone();
            let web_socket = web_socket.clone();
            let state = state.clone();
            async move {
                tokio::select! {
                    () = task_supervisor.stopped() => {},
                    r = Self::start_recv_loop(recv_stream, web_socket, &state) => {
                        if let Err(e) = r {
                            tracing::error!("recv loop for {addr} failed: {e}");
                            let mut socket_map = this.socket_map.write().await;
                            socket_map.remove(&addr);
                        }
                    }
                }
            }
        });
        web_socket
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
        while let Some(msg) = recv_stream.next().await {
            let msg = msg.map_err(|e| Error::new(ErrorKind::WebSocketRecvFailed, e.to_string()))?;
            match msg {
                tungstenite::Message::Binary(bytes) => {
                    let msg = Message::parse(bytes)?;
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
