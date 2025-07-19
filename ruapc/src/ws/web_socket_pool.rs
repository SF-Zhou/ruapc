use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use foldhash::fast::RandomState;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    net::TcpStream,
    sync::{RwLock, mpsc},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_async, connect_async, tungstenite::Message,
};
use tokio_util::sync::{CancellationToken, DropGuard};

use super::WebSocket;
use crate::{
    RecvMsg, Socket, State,
    error::{Error, ErrorKind, Result},
};

type Stream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct WebSocketPool {
    socket_map: RwLock<HashMap<SocketAddr, WebSocket, RandomState>>,
    running: AtomicUsize,
    stop_token: CancellationToken,
    stopped_token: CancellationToken,
}

impl WebSocketPool {
    pub fn new() -> Arc<Self> {
        let this = Arc::new(Self {
            socket_map: RwLock::default(),
            running: AtomicUsize::new(1),
            stop_token: CancellationToken::default(),
            stopped_token: CancellationToken::default(),
        });

        tokio::spawn({
            let this = this.clone();
            async move {
                this.stop_token.cancelled().await;
                this.finish_one_task();
            }
        });

        this
    }

    pub async fn start_listen(
        self: &Arc<Self>,
        addr: SocketAddr,
        state: &Arc<State>,
    ) -> Result<SocketAddr> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpBindFailed, e.to_string()))?;
        let listener_addr = listener
            .local_addr()
            .map_err(|e| Error::new(ErrorKind::TcpBindFailed, e.to_string()))?;
        let this = self.clone();
        let state = state.clone();

        this.running.fetch_add(1, Ordering::AcqRel);
        tokio::spawn(async move {
            tokio::select! {
                () = this.stop_token.cancelled() => {
                    tracing::info!("stop accept loop");
                }
                () = async {
                    tracing::info!("start listening: ws://{listener_addr}");
                    while let Ok((stream, addr)) = listener.accept().await {
                        if let Ok(stream) = accept_async(MaybeTlsStream::Plain(stream)).await {
                            let _ = this.add_socket(addr, stream, &state);
                        }
                    }
                } => {}
            }
            this.finish_one_task();
        });

        Ok(listener_addr)
    }

    pub fn stop(&self) {
        self.stop_token.cancel();
    }

    pub fn drop_guard(&self) -> DropGuard {
        self.stop_token.clone().drop_guard()
    }

    pub async fn join(&self) {
        self.stopped_token.cancelled().await;
    }

    pub async fn acquire(
        self: &Arc<Self>,
        addr: &SocketAddr,
        state: &Arc<State>,
    ) -> Result<WebSocket> {
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

    pub fn add_socket(
        self: &Arc<Self>,
        addr: SocketAddr,
        stream: Stream,
        state: &Arc<State>,
    ) -> WebSocket {
        let (send_stream, recv_stream) = stream.split();
        let (sender, receiver) = mpsc::channel(1024);
        self.running.fetch_add(1, Ordering::AcqRel);
        tokio::spawn({
            let this = self.clone();
            async move {
                tokio::select! {
                    () = this.stop_token.cancelled() => {},
                    _ = Self::start_send_loop(send_stream, receiver) => {}
                }
                this.finish_one_task();
            }
        });
        let web_socket = WebSocket::new(sender, state.waiter.clone());
        self.running.fetch_add(1, Ordering::AcqRel);
        tokio::spawn({
            let this = self.clone();
            let web_socket = web_socket.clone();
            let state = state.clone();
            async move {
                tokio::select! {
                    () = this.stop_token.cancelled() => {},
                    r = Self::start_recv_loop(recv_stream, web_socket, &state) => {
                        if let Err(e) = r {
                            tracing::error!("recv loop for {addr} failed: {e}");
                            let mut socket_map = this.socket_map.write().await;
                            socket_map.remove(&addr);
                        }
                    }
                }
                this.finish_one_task();
            }
        });
        web_socket
    }

    async fn start_recv_loop(
        mut recv_stream: SplitStream<Stream>,
        web_socket: WebSocket,
        state: &Arc<State>,
    ) -> Result<()> {
        let socket = Socket::WS(web_socket);
        while let Some(msg) = recv_stream.next().await {
            let msg = msg.map_err(|e| Error::new(ErrorKind::WebSocketRecvFailed, e.to_string()))?;
            match msg {
                Message::Binary(bytes) => {
                    let msg = RecvMsg::parse(&bytes)?;
                    state.handle_recv(&socket, msg)?;
                }
                Message::Close(_) => {
                    return Err(Error::kind(ErrorKind::WebSocketClosed));
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn start_send_loop(
        mut send_stream: SplitSink<Stream, Message>,
        mut receiver: mpsc::Receiver<Bytes>,
    ) -> Result<()> {
        while let Some(bytes) = receiver.recv().await {
            send_stream
                .send(Message::Binary(bytes))
                .await
                .map_err(|e| Error::new(ErrorKind::WebSocketSendFailed, e.to_string()))?;
        }
        Ok(())
    }

    fn finish_one_task(&self) {
        let running = self.running.fetch_sub(1, Ordering::AcqRel) - 1;
        if running == 0 {
            self.stopped_token.cancel();
        }
    }
}

impl std::fmt::Debug for WebSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketPool").finish()
    }
}
