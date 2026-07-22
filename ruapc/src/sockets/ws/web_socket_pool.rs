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
use tokio_tungstenite::{
    WebSocketStream, accept_async_with_config, client_async_with_config, tungstenite,
};
use tokio_util::sync::DropGuard;

use super::WebSocket;
use crate::{
    Message, RawStream, Socket, SocketPoolConfig, SocketPoolTrait, SocketType, State,
    TaskSupervisor,
    error::{Error, ErrorKind, Result},
    sockets::tcp,
};

/// WebSocket protocol limits aligned with the TCP transport's
/// [`MAX_MSG_SIZE`](crate::sockets::tcp::MAX_MSG_SIZE).
pub(crate) fn web_socket_config() -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(tcp::MAX_MSG_SIZE))
        .max_frame_size(Some(16 << 20))
}

pub struct WebSocketPool {
    socket_map: Arc<RwLock<HashMap<SocketAddr, WebSocket, RandomState>>>,
    task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for WebSocketPool {
    fn create(
        _config: &SocketPoolConfig,
        _devices: &Arc<crate::Devices>,
        _buffer_pool: &Arc<crate::BufferPool>,
    ) -> Result<Self> {
        Ok(Self {
            socket_map: Arc::default(),
            task_supervisor: TaskSupervisor::create(),
        })
    }

    async fn handle_new_stream(
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
                self.add_socket(addr, stream, state);
            }
            RawStream::WS(web_socket_stream) => {
                self.add_socket(addr, *web_socket_stream, state);
            }
        }
        Ok(())
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
            return Ok(socket.into());
        }

        // If not, create a new socket and insert it into the socket map.
        let mut socket_map = self.socket_map.write().await;
        if let Some(socket) = socket_map.get(addr) {
            return Ok(socket.into());
        }

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

        let send_socket = self.add_socket(*addr, stream, state);
        socket_map.insert(*addr, send_socket.clone());
        Ok(send_socket.into())
    }
}

impl WebSocketPool {
    pub fn add_socket<S>(
        &self,
        addr: SocketAddr,
        stream: WebSocketStream<S>,
        state: &Arc<State>,
    ) -> WebSocket
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (send_stream, recv_stream) = stream.split();
        let (sender, receiver) = mpsc::channel(1024);
        let web_socket = WebSocket::new(sender);

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let web_socket = web_socket.clone();
            let state = state.clone();
            async move {
                tokio::select! {
                    () = task_supervisor.stopped() => {},
                    r = Self::start_send_loop(send_stream, receiver) => {
                        if let Err(e) = r {
                            tracing::error!("send loop for {addr} failed: {e}");
                            Self::evict_socket(&socket_map, &addr, &web_socket, &state, &e).await;
                        }
                    }
                }
            }
        });

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let web_socket = web_socket.clone();
            let state = state.clone();
            async move {
                tokio::select! {
                    () = task_supervisor.stopped() => {},
                    r = Self::start_recv_loop(recv_stream, web_socket.clone(), &state) => {
                        let e = r.err().unwrap_or_else(|| {
                            Error::new(ErrorKind::ConnectionClosed, "connection closed".into())
                        });
                        tracing::error!("recv loop for {addr} failed: {e}");
                        Self::evict_socket(&socket_map, &addr, &web_socket, &state, &e).await;
                    }
                }
            }
        });
        web_socket
    }

    /// Removes a dead socket from the map (if it is still the mapped one)
    /// and eagerly fails every request pending on the connection.
    async fn evict_socket(
        socket_map: &Arc<RwLock<HashMap<SocketAddr, WebSocket, RandomState>>>,
        addr: &SocketAddr,
        socket: &WebSocket,
        state: &Arc<State>,
        err: &Error,
    ) {
        {
            let mut socket_map = socket_map.write().await;
            // Identity check: don't evict a replacement connection.
            if let Some(existing) = socket_map.get(addr)
                && existing.same_socket(socket)
            {
                socket_map.remove(addr);
            }
        }
        let err = Error::new(
            ErrorKind::ConnectionClosed,
            format!("connection to {addr} closed: {err}"),
        );
        state.waiter.fail_connection(socket.conn_id(), &err);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_web_socket_pool_debug_format() {
        let config = crate::SocketPoolConfig {
            socket_type: crate::SocketType::WS,
            ..Default::default()
        };
        let devices = Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = WebSocketPool::create(&config, &devices, &buffer_pool).unwrap();
        let debug = format!("{pool:?}");
        assert!(debug.contains("WebSocketPool"));
        // Exercise stop / drop_guard / join.
        pool.stop();
        drop(pool.drop_guard());
        pool.join().await;
    }

    #[tokio::test]
    async fn test_web_socket_pool_acquire_wrong_type_returns_err() {
        let config = crate::SocketPoolConfig {
            socket_type: crate::SocketType::WS,
            ..Default::default()
        };
        let devices = Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = WebSocketPool::create(&config, &devices, &buffer_pool).unwrap();

        let (state, _guard) = crate::State::create(
            crate::Router::default(),
            &crate::SocketPoolConfig {
                socket_type: crate::SocketType::TCP,
                ..Default::default()
            },
        )
        .unwrap();
        let addr = "127.0.0.1:9999".parse().unwrap();
        let result = pool.acquire(&addr, crate::SocketType::TCP, &state).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().kind,
            crate::error::ErrorKind::InvalidArgument
        ));
    }
}
