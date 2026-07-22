use std::{collections::HashMap, io::IoSlice, net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use foldhash::fast::RandomState;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{RwLock, mpsc},
};
use tokio_util::sync::DropGuard;

use super::TcpSocket;
use crate::{
    Message, RawStream, Socket, SocketPoolConfig, SocketPoolTrait, SocketType, State,
    TaskSupervisor,
    error::{Error, ErrorKind, Result},
};

pub struct TcpSocketPool {
    socket_map: Arc<RwLock<HashMap<SocketAddr, TcpSocket, RandomState>>>,
    task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for TcpSocketPool {
    fn create(
        _config: &SocketPoolConfig,
        _devices: &Arc<crate::Devices>,
        _buffer_pool: &Arc<crate::BufferPool>,
    ) -> Result<Self> {
        Ok(Self::new())
    }

    async fn handle_new_stream(
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

        let _ = self.add_socket(addr, tcp_stream, state);
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
        if socket_type != SocketType::TCP {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("invalid socket type {socket_type} for TcpSocketPool"),
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

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;
        super::configure_stream(&stream);

        let send_socket = self.add_socket(*addr, stream, state);
        socket_map.insert(*addr, send_socket.clone());
        Ok(send_socket.into())
    }
}

impl TcpSocketPool {
    pub fn new() -> Self {
        Self {
            socket_map: Arc::default(),
            task_supervisor: TaskSupervisor::create(),
        }
    }

    pub fn add_socket(
        &self,
        addr: SocketAddr,
        stream: tokio::net::TcpStream,
        state: &Arc<State>,
    ) -> TcpSocket {
        let (recv_stream, send_stream) = stream.into_split();
        let (sender, receiver) = mpsc::channel(1024);
        let tcp_socket = TcpSocket::new(sender);

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let tcp_socket = tcp_socket.clone();
            let state = state.clone();
            async move {
                tokio::select! {
                    () = task_supervisor.stopped() => {},
                    r = Self::start_send_loop(send_stream, receiver) => {
                        if let Err(e) = r {
                            tracing::error!("send loop for {addr} failed: {e}");
                            Self::evict_socket(&socket_map, &addr, &tcp_socket, &state, &e).await;
                        }
                    }
                }
            }
        });

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let tcp_socket = tcp_socket.clone();
            let state = state.clone();
            async move {
                tokio::select! {
                    () = task_supervisor.stopped() => {},
                    r = Self::start_recv_loop(recv_stream, tcp_socket.clone(), &state) => {
                        let e = r.err().unwrap_or_else(|| {
                            Error::new(ErrorKind::ConnectionClosed, "connection closed".into())
                        });
                        tracing::error!("recv loop for {addr} failed: {e}");
                        Self::evict_socket(&socket_map, &addr, &tcp_socket, &state, &e).await;
                    }
                }
            }
        });
        tcp_socket
    }

    /// Removes a dead socket from the map (if it is still the mapped one)
    /// and eagerly fails every request pending on the connection.
    async fn evict_socket(
        socket_map: &Arc<RwLock<HashMap<SocketAddr, TcpSocket, RandomState>>>,
        addr: &SocketAddr,
        socket: &TcpSocket,
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

    fn parse_message(buffer: &mut BytesMut) -> Result<Option<Bytes>> {
        super::parse_message(buffer)
    }

    async fn start_recv_loop(
        mut recv_stream: OwnedReadHalf,
        tcp_socket: TcpSocket,
        state: &Arc<State>,
    ) -> Result<()> {
        let mut buffer = BytesMut::with_capacity(1 << 20);
        let socket = Socket::TCP(tcp_socket);
        loop {
            if let Some(bytes) = Self::parse_message(&mut buffer)? {
                let msg = Message::parse(bytes)?;
                state.handle_recv(&socket, msg)?;
            } else {
                let n = recv_stream
                    .read_buf(&mut buffer)
                    .await
                    .map_err(|e| Error::new(ErrorKind::TcpRecvMsgFailed, e.to_string()))?;
                if n == 0 {
                    return Err(Error::new(
                        ErrorKind::TcpRecvMsgFailed,
                        "socket eof".to_string(),
                    ));
                }
            }
        }
    }

    async fn start_send_loop(
        mut send_stream: OwnedWriteHalf,
        mut receiver: mpsc::Receiver<Bytes>,
    ) -> Result<()> {
        const LIMIT: usize = 64;
        let mut msgs = Vec::with_capacity(LIMIT);
        loop {
            let mut bufs = [IoSlice::new(&[]); LIMIT];

            let n = receiver.recv_many(&mut msgs, LIMIT).await;
            if n == 0 {
                return Ok(());
            }

            for (msg, io_slice) in msgs.iter().zip(&mut bufs) {
                *io_slice = IoSlice::new(msg);
            }

            let mut slices = &mut bufs[..msgs.len()];
            while !slices.is_empty() {
                match send_stream.write_vectored(slices).await {
                    Ok(n) => {
                        IoSlice::advance_slices(&mut slices, n);
                    }
                    Err(e) => {
                        return Err(Error::new(ErrorKind::TcpSendMsgFailed, e.to_string()));
                    }
                }
            }
            msgs.clear();
        }
    }
}

impl std::fmt::Debug for TcpSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocketPool").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SocketType, State};

    async fn make_state() -> Arc<State> {
        let (state, _guard) = State::create(
            crate::Router::default(),
            &crate::SocketPoolConfig {
                socket_type: SocketType::TCP,
                ..Default::default()
            },
        )
        .unwrap();
        state
    }

    #[tokio::test]
    async fn test_acquire_wrong_socket_type_returns_err() {
        let pool = TcpSocketPool::new();
        let state = make_state().await;
        let addr = "127.0.0.1:9999".parse().unwrap();
        // Asking for a WS socket from a TCP pool is invalid.
        let result = pool.acquire(&addr, SocketType::WS, &state).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().kind,
            crate::error::ErrorKind::InvalidArgument
        ));
    }

    #[tokio::test]
    async fn test_tcp_socket_pool_debug_format() {
        let pool = TcpSocketPool::new();
        let debug = format!("{pool:?}");
        assert!(debug.contains("TcpSocketPool"));
    }
}
