use std::{io::IoSlice, net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
};
use tokio_util::sync::DropGuard;

use super::TcpSocket;
use crate::{
    ConnectionMap, Message, RawStream, Socket, SocketPoolConfig, SocketPoolTrait, State,
    TaskSupervisor,
    error::{Error, ErrorKind, Result},
};

pub struct TcpSocketPool {
    socket_map: ConnectionMap<TcpSocket>,
    connect_locks: crate::sockets::connect::ConnectLocks,
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

        let _ = self.add_socket(addr, tcp_stream, state)?;
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

    async fn acquire(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket> {
        // The map lock is never held over network I/O. A per-address lock
        // coalesces concurrent misses without blocking unrelated peers.
        if let Some(socket) = self.socket_map.try_get_live(addr) {
            return Ok(socket.into());
        }

        let _connect = self.connect_locks.lock(*addr).await;
        if let Some(socket) = self.socket_map.get_live(addr).await {
            return Ok(socket.into());
        }

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;
        super::configure_stream(&stream);

        let send_socket = self
            .socket_map
            .try_publish_with(*addr, || self.add_socket(*addr, stream, state))
            .await?;
        Ok(send_socket.into())
    }
}

impl TcpSocketPool {
    pub(crate) fn try_acquire(&self, addr: &SocketAddr) -> Option<Socket> {
        self.socket_map.try_get_live(addr).map(Into::into)
    }

    pub(crate) async fn acquire_existing(&self, addr: &SocketAddr) -> Option<Socket> {
        self.socket_map.get_live(addr).await.map(Into::into)
    }

    pub fn new() -> Self {
        Self {
            socket_map: ConnectionMap::default(),
            connect_locks: Default::default(),
            task_supervisor: TaskSupervisor::create(),
        }
    }

    pub fn add_socket(
        &self,
        addr: SocketAddr,
        stream: tokio::net::TcpStream,
        state: &Arc<State>,
    ) -> Result<TcpSocket> {
        let (recv_stream, send_stream) = stream.into_split();
        let (sender, receiver) = mpsc::channel(1024);
        let tcp_socket = TcpSocket::new(sender);
        let task_supervisor = self
            .task_supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "TCP pool stopped".into()))?;
        let recv_task = self
            .task_supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "TCP pool stopped".into()))?;
        state.metrics.connection_opened("TCP");

        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let tcp_socket = tcp_socket.clone();
            let state = state.clone();
            async move {
                let error = tokio::select! {
                    () = task_supervisor.stopped() => {
                        Error::new(ErrorKind::ConnectionClosed, "TCP pool stopped".into())
                    }
                    result = Self::start_send_loop(send_stream, receiver) => result.err()
                        .unwrap_or_else(|| Error::new(
                            ErrorKind::ConnectionClosed,
                            "TCP send loop ended".into(),
                        )),
                };
                tracing::debug!("send loop for {addr} ended: {error}");
                Self::evict_socket(&socket_map, &addr, &tcp_socket, &state, &error).await;
            }
        });

        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let tcp_socket = tcp_socket.clone();
            let state = state.clone();
            async move {
                let error = tokio::select! {
                    () = recv_task.stopped() => {
                        Error::new(ErrorKind::ConnectionClosed, "TCP pool stopped".into())
                    }
                    result = Self::start_recv_loop(recv_stream, tcp_socket.clone(), &state) =>
                        result.err().unwrap_or_else(|| Error::new(
                            ErrorKind::ConnectionClosed,
                            "TCP receive loop ended".into(),
                        )),
                };
                tracing::debug!("receive loop for {addr} ended: {error}");
                Self::evict_socket(&socket_map, &addr, &tcp_socket, &state, &error).await;
            }
        });
        Ok(tcp_socket)
    }

    /// Removes a dead socket from the map (if it is still the mapped one)
    /// and eagerly fails every request pending on the connection.
    async fn evict_socket(
        socket_map: &ConnectionMap<TcpSocket>,
        addr: &SocketAddr,
        socket: &TcpSocket,
        state: &Arc<State>,
        err: &Error,
    ) {
        // The send and recv loops both report failures; run teardown once.
        if !socket.mark_closed() {
            return;
        }
        state.metrics.connection_closed("TCP");
        socket_map.evict_if_current(addr, socket).await;
        let err = Error::new(
            ErrorKind::ConnectionClosed,
            format!("connection to {addr} closed: {err}"),
        );
        state.connection_closed(socket.conn_id(), &err);
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

    #[tokio::test]
    async fn test_tcp_socket_pool_debug_format() {
        let pool = TcpSocketPool::new();
        let debug = format!("{pool:?}");
        assert!(debug.contains("TcpSocketPool"));
    }
}
