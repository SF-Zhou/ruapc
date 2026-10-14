use std::{io::IoSlice, net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, tcp::OwnedReadHalf},
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
        self.acquire_until(addr, state, None).await
    }
}

impl TcpSocketPool {
    pub(crate) async fn acquire_until(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
        deadline: Option<std::time::Instant>,
    ) -> Result<Socket> {
        let acquire = self.acquire_inner(addr, state, deadline);
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), acquire)
                .await
                .map_err(|_| {
                    Error::new(
                        ErrorKind::Timeout,
                        "TCP acquisition deadline expired".into(),
                    )
                })?,
            None => acquire.await,
        }
    }

    async fn acquire_inner(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
        deadline: Option<std::time::Instant>,
    ) -> Result<Socket> {
        // The map lock is never held over network I/O. A per-address lock
        // coalesces concurrent misses without blocking unrelated peers.
        if let Some(socket) = self.socket_map.try_get_live(addr) {
            return Ok(socket.into());
        }

        let _connect = self.connect_locks.lock(*addr).await;
        if let Some(socket) = self.socket_map.get_live(addr).await {
            return Ok(socket.into());
        }

        // Reaching a deadline while waiting for local ownership is not remote
        // failure evidence. Only the actual connect operation below can tag it.
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(Error::new(
                ErrorKind::Timeout,
                "connection deadline expired before connect".into(),
            ));
        }
        let connecting = TcpStream::connect(addr);
        let result = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), connecting)
                .await
                .map_err(|_| {
                    Error::new(ErrorKind::Timeout, "TCP connect deadline expired".into())
                        .with_local_failure(crate::LocalFailure::TcpConnect(
                            std::io::ErrorKind::TimedOut,
                        ))
                })?,
            None => connecting.await,
        };
        let stream = result.map_err(|e| {
            Error::new(ErrorKind::TcpConnectFailed, e.to_string())
                .with_local_failure(crate::LocalFailure::TcpConnect(e.kind()))
        })?;
        super::configure_stream(&stream);

        let send_socket = self
            .socket_map
            .try_publish_with(*addr, || self.add_socket(*addr, stream, state))
            .await?;
        Ok(send_socket.into())
    }

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
                    () = tcp_socket.closed() => Error::new(ErrorKind::ConnectionClosed, "TCP connection closed".into()),
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
                    () = tcp_socket.closed() => Error::new(ErrorKind::ConnectionClosed, "TCP connection closed".into()),
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
        crate::msg::frame::parse_message(buffer)
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

    async fn start_send_loop<W: AsyncWrite + Unpin>(
        mut send_stream: W,
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
                    Ok(0) => {
                        return Err(Error::new(
                            ErrorKind::TcpSendMsgFailed,
                            "TCP write zero".into(),
                        ));
                    }
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
    use std::time::Duration;

    #[tokio::test]
    async fn receive_failure_stops_idle_send_loop() {
        let context = crate::Context::create(&SocketPoolConfig::default()).unwrap();
        let pool = TcpSocketPool::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let socket = pool.acquire(&address, &context.state).await.unwrap();
        let (mut remote, _) = listener.accept().await.unwrap();

        // Keep the peer and caller's socket open. A parse failure must close
        // the otherwise-idle sender without waiting for another queued frame.
        remote.write_all(b"BAD!\0\0\0\0").await.unwrap();
        let mut byte = [0];
        let read = tokio::time::timeout(Duration::from_secs(1), remote.read(&mut byte))
            .await
            .expect("receive failure must cancel the send loop")
            .unwrap();
        assert_eq!(read, 0);
        let Socket::TCP(socket) = socket else {
            panic!("TCP expected")
        };
        assert!(socket.is_closed());
        assert!(pool.acquire_existing(&address).await.is_none());
        pool.stop();
        pool.join().await;
    }

    #[tokio::test]
    async fn zero_byte_write_fails_instead_of_retrying() {
        use std::{
            pin::Pin,
            task::{Context, Poll},
        };

        struct PartialThenZero {
            writes: usize,
        }

        impl AsyncWrite for PartialThenZero {
            fn poll_write(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                bytes: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                self.writes += 1;
                match self.writes {
                    1 => {
                        assert_eq!(bytes, b"hello");
                        Poll::Ready(Ok(2))
                    }
                    2 => {
                        assert_eq!(bytes, b"llo");
                        Poll::Ready(Ok(0))
                    }
                    _ => panic!("a zero-byte write must not be retried"),
                }
            }

            fn poll_flush(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let (sender, receiver) = mpsc::channel(1);
        sender.send(Bytes::from_static(b"hello")).await.unwrap();
        let mut writer = PartialThenZero { writes: 0 };

        // This writer makes partial progress, then reports a zero-byte write
        // while the rest of the frame is still queued.
        let error = TcpSocketPool::start_send_loop(&mut writer, receiver)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::TcpSendMsgFailed);
        assert_eq!(writer.writes, 2);
        assert!(sender.is_closed());
    }

    #[tokio::test]
    async fn concurrent_outbound_cache_misses_share_one_tcp_connection() {
        let config = SocketPoolConfig::default();
        let context = crate::Context::create(&config).unwrap();
        let pool = Arc::new(
            TcpSocketPool::create(&config, &context.state.devices, &context.state.buffer_pool)
                .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut calls = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let pool = pool.clone();
            let state = context.state.clone();
            calls.spawn(async move { pool.acquire(&address, &state).await.unwrap() });
        }
        let mut connections = Vec::new();
        while let Some(result) = calls.join_next().await {
            connections.push(result.unwrap());
        }
        let (remote, _) =
            tokio::time::timeout(std::time::Duration::from_secs(1), listener.accept())
                .await
                .unwrap()
                .unwrap();
        let Socket::TCP(first) = &connections[0] else {
            panic!("TCP expected")
        };
        for socket in &connections[1..] {
            let Socket::TCP(socket) = socket else {
                panic!("TCP expected")
            };
            assert!(first.same_socket(socket));
        }
        pool.stop();
        tokio::time::timeout(std::time::Duration::from_secs(1), pool.join())
            .await
            .unwrap();
        drop((connections, remote));
    }

    #[tokio::test]
    async fn test_tcp_socket_pool_debug_format() {
        let pool = TcpSocketPool::new();
        let debug = format!("{pool:?}");
        assert!(debug.contains("TcpSocketPool"));
    }
}

#[cfg(test)]
mod local_connect_failure_tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn local_connect_lock_wait_is_never_network_timeout_evidence() {
        let context = crate::Context::create(&SocketPoolConfig::default()).unwrap();
        let pool = TcpSocketPool::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let held = pool.connect_locks.lock(address).await;
        let deadline = Instant::now() + Duration::from_millis(20);
        let error = pool
            .acquire_until(&address, &context.state, Some(deadline))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(error.local_failure(), None);
        drop(held);
        let expired = pool
            .acquire_until(&address, &context.state, Some(deadline))
            .await
            .err()
            .unwrap();
        assert_eq!(expired.kind, ErrorKind::Timeout);
        assert_eq!(expired.local_failure(), None);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), listener.accept())
                .await
                .is_err()
        );
    }
}
