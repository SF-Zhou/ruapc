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
    sockets::ConnectGate,
};

pub struct TcpSocketPool {
    socket_map: Arc<RwLock<HashMap<SocketAddr, TcpSocket, RandomState>>>,
    connect_gate: ConnectGate,
    task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for TcpSocketPool {
    fn create(
        config: &SocketPoolConfig,
        _devices: &Arc<crate::Devices>,
        _buffer_pool: &Arc<crate::BufferPool>,
    ) -> Result<Self> {
        Ok(Self::with_config(config))
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

        // Serialize connects per address (never pool-wide): concurrent
        // acquires for one address dial once, while a stalled connect to a
        // dead peer cannot block acquires for other addresses.
        let permit = self.connect_gate.lock(addr).await;
        if let Some(socket) = self.socket_map.read().await.get(addr) {
            return Ok(socket.into());
        }

        let stream = self
            .connect_gate
            .with_timeout(addr, ErrorKind::TcpConnectFailed, async {
                TcpStream::connect(addr)
                    .await
                    .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))
            })
            .await?;
        super::configure_stream(&stream);

        // Insert into the map *before* spawning the IO loops: eviction
        // (identity-checked) must always observe the entry, otherwise an
        // instantly failing connection could leave a dead socket mapped.
        let (send_socket, receiver) = Self::make_socket(state);
        self.socket_map
            .write()
            .await
            .insert(*addr, send_socket.clone());
        self.spawn_io_loops(*addr, stream, send_socket.clone(), receiver, state);
        drop(permit);
        Ok(send_socket.into())
    }
}

impl TcpSocketPool {
    pub fn new() -> Self {
        Self::with_config(&SocketPoolConfig::default())
    }

    fn with_config(config: &SocketPoolConfig) -> Self {
        Self {
            socket_map: Arc::default(),
            connect_gate: ConnectGate::new(config.connect_timeout_ms),
            task_supervisor: TaskSupervisor::create(),
        }
    }

    pub fn add_socket(
        &self,
        addr: SocketAddr,
        stream: tokio::net::TcpStream,
        state: &Arc<State>,
    ) -> TcpSocket {
        let (tcp_socket, receiver) = Self::make_socket(state);
        self.spawn_io_loops(addr, stream, tcp_socket.clone(), receiver, state);
        tcp_socket
    }

    fn make_socket(state: &Arc<State>) -> (TcpSocket, mpsc::Receiver<Bytes>) {
        let (sender, receiver) = mpsc::channel(1024);
        let tcp_socket = TcpSocket::new(sender);
        state.metrics.connection_opened("TCP");
        (tcp_socket, receiver)
    }

    fn spawn_io_loops(
        &self,
        addr: SocketAddr,
        stream: tokio::net::TcpStream,
        tcp_socket: TcpSocket,
        receiver: mpsc::Receiver<Bytes>,
        state: &Arc<State>,
    ) {
        let (recv_stream, send_stream) = stream.into_split();

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
        // The send and recv loops both report failures; run teardown once.
        if !socket.mark_closed() {
            return;
        }
        state.metrics.connection_closed("TCP");
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

    #[tokio::test]
    async fn test_acquire_connect_timeout_bounds_unreachable_addr() {
        // 203.0.113.0/24 (TEST-NET-3) is reserved and typically
        // blackholed: without a connect timeout this would stall for the
        // OS SYN-retry limit (minutes). Environments that instead reject
        // instantly still produce a connect error, just faster.
        let pool = TcpSocketPool::with_config(&crate::SocketPoolConfig {
            socket_type: SocketType::TCP,
            connect_timeout_ms: 200,
            ..Default::default()
        });
        let state = make_state().await;
        let addr = "203.0.113.1:9".parse().unwrap();

        let started = std::time::Instant::now();
        let err = pool
            .acquire(&addr, SocketType::TCP, &state)
            .await
            .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::TcpConnectFailed);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "connect must fail in bounded time, took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn test_stalled_connect_does_not_block_other_addrs() {
        // A hanging connect to a dead address must not block acquires for
        // other addresses (per-address gating, no pool-wide lock across
        // connect).
        let pool = Arc::new(TcpSocketPool::with_config(&crate::SocketPoolConfig {
            socket_type: SocketType::TCP,
            connect_timeout_ms: 2_000,
            ..Default::default()
        }));
        let state = make_state().await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });

        // Start a connect to a (likely blackholed) dead address.
        let dead: SocketAddr = "203.0.113.1:9".parse().unwrap();
        let dead_task = tokio::spawn({
            let (pool, state) = (pool.clone(), state.clone());
            async move { pool.acquire(&dead, SocketType::TCP, &state).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The live address must connect promptly meanwhile.
        let started = std::time::Instant::now();
        pool.acquire(&live_addr, SocketType::TCP, &state)
            .await
            .unwrap();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "live acquire blocked for {:?}",
            started.elapsed()
        );
        let _ = dead_task.await.unwrap();
    }
}
