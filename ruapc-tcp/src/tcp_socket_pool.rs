use std::{collections::HashMap, io::IoSlice, net::SocketAddr, sync::Arc};

use bytes::{Buf, Bytes, BytesMut};
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

use crate::{
    BoxFuture, Error, ErrorKind, Message, MessageHandler, RawStream, Result, RuapcSocket,
    RuapcSocketPool, SocketType, TaskSupervisor, TcpSocket,
};

/// Inner state for the TCP socket pool.
struct TcpSocketPoolInner {
    socket_map: RwLock<HashMap<SocketAddr, TcpSocket, RandomState>>,
    task_supervisor: TaskSupervisor,
}

/// TCP socket pool for managing TCP connections.
///
/// This pool manages a collection of TCP sockets, handling:
/// - Connection pooling and reuse
/// - Background tasks for sending and receiving
/// - Graceful shutdown
#[derive(Clone)]
pub struct TcpSocketPool {
    inner: Arc<TcpSocketPoolInner>,
}

impl TcpSocketPool {
    /// Creates a new TCP socket pool.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TcpSocketPoolInner {
                socket_map: RwLock::default(),
                task_supervisor: TaskSupervisor::create(),
            }),
        }
    }

    /// Adds a new socket connection.
    pub fn add_socket(
        &self,
        addr: SocketAddr,
        stream: TcpStream,
        handler: Arc<dyn MessageHandler>,
    ) -> TcpSocket {
        let (recv_stream, send_stream) = stream.into_split();
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
        let tcp_socket = TcpSocket::new(sender);
        let task_guard = self.inner.task_supervisor.start_async_task();
        let inner = self.inner.clone();
        let tcp_socket_clone = tcp_socket.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = task_guard.stopped() => {},
                r = Self::start_recv_loop(recv_stream, tcp_socket_clone, handler) => {
                    if let Err(e) = r {
                        tracing::error!("recv loop for {addr} failed: {e}");
                        let mut socket_map = inner.socket_map.write().await;
                        socket_map.remove(&addr);
                    }
                }
            }
        });

        tcp_socket
    }

    fn parse_message(buffer: &mut BytesMut) -> Result<Option<Bytes>> {
        const S: usize = std::mem::size_of::<u64>();
        if buffer.len() < S {
            return Ok(None);
        }
        let header = u64::from_be_bytes(buffer[..S].try_into().unwrap());
        if (header >> 32) as u32 != crate::MAGIC_NUM {
            return Err(Error::new(
                ErrorKind::TcpParseMsgFailed,
                format!("invalid header: {header:08X}"),
            ));
        }

        let len = usize::try_from(header & u64::from(u32::MAX))?;
        if S + len >= crate::MAX_MSG_SIZE {
            return Err(Error::new(
                ErrorKind::TcpParseMsgFailed,
                format!("msg is too long: {len}"),
            ));
        }

        if buffer.len() < S + len {
            Ok(None)
        } else {
            buffer.advance(S);
            Ok(Some(buffer.split_to(len).into()))
        }
    }

    async fn start_recv_loop(
        mut recv_stream: OwnedReadHalf,
        tcp_socket: TcpSocket,
        handler: Arc<dyn MessageHandler>,
    ) -> Result<()> {
        let mut buffer = BytesMut::with_capacity(1 << 20);
        let socket: Box<dyn RuapcSocket> = Box::new(tcp_socket);
        loop {
            if let Some(bytes) = Self::parse_message(&mut buffer)? {
                let msg = Message::parse(bytes)?;
                handler.handle_recv(socket.as_ref(), msg)?;
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

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;

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
        let RawStream::TCP(tcp_stream) = stream else {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "invalid socket type".into(),
            ));
        };

        let _ = self.add_socket(addr, tcp_stream, handler);
        Ok(())
    }
}

impl Default for TcpSocketPool {
    fn default() -> Self {
        Self::new()
    }
}

impl RuapcSocketPool for TcpSocketPool {
    fn socket_type(&self) -> SocketType {
        SocketType::TCP
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

impl std::fmt::Debug for TcpSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocketPool").finish()
    }
}
