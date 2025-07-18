use std::{
    collections::HashMap,
    io::IoSlice,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

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
use tokio_util::sync::{CancellationToken, DropGuard};

use super::TcpSocket;
use crate::{
    RecvMsg, Socket, State,
    error::{Error, ErrorKind, Result},
};

pub struct TcpSocketPool {
    socket_map: RwLock<HashMap<SocketAddr, TcpSocket, RandomState>>,
    running: AtomicUsize,
    stop_token: CancellationToken,
    stopped_token: CancellationToken,
}

impl TcpSocketPool {
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
                    tracing::info!("start listening: {listener_addr}");
                    while let Ok((stream, addr)) = listener.accept().await {
                        let _ = this.add_socket(addr, stream, &state);
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
    ) -> Result<TcpSocket> {
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

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;

        let send_socket = self.add_socket(*addr, stream, state);
        socket_map.insert(*addr, send_socket.clone());
        Ok(send_socket)
    }

    pub fn add_socket(
        self: &Arc<Self>,
        addr: SocketAddr,
        stream: tokio::net::TcpStream,
        state: &Arc<State>,
    ) -> TcpSocket {
        let (recv_stream, send_stream) = stream.into_split();
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
        let tcp_socket = TcpSocket::new(sender, state.waiter.clone());
        self.running.fetch_add(1, Ordering::AcqRel);
        tokio::spawn({
            let this = self.clone();
            let tcp_socket = tcp_socket.clone();
            let state = state.clone();
            async move {
                tokio::select! {
                    () = this.stop_token.cancelled() => {},
                    r = Self::start_recv_loop(recv_stream, tcp_socket, &state) => {
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
        tcp_socket
    }

    fn parse_message(buffer: &mut BytesMut) -> Result<Option<Bytes>> {
        const S: usize = std::mem::size_of::<u64>();
        if buffer.len() < S {
            return Ok(None);
        }
        let header = u64::from_be_bytes(buffer[..S].try_into().unwrap());
        if (header >> 32) as u32 != super::MAGIC_NUM {
            return Err(Error::new(
                ErrorKind::TcpParseMsgFailed,
                format!("invalid header: {header:08X}"),
            ));
        }

        let len = usize::try_from(header & u64::from(u32::MAX))?;
        if S + len >= super::MAX_MSG_SIZE {
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
        state: &Arc<State>,
    ) -> Result<()> {
        let mut buffer = bytes::BytesMut::with_capacity(1 << 20);
        let socket = Socket::Tcp(tcp_socket);
        loop {
            if let Some(bytes) = Self::parse_message(&mut buffer)? {
                let msg = RecvMsg::parse(&bytes)?;
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

    fn finish_one_task(&self) {
        let running = self.running.fetch_sub(1, Ordering::AcqRel) - 1;
        if running == 0 {
            self.stopped_token.cancel();
        }
    }
}

impl std::fmt::Debug for TcpSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocketPool").finish()
    }
}
