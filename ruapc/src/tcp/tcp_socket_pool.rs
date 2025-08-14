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

use super::TcpSocket;
use crate::{
    RecvMsg, Socket, State, TaskSupervisor,
    error::{Error, ErrorKind, Result},
};

pub struct TcpSocketPool {
    socket_map: RwLock<HashMap<SocketAddr, TcpSocket, RandomState>>,
    task_supervisor: TaskSupervisor,
}

impl TcpSocketPool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            socket_map: RwLock::default(),
            task_supervisor: TaskSupervisor::create(),
        })
    }

    pub fn handle_new_tcp_stream(
        self: &Arc<Self>,
        state: &Arc<State>,
        tcp_stream: TcpStream,
        addr: SocketAddr,
    ) {
        let _ = self.add_socket(addr, tcp_stream, state);
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
        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn({
            async move {
                tokio::select! {
                    () = task_supervisor.stopped() => {},
                    _ = Self::start_send_loop(send_stream, receiver) => {}
                }
            }
        });

        let tcp_socket = TcpSocket::new(sender);
        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn({
            let this = self.clone();
            let tcp_socket = tcp_socket.clone();
            let state = state.clone();
            async move {
                tokio::select! {
                    () = task_supervisor.stopped() => {},
                    r = Self::start_recv_loop(recv_stream, tcp_socket, &state) => {
                        if let Err(e) = r {
                            tracing::error!("recv loop for {addr} failed: {e}");
                            let mut socket_map = this.socket_map.write().await;
                            socket_map.remove(&addr);
                        }
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
                let msg = RecvMsg::parse(bytes)?;
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
