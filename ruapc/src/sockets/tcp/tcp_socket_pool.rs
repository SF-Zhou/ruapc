use std::{collections::VecDeque, io::IoSlice, net::SocketAddr, sync::Arc};

use bytes::{Buf, BufMut, BytesMut};
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
    ConnectionMap, Endpoint, Message, Socket, State, TaskSupervisor,
    error::{Error, ErrorKind, Result},
    msg::frame::{self, Frame, Parsed},
    remote_memory::IncomingData,
    sockets::{Outbound, connect::ConnectGroups},
};

pub struct TcpSocketPool {
    socket_map: ConnectionMap<TcpSocket>,
    connect_groups: ConnectGroups,
    task_supervisor: TaskSupervisor,
}

impl TcpSocketPool {
    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    pub(crate) async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

    pub(crate) async fn acquire_with_deadline(
        &self,
        endpoint: &Endpoint,
        deadline: Option<std::time::Instant>,
        state: &Arc<State>,
    ) -> Result<Socket> {
        if let Some(socket) = self.try_acquire(endpoint) {
            return Ok(socket);
        }
        let mut group = self.connect_groups.lock(endpoint, deadline).await?;
        if let Some(socket) = self.acquire_existing(endpoint).await {
            return Ok(socket);
        }
        group
            .connect(deadline, |addr| self.connect_address(endpoint, addr, state))
            .await
    }

    async fn connect_address(
        &self,
        endpoint: &Endpoint,
        addr: SocketAddr,
        state: &Arc<State>,
    ) -> Result<Socket> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?;
        super::configure_stream(&stream);

        let send_socket = self
            .socket_map
            .try_publish_with(endpoint.clone(), || {
                self.add_socket(Some(endpoint.clone()), addr, stream, state)
            })
            .await?;
        Ok(send_socket.into())
    }

    pub(crate) fn try_acquire(&self, endpoint: &Endpoint) -> Option<Socket> {
        self.socket_map.try_get_live(endpoint).map(Into::into)
    }

    pub(crate) async fn acquire_existing(&self, endpoint: &Endpoint) -> Option<Socket> {
        self.socket_map.get_live(endpoint).await.map(Into::into)
    }

    pub fn new() -> Self {
        Self {
            socket_map: ConnectionMap::default(),
            connect_groups: Default::default(),
            task_supervisor: TaskSupervisor::create(),
        }
    }

    pub fn add_socket(
        &self,
        endpoint: Option<Endpoint>,
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
            let endpoint = endpoint.clone();
            let tcp_socket = tcp_socket.clone();
            let state = state.clone();
            async move {
                let error = tokio::select! {
                    () = task_supervisor.stopped() => {
                        Error::new(ErrorKind::ConnectionClosed, "TCP pool stopped".into())
                    }
                    // Teardown from the receive loop must also end this task:
                    // it owns the write half and keeps the send queue's only
                    // sender alive through `tcp_socket`, so dropping them is
                    // what reports EOF to the peer and releases the descriptor.
                    // Waiting for `recv_many` to observe channel closure cannot
                    // work while this task itself keeps the sender alive.
                    () = tcp_socket.closed() => {
                        Error::new(ErrorKind::ConnectionClosed, "TCP connection closed".into())
                    }
                    result = Self::start_send_loop(send_stream, receiver) => result.err()
                        .unwrap_or_else(|| Error::new(
                            ErrorKind::ConnectionClosed,
                            "TCP send loop ended".into(),
                        )),
                };
                tracing::debug!("send loop for {addr} ended: {error}");
                Self::evict_socket(
                    &socket_map,
                    endpoint.as_ref(),
                    &addr,
                    &tcp_socket,
                    &state,
                    &error,
                )
                .await;
            }
        });

        tokio::spawn({
            let socket_map = self.socket_map.clone();
            let endpoint = endpoint.clone();
            let tcp_socket = tcp_socket.clone();
            let state = state.clone();
            async move {
                let error = tokio::select! {
                    () = recv_task.stopped() => {
                        Error::new(ErrorKind::ConnectionClosed, "TCP pool stopped".into())
                    }
                    // Symmetric teardown: a failed send loop must release the
                    // read half too instead of leaving the descriptor alive in
                    // the pool.
                    () = tcp_socket.closed() => {
                        Error::new(ErrorKind::ConnectionClosed, "TCP connection closed".into())
                    }
                    result = Self::start_recv_loop(recv_stream, tcp_socket.clone(), &state) =>
                        result.err().unwrap_or_else(|| Error::new(
                            ErrorKind::ConnectionClosed,
                            "TCP receive loop ended".into(),
                        )),
                };
                tracing::debug!("receive loop for {addr} ended: {error}");
                Self::evict_socket(
                    &socket_map,
                    endpoint.as_ref(),
                    &addr,
                    &tcp_socket,
                    &state,
                    &error,
                )
                .await;
            }
        });
        Ok(tcp_socket)
    }

    /// Removes a dead socket from the map (if it is still the mapped one)
    /// and eagerly fails every request pending on the connection.
    async fn evict_socket(
        socket_map: &ConnectionMap<TcpSocket>,
        endpoint: Option<&Endpoint>,
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
        if let Some(endpoint) = endpoint {
            socket_map.evict_if_current(endpoint, socket).await;
        }
        let err = Error::new(
            ErrorKind::ConnectionClosed,
            format!("connection to {addr} closed: {err}"),
        );
        state.connection_closed(socket.conn_id(), &err);
    }

    async fn start_recv_loop(
        mut recv_stream: OwnedReadHalf,
        tcp_socket: TcpSocket,
        state: &Arc<State>,
    ) -> Result<()> {
        let mut buffer = BytesMut::with_capacity(1 << 20);
        let socket = Socket::TCP(tcp_socket);
        loop {
            match frame::parse_frame(&mut buffer)? {
                Parsed::Frame(Frame::Message(bytes)) => {
                    let msg = Message::parse(bytes)?;
                    state.handle_recv(&socket, msg)?;
                }
                Parsed::Frame(Frame::Data { head, len }) => {
                    let mut incoming = IncomingData::begin(state, &socket, head, len);
                    Self::receive_data(&mut recv_stream, &mut buffer, &mut incoming).await?;
                    incoming.finish(state, &socket)?;
                }
                // The parser sized the buffer for a frame known to be
                // incomplete, instead of doubling and copying it through
                // repeated reads. Read little beyond that frame: bytes of a
                // data frame that land here are copied once more, those read
                // later go straight to their destination.
                Parsed::Incomplete { missing } => {
                    let limit = missing.saturating_add(READ_AHEAD);
                    if buffer.capacity() - buffer.len() < READ_AHEAD {
                        buffer.reserve(READ_AHEAD);
                    }
                    let n = recv_stream
                        .read_buf(&mut (&mut buffer).limit(limit))
                        .await
                        .map_err(recv_failed)?;
                    if n == 0 {
                        return Err(end_of_stream());
                    }
                }
            }
        }
    }

    /// Consumes a data frame's raw bytes, which follow its head on the stream.
    ///
    /// Bytes that arrived in the receive buffer with the head are copied from
    /// there. The others are read from the socket straight into their
    /// destination while its write space is locked: only the nonblocking
    /// read runs under the lock, never the wait for readiness. Ranges too
    /// short for a read of their own, and bytes without a destination, pass
    /// through the receive buffer, which is empty by then.
    async fn receive_data(
        recv_stream: &mut OwnedReadHalf,
        buffer: &mut BytesMut,
        incoming: &mut IncomingData,
    ) -> Result<()> {
        let buffered = incoming.remaining().min(buffer.len());
        incoming.write(&buffer[..buffered]);
        buffer.advance(buffered);
        while incoming.remaining() > 0 {
            recv_stream.readable().await.map_err(recv_failed)?;
            let direct = incoming.read_with(DIRECT_READ_MIN, |slices| {
                recv_stream.try_read_vectored(slices)
            });
            let read = direct.unwrap_or_else(|| {
                let limit = incoming.remaining().min(READ_AHEAD);
                buffer.reserve(limit);
                let read = recv_stream.try_read_buf(&mut (&mut *buffer).limit(limit));
                incoming.write(buffer);
                buffer.clear();
                read
            });
            match read {
                Ok(0) => return Err(end_of_stream()),
                Ok(_) => {}
                // Readiness was stale; wait for the socket again.
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(recv_failed(error)),
            }
        }
        Ok(())
    }

    async fn start_send_loop(
        mut send_stream: OwnedWriteHalf,
        mut receiver: mpsc::Receiver<Outbound>,
    ) -> Result<()> {
        const LIMIT: usize = 64;
        let mut frames = Vec::with_capacity(LIMIT);
        // Unwritten segments of the received frames, in wire order.
        let mut segments = VecDeque::with_capacity(LIMIT);
        loop {
            if receiver.recv_many(&mut frames, LIMIT).await == 0 {
                return Ok(());
            }
            segments.extend(frames.drain(..).flat_map(Outbound::segments));

            while !segments.is_empty() {
                let mut slices = [IoSlice::new(&[]); LIMIT];
                let mut count = 0;
                for (slice, segment) in slices.iter_mut().zip(&segments) {
                    *slice = IoSlice::new(&segment.bytes);
                    count += 1;
                }
                let mut written = match send_stream.write_vectored(&slices[..count]).await {
                    Ok(0) => {
                        return Err(Error::new(
                            ErrorKind::TcpSendMsgFailed,
                            "socket accepted no bytes".into(),
                        ));
                    }
                    Ok(written) => written,
                    Err(e) => return Err(Error::new(ErrorKind::TcpSendMsgFailed, e.to_string())),
                };
                // Release every segment as soon as it is written: a data
                // frame's segments share buffers that their owner waits for,
                // and its last one returns the frame's queued bytes.
                while written > 0 {
                    let first = segments.front_mut().expect("written bytes were queued");
                    if written < first.bytes.len() {
                        first.bytes.advance(written);
                        break;
                    }
                    written -= first.bytes.len();
                    segments.pop_front();
                }
            }
        }
    }
}

/// Bytes read beyond what the frame under assembly needs. Batches small
/// messages into few reads, and bounds how much of a data frame is received
/// into the buffer instead of its destination.
const READ_AHEAD: usize = 64 << 10;

/// Shortest remainder of a destination range that gets a socket read of its
/// own. Shorter ones are cheaper to read together and copy.
const DIRECT_READ_MIN: usize = 4 << 10;

fn recv_failed(error: std::io::Error) -> Error {
    Error::new(ErrorKind::TcpRecvMsgFailed, error.to_string())
}

fn end_of_stream() -> Error {
    Error::new(ErrorKind::TcpRecvMsgFailed, "socket eof".to_string())
}

impl std::fmt::Debug for TcpSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocketPool").finish()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::*;
    use crate::{Context, MsgFlags, MsgMeta, RawStream, SocketPoolConfig, sockets::AcquireOptions};

    #[tokio::test]
    async fn test_tcp_socket_pool_debug_format() {
        let pool = TcpSocketPool::new();
        let debug = format!("{pool:?}");
        assert!(debug.contains("TcpSocketPool"));
    }

    /// A peer that sends EOF is gone: the server must close its own write half
    /// (and thus the descriptor) instead of keeping the connection half open in
    /// `CLOSE_WAIT` until the pool stops.
    #[tokio::test]
    async fn peer_eof_closes_server_write_half() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = ctx.state.clone();
        let accept = tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                state
                    .clone()
                    .handle_new_stream(RawStream::TCP(stream), peer)
                    .await;
            }
        });

        // Send FIN without waiting for the connection to be fully established
        // on the server side; EOF is observable as soon as the accept loop ran.
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.shutdown().await.unwrap();
        let mut buf = [0u8; 1];
        let closed = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf)).await;
        accept.abort();
        let n = closed
            .expect("server must close the connection after peer EOF")
            .expect("client read must succeed");
        assert_eq!(n, 0, "client must observe the server FIN");
    }

    /// The client-side mirror: when the remote closes, the recv loop must evict
    /// the socket and close the local half instead of holding the pooled
    /// connection (and its descriptor) open.
    #[tokio::test]
    async fn remote_eof_evicts_and_closes_client_socket() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.shutdown().await.unwrap();
            let mut buf = [0u8; 1];
            let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
                .await
                .expect("client must close its half after the server EOF")
                .expect("server read must succeed");
            assert_eq!(n, 0);
        });

        let endpoint: Endpoint = format!("tcp://{addr}").parse().unwrap();
        let socket = ctx
            .state
            .socket_pool
            .acquire(&endpoint, &ctx.state)
            .await
            .unwrap();
        let Socket::TCP(tcp_socket) = &socket else {
            panic!("TCP endpoint must acquire a TCP socket");
        };
        server.await.unwrap();

        // The receive loop runs teardown asynchronously with respect to the
        // FIN the raw peer observed; wait for the eviction to become visible.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if ctx
                .state
                .socket_pool
                .try_acquire(&endpoint, AcquireOptions::default())
                .is_none()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "closed connection must be evicted from the pool"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(tcp_socket.is_closed());
    }

    /// A failed send loop must end the pending receive loop of the same
    /// connection without stopping the pool.
    ///
    /// The receive loop cannot observe the failure by itself: the peer keeps its
    /// write direction open, so reads stay pending forever. Only the connection's
    /// teardown signal raced into the select ends it. Without that arm the
    /// receive task keeps the read half and its descriptor registered, so the
    /// registration count stays above the idle baseline and only stopping the
    /// whole pool would reclaim them.
    #[tokio::test]
    async fn send_failure_releases_pending_receive_half() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let pool = TcpSocketPool::new();
        let idle_tasks = pool.task_supervisor.running_tasks();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // The test peer owns the connection for the whole test: dropping it would
        // send a FIN and end the receive loop through the EOF path instead.
        let _client = TcpStream::connect(addr).await.unwrap();
        let (mut server_stream, peer) = listener.accept().await.unwrap();

        // Half-close only the write direction before the pool takes the stream:
        // the send loop's next write fails, while the receive loop stays blocked
        // because the peer never sends data or EOF.
        server_stream.shutdown().await.unwrap();
        let socket = pool
            .add_socket(None, peer, server_stream, &ctx.state)
            .unwrap();

        // One queued request makes the send loop attempt the failing write. The
        // response receiver stays bound: teardown eagerly fails it.
        let (msgid, _response) = ctx.state.waiter.alloc(Duration::from_secs(10));
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq,
            ..Default::default()
        };
        socket.send(&mut meta, &(), &ctx.state).await.unwrap();

        // The receive loop future owned the read half and one task registration;
        // returning to the idle count proves both were released. The supervisor
        // keeps one guard for its internal stop observer, so the idle count
        // also shows the pool itself is still running.
        let stopped = tokio::time::timeout(Duration::from_secs(2), async {
            while pool.task_supervisor.running_tasks() != idle_tasks {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            stopped.is_ok(),
            "receive task stayed registered: {} != {idle_tasks}",
            pool.task_supervisor.running_tasks(),
        );
        assert!(socket.is_closed());

        // Only the failed connection may end: the pool keeps accepting new
        // connections and their loops stay registered.
        let _client2 = TcpStream::connect(addr).await.unwrap();
        let (server_stream2, peer2) = listener.accept().await.unwrap();
        let second = pool
            .add_socket(None, peer2, server_stream2, &ctx.state)
            .expect("a connection failure must not stop the pool");
        assert!(!second.is_closed());
        assert_eq!(pool.task_supervisor.running_tasks(), idle_tasks + 2);
    }
}
