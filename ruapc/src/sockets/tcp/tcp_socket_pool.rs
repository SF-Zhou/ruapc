use std::{collections::VecDeque, io::IoSlice, net::SocketAddr, sync::Arc};

use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, tcp::OwnedReadHalf},
    sync::mpsc,
};
use tokio_util::sync::DropGuard;

use super::TcpSocket;
use crate::{
    BufferPool, ConnectionMap, Devices, Endpoint, Message, RawStream, Socket, SocketPoolConfig,
    SocketPoolTrait, State, TaskSupervisor,
    error::{Error, ErrorKind, Result},
    msg::frame::{self, SendFrame},
    sockets::connect::ConnectGroups,
};

pub struct TcpSocketPool {
    socket_map: ConnectionMap<TcpSocket>,
    connect_groups: ConnectGroups,
    task_supervisor: TaskSupervisor,
}

impl SocketPoolTrait for TcpSocketPool {
    fn create(
        _config: &SocketPoolConfig,
        _devices: &Arc<Devices>,
        _buffer_pool: &Arc<BufferPool>,
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

        let _ = self.add_socket(None, addr, tcp_stream, state)?;
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

    async fn acquire(&self, endpoint: &Endpoint, state: &Arc<State>) -> Result<Socket> {
        self.acquire_with_deadline(endpoint, None, state).await
    }
}

impl TcpSocketPool {
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
                    biased;
                    () = tcp_socket.closed() => return,
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
                    biased;
                    () = tcp_socket.closed() => return,
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

    fn parse_message(buffer: &mut BytesMut) -> Result<Option<Bytes>> {
        frame::parse_message(buffer).map_err(Into::into)
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
        mut receiver: mpsc::Receiver<SendFrame>,
    ) -> Result<()> {
        const LIMIT: usize = 64;
        let mut msgs = Vec::with_capacity(LIMIT);
        let mut chunks = VecDeque::with_capacity(LIMIT);
        loop {
            if receiver.recv_many(&mut msgs, LIMIT).await == 0 {
                return Ok(());
            }
            for frame in msgs.drain(..) {
                let (prefix, parts) = frame.into_parts();
                chunks.extend(
                    std::iter::once(prefix)
                        .chain(parts)
                        .filter(|part| !part.is_empty()),
                );
            }
            while !chunks.is_empty() {
                // Bound the iovec count independently of frame count. Retain
                // owners across partial writes, then release consumed chunks
                // before awaiting later frames so unrelated backpressure cannot
                // prevent source recovery after a peer has acknowledged a copy.
                let mut written = {
                    let mut bufs = [IoSlice::new(&[]); LIMIT];
                    let count = chunks.len().min(LIMIT);
                    for (part, slot) in chunks.iter().take(LIMIT).zip(&mut bufs) {
                        *slot = IoSlice::new(part);
                    }
                    match send_stream.write_vectored(&bufs[..count]).await {
                        Ok(0) => {
                            return Err(Error::new(
                                ErrorKind::TcpSendMsgFailed,
                                "socket write returned zero before the frame was sent".into(),
                            ));
                        }
                        Ok(n) => n,
                        Err(e) => {
                            return Err(Error::new(ErrorKind::TcpSendMsgFailed, e.to_string()));
                        }
                    }
                };
                while let Some(part) = chunks.front_mut() {
                    if written < part.len() {
                        part.advance(written);
                        break;
                    }
                    written -= part.len();
                    chunks.pop_front();
                }
            }
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
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use crate::{Context as RpcContext, MsgFlags, MsgMeta};
    use socket2::SockRef;
    use tokio::net::TcpListener;

    #[derive(Default)]
    struct LimitedWriter {
        bytes: Vec<u8>,
        limit: usize,
        largest_iovec: usize,
    }

    impl AsyncWrite for LimitedWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let count = bytes.len().min(self.limit);
            self.bytes.extend_from_slice(&bytes[..count]);
            Poll::Ready(Ok(count))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            slices: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            self.largest_iovec = self.largest_iovec.max(slices.len());
            let mut written = 0;
            for slice in slices {
                let count = slice.len().min(self.limit - written);
                self.bytes.extend_from_slice(&slice[..count]);
                written += count;
                if written == self.limit {
                    break;
                }
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn receive_eof_releases_idle_writer_without_stopping_pool() {
        let ctx = RpcContext::create(&SocketPoolConfig {
            rdma: None,
            ..Default::default()
        })
        .unwrap();
        let weak_state = Arc::downgrade(&ctx.state);
        let pool = TcpSocketPool::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, addr) = listener.accept().await.unwrap();
        let socket = pool.add_socket(None, addr, stream, &ctx.state).unwrap();
        peer.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), socket.closed())
            .await
            .expect("receive EOF must close the logical connection");
        assert!(socket.is_closed());
        drop(socket);
        drop(ctx);
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
            while weak_state.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection tasks must release their socket and State without pool shutdown");
        pool.stop();
        pool.join().await;
    }

    #[tokio::test]
    async fn receive_eof_cancels_blocked_writer_and_releases_raw_sources() {
        struct Owner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let ctx = RpcContext::create(&SocketPoolConfig {
            rdma: None,
            ..Default::default()
        })
        .unwrap();
        let pool = TcpSocketPool::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, addr) = listener.accept().await.unwrap();
        SockRef::from(&stream).set_send_buffer_size(1024).unwrap();
        SockRef::from(&peer).set_recv_buffer_size(1024).unwrap();
        let socket = pool.add_socket(None, addr, stream, &ctx.state).unwrap();
        let source = Arc::new(vec![7; 8 << 20]);
        Socket::TCP(socket.clone())
            .send_raw(
                &mut MsgMeta {
                    flags: MsgFlags::IsRsp,
                    ..Default::default()
                },
                &(),
                &[Bytes::from_owner(Owner(source.clone()))],
                &ctx.state,
            )
            .await
            .unwrap();
        // The peer accepts the frame prefix but never drains its large payload.
        // This proves the sender owns a partially written frame, not just a
        // message that has not yet been polled from its queue.
        let mut prefix = [0; 8];
        peer.read_exact(&mut prefix).await.unwrap();
        assert_eq!(Arc::strong_count(&source), 2);
        peer.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            socket.closed().await;
            while Arc::strong_count(&source) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection closure must drop a blocked writer's immutable source owners");
        pool.stop();
        pool.join().await;
    }

    #[tokio::test]
    async fn segmented_frames_keep_order_across_partial_writes_and_iovec_batches() {
        let parts: Vec<_> = (0..150).map(|i| Bytes::from(vec![i; 11])).collect();
        let first = frame::encode_parts(&MsgMeta::default(), &parts).unwrap();
        let second: SendFrame = frame::encode(&MsgMeta::default(), &"next").unwrap().into();
        let expected = [first.clone().into_bytes(), second.clone().into_bytes()].concat();
        let (sender, receiver) = mpsc::channel(2);
        sender.try_send(first).unwrap();
        sender.try_send(second).unwrap();
        drop(sender);
        let mut writer = LimitedWriter {
            limit: 7,
            ..Default::default()
        };
        TcpSocketPool::start_send_loop(&mut writer, receiver)
            .await
            .unwrap();
        assert_eq!(writer.bytes, expected);
        assert_eq!(writer.largest_iovec, 64);
    }

    #[tokio::test]
    async fn zero_byte_write_fails_instead_of_spinning() {
        let (sender, receiver) = mpsc::channel(1);
        sender
            .try_send(Bytes::from_static(b"pending").into())
            .unwrap();
        drop(sender);
        let error = TcpSocketPool::start_send_loop(LimitedWriter::default(), receiver)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::TcpSendMsgFailed);
    }

    #[tokio::test]
    async fn partial_send_holds_sources_until_completion_or_cancellation() {
        struct Owner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let source = Arc::new(vec![7; 1024]);
        let frame = frame::encode_parts(
            &MsgMeta::default(),
            &[Bytes::from_owner(Owner(source.clone()))],
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(1);
        sender.try_send(frame).unwrap();
        drop(sender);
        let (writer, _reader) = tokio::io::duplex(1);
        let mut sending = Box::pin(TcpSocketPool::start_send_loop(writer, receiver));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        assert_eq!(Arc::strong_count(&source), 2);
        drop(sending);
        assert_eq!(Arc::strong_count(&source), 1);
    }

    #[tokio::test]
    async fn completed_frame_releases_source_before_next_frame_can_finish() {
        struct Owner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let first = Arc::new(vec![1; 16]);
        let second = Arc::new(vec![2; 16]);
        let first_frame = frame::encode_parts(
            &MsgMeta::default(),
            &[Bytes::from_owner(Owner(first.clone()))],
        )
        .unwrap();
        let first_len: usize = first_frame.chunks().map(Bytes::len).sum();
        let second_frame = frame::encode_parts(
            &MsgMeta::default(),
            &[Bytes::from_owner(Owner(second.clone()))],
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(2);
        sender.try_send(first_frame).unwrap();
        sender.try_send(second_frame).unwrap();
        drop(sender);
        // The stream accepts exactly the first frame, then blocks until read.
        let (writer, _reader) = tokio::io::duplex(first_len);
        let mut sending = Box::pin(TcpSocketPool::start_send_loop(writer, receiver));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        assert_eq!(Arc::strong_count(&first), 1);
        assert_eq!(Arc::strong_count(&second), 2);
        drop(sending);
        assert_eq!(Arc::strong_count(&second), 1);
    }

    #[tokio::test]
    async fn test_tcp_socket_pool_debug_format() {
        let pool = TcpSocketPool::new();
        let debug = format!("{pool:?}");
        assert!(debug.contains("TcpSocketPool"));
    }
}
