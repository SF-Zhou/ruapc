use std::{net::SocketAddr, sync::Arc};

use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::DropGuard;

#[cfg(feature = "rdma")]
use crate::rdma::{ConnectRequest, Endpoint, RdmaSocketPool};
use crate::{
    Endpoint as RpcEndpoint, Error, ErrorKind, ListenMode, Result, Socket, SocketPoolConfig, State,
    TaskSupervisor, Transport, http::HttpSocketPool, tcp::TcpSocketPool, ws::WebSocketPool,
};

/// Constraints and budget applied when acquiring a connection.
///
/// The fields are plain std types, so the struct exists in every build and
/// callers need no `rdma` feature gates: this is the single choke point
/// where transport-specific capabilities meet the generic acquire path.
/// Transports that cannot honor a field ignore it.
#[derive(Clone, Copy, Default)]
pub(crate) struct AcquireOptions<'a> {
    /// Remote RDMA NICs to avoid when picking or establishing a
    /// connection (soft constraint; only the RDMA pool reads it).
    #[cfg_attr(not(feature = "rdma"), expect(dead_code))]
    pub(crate) avoided_remote_nics: Option<&'a std::collections::HashSet<String>>,
    /// Deadline for establishing a new connection. All transports honor
    /// it; the RDMA pool additionally threads it through its multi-step
    /// bootstrap handshake.
    pub(crate) deadline: Option<std::time::Instant>,
}

#[cfg(feature = "rdma")]
impl AcquireOptions<'_> {
    /// The avoided-NIC set, normalized to a (possibly empty) set.
    fn avoided_nics(&self) -> &std::collections::HashSet<String> {
        static EMPTY: std::sync::LazyLock<std::collections::HashSet<String>> =
            std::sync::LazyLock::new(std::collections::HashSet::new);
        self.avoided_remote_nics.unwrap_or(&EMPTY)
    }
}

/// Composite transport pool. Outbound requests select a child by
/// [`Endpoint::transport`](crate::Endpoint::transport); `listen_mode` only
/// controls how accepted streams are decoded.
pub struct SocketPool {
    tcp: TcpSocketPool,
    ws: WebSocketPool,
    http: HttpSocketPool,
    #[cfg(feature = "rdma")]
    rdma: Option<RdmaSocketPool>,
    listen_mode: ListenMode,
    task_supervisor: TaskSupervisor,
}

/// Raw network stream types.
pub enum RawStream {
    /// Raw TCP stream.
    TCP(TcpStream),
    /// WebSocket stream over upgraded HTTP connection.
    WS(Box<WebSocketStream<TokioIo<Upgraded>>>),
}

/// Trait defining the interface for individual socket pool implementations.
///
/// Used by `TcpSocketPool`, `WebSocketPool`, `HttpSocketPool`, etc.
/// `SocketPool` (the enum) dispatches to these via its own methods.
pub trait SocketPoolTrait: Sized {
    fn create(
        config: &SocketPoolConfig,
        devices: &Arc<crate::Devices>,
        buffer_pool: &Arc<crate::BufferPool>,
    ) -> Result<Self>;

    async fn acquire(&self, addr: &SocketAddr, state: &Arc<State>) -> Result<Socket>;

    async fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()>;

    fn stop(&self);

    fn drop_guard(&self) -> DropGuard;

    async fn join(&self);
}

impl SocketPool {
    /// Creates all lightweight stream pools and, when explicitly enabled,
    /// the RDMA pool.
    pub fn create(
        config: &SocketPoolConfig,
        devices: &Arc<crate::Devices>,
        buffer_pool: &Arc<crate::BufferPool>,
    ) -> Result<Self> {
        let task_supervisor = TaskSupervisor::create();
        let tcp = TcpSocketPool::create(config, devices, buffer_pool)?;
        let ws = WebSocketPool::create(config, devices, buffer_pool)?;
        let http = HttpSocketPool::create(config, devices, buffer_pool)?;
        #[cfg(feature = "rdma")]
        let rdma = config
            .rdma
            .as_ref()
            .map(|_| RdmaSocketPool::create(config, devices, buffer_pool))
            .transpose()?;

        let task = task_supervisor.start_async_task();
        let tcp_guard = tcp.drop_guard();
        let ws_guard = ws.drop_guard();
        let http_guard = http.drop_guard();
        #[cfg(feature = "rdma")]
        let rdma_guard = rdma.as_ref().map(SocketPoolTrait::drop_guard);
        tokio::spawn(async move {
            task.stopped().await;
            drop(http_guard);
            drop(ws_guard);
            drop(tcp_guard);
            #[cfg(feature = "rdma")]
            drop(rdma_guard);
        });

        Ok(Self {
            tcp,
            ws,
            http,
            #[cfg(feature = "rdma")]
            rdma,
            listen_mode: config.listen_mode,
            task_supervisor,
        })
    }

    /// Acquires a socket using the transport carried by `endpoint`.
    pub async fn acquire(&self, endpoint: RpcEndpoint, state: &Arc<State>) -> Result<Socket> {
        match endpoint.transport() {
            Transport::TCP => self.tcp.acquire(&endpoint.addr(), state).await,
            Transport::WS => self.ws.acquire(&endpoint.addr(), state).await,
            Transport::HTTP => self.http.acquire(&endpoint.addr(), state).await,
            #[cfg(feature = "rdma")]
            Transport::RDMA => match &self.rdma {
                Some(pool) => pool.acquire(&endpoint.addr(), state).await,
                None => Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "RDMA endpoint requires SocketPoolConfig.rdma = Some(...)".into(),
                )),
            },
        }
    }

    /// Like [`SocketPool::acquire`], applying the constraints and budget in
    /// `options`. This is the single point where [`AcquireOptions`] meets
    /// transport-specific behavior.
    pub(crate) async fn acquire_with_options(
        &self,
        endpoint: RpcEndpoint,
        options: AcquireOptions<'_>,
        state: &Arc<State>,
    ) -> Result<Socket> {
        #[cfg(feature = "rdma")]
        if endpoint.transport() == Transport::RDMA {
            return match self.rdma_pool() {
                Some(pool) => {
                    pool.acquire_with_deadline(
                        &endpoint.addr(),
                        options.avoided_nics(),
                        options.deadline,
                        state,
                    )
                    .await
                }
                None => Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "RDMA endpoint requires SocketPoolConfig.rdma = Some(...)".into(),
                )),
            };
        }
        match options.deadline {
            Some(deadline) => {
                tokio::time::timeout_at(deadline.into(), self.acquire(endpoint, state))
                    .await
                    .map_err(|_| {
                        Error::new(ErrorKind::Timeout, "connection deadline expired".into())
                    })?
            }
            None => self.acquire(endpoint, state).await,
        }
    }

    /// Non-blocking fast path: an already-established live connection, or
    /// `None` when a new one would have to be created.
    #[cfg_attr(not(feature = "rdma"), expect(unused_variables))]
    pub(crate) fn try_acquire(
        &self,
        endpoint: RpcEndpoint,
        options: AcquireOptions<'_>,
    ) -> Option<Result<Socket>> {
        match endpoint.transport() {
            Transport::TCP => self.tcp.try_acquire(&endpoint.addr()).map(Ok),
            Transport::WS => self.ws.try_acquire(&endpoint.addr()).map(Ok),
            Transport::HTTP => self.http.try_acquire(&endpoint.addr()).map(Ok),
            #[cfg(feature = "rdma")]
            Transport::RDMA => match &self.rdma {
                Some(pool) => pool
                    .try_acquire(&endpoint.addr(), options.avoided_nics())
                    .map(Ok),
                None => Some(Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "RDMA endpoint requires SocketPoolConfig.rdma = Some(...)".into(),
                ))),
            },
        }
    }

    /// Awaiting variant of [`SocketPool::try_acquire`]: still never creates
    /// a connection, but may briefly wait on pool locks.
    #[cfg_attr(not(feature = "rdma"), expect(unused_variables))]
    pub(crate) async fn acquire_existing(
        &self,
        endpoint: RpcEndpoint,
        options: AcquireOptions<'_>,
    ) -> Option<Result<Socket>> {
        match endpoint.transport() {
            Transport::TCP => self.tcp.acquire_existing(&endpoint.addr()).await.map(Ok),
            Transport::WS => self.ws.acquire_existing(&endpoint.addr()).await.map(Ok),
            Transport::HTTP => self.http.acquire_existing(&endpoint.addr()).await.map(Ok),
            #[cfg(feature = "rdma")]
            Transport::RDMA => match &self.rdma {
                Some(pool) => pool
                    .acquire_existing(&endpoint.addr(), options.avoided_nics())
                    .await
                    .map(Ok),
                None => Some(Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "RDMA endpoint requires SocketPoolConfig.rdma = Some(...)".into(),
                ))),
            },
        }
    }

    pub(crate) fn task_supervisor_handle(&self) -> crate::TaskSupervisorHandle {
        self.task_supervisor.handle()
    }

    /// Handles a new incoming connection stream.
    pub async fn handle_new_stream(
        &self,
        state: &Arc<State>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        let RawStream::TCP(stream) = stream else {
            return self.ws.handle_new_stream(state, stream, addr).await;
        };
        match self.listen_mode {
            ListenMode::TCP => {
                self.tcp
                    .handle_new_stream(state, RawStream::TCP(stream), addr)
                    .await
            }
            ListenMode::WS => {
                self.ws
                    .handle_new_stream(state, RawStream::TCP(stream), addr)
                    .await
            }
            ListenMode::HTTP => {
                self.http
                    .handle_new_stream(state, RawStream::TCP(stream), addr)
                    .await
            }
            ListenMode::UNIFIED => {
                if Self::peek_is_tcp_magic(&stream).await? {
                    self.tcp
                        .handle_new_stream(state, RawStream::TCP(stream), addr)
                        .await
                } else {
                    self.http
                        .handle_new_stream(state, RawStream::TCP(stream), addr)
                        .await
                }
            }
        }
    }

    /// Decides whether an accepted stream speaks the RuaPC TCP protocol by
    /// peeking at its first bytes.
    ///
    /// `peek` returns whatever is buffered, which can be a strict prefix of
    /// the magic (e.g. the lone `R` of a fragmented HTTP `REPORT` request).
    /// Committing on a prefix would misroute, so wait until the full magic
    /// arrived or a byte diverged. Peeking cannot block on "more than N
    /// bytes", hence the short sleep between polls; the deadline bounds a
    /// peer that stalls mid-prefix (then only a genuine — if broken — RuaPC
    /// client is plausible, so fall back to TCP).
    async fn peek_is_tcp_magic(stream: &tokio::net::TcpStream) -> Result<bool> {
        let magic = crate::sockets::tcp::MAGIC_NUM.to_be_bytes();
        let mut buf = [0u8; std::mem::size_of::<u32>()];
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let n = stream
                .peek(&mut buf)
                .await
                .map_err(|e| Error::new(ErrorKind::TcpRecvMsgFailed, e.to_string()))?;
            if n == 0 {
                // EOF before any data: hand to HTTP for a graceful close.
                return Ok(false);
            }
            if buf[..n] != magic[..n] {
                return Ok(false);
            }
            if n >= magic.len() || tokio::time::Instant::now() >= deadline {
                return Ok(true);
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// Returns the underlying RDMA socket pool, if this pool has one.
    #[cfg(feature = "rdma")]
    pub(crate) fn rdma_pool(&self) -> Option<&RdmaSocketPool> {
        self.rdma.as_ref()
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_device_list(&self) -> Result<crate::rdma::RdmaInfo> {
        match &self.rdma {
            Some(pool) => pool.rdma_device_list(),
            None => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not enabled".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_accept(&self, request: &ConnectRequest, state: &Arc<State>) -> Result<Endpoint> {
        match &self.rdma {
            Some(pool) => pool.rdma_accept(request, state),
            None => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not enabled".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_confirm(&self, control: &crate::rdma::ConnectionControl) -> Result<()> {
        match &self.rdma {
            Some(pool) => pool.rdma_confirm(control),
            None => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not enabled".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_abort(&self, control: &crate::rdma::ConnectionControl) -> Result<()> {
        match &self.rdma {
            Some(pool) => {
                pool.rdma_abort(control);
                Ok(())
            }
            None => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not enabled".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_receive_observed(
        &self,
        connection_id: u64,
        socket: &std::sync::Arc<crate::rdma::RdmaSocket>,
    ) -> Result<()> {
        match &self.rdma {
            Some(pool) => {
                pool.rdma_receive_observed(connection_id, socket);
                Ok(())
            }
            None => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not enabled".into(),
            )),
        }
    }

    /// Stops the socket pool and initiates connection cleanup.
    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    /// Creates a drop guard for this socket pool.
    pub fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    /// Waits for all connections in the pool to close.
    pub async fn join(&self) {
        self.task_supervisor.all_stopped().await;
        self.tcp.join().await;
        self.ws.join().await;
        self.http.join().await;
        #[cfg(feature = "rdma")]
        if let Some(pool) = &self.rdma {
            pool.join().await;
        }
    }
}

impl std::fmt::Debug for SocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketPool")
            .field("listen_mode", &self.listen_mode)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "rdma")]
    use crate::RdmaSocketPoolConfig;

    #[tokio::test]
    async fn composite_pool_lifecycle() {
        let config = SocketPoolConfig::default();
        let devices = std::sync::Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        pool.stop();
        drop(pool.drop_guard());
        pool.join().await;
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn rdma_capability_is_explicit() {
        let devices = crate::rdma::test_utils::make_rdma_devices();
        let config = SocketPoolConfig {
            rdma: Some(RdmaSocketPoolConfig::default()),
            ..Default::default()
        };
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let pool = SocketPool::create(&config, &devices, &buffer_pool).unwrap();
        let info = pool.rdma_device_list().unwrap();
        assert!(!info.devices.is_empty());
        let disabled = SocketPoolConfig::default();
        let disabled_devices = std::sync::Arc::new(crate::Devices::default());
        let disabled_buffers =
            ruapc_bufpool::BufferPoolBuilder::new(disabled_devices.clone()).build();
        let disabled_pool =
            SocketPool::create(&disabled, &disabled_devices, &disabled_buffers).unwrap();
        assert!(disabled_pool.rdma_device_list().is_err());
    }
}
