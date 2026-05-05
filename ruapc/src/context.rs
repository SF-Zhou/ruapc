use std::{net::SocketAddr, sync::Arc};

use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{
    Error, Result, Router, Socket, SocketPoolConfig, SocketTrait, State,
    msg::{MsgFlags, MsgMeta},
};

/// Socket endpoint information for RPC contexts.
///
/// Represents the connection endpoint for an RPC operation, which can be:
/// - Invalid: No endpoint specified
/// - Connected: An existing socket connection
/// - Address: A socket address to connect to
#[derive(Clone, Debug, Default)]
pub enum SocketEndpoint {
    /// No valid endpoint (default state).
    #[default]
    Invalid,
    /// An established socket connection.
    Connected(Socket),
    /// A socket address to establish a connection to.
    Address(SocketAddr),
}

/// RPC context carrying request metadata and connection information.
///
/// The `Context` is passed to all RPC service methods and contains:
/// - Shared state (router, socket pool, etc.)
/// - Connection endpoint information
/// - Lifecycle management through drop guards
///
/// # Examples
///
/// Creating a client context:
///
/// ```rust,no_run
/// # use ruapc::{Context, SocketPoolConfig};
/// # use std::{net::SocketAddr, str::FromStr};
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
/// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
/// let ctx = ctx.with_addr(addr);
/// ```
#[derive(Clone)]
pub struct Context {
    pub(crate) drop_guard: Option<Arc<DropGuard>>,
    /// Shared state containing router and socket pool.
    pub state: Arc<State>,
    pub(crate) endpoint: SocketEndpoint,
    /// Message metadata for the current RPC operation.
    pub msg_meta: MsgMeta,
}

impl Context {
    /// Creates a new context with the given socket pool configuration.
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        Self::create_with_router(Router::default(), config)
    }

    /// Creates a new context with a custom router and configuration.
    pub fn create_with_router(router: Router, config: &SocketPoolConfig) -> Result<Self> {
        let (state, drop_guard) = State::create(router, config)?;
        Ok(Self {
            state,
            endpoint: SocketEndpoint::Invalid,
            drop_guard: Some(Arc::new(drop_guard)),
            msg_meta: MsgMeta::default(),
        })
    }

    /// Creates a context with a specific state and address.
    ///
    /// Internal method used by RDMA implementation.
    #[cfg(feature = "rdma")]
    pub(crate) fn create_with_state_and_addr(state: &Arc<State>, addr: &SocketAddr) -> Self {
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Address(*addr),
            drop_guard: None,
            msg_meta: MsgMeta::default(),
        }
    }

    /// Creates a new context with the specified target address.
    #[must_use]
    pub fn with_addr(&self, addr: SocketAddr) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: SocketEndpoint::Address(addr),
            drop_guard: self.drop_guard.clone(),
            msg_meta: MsgMeta::default(),
        }
    }

    /// Creates a server-side context with an established socket connection.
    #[must_use]
    pub(crate) fn server_ctx(state: &Arc<State>, socket: Socket, msg_meta: MsgMeta) -> Self {
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Connected(socket),
            drop_guard: None,
            msg_meta,
        }
    }

    /// Sends an RPC response back to the client.
    pub async fn send_rsp<Rsp, E>(&mut self, rsp: std::result::Result<Rsp, E>)
    where
        Rsp: Serialize,
        E: std::error::Error + From<Error> + Serialize,
    {
        let mut meta = self.msg_meta.clone();
        meta.flags.remove(MsgFlags::IsReq);
        meta.flags.insert(MsgFlags::IsRsp);
        match &mut self.endpoint {
            SocketEndpoint::Connected(socket) => {
                let _ = socket.send(&mut meta, &rsp, &self.state).await;
            }
            _ => {
                tracing::error!("invalid argument: send rsp without connected socket");
            }
        }
    }

    /// Sends an error response back to the client.
    pub async fn send_err_rsp(&mut self, err: Error) {
        self.send_rsp::<(), Error>(Err(err)).await;
    }

    /// Reads data from a remote peer's registered memory.
    ///
    /// For TCP: sends a `MemoryService/read` reverse RPC request.
    /// For RDMA: issues a one-sided RDMA Read via QP verbs.
    pub async fn remote_read(
        &self,
        remote: &crate::RemoteBufferInfo,
        local_buf: &mut crate::Buffer,
    ) -> Result<()> {
        #[cfg(feature = "rdma")]
        if let crate::MemoryKey::Rdma { lkey: _, rkey } = remote.key {
            return self.rdma_remote_read(remote, local_buf, rkey).await;
        }

        self.tcp_remote_read(remote, local_buf).await
    }

    /// Writes data from a local buffer to a remote peer's registered memory.
    ///
    /// For TCP: sends a `MemoryService/write` reverse RPC request.
    /// For RDMA: issues a one-sided RDMA Write via QP verbs.
    pub async fn remote_write(
        &self,
        remote: &crate::RemoteBufferInfo,
        local_buf: &crate::Buffer,
        len: usize,
    ) -> Result<()> {
        #[cfg(feature = "rdma")]
        if let crate::MemoryKey::Rdma { lkey: _, rkey } = remote.key {
            return self.rdma_remote_write(remote, local_buf, len, rkey).await;
        }

        self.tcp_remote_write(remote, local_buf, len).await
    }

    async fn tcp_remote_read(
        &self,
        remote: &crate::RemoteBufferInfo,
        local_buf: &mut crate::Buffer,
    ) -> Result<()> {
        use crate::services::{MemoryReadReq, MemoryService};

        let req = MemoryReadReq {
            key: remote.key,
            addr: remote.addr,
            len: remote.len,
        };
        let client = crate::Client::default();
        let data: Vec<u8> = client.read(self, &req).await?;
        if data.len() > local_buf.len() {
            return Err(Error::new(
                crate::ErrorKind::InvalidArgument,
                format!(
                    "remote read returned {} bytes but local buffer is {} bytes",
                    data.len(),
                    local_buf.len()
                ),
            ));
        }
        local_buf[..data.len()].copy_from_slice(&data);
        Ok(())
    }

    async fn tcp_remote_write(
        &self,
        remote: &crate::RemoteBufferInfo,
        local_buf: &crate::Buffer,
        len: usize,
    ) -> Result<()> {
        use crate::services::{MemoryService, MemoryWriteReq};

        let req = MemoryWriteReq {
            key: remote.key,
            addr: remote.addr,
            data: local_buf[..len].to_vec(),
        };
        let client = crate::Client::default();
        client.write(self, &req).await?;
        Ok(())
    }

    #[cfg(feature = "rdma")]
    async fn rdma_remote_read(
        &self,
        remote: &crate::RemoteBufferInfo,
        local_buf: &mut crate::Buffer,
        rkey: u32,
    ) -> Result<()> {
        let rdma_socket = self.get_rdma_socket()?;
        let mr = Self::register_on_qp_device(&rdma_socket, local_buf)?;

        let rx = Self::post_rdma_verb(
            &rdma_socket,
            &mr,
            remote.len as usize,
            remote.addr,
            rkey,
            true,
        )?;

        let result = Self::await_rdma_completion(rx).await;
        drop(mr);
        result
    }

    #[cfg(feature = "rdma")]
    async fn rdma_remote_write(
        &self,
        remote: &crate::RemoteBufferInfo,
        local_buf: &crate::Buffer,
        len: usize,
        rkey: u32,
    ) -> Result<()> {
        let rdma_socket = self.get_rdma_socket()?;
        let mr = Self::register_on_qp_device(&rdma_socket, local_buf)?;

        let rx = Self::post_rdma_verb(&rdma_socket, &mr, len, remote.addr, rkey, false)?;

        let result = Self::await_rdma_completion(rx).await;
        drop(mr);
        result
    }

    /// Posts an RDMA Read or Write verb via the safe QueuePair API.
    ///
    /// Separated from the async methods so that the `!Send` types inside
    /// `QueuePair` do not live across an await point.
    #[cfg(feature = "rdma")]
    fn post_rdma_verb(
        rdma_socket: &crate::rdma::RdmaSocket,
        mr: &ruapc_rdma_sys::MemoryRegion,
        length: usize,
        remote_addr: u64,
        rkey: u32,
        is_read: bool,
    ) -> Result<tokio::sync::oneshot::Receiver<ruapc_rdma_sys::ibv_wc_status>> {
        let buf_ref = crate::rdma::MrBufferRef::new(mr, length);

        let wr_id = if is_read {
            rdma_socket
                .queue_pair
                .read_untracked(&buf_ref, remote_addr, rkey)
        } else {
            rdma_socket
                .queue_pair
                .write_untracked(&buf_ref, remote_addr, rkey)
        }
        .map_err(|e| Error::new(crate::ErrorKind::RdmaSendFailed, e.to_string()))?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        rdma_socket.rdma_completions.insert(wr_id, tx);
        Ok(rx)
    }

    #[cfg(feature = "rdma")]
    fn get_rdma_socket(&self) -> Result<Arc<crate::rdma::RdmaSocket>> {
        match &self.endpoint {
            SocketEndpoint::Connected(Socket::RDMA(s)) => Ok(s.clone()),
            _ => Err(Error::new(
                crate::ErrorKind::InvalidArgument,
                "RDMA remote read/write requires a connected RDMA socket".into(),
            )),
        }
    }

    /// Registers a buffer's memory on the QP's protection domain.
    #[cfg(feature = "rdma")]
    #[allow(unsafe_code)]
    fn register_on_qp_device(
        rdma_socket: &crate::rdma::RdmaSocket,
        buf: &crate::Buffer,
    ) -> Result<ruapc_rdma_sys::MemoryRegion> {
        let pd = match rdma_socket.device.as_ref() {
            crate::Device::Rdma(r) => r.pd(),
            _ => unreachable!("RdmaSocket should always have an RDMA device"),
        };
        let access = ruapc_rdma_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
            | ruapc_rdma_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
            | ruapc_rdma_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
            | ruapc_rdma_sys::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING.0;
        let mr = unsafe {
            ruapc_rdma_sys::MemoryRegion::register(
                pd,
                buf.as_ptr() as *mut _,
                buf.len(),
                access as _,
            )
        }
        .map_err(|e| {
            Error::new(
                crate::ErrorKind::RdmaSendFailed,
                format!("failed to register local buffer on QP device: {e}"),
            )
        })?;
        Ok(mr)
    }

    #[cfg(feature = "rdma")]
    async fn await_rdma_completion(
        rx: tokio::sync::oneshot::Receiver<ruapc_rdma_sys::ibv_wc_status>,
    ) -> Result<()> {
        let status = rx.await.map_err(|_| {
            Error::new(
                crate::ErrorKind::RdmaSendFailed,
                "RDMA completion channel closed".into(),
            )
        })?;
        if status != ruapc_rdma_sys::ibv_wc_status::IBV_WC_SUCCESS {
            return Err(Error::new(
                crate::ErrorKind::RdmaSendFailed,
                format!("RDMA operation failed with status {:?}", status),
            ));
        }
        Ok(())
    }
}
