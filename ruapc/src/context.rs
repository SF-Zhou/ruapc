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
/// - Shared state (router, socket pool, devices, buffer pool, etc.)
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
    ///
    /// This creates a context with a default router (containing only MetaService).
    /// Devices and the shared buffer pool are constructed internally.
    ///
    /// # Arguments
    ///
    /// * `config` - Socket pool configuration
    ///
    /// # Errors
    ///
    /// Returns an error if state creation fails.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Context, SocketPoolConfig};
    /// let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    /// ```
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        Self::create_with_router(Router::default(), config)
    }

    /// Creates a new context with a custom router and configuration.
    ///
    /// Use this when you need to create a client context with custom services registered.
    /// Devices (TCP and any available RDMA NICs) and the shared buffer pool are
    /// constructed internally. A `MemoryService` is automatically registered.
    ///
    /// # Arguments
    ///
    /// * `router` - Custom router with registered services
    /// * `config` - Socket pool configuration
    ///
    /// # Errors
    ///
    /// Returns an error if state creation fails.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Context, Router, SocketPoolConfig};
    /// let router = Router::default();
    /// let ctx = Context::create_with_router(router, &SocketPoolConfig::default()).unwrap();
    /// ```
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
    ///
    /// This method clones the current context and sets a new target address,
    /// useful for making requests to a specific server.
    ///
    /// # Arguments
    ///
    /// * `addr` - The target socket address
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Context, SocketPoolConfig};
    /// # use std::{net::SocketAddr, str::FromStr};
    /// # let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    /// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
    /// let client_ctx = ctx.with_addr(addr);
    /// ```
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
    ///
    /// Internal method used by the server to create contexts for incoming requests.
    ///
    /// # Arguments
    ///
    /// * `state` - Shared state
    /// * `socket` - Connected socket
    /// * `msg_meta` - Message metadata from the incoming request
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
    ///
    /// This method is called by service implementations to return results.
    ///
    /// # Type Parameters
    ///
    /// * `Rsp` - The response type
    /// * `E` - The error type
    ///
    /// # Arguments
    ///
    /// * `rsp` - The result to send back (Ok or Err)
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
    ///
    /// Convenience method for sending error responses.
    ///
    /// # Arguments
    ///
    /// * `err` - The error to send back
    pub async fn send_err_rsp(&mut self, err: Error) {
        self.send_rsp::<(), Error>(Err(err)).await;
    }

    /// Reads data from a remote peer's registered memory.
    ///
    /// For TCP: sends a `MemoryService/read` reverse RPC request.
    /// For RDMA: issues a one-sided RDMA Read via QP verbs.
    ///
    /// # Arguments
    ///
    /// * `remote` - Remote buffer info (key, addr, len) obtained from the peer
    /// * `local_buf` - Local buffer to write the received data into
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
    ///
    /// # Arguments
    ///
    /// * `remote` - Remote buffer info (key, addr, len) obtained from the peer
    /// * `local_buf` - Local buffer containing the data to write
    /// * `len` - Number of bytes to write from `local_buf`
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

        // The state's RDMA device shares the same protection domain as the QP,
        // so we can obtain the lkey directly from the buffer's existing registration.
        let rdma_device = self.state.devices().rdma_device(0)?;
        let local_rbi = local_buf.remote_buffer_info(&rdma_device)?;
        let local_lkey = match local_rbi.key {
            crate::MemoryKey::Rdma { lkey, .. } => lkey,
            _ => {
                return Err(Error::new(
                    crate::ErrorKind::InvalidArgument,
                    "expected RDMA key for local buffer".into(),
                ));
            }
        };

        let wr_id = ruapc_rdma::verbs::WRID::send_data(0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        rdma_socket.rdma_completions.insert(wr_id, tx);

        rdma_socket.queue_pair.rdma_read_raw(
            wr_id,
            local_buf.as_ptr() as u64,
            remote.len as u32,
            local_lkey,
            remote.addr,
            rkey,
        )?;

        Self::await_rdma_completion(rx).await
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

        // The state's RDMA device shares the same protection domain as the QP,
        // so we can obtain the lkey directly from the buffer's existing registration.
        let rdma_device = self.state.devices().rdma_device(0)?;
        let local_rbi = local_buf.remote_buffer_info(&rdma_device)?;
        let local_lkey = match local_rbi.key {
            crate::MemoryKey::Rdma { lkey, .. } => lkey,
            _ => {
                return Err(Error::new(
                    crate::ErrorKind::InvalidArgument,
                    "expected RDMA key for local buffer".into(),
                ));
            }
        };

        let wr_id = ruapc_rdma::verbs::WRID::send_data(0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        rdma_socket.rdma_completions.insert(wr_id, tx);

        rdma_socket.queue_pair.rdma_write_raw(
            wr_id,
            local_buf.as_ptr() as u64,
            len as u32,
            local_lkey,
            remote.addr,
            rkey,
        )?;

        Self::await_rdma_completion(rx).await
    }

    #[cfg(feature = "rdma")]
    fn get_rdma_socket(&self) -> Result<std::sync::Arc<crate::rdma::RdmaSocket>> {
        match &self.endpoint {
            SocketEndpoint::Connected(Socket::RDMA(s)) => Ok(s.clone()),
            _ => Err(Error::new(
                crate::ErrorKind::InvalidArgument,
                "RDMA remote read/write requires a connected RDMA socket".into(),
            )),
        }
    }

    #[cfg(feature = "rdma")]
    async fn await_rdma_completion(
        rx: tokio::sync::oneshot::Receiver<ruapc_rdma::verbs::ibv_wc_status>,
    ) -> Result<()> {
        let status = rx.await.map_err(|_| {
            Error::new(
                crate::ErrorKind::RdmaSendFailed,
                "RDMA completion channel closed".into(),
            )
        })?;
        if status != ruapc_rdma::verbs::ibv_wc_status::IBV_WC_SUCCESS {
            return Err(Error::new(
                crate::ErrorKind::RdmaSendFailed,
                format!("RDMA operation failed with status {:?}", status),
            ));
        }
        Ok(())
    }
}
