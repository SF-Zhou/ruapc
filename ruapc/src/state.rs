use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{
    BufferPool, Context, Devices, Message, RawStream, Result, Router, Socket, SocketPool,
    SocketPoolConfig, SocketPoolTrait, Waiter,
    services::{MemoryService, MemoryServiceImpl},
};

/// Shared state for the RPC system.
///
/// The `State` contains shared components used throughout the RPC system:
/// - Router for method dispatch
/// - Waiter for request/response correlation
/// - Socket pool for connection management
/// - Device collection and buffer pool for Remote Read/Write
///
/// This state is shared between the server and all active connections.
/// Devices and the buffer pool are constructed internally; callers access
/// them via [`State::devices`] and [`State::buffer_pool`].
pub struct State {
    /// Router containing registered service methods.
    pub router: Router,
    /// Waiter for correlating requests with responses.
    pub(crate) waiter: Arc<Waiter>,
    /// Socket pool for managing connections.
    pub(crate) socket_pool: SocketPool,
    /// Device collection for memory registration and Remote Read/Write.
    pub(crate) devices: Arc<Devices>,
    /// Shared user-level buffer pool registered on all devices.
    pub(crate) buffer_pool: Arc<BufferPool>,
}

impl State {
    /// Creates a new state with the given router and configuration.
    ///
    /// This method:
    /// 1. Constructs the device collection (TCP + available RDMA devices)
    /// 2. Creates the shared user-level buffer pool registered on all devices
    /// 3. Registers a `MemoryService` in the router for Remote Read/Write
    /// 4. Builds the OpenAPI specification from the router
    /// 5. Creates the socket pool with the specified configuration
    /// 6. Initializes the waiter for request/response correlation
    ///
    /// # Arguments
    ///
    /// * `router` - Router containing registered service methods
    /// * `config` - Socket pool configuration
    ///
    /// # Returns
    ///
    /// Returns an Arc-wrapped state and a drop guard for lifecycle management.
    ///
    /// # Errors
    ///
    /// Returns an error if OpenAPI generation or socket pool creation fails.
    pub(crate) fn create(
        mut router: Router,
        config: &SocketPoolConfig,
    ) -> Result<(Arc<Self>, DropGuard)> {
        let mut devs = Devices::new();
        devs.add_tcp_device();
        #[cfg(feature = "rdma")]
        if let Ok(rdma_devs) = ruapc_rdma::Devices::availables() {
            for rdma_dev in rdma_devs.iter() {
                devs.add_rdma_device(rdma_dev.clone());
            }
        }
        let devices = Arc::new(devs);
        let buffer_pool = BufferPool::new(devices.clone(), 4 * 1024 * 1024, 8 * 1024 * 1024, 0);

        let mem_svc = Arc::new(MemoryServiceImpl {
            devices: devices.clone(),
        });
        mem_svc.ruapc_export(&mut router);
        router.build_open_api()?;

        let socket_pool = {
            #[cfg(feature = "rdma")]
            {
                let uses_rdma = matches!(
                    config.socket_type,
                    crate::SocketType::RDMA | crate::SocketType::UNIFIED
                );
                let rdma_inner = devices.rdma_inner_devices();
                if uses_rdma && !rdma_inner.is_empty() {
                    SocketPool::create_with_rdma_devices(config, rdma_inner, buffer_pool.clone())?
                } else {
                    SocketPool::create(config)?
                }
            }
            #[cfg(not(feature = "rdma"))]
            SocketPool::create(config)?
        };

        let state = Self {
            router,
            waiter: Arc::default(),
            socket_pool,
            devices,
            buffer_pool,
        };
        let state = Arc::new(state);
        let drop_guard = state.drop_guard();
        Ok((state, drop_guard))
    }

    /// Returns the shared device collection.
    pub fn devices(&self) -> &Arc<Devices> {
        &self.devices
    }

    /// Returns the shared user-level buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Handles a received message from a socket.
    ///
    /// This method routes the message based on whether it's a request or response:
    /// - Requests are dispatched to the router
    /// - Responses are posted to the waiter
    ///
    /// # Arguments
    ///
    /// * `socket` - The socket that received the message
    /// * `msg` - The received message
    ///
    /// # Errors
    ///
    /// Currently always returns Ok, but has Result type for future extensibility.
    pub fn handle_recv(self: &Arc<Self>, socket: &Socket, msg: Message) -> Result<()> {
        if msg.meta.is_req() {
            let ctx = Context::server_ctx(self, socket.clone(), msg.meta);
            self.router.dispatch(ctx, msg.payload);
        } else if msg.meta.is_rsp() {
            self.waiter.post(msg.meta.msgid, msg);
        } else {
            tracing::warn!("invalid msg type {:?}", msg.meta);
        }
        Ok(())
    }

    /// Handles a new incoming stream connection.
    ///
    /// This method delegates stream handling to the socket pool, which will
    /// perform any necessary protocol negotiation and start message processing.
    ///
    /// # Arguments
    ///
    /// * `stream` - The raw network stream
    /// * `addr` - The remote address
    pub async fn handle_new_stream(self: Arc<Self>, stream: RawStream, addr: SocketAddr) {
        if let Err(e) = self
            .socket_pool
            .handle_new_stream(&self, stream, addr)
            .await
        {
            tracing::error!("handle new tcp stream error: {e}");
        }
    }

    /// Creates a drop guard for state lifecycle management.
    ///
    /// The drop guard ensures proper cleanup when the state is no longer needed.
    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.socket_pool.drop_guard()
    }
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State").finish()
    }
}
