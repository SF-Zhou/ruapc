use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{
    BufferPool, Context, Devices, Message, RawStream, Result, Router, Socket, SocketPool,
    SocketPoolConfig, Waiter,
};

/// Shared state for the RPC system.
///
/// The `State` contains shared components used throughout the RPC system:
/// - Router for method dispatch
/// - Waiter for request/response correlation
/// - Socket pool for connection management
/// - Device collection and buffer pool for memory operations
///
/// This state is shared between the server and all active connections.
pub struct State {
    /// Router containing registered service methods.
    pub router: Router,
    /// Waiter for correlating requests with responses.
    pub(crate) waiter: Arc<Waiter>,
    /// Socket pool for managing connections.
    pub(crate) socket_pool: SocketPool,
    /// Device collection for memory registration and Remote Read/Write.
    pub devices: Arc<Devices>,
    /// Shared buffer pool for RDMA and memory operations.
    pub buffer_pool: Arc<BufferPool>,
}

impl State {
    /// Creates a new state with the given router and configuration.
    ///
    /// Internally discovers devices, creates a shared buffer pool, registers
    /// `MemoryService`, and creates the socket pool.
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
        // Build the Devices collection based on configuration.
        let devices = Arc::new(Self::discover_devices(config));

        // Create a shared buffer pool backed by all discovered devices.
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();

        router.build_open_api()?;
        let socket_pool = SocketPool::create(config, &devices, &buffer_pool)?;

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

    /// Discovers and creates devices based on the socket pool configuration.
    ///
    /// Always adds a TCP device. When the `rdma` feature is enabled and the
    /// socket type is RDMA or UNIFIED, automatically discovers available
    /// RDMA devices.
    fn discover_devices(config: &SocketPoolConfig) -> Devices {
        #[cfg(feature = "rdma")]
        {
            let mut devices = Devices::default();
            use crate::SocketType;
            if matches!(config.socket_type, SocketType::RDMA | SocketType::UNIFIED)
                && let Ok(active_devices) = ruapc_rdma::ActiveDevice::available()
            {
                let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
                for dev in active_devices {
                    if prefer_rxe && !dev.info().name.starts_with("rxe") {
                        continue;
                    }
                    devices.add_rdma_device(dev);
                }
            }
            devices
        }
        #[cfg(not(feature = "rdma"))]
        {
            let _ = config;
            Devices::default()
        }
    }

    /// Handles a received message from a socket.
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
    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.socket_pool.drop_guard()
    }
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Message, MsgFlags, MsgMeta, Payload, SocketPoolConfig, SocketType, sockets::tcp::TcpSocket,
    };

    #[tokio::test]
    async fn test_handle_recv_invalid_msg_type_warns_and_ok() {
        let config = SocketPoolConfig {
            socket_type: SocketType::TCP,
        };
        let router = crate::Router::default();
        let (state, _guard) = State::create(router, &config).unwrap();

        // Create a socket just so we have a value to pass (not actually used for this path).
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let tcp_socket = TcpSocket::new(tx);
        let socket = crate::Socket::TCP(tcp_socket);

        // A message with flags = 0 is neither IsReq nor IsRsp → triggers the warn! branch.
        let msg = Message {
            meta: MsgMeta {
                method: "any/method".into(),
                flags: MsgFlags::empty(),
                msgid: 0,
                buffer_info: None,
            },
            payload: Payload::Empty,
        };

        let result = state.handle_recv(&socket, msg);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_state_debug_format() {
        // State::Debug is a stub that just prints "State { }".
        // Trigger it through Context which holds an Arc<State>.
        use crate::{Context, SocketPoolConfig};
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let debug = format!("{:?}", *ctx.state);
        assert!(debug.contains("State"));
    }
}
