use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{
    BufferPool, Context, Devices, Error, Message, Metrics, RawStream, Result, Router, Socket,
    SocketPool, SocketPoolConfig, Waiter,
};

#[cfg(feature = "rdma")]
use crate::ErrorKind;
#[cfg(feature = "rdma")]
use crate::rdma::{RdmaDevice, RdmaPathPolicyConfig, RdmaPathReport};

/// Router, connection pools, request correlation and registered-memory resources.
///
/// Created by [`Server::create`](crate::Server::create) or [`Context::create`]
/// and shared by their contexts and active connections. Use [`Context::state`]
/// or [`Server::state`](crate::Server::state) to access it.
pub struct State {
    /// Router containing registered service methods.
    pub router: Router,
    /// Waiter for correlating requests with responses.
    pub(crate) waiter: Arc<Waiter>,
    /// Socket pool for managing connections.
    pub(crate) socket_pool: SocketPool,
    /// Devices used to register buffer-pool memory; TCP occupies index zero.
    pub devices: Arc<Devices>,
    /// Shared buffer pool for transports and application memory attachments.
    pub buffer_pool: Arc<BufferPool>,
    /// Metric emission helpers (see [`crate::metrics`]); the actual
    /// storage/export belongs to the user-installed [`metrics::Recorder`].
    pub(crate) metrics: Arc<Metrics>,
    /// Server-side in-flight request cap (0 = unlimited); excess requests
    /// are rejected with [`ErrorKind::Overloaded`](crate::ErrorKind).
    pub(crate) max_inflight_requests: usize,
}

impl State {
    /// Validates configuration, discovers devices, builds public schemas, and
    /// creates buffer/connection pools and response-waiter expiry tracking.
    /// Requires a Tokio runtime. Built-in methods are registered by
    /// [`Router::default`], before this function receives the router.
    ///
    /// Returns shared state and a guard that requests connection-pool shutdown
    /// on drop. Configuration, OpenAPI conversion and pool initialization errors
    /// are propagated.
    pub(crate) fn create(
        mut router: Router,
        config: &SocketPoolConfig,
    ) -> Result<(Arc<Self>, DropGuard)> {
        config.validate()?;
        // Build the Devices collection based on configuration.
        let devices = Arc::new(Self::discover_devices(config));

        // Create a shared buffer pool backed by all discovered devices.
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone())
            .max_memory(config.buffer_pool_memory)
            .build();

        router.build_open_api()?;
        let http_base_path = config.normalized_http_base_path()?;
        if !http_base_path.is_empty() {
            router.openapi.servers.push(openapiv3::Server {
                url: http_base_path,
                ..Default::default()
            });
        }
        let socket_pool = SocketPool::create(config, &devices, &buffer_pool)?;

        let waiter: Arc<Waiter> = Arc::default();
        // Coarse request-timeout sweeping (no per-request timers).
        waiter.spawn_sweeper();

        let state = Self {
            router,
            waiter,
            socket_pool,
            devices,
            buffer_pool,
            metrics: Arc::default(),
            max_inflight_requests: config.max_inflight_requests,
        };
        let state = Arc::new(state);
        let drop_guard = state.drop_guard();
        Ok((state, drop_guard))
    }

    /// Discovers and creates devices based on the socket pool configuration.
    ///
    /// Always adds a TCP device. When the `rdma` feature and
    /// `SocketPoolConfig::rdma` is `Some`, discovers available RDMA devices
    /// before constructing the shared buffer pool. Devices with a DOWN port
    /// are retained when `rdma.path.allow_down_ports` is enabled so the
    /// periodic refresher can observe a later transition to ACTIVE.
    fn discover_devices(_config: &SocketPoolConfig) -> Devices {
        let devices = Devices::default();
        #[cfg(feature = "rdma")]
        let devices = {
            let mut devices = devices;
            if let Some(rdma) = &_config.rdma
                && let Ok(mut active_devices) = ruapc_rdma::ActiveDevice::available()
            {
                sort_rdma_devices(&mut active_devices);
                let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
                for dev in active_devices {
                    if !rdma_device_allowed(dev.info(), prefer_rxe, &rdma.path) {
                        continue;
                    }
                    devices.push(RdmaDevice::new(dev));
                }
            }
            devices
        };
        devices
    }

    /// Dispatches requests with an incoming deadline, or delivers responses to
    /// their waiter. Messages with neither flag are logged and ignored.
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

    /// Eagerly fails waiters bound to a dead connection. Running handlers are
    /// not cancelled; a response send failure is discarded.
    pub(crate) fn connection_closed(&self, conn_id: u64, err: &Error) {
        self.waiter.fail_connection(conn_id, err);
    }

    /// Hands an accepted stream to the configured transport pool and logs
    /// setup errors. Connection tasks are owned by that pool.
    pub async fn handle_new_stream(self: Arc<Self>, stream: RawStream, addr: SocketAddr) {
        if let Err(e) = self
            .socket_pool
            .handle_new_stream(&self, stream, addr)
            .await
        {
            tracing::error!("handle new tcp stream error: {e}");
        }
    }

    /// Requests connection-pool shutdown when the returned guard is dropped.
    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.socket_pool.drop_guard()
    }

    /// Snapshot of every live RDMA connection with its path (local NIC,
    /// remote NIC) and the per-device connection counts.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument)
    /// when RDMA resources were not enabled in `SocketPoolConfig`.
    #[cfg(feature = "rdma")]
    pub async fn rdma_path_report(&self) -> Result<RdmaPathReport> {
        match self.socket_pool.rdma_pool() {
            Some(pool) => Ok(pool.path_report().await),
            None => Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA is not supported: invalid socket type".into(),
            )),
        }
    }
}

#[cfg(feature = "rdma")]
fn sort_rdma_devices(devices: &mut [ruapc_rdma::ActiveDevice]) {
    devices.sort_by(|left, right| left.info().name.cmp(&right.info().name));
}

#[cfg(feature = "rdma")]
fn rdma_device_allowed(
    info: &ruapc_rdma::DeviceInfo,
    prefer_rxe: bool,
    path: &RdmaPathPolicyConfig,
) -> bool {
    let has_allowed_port = info.ports.iter().any(|port| {
        port.is_usable()
            || (path.allow_down_ports
                && port.port_attr.state == ruapc_rdma::ibv_port_state::IBV_PORT_DOWN)
    });
    has_allowed_port
        && (!prefer_rxe || info.name.starts_with("rxe"))
        && (path.device_filter.is_empty()
            || path.device_filter.iter().any(|item| item == &info.name))
        && !path.device_exclude.iter().any(|item| item == &info.name)
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, MsgFlags, MsgMeta, Payload, SocketPoolConfig, sockets::tcp::TcpSocket};

    #[cfg(feature = "rdma")]
    #[test]
    fn rdma_devices_are_sorted_by_name() {
        let mut devices = ruapc_rdma::ActiveDevice::available().expect("no RDMA devices");
        devices.reverse();
        sort_rdma_devices(&mut devices);

        assert!(devices.is_sorted_by(|left, right| left.info().name <= right.info().name));
    }

    #[cfg(feature = "rdma")]
    fn rdma_device_info(name: &str, state: ruapc_rdma::ibv_port_state) -> ruapc_rdma::DeviceInfo {
        ruapc_rdma::DeviceInfo {
            name: name.to_owned(),
            ports: vec![ruapc_rdma::Port {
                port_num: 1,
                port_attr: ruapc_rdma::ibv_port_attr {
                    state,
                    link_layer: ruapc_rdma::LinkLayer::Ethernet,
                    ..Default::default()
                },
                gids: Vec::new(),
            }],
            ..Default::default()
        }
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn rdma_device_discovery_respects_down_port_policy() {
        use ruapc_rdma::ibv_port_state::{
            IBV_PORT_ACTIVE, IBV_PORT_ACTIVE_DEFER, IBV_PORT_ARMED, IBV_PORT_DOWN, IBV_PORT_INIT,
            IBV_PORT_NOP,
        };

        let active = rdma_device_info("mlx5_0", IBV_PORT_ACTIVE);
        let active_defer = rdma_device_info("mlx5_0", IBV_PORT_ACTIVE_DEFER);
        let down = rdma_device_info("mlx5_0", IBV_PORT_DOWN);
        let no_ports = ruapc_rdma::DeviceInfo {
            name: "mlx5_0".to_owned(),
            ..Default::default()
        };
        let mut path = RdmaPathPolicyConfig::default();

        assert!(rdma_device_allowed(&active, false, &path));
        assert!(rdma_device_allowed(&active_defer, false, &path));
        assert!(!rdma_device_allowed(&down, false, &path));

        path.allow_down_ports = true;
        assert!(rdma_device_allowed(&down, false, &path));
        for state in [IBV_PORT_NOP, IBV_PORT_INIT, IBV_PORT_ARMED] {
            assert!(!rdma_device_allowed(
                &rdma_device_info("mlx5_0", state),
                false,
                &path,
            ));
        }
        assert!(!rdma_device_allowed(&no_ports, false, &path));
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn rdma_device_discovery_applies_name_policies_after_port_policy() {
        use ruapc_rdma::ibv_port_state::IBV_PORT_DOWN;

        let mlx5_0 = rdma_device_info("mlx5_0", IBV_PORT_DOWN);
        let mlx5_1 = rdma_device_info("mlx5_1", IBV_PORT_DOWN);
        let path = RdmaPathPolicyConfig {
            device_filter: vec!["mlx5_0".to_owned(), "mlx5_1".to_owned()],
            device_exclude: vec!["mlx5_1".to_owned()],
            allow_down_ports: true,
            ..Default::default()
        };

        assert!(rdma_device_allowed(&mlx5_0, false, &path));
        assert!(!rdma_device_allowed(&mlx5_1, false, &path));
        assert!(!rdma_device_allowed(
            &rdma_device_info("mlx5_2", IBV_PORT_DOWN),
            false,
            &path,
        ));
        assert!(!rdma_device_allowed(&mlx5_0, true, &path));
    }

    #[tokio::test]
    async fn test_handle_recv_invalid_msg_type_warns_and_ok() {
        let config = SocketPoolConfig::default();
        let router = Router::default();
        let (state, _guard) = State::create(router, &config).unwrap();

        // Create a socket just so we have a value to pass (not actually used for this path).
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let tcp_socket = TcpSocket::new(tx);
        let socket = Socket::TCP(tcp_socket);

        // A message with flags = 0 is neither IsReq nor IsRsp → triggers the warn! branch.
        let msg = Message {
            meta: MsgMeta {
                method: "any/method".into(),
                flags: MsgFlags::empty(),
                msgid: 0,
                read_regions: Vec::new(),
                write_regions: Vec::new(),
                timeout_ms: 0,
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
