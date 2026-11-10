use std::{sync::Arc, time::Duration};

use ruapc_rdma::{ReadPosting, ReadRequest, ReadSegment};

use crate::{
    Context, Endpoint, ListenMode, Router, Server, Socket, SocketPoolConfig, Transport,
    rdma::{RdmaDevice, RdmaSocket},
    remote_memory::WriteTarget,
};

/// Opens the first available RDMA device, honoring the `RUAPC_PREFER_RXE`
/// env var (used in CI to select the `rxe_0` virtual device).
pub(crate) fn open_rdma_device() -> ruapc_rdma::ActiveDevice {
    let active_devices =
        ruapc_rdma::ActiveDevice::available().expect("RDMA devices should be available");
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    active_devices
        .into_iter()
        .find(|d| !prefer_rxe || d.info().name.starts_with("rxe"))
        .expect("no RDMA device matching filter found")
}

/// Small same-host fixture on one usable device. Background work starts only
/// when a test explicitly enables it; callers choose their own stripe count.
pub(crate) fn small_rdma_config() -> SocketPoolConfig {
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    let device = ruapc_rdma::ActiveDevice::available()
        .expect("RDMA devices must be available")
        .into_iter()
        .find(|device| {
            (!prefer_rxe || device.info().name.starts_with("rxe"))
                && device.info().ports.iter().any(|port| {
                    port.is_usable()
                        && (!port.port_attr.link_layer.is_ethernet() || !port.gids.is_empty())
                })
        })
        .expect("RDMA tests require one connectable device");
    let mut config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 128 << 20,
        ..Default::default()
    };
    let rdma = config.rdma.as_mut().unwrap();
    rdma.path.device_filter = vec![device.info().name.clone()];
    rdma.connection.qp.max_send_wr = 16;
    rdma.connection.qp.max_recv_wr = 16;
    rdma.connection.qp.max_send_sge = 1;
    rdma.connection.recv_queue_len = 4;
    rdma.connection.max_msg_size = 16 * 1024;
    rdma.polling.poll_threads_per_device = 1;
    rdma.polling.dispatch_workers = 1;
    rdma.peers.min_connections_per_remote_nic = 0;
    rdma.maintenance.interval_ms = 0;
    rdma.probe.interval_ms = 0;
    config
}

/// Opens all available RDMA devices and assigns registration indices,
/// honoring the `RUAPC_PREFER_RXE` env var.
pub(crate) fn make_rdma_devices() -> Arc<[RdmaDevice]> {
    let active_devices =
        ruapc_rdma::ActiveDevice::available().expect("RDMA devices should be available");
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    let mut devices = Vec::new();
    for dev in active_devices {
        if prefer_rxe && !dev.info().name.starts_with("rxe") {
            continue;
        }
        devices.push(RdmaDevice::new(devices.len(), dev));
    }
    assert!(!devices.is_empty(), "no RDMA device found");
    devices.into()
}

/// Builds an endpoint for a transport loop, using TCP as the RDMA bootstrap.
pub(crate) fn transport_endpoint(transport: Transport, address: std::net::SocketAddr) -> Endpoint {
    if transport == Transport::RDMA {
        Endpoint::rdma(Transport::TCP, [address]).unwrap()
    } else {
        Endpoint::new(transport, [address]).unwrap()
    }
}

/// A served RDMA connection for tests that need destinations owned by a real
/// READ plan. Allocate those destinations from `context`'s buffer pool.
pub(crate) struct RdmaConnection {
    pub server: Server,
    pub context: Context,
    pub socket: Arc<RdmaSocket>,
}

impl RdmaConnection {
    pub(crate) async fn establish() -> Self {
        let config = small_rdma_config();
        let server = Server::create(Router::default(), &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let context = Context::create(&config).unwrap();
        let endpoint = Endpoint::rdma(Transport::TCP, [address]).unwrap();
        let Socket::RDMA(socket) = context
            .state
            .socket_pool
            .acquire(&endpoint, &context.state)
            .await
            .unwrap()
        else {
            panic!("an RDMA endpoint must provide an RDMA connection");
        };
        Self {
            server,
            context,
            socket,
        }
    }

    /// Moves `target`'s nonempty destinations into a READ plan that is never
    /// posted. The plan owns them until the returned cursor is dropped.
    pub(crate) fn hold(&self, target: &WriteTarget) -> ReadPosting {
        target
            .start_read(Arc::downgrade(&self.socket), |destinations| {
                let read = ReadRequest {
                    remote_addr: 0,
                    rkey: 0,
                    segments: vec![ReadSegment {
                        buffer: 0,
                        offset: 0,
                        len: 1,
                    }],
                };
                match self
                    .socket
                    .queue_pair
                    .prepare_recoverable_reads(destinations.into_buffers(), vec![read])
                {
                    Ok((posting, _status, recovery)) => Ok((posting, recovery)),
                    Err((error, buffers)) => Err((error.into(), buffers)),
                }
            })
            .expect("available destinations registered with this connection's device")
    }

    pub(crate) async fn shutdown(self) {
        drop(self.socket);
        self.context.state.socket_pool.stop();
        self.server.stop();
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(self.context.state.socket_pool.join(), self.server.join());
        })
        .await
        .expect("both RDMA pools must stop");
    }
}
