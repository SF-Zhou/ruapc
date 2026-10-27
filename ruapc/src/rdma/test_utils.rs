use std::sync::Arc;

use crate::{Devices, Endpoint, ListenMode, SocketPoolConfig, Transport, rdma::RdmaDevice};

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

/// Builds a [`crate::Devices`] populated with all available RDMA devices,
/// honoring the `RUAPC_PREFER_RXE` env var.
pub(crate) fn make_rdma_devices() -> Arc<Devices> {
    let active_devices =
        ruapc_rdma::ActiveDevice::available().expect("RDMA devices should be available");
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    let mut devices = Devices::default();
    for dev in active_devices {
        if prefer_rxe && !dev.info().name.starts_with("rxe") {
            continue;
        }
        devices.push(RdmaDevice::new(dev));
    }
    assert!(!devices.devices().is_empty(), "no RDMA device found");
    Arc::new(devices)
}

/// Builds an endpoint for a transport loop, using TCP as the RDMA bootstrap.
pub(crate) fn transport_endpoint(transport: Transport, address: std::net::SocketAddr) -> Endpoint {
    if transport == Transport::RDMA {
        Endpoint::rdma(Transport::TCP, [address]).unwrap()
    } else {
        Endpoint::new(transport, [address]).unwrap()
    }
}
