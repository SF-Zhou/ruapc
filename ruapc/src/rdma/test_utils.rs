use std::sync::Arc;

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

/// Builds a [`crate::Devices`] populated with all available RDMA devices,
/// honoring the `RUAPC_PREFER_RXE` env var.
pub(crate) fn make_rdma_devices() -> Arc<crate::Devices> {
    let active_devices =
        ruapc_rdma::ActiveDevice::available().expect("RDMA devices should be available");
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    let mut devices = crate::Devices::default();
    for dev in active_devices {
        if prefer_rxe && !dev.info().name.starts_with("rxe") {
            continue;
        }
        devices.add_rdma_device(dev);
    }
    assert!(!devices.rdma_devices().is_empty(), "no RDMA device found");
    Arc::new(devices)
}
