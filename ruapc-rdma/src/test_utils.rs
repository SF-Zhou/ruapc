//! Shared helpers for this crate's hardware-backed tests.

use crate::ActiveDevice;

/// Opens the first available RDMA device.
///
/// When `RUAPC_PREFER_RXE` is set (e.g. in CI with a Soft-RoCE `rxe_0`
/// device), only devices whose name starts with `rxe` are considered.
pub fn open_device() -> ActiveDevice {
    let devices = ActiveDevice::available().expect("no RDMA devices");
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    devices
        .into_iter()
        .find(|d| !prefer_rxe || d.info().name.starts_with("rxe"))
        .expect("no matching RDMA device")
}
