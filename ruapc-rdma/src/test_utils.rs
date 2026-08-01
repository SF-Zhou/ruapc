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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::open_device;
    use crate::*;

    /// Drop only the parent context while children are still alive.
    #[test]
    fn test_context_dropped_while_children_alive() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = Arc::clone(dev.pd());

        let cq = CompletionQueue::create(&ctx, 16, None).unwrap();
        let cc = CompChannel::create(&ctx).unwrap();

        drop(ctx);

        let mut wc = [ibv_wc::default(); 1];
        let n = cq.poll(&mut wc).unwrap();
        assert_eq!(n, 0);
        assert!(!cc.as_ptr().is_null());
        assert!(!pd.as_ptr().is_null());
    }

    /// Create multiple CQs sharing a context, drop them independently.
    #[test]
    fn test_multiple_cqs_shared_context() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());

        let cq1 = CompletionQueue::create(&ctx, 16, None).unwrap();
        let cq2 = CompletionQueue::create(&ctx, 16, None).unwrap();
        let cq3 = CompletionQueue::create(&ctx, 16, None).unwrap();

        drop(ctx);
        drop(cq2);

        let mut wc = [ibv_wc::default(); 1];
        assert_eq!(cq1.poll(&mut wc).unwrap(), 0);
        assert_eq!(cq3.poll(&mut wc).unwrap(), 0);
    }
}
