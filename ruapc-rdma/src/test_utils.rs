use std::sync::Arc;

use ruapc_bufpool::AlignedMemory;

use crate::{ActiveDevice, MemoryRegion, ProtectionDomain, RdmaBuffer, ibv_access_flags};

pub fn open_device() -> ActiveDevice {
    let devices = ActiveDevice::available().expect("no RDMA devices");
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    devices
        .into_iter()
        .find(|d| !prefer_rxe || d.info().name.starts_with("rxe"))
        .expect("no matching RDMA device")
}

/// A minimal buffer type for test purposes.
pub struct TestBuffer {
    _mr: MemoryRegion,
    _data: Arc<AlignedMemory>,
}

impl TestBuffer {
    pub fn as_slice(&self) -> &[u8] {
        self._data.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self._data.as_mut_slice()
    }
}

impl RdmaBuffer for TestBuffer {
    fn addr(&self) -> *mut std::ffi::c_void {
        self._mr.addr()
    }
    fn len(&self) -> usize {
        self._mr.length()
    }
    fn lkey(&self) -> u32 {
        self._mr.lkey()
    }
    fn rkey(&self) -> u32 {
        self._mr.rkey()
    }
}

pub fn test_buffer(pd: &Arc<ProtectionDomain>) -> TestBuffer {
    test_buffer_with(
        pd,
        64,
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_READ,
    )
}

pub fn test_buffer_with(
    pd: &Arc<ProtectionDomain>,
    size: usize,
    access: ibv_access_flags,
) -> TestBuffer {
    let mem = Arc::new(AlignedMemory::new(size).unwrap());
    let mr = MemoryRegion::register(pd, &mem, access.0 as _).unwrap();
    TestBuffer {
        _mr: mr,
        _data: mem,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_utils::open_device;
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
