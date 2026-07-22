use std::sync::Arc;

use arc_swap::ArcSwap;
use ruapc_bufpool::DeviceIndex;
use ruapc_rdma::{ActiveDevice, Context, DeviceInfo, ProtectionDomain};

pub struct RdmaDevice {
    index: DeviceIndex,
    inner: ActiveDevice,
    info: ArcSwap<DeviceInfo>,
}

impl RdmaDevice {
    pub fn new(inner: ActiveDevice) -> Self {
        let info = Arc::new(inner.info().clone());
        Self {
            index: DeviceIndex::default(),
            inner,
            info: ArcSwap::from(info),
        }
    }

    pub fn context(&self) -> &Arc<Context> {
        self.inner.context()
    }

    pub fn pd(&self) -> &Arc<ProtectionDomain> {
        self.inner.pd()
    }

    pub fn info(&self) -> Arc<DeviceInfo> {
        self.info.load_full()
    }

    /// Refreshes the cached device info snapshot from the hardware.
    ///
    /// GID filtering (RoCE v2 loopback / link-local) happens at collection
    /// time inside [`ActiveDevice::query_device_info`].
    pub fn refresh_port_attrs(&self) -> ruapc_rdma::Result<()> {
        self.info.store(Arc::new(self.inner.query_device_info()?));
        Ok(())
    }
}

impl ruapc_bufpool::Device for RdmaDevice {
    fn index(&self) -> DeviceIndex {
        self.index
    }

    fn set_index(&mut self, idx: DeviceIndex) {
        self.index = idx;
    }

    fn register(
        &self,
        mem: &Arc<ruapc_bufpool::AlignedMemory>,
    ) -> std::io::Result<Box<dyn ruapc_bufpool::Registration>> {
        let mr = self
            .inner
            .register(mem)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(Box::new(mr))
    }
}

impl std::fmt::Debug for RdmaDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaDevice")
            .field("index", &self.index)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruapc_bufpool::Device as _;

    #[test]
    fn test_rdma_device_debug_format() {
        let mut rdma = RdmaDevice::new(crate::rdma::test_utils::open_rdma_device());
        rdma.set_index(DeviceIndex { magic: 0, index: 3 });
        let debug = format!("{rdma:?}");
        assert!(debug.contains("RdmaDevice"));
    }

    #[test]
    fn test_rdma_device_index_and_inner() {
        let mut rdma = RdmaDevice::new(crate::rdma::test_utils::open_rdma_device());
        rdma.set_index(DeviceIndex {
            magic: 0,
            index: 42,
        });
        assert_eq!(rdma.index().index, 42);
        // context() and pd() should not panic.
        let _ = rdma.context();
        let _ = rdma.pd();
    }
}
