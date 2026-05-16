use std::sync::Arc;

use ruapc_bufpool::DeviceIndex;
use ruapc_rdma::{ActiveDevice, ProtectionDomain};

pub struct RdmaDevice {
    index: DeviceIndex,
    inner: ActiveDevice,
}

impl RdmaDevice {
    pub fn new(inner: ActiveDevice) -> Self {
        Self {
            index: DeviceIndex::default(),
            inner,
        }
    }

    pub fn inner(&self) -> &ActiveDevice {
        &self.inner
    }

    pub fn pd(&self) -> &Arc<ProtectionDomain> {
        self.inner.pd()
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
            .inner()
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

    fn open_rdma_device() -> ActiveDevice {
        let active_devices =
            ruapc_rdma::ActiveDevice::available().expect("RDMA devices should be available");
        let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
        active_devices
            .into_iter()
            .find(|d| !prefer_rxe || d.info().name.starts_with("rxe"))
            .expect("no RDMA device matching filter found")
    }

    #[test]
    fn test_rdma_device_debug_format() {
        let mut rdma = RdmaDevice::new(open_rdma_device());
        rdma.set_index(DeviceIndex { magic: 0, index: 3 });
        let debug = format!("{rdma:?}");
        assert!(debug.contains("RdmaDevice"));
    }

    #[test]
    fn test_rdma_device_index_and_inner() {
        let mut rdma = RdmaDevice::new(open_rdma_device());
        rdma.set_index(DeviceIndex {
            magic: 0,
            index: 42,
        });
        assert_eq!(rdma.index().index, 42);
        // inner() and pd() should not panic.
        let _ = rdma.inner();
        let _ = rdma.pd();
    }
}
