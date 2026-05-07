use std::sync::Arc;

use ruapc_rdma_sys::{ActiveDevice, ProtectionDomain};

/// An RDMA device wrapper around `ruapc_rdma_sys::ActiveDevice`.
///
/// This provides the ruapc-level abstraction over the raw RDMA device,
/// exposing only the interfaces needed for memory registration and
/// remote memory operations.
pub struct RdmaDevice {
    index: usize,
    inner: ActiveDevice,
}

impl RdmaDevice {
    /// Creates a new RDMA device.
    ///
    /// The `index` field is set to `0` initially and will be assigned
    /// by [`Devices::add`](ruapc_bufpool::Devices::add) via `set_index`.
    pub fn new(inner: ActiveDevice) -> Self {
        Self { index: 0, inner }
    }

    /// Returns the device index assigned by `Devices::add`.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Sets the device index. Called by `Devices::add`.
    pub fn set_index(&mut self, idx: usize) {
        self.index = idx;
    }

    /// Returns a reference to the underlying `ActiveDevice`.
    pub fn inner(&self) -> &ActiveDevice {
        &self.inner
    }

    /// Returns the protection domain for memory registration.
    pub fn pd(&self) -> &Arc<ProtectionDomain> {
        self.inner.pd()
    }
}

impl std::fmt::Debug for RdmaDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaDevice")
            .field("index", &self.index)
            .finish()
    }
}
