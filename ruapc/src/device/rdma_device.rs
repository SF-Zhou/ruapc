use std::sync::Arc;

/// An RDMA device wrapper around `ruapc_rdma::Device`.
///
/// This provides the ruapc-level abstraction over the raw RDMA device,
/// exposing only the interfaces needed for memory registration and
/// remote memory operations.
pub struct RdmaDevice {
    index: usize,
    inner: Arc<ruapc_rdma::Device>,
}

impl RdmaDevice {
    /// Creates a new RDMA device. The `index` is typically overwritten
    /// by [`Devices::add`](ruapc_bufpool::Devices::add) via `set_index`.
    pub fn new(index: usize, inner: Arc<ruapc_rdma::Device>) -> Self {
        Self { index, inner }
    }

    /// Returns the device index assigned by `Devices::add`.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Sets the device index. Called by `Devices::add`.
    pub fn set_index(&mut self, idx: usize) {
        self.index = idx;
    }

    /// Returns a reference to the underlying `ruapc_rdma::Device`.
    pub fn inner(&self) -> &Arc<ruapc_rdma::Device> {
        &self.inner
    }

    /// Returns the raw protection domain pointer for memory registration.
    pub fn pd_ptr(&self) -> *mut ruapc_rdma::verbs::ibv_pd {
        self.inner.pd_ptr()
    }
}

impl std::fmt::Debug for RdmaDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaDevice")
            .field("index", &self.index)
            .finish()
    }
}
