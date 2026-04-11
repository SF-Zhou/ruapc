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
    pub(crate) fn new(index: usize, inner: Arc<ruapc_rdma::Device>) -> Self {
        Self { index, inner }
    }

    /// Returns the device index assigned by `Devices::add_device`.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns a reference to the underlying `ruapc_rdma::Device`.
    pub fn inner(&self) -> &Arc<ruapc_rdma::Device> {
        &self.inner
    }
}

impl std::fmt::Debug for RdmaDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaDevice")
            .field("index", &self.index)
            .finish()
    }
}
