pub type Buffer = ruapc_bufpool::Buffer;
pub type BufferPool = ruapc_bufpool::BufferPool;

/// The TCP registration device and the configured RDMA devices.
#[cfg(feature = "rdma")]
pub type Devices = ruapc_bufpool::DeviceSet<crate::rdma::RdmaDevice>;

/// The TCP registration device for stream transports.
#[cfg(not(feature = "rdma"))]
pub type Devices = ruapc_bufpool::DeviceSet;
