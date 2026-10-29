//! Built-in RPC services.
//!
//! Remote-memory services are automatically registered with every router.
//! RDMA bootstrap and connection probe services are also registered when
//! that feature is enabled.

pub(crate) use crate::remote_memory::rpc_transfer::MemoryService;
#[cfg(feature = "rdma")]
pub(crate) use crate::remote_memory::rpc_transfer::RequestStatusRequest;
