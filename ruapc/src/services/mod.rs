//! Built-in RPC services.
//!
//! Remote-memory services are automatically registered with every router.
//! RDMA bootstrap and connection probe services are also registered when
//! that feature is enabled.

mod memory_service;
pub(crate) use memory_service::{MemoryService, ReadInlineRequest, WriteInlineRequest};
#[cfg(feature = "rdma")]
pub(crate) use memory_service::{ReadIntoTargetRequest, RequestStatusRequest};
