//! Built-in RPC services.
//!
//! This module provides built-in services that are automatically
//! registered with every RuaPC server, including:
//! - [`ReflectionService`]: public service and schema discovery
//! - internal remote-memory and RDMA bootstrap services

pub use ruapc_schema::{
    DescribeRequest, MethodDescription, REFLECTION_PROTOCOL_VERSION, ServerDescription,
    ServiceDescription,
};

mod meta_service;
pub use meta_service::ReflectionService;

mod memory_service;
pub(crate) use memory_service::{MemoryService, ReadInlineRequest, WriteInlineRequest};
#[cfg(feature = "rdma")]
pub(crate) use memory_service::{ReadIntoTargetRequest, RequestStatusRequest};
