//! Built-in RPC services.
//!
//! This module provides built-in services that are automatically
//! registered with every RuaPC server, including:
//! - [`ReflectionService`]: public service and schema discovery
//! - internal remote-memory and RDMA bootstrap services

mod meta_service;
pub use meta_service::{
    DescribeRequest, MethodDescription, REFLECTION_PROTOCOL_VERSION, ReflectionService,
    ServerDescription, ServiceDescription,
};

mod memory_service;
pub(crate) use memory_service::{MemoryService, ReadInlineRequest, WriteInlineRequest};
#[cfg(feature = "rdma")]
pub(crate) use memory_service::{ReadIntoTargetRequest, RequestStatusRequest};
