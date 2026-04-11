//! Built-in RPC services.
//!
//! This module provides built-in services that are automatically
//! registered with every RuaPC server, including:
//! - MetaService: Introspection and metadata service
//! - MemoryService: Remote Read/Write operations over TCP

mod meta_service;
pub use meta_service::{MetaService, Metadata};

mod memory_service;
pub use memory_service::{MemoryReadReq, MemoryService, MemoryServiceImpl, MemoryWriteReq};
