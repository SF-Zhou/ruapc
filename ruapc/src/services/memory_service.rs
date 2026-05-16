use ruapc_bufpool::MemoryKey;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, Result};

/// Request to read from a remote memory region.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryReadReq {
    pub key: MemoryKey,
    pub addr: u64,
    pub len: u64,
}

/// Request to write to a remote memory region.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryWriteReq {
    pub key: MemoryKey,
    pub addr: u64,
    pub data: Vec<u8>,
}

/// Built-in service for Remote Read/Write operations over TCP.
///
/// When a peer wants to read from or write to registered memory on this
/// side, it sends a reverse RPC to this service. The service validates
/// the access against the device's registry and performs the operation.
#[ruapc_macro::service]
pub trait MemoryService {
    /// Reads data from a registered memory region over TCP.
    async fn tcp_read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<Vec<u8>>;

    /// Writes data to a registered memory region over TCP.
    async fn tcp_write(&self, ctx: &Context, req: &MemoryWriteReq) -> Result<()>;
}

/// Implementation of MemoryService backed by a Devices collection.
pub struct MemoryServiceImpl;

impl MemoryService for MemoryServiceImpl {
    async fn tcp_read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<Vec<u8>> {
        ctx.state
            .devices
            .tcp_device()
            .read_memory(req.key.lkey, req.addr, req.len)
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))
    }

    async fn tcp_write(&self, ctx: &Context, req: &MemoryWriteReq) -> Result<()> {
        ctx.state
            .devices
            .tcp_device()
            .write_memory(req.key.lkey, req.addr, &req.data)
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))
    }
}
