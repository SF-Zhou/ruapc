use ruapc_bufpool::MemoryKey;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, Result};

/// Request to read from a remote memory region.
///
/// After reading, the service verifies that the original request (identified
/// by `msgid`) is still being awaited. If the request has already timed out,
/// the data is discarded and a Timeout error is returned, since the buffer
/// may have been reclaimed.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryReadReq {
    pub key: MemoryKey,
    pub addr: u64,
    pub len: u64,
    /// Message ID of the original request. Used to verify the request
    /// is still alive after reading, ensuring the buffer data is valid.
    pub msgid: u64,
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
    ///
    /// If `req.msgid` is set, first verifies that the original request
    /// is still alive (not timed out) before reading. This ensures the
    /// buffer hasn't been reclaimed.
    async fn tcp_read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<Vec<u8>>;

    /// Writes data to a registered memory region over TCP.
    async fn tcp_write(&self, ctx: &Context, req: &MemoryWriteReq) -> Result<()>;
}

/// Implementation of MemoryService backed by a Devices collection.
pub struct MemoryServiceImpl;

impl MemoryService for MemoryServiceImpl {
    async fn tcp_read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<Vec<u8>> {
        let data = ctx
            .state
            .devices
            .tcp_device()
            .read_memory(req.key.lkey, req.addr, req.len)
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;

        // After reading, verify the original request is still alive.
        // If the client has timed out, the buffer may have been reclaimed
        // and the data we just read could be invalid.
        if !ctx.state.waiter.contains_message_id(req.msgid) {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "tcp_read: original request has already timed out, data discarded".into(),
            ));
        }

        Ok(data)
    }

    async fn tcp_write(&self, ctx: &Context, req: &MemoryWriteReq) -> Result<()> {
        ctx.state
            .devices
            .tcp_device()
            .write_memory(req.key.lkey, req.addr, &req.data)
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))
    }
}
