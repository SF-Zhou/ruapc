//! Memory key and remote buffer info types for device registration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Memory key containing local and remote keys for device-registered memory.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemoryKey {
    /// Local key for local access.
    pub lkey: u32,
    /// Remote key for remote access.
    pub rkey: u32,
}

/// Information about a remote buffer, used for RDMA-style remote read/write.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RemoteBufferInfo {
    /// The memory key for the buffer.
    pub key: MemoryKey,
    /// The address of the buffer.
    pub addr: u64,
    /// The number of valid data bytes in the buffer (the buffer's logical
    /// length, not its capacity). A remote read transfers exactly this
    /// many bytes.
    pub len: u64,
}
