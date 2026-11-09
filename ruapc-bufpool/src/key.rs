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

/// One advertised region in a logical remote-memory space.
///
/// RDMA uses its registered address and keys. Stream transports use only the
/// logical length and leave the address and keys zero.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RemoteBufferInfo {
    /// The memory key for the buffer, or zero for stream transfers.
    pub key: MemoryKey,
    /// The registered address of the buffer, or zero for stream transfers.
    pub addr: u64,
    /// Advertised logical length, not allocation capacity. Operations may
    /// select any validated subrange within it.
    pub len: u64,
}
