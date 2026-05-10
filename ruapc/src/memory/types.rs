use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A device-specific key for remote memory access.
///
/// After memory is registered on a device, a `MemoryKey` is produced.
/// This key is sent to the remote peer (via normal RPC messages) so
/// it can perform Remote Read/Write operations against the registered memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum MemoryKey {
    /// TCP: a software-assigned registration ID.
    Tcp { id: u64 },
    /// RDMA: hardware-assigned local and remote keys.
    #[cfg(feature = "rdma")]
    Rdma { lkey: u32, rkey: u32 },
}

/// Information needed by a remote peer to access a registered memory region.
///
/// Transmitted via normal RPC messages so the remote side can issue
/// Remote Read/Write operations against the described buffer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct RemoteBufferInfo {
    pub key: MemoryKey,
    pub addr: u64,
    pub len: u64,
}
