use serde::{Deserialize, Serialize};

/// A device-specific key for remote memory access.
///
/// After memory is registered on a device, a `MemoryKey` is produced.
/// This key is sent to the remote peer (via normal RPC messages) so
/// it can perform Remote Read/Write operations against the registered memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryKey {
    /// TCP: a software-assigned registration ID.
    Tcp { id: u32 },
    /// RDMA: hardware-assigned local and remote keys.
    #[cfg(feature = "rdma")]
    Rdma { lkey: u32, rkey: u32 },
}
