use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::MemoryKey;

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
