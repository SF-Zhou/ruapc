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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_key_tcp_serde_roundtrip() {
        let key = MemoryKey::Tcp { id: 12345 };
        let json = serde_json::to_string(&key).unwrap();
        let recovered: MemoryKey = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, key);
    }

    #[test]
    fn test_memory_key_tcp_copy_and_clone() {
        let key = MemoryKey::Tcp { id: 99 };
        let key2 = key;
        let key3 = key.clone();
        assert_eq!(key, key2);
        assert_eq!(key, key3);
    }

    #[test]
    fn test_memory_key_debug_format() {
        let key = MemoryKey::Tcp { id: 7 };
        let s = format!("{:?}", key);
        assert!(s.contains("Tcp"));
        assert!(s.contains("7"));
    }

    #[test]
    fn test_remote_buffer_info_serde_roundtrip() {
        let rbi = RemoteBufferInfo {
            key: MemoryKey::Tcp { id: 1 },
            addr: 0xDEAD_BEEF,
            len: 4096,
        };
        let json = serde_json::to_string(&rbi).unwrap();
        let recovered: RemoteBufferInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered.addr, rbi.addr);
        assert_eq!(recovered.len, rbi.len);
        assert_eq!(recovered.key, rbi.key);
    }

    #[test]
    fn test_remote_buffer_info_debug_format() {
        let rbi = RemoteBufferInfo {
            key: MemoryKey::Tcp { id: 5 },
            addr: 0x1000,
            len: 256,
        };
        let s = format!("{:?}", rbi);
        assert!(s.contains("RemoteBufferInfo"));
    }
}
