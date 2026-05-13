use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::device::TcpMemoryRegistration;

pub type Buffer = ruapc_bufpool::Buffer<crate::device::Devices>;

pub type BufferPool = ruapc_bufpool::BufferPool<crate::device::Devices>;

/// A device-specific key for remote memory access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MemoryKey {
    Tcp {
        id: u64,
    },
    #[cfg(feature = "rdma")]
    Rdma {
        lkey: u32,
        rkey: u32,
    },
}

/// Information needed by a remote peer to access a registered memory region.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct RemoteBufferInfo {
    pub key: MemoryKey,
    pub addr: u64,
    pub len: u64,
}

/// Represents the registration state of an `AlignedMemory` on a specific device.
pub enum MemoryRegistration {
    Tcp {
        mr: TcpMemoryRegistration,
    },
    #[cfg(feature = "rdma")]
    Rdma {
        mr: ruapc_rdma_sys::MemoryRegion,
    },
}

impl MemoryRegistration {
    pub fn memory_key(&self) -> MemoryKey {
        match self {
            MemoryRegistration::Tcp { mr, .. } => MemoryKey::Tcp { id: mr.id },
            #[cfg(feature = "rdma")]
            MemoryRegistration::Rdma { mr, .. } => MemoryKey::Rdma {
                lkey: mr.lkey(),
                rkey: mr.rkey(),
            },
        }
    }
}

impl std::fmt::Debug for MemoryRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryRegistration::Tcp { mr, .. } => f
                .debug_struct("MemoryRegistration::Tcp")
                .field("id", &mr.id)
                .finish(),
            #[cfg(feature = "rdma")]
            MemoryRegistration::Rdma { mr, .. } => f
                .debug_struct("MemoryRegistration::Rdma")
                .field("lkey", &mr.lkey())
                .field("rkey", &mr.rkey())
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Device,
        device::{Devices, TcpDevice},
    };
    use ruapc_bufpool::AlignedMemory;
    use std::sync::Arc;

    fn as_tcp_device(device: &Device) -> Option<&TcpDevice> {
        match device {
            Device::Tcp(d) => Some(d),
            #[cfg(feature = "rdma")]
            _ => None,
        }
    }

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

    fn make_tcp_registration() -> (MemoryRegistration, Arc<AlignedMemory>) {
        let devices = Devices::new();
        let tcp = devices.tcp_device();
        let mem = Arc::new(AlignedMemory::new(4096).unwrap());
        let mr = as_tcp_device(tcp)
            .expect("expected TCP device")
            .register(Arc::clone(&mem));
        let reg = MemoryRegistration::Tcp { mr };
        (reg, mem)
    }

    #[test]
    fn test_memory_registration_tcp_memory_key() {
        let (reg, _mem) = make_tcp_registration();
        let key = reg.memory_key();
        assert!(matches!(key, MemoryKey::Tcp { .. }));
    }

    #[test]
    fn test_memory_registration_tcp_debug() {
        let (reg, _mem) = make_tcp_registration();
        let debug = format!("{reg:?}");
        assert!(debug.contains("MemoryRegistration::Tcp"));
    }

    #[test]
    fn test_memory_registration_unregister_removes_id() {
        let devices = Devices::new();
        let tcp_arc = devices.tcp_device();
        let mem = Arc::new(AlignedMemory::new(4096).unwrap());
        let tcp = as_tcp_device(tcp_arc).expect("expected TCP device");
        let mr = tcp.register(Arc::clone(&mem));
        let id = mr.id;
        let ptr = mem.as_ptr();
        let size = mem.size();

        drop(mr);
        assert!(tcp.read_memory(id, ptr as u64, size as u64).is_err());
    }

    #[cfg(feature = "rdma")]
    fn with_rdma_registration<F: FnOnce(&MemoryRegistration)>(f: F) {
        let active_devices =
            ruapc_rdma_sys::ActiveDevice::available().expect("RDMA devices should be available");
        let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
        let active = active_devices
            .into_iter()
            .find(|d| !prefer_rxe || d.info().name.starts_with("rxe"))
            .expect("no RDMA device matching filter found");

        let mut devices = Devices::new();
        devices.add_rdma_device(active);
        let devices = Arc::new(devices);

        let pool = crate::memory::BufferPool::new(devices.clone(), 4096, 4096, 0);
        let buf = pool.allocate().expect("failed to allocate RDMA buffer");
        let rdma_arc = &devices.rdma_devices()[0];
        let reg = buf
            .registration(rdma_arc)
            .expect("registration should exist");
        f(reg);
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn test_memory_registration_rdma_memory_key() {
        with_rdma_registration(|reg| {
            let key = reg.memory_key();
            assert!(matches!(key, MemoryKey::Rdma { .. }));
        });
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn test_memory_registration_rdma_debug() {
        with_rdma_registration(|reg| {
            let debug = format!("{reg:?}");
            assert!(debug.contains("MemoryRegistration::Rdma"));
        });
    }
}
