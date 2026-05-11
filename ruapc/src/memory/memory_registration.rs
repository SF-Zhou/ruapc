use std::sync::Arc;

use ruapc_bufpool::AlignedMemory;

use crate::device::{Device, TcpDevice};
use crate::memory::MemoryKey;

/// Represents the registration state of an `AlignedMemory` on a specific device.
///
/// Each variant holds an `Arc<Device>` to keep the device alive, and
/// stores the device-specific registration handle needed for unregistration.
pub enum MemoryRegistration {
    Tcp {
        device: Arc<Device>,
        id: u64,
    },
    #[cfg(feature = "rdma")]
    Rdma {
        device: Arc<Device>,
        mr: ruapc_rdma_sys::MemoryRegion,
    },
}

impl MemoryRegistration {
    /// Returns the `MemoryKey` for this registration.
    pub fn memory_key(&self) -> MemoryKey {
        match self {
            MemoryRegistration::Tcp { id, .. } => MemoryKey::Tcp { id: *id },
            #[cfg(feature = "rdma")]
            MemoryRegistration::Rdma { mr, .. } => MemoryKey::Rdma {
                lkey: mr.lkey(),
                rkey: mr.rkey(),
            },
        }
    }

    /// Returns a reference to the device this registration belongs to.
    pub fn device(&self) -> &Arc<Device> {
        match self {
            MemoryRegistration::Tcp { device, .. } => device,
            #[cfg(feature = "rdma")]
            MemoryRegistration::Rdma { device, .. } => device,
        }
    }
}

impl ruapc_bufpool::Registration for MemoryRegistration {
    /// Unregisters the memory from the associated device.
    ///
    /// For TCP, removes the ID from the registry.
    /// For RDMA, the `MemoryRegion` is dropped (handled by the enum
    /// variant being dropped), which calls `ibv_dereg_mr` automatically.
    fn unregister(&self, _buf: &AlignedMemory) {
        match self {
            MemoryRegistration::Tcp { device, id } => {
                if let Some(tcp) = as_tcp_device(device) {
                    tcp.unregister(*id);
                }
            }
            #[cfg(feature = "rdma")]
            MemoryRegistration::Rdma { .. } => {
                // MemoryRegion's Drop calls ibv_dereg_mr.
                // Nothing extra to do here — the variant will be dropped
                // when the MemoryRegistration is dropped.
            }
        }
    }
}

impl std::fmt::Debug for MemoryRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryRegistration::Tcp { id, .. } => f
                .debug_struct("MemoryRegistration::Tcp")
                .field("id", id)
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

/// Helper to extract the `TcpDevice` from a `Device` enum.
pub(crate) fn as_tcp_device(device: &Device) -> Option<&TcpDevice> {
    match device {
        Device::Tcp(d) => Some(d),
        #[cfg(feature = "rdma")]
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{Device, Devices};
    use ruapc_bufpool::AlignedMemory;

    fn make_tcp_registration() -> (MemoryRegistration, Arc<AlignedMemory>) {
        let devices = Devices::new();
        let tcp_arc = devices.tcp_device().clone();
        let mem = Arc::new(AlignedMemory::new(4096).unwrap());
        let id = if let Device::Tcp(d) = tcp_arc.as_ref() {
            d.register(mem.as_ptr(), mem.size())
        } else {
            panic!("expected TCP device");
        };
        let reg = MemoryRegistration::Tcp {
            device: tcp_arc,
            id,
        };
        (reg, mem)
    }

    #[test]
    fn test_memory_registration_tcp_memory_key() {
        let (reg, _mem) = make_tcp_registration();
        let key = reg.memory_key();
        assert!(matches!(key, crate::memory::MemoryKey::Tcp { .. }));
    }

    #[test]
    fn test_memory_registration_tcp_device_ref() {
        let (reg, _mem) = make_tcp_registration();
        // `device()` must return the same Arc.
        let dev = reg.device();
        assert!(matches!(dev.as_ref(), Device::Tcp(_)));
    }

    #[test]
    fn test_memory_registration_tcp_debug() {
        let (reg, _mem) = make_tcp_registration();
        let debug = format!("{reg:?}");
        assert!(debug.contains("MemoryRegistration::Tcp"));
    }

    #[test]
    fn test_memory_registration_unregister_removes_id() {
        use ruapc_bufpool::Registration as _;
        let devices = Devices::new();
        let tcp_arc = devices.tcp_device().clone();
        let mem = Arc::new(AlignedMemory::new(4096).unwrap());
        let (id, ptr, size) = if let Device::Tcp(d) = tcp_arc.as_ref() {
            let id = d.register(mem.as_ptr(), mem.size());
            (id, mem.as_ptr(), mem.size())
        } else {
            panic!("expected TCP device");
        };

        let reg = MemoryRegistration::Tcp {
            device: tcp_arc.clone(),
            id,
        };

        // After unregister, reading should fail.
        reg.unregister(&mem);
        if let Device::Tcp(d) = tcp_arc.as_ref() {
            assert!(d.read_memory(id, ptr as u64, size as u64).is_err());
        }
    }

    #[cfg(feature = "rdma")]
    fn with_rdma_registration<F: FnOnce(&MemoryRegistration)>(f: F) -> bool {
        use crate::device::{Device, Devices, RdmaDevice};
        use std::sync::Arc;

        let active_devices = match ruapc_rdma_sys::ActiveDevice::available() {
            Ok(d) => d,
            Err(_) => return false,
        };
        let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
        let active = match active_devices.into_iter().find(|d| {
            if prefer_rxe {
                d.info().name.starts_with("rxe")
            } else {
                true
            }
        }) {
            Some(a) => a,
            None => return false,
        };

        // Build Devices with TCP at 0 and RDMA at 1.
        let mut devices = Devices::new();
        devices.add_rdma_device(active);
        let devices = Arc::new(devices);

        let pool = crate::memory::BufferPool::new(devices.clone(), 4096, 4096, 0);
        let buf = match pool.allocate() {
            Ok(b) => b,
            Err(_) => return false,
        };
        let rdma_arc = devices.rdma_devices()[0].clone();
        let reg = match buf.registration(rdma_arc.as_ref()) {
            Ok(r) => r,
            Err(_) => return false,
        };
        f(reg);
        true
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn test_memory_registration_rdma_memory_key() {
        let found = with_rdma_registration(|reg| {
            let key = reg.memory_key();
            assert!(matches!(key, crate::memory::MemoryKey::Rdma { .. }));
        });
        if !found {
            eprintln!("no RDMA device available; skipping");
        }
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn test_memory_registration_rdma_device_ref() {
        let found = with_rdma_registration(|reg| {
            let dev = reg.device();
            assert!(matches!(dev.as_ref(), Device::Rdma(_)));
        });
        if !found {
            eprintln!("no RDMA device available; skipping");
        }
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn test_memory_registration_rdma_debug() {
        let found = with_rdma_registration(|reg| {
            let debug = format!("{reg:?}");
            assert!(debug.contains("MemoryRegistration::Rdma"));
        });
        if !found {
            eprintln!("no RDMA device available; skipping");
        }
    }
}
