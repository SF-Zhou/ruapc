use std::sync::Arc;

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
}

impl ruapc_bufpool::Registration for MemoryRegistration {
    /// Unregisters the memory from the associated device.
    ///
    /// For TCP, removes the ID from the registry (which also releases the
    /// `Arc<AlignedMemory>` held by `TcpDevice`).
    /// For RDMA, the `MemoryRegion` is dropped (handled by the enum
    /// variant being dropped), which calls `ibv_dereg_mr` automatically.
    fn unregister(&self) {
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
    use crate::device::Devices;
    use ruapc_bufpool::AlignedMemory;

    fn make_tcp_registration() -> (MemoryRegistration, Arc<AlignedMemory>) {
        let devices = Devices::new();
        let tcp_arc = devices.tcp_device().clone();
        let mem = Arc::new(AlignedMemory::new(4096).unwrap());
        let id = as_tcp_device(tcp_arc.as_ref())
            .expect("expected TCP device")
            .register(Arc::clone(&mem));
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
        let tcp = as_tcp_device(tcp_arc.as_ref()).expect("expected TCP device");
        let id = tcp.register(Arc::clone(&mem));
        let ptr = mem.as_ptr();
        let size = mem.size();

        let reg = MemoryRegistration::Tcp {
            device: tcp_arc.clone(),
            id,
        };

        // After unregister, reading should fail.
        reg.unregister();
        let tcp2 = as_tcp_device(tcp_arc.as_ref()).expect("expected TCP device");
        assert!(tcp2.read_memory(id, ptr as u64, size as u64).is_err());
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

        // Build Devices with TCP at 0 and RDMA at 1.
        let mut devices = Devices::new();
        devices.add_rdma_device(active);
        let devices = Arc::new(devices);

        let pool = crate::memory::BufferPool::new(devices.clone(), 4096, 4096, 0);
        let buf = pool.allocate().expect("failed to allocate RDMA buffer");
        let rdma_arc = devices.rdma_devices()[0].clone();
        let reg = buf
            .registration(rdma_arc.as_ref())
            .expect("registration should exist");
        f(reg);
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn test_memory_registration_rdma_memory_key() {
        with_rdma_registration(|reg| {
            let key = reg.memory_key();
            assert!(matches!(key, crate::memory::MemoryKey::Rdma { .. }));
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
