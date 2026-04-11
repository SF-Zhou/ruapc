use std::sync::Arc;

use crate::device::{Device, TcpDevice};
use crate::memory::AlignedMemory;
use crate::memory::MemoryKey;

/// Represents the registration state of an `AlignedMemory` on a specific device.
///
/// Each variant holds an `Arc<Device>` to keep the device alive, and
/// stores the device-specific registration handle needed for unregistration.
pub enum MemoryRegistration {
    Tcp {
        device: Arc<Device>,
        id: u32,
    },
    #[cfg(feature = "rdma")]
    Rdma {
        device: Arc<Device>,
        mr: ruapc_rdma::RawMemoryRegion,
    },
}

impl MemoryRegistration {
    /// Returns the `MemoryKey` for this registration.
    pub fn memory_key(&self) -> MemoryKey {
        match self {
            MemoryRegistration::Tcp { id, .. } => MemoryKey::Tcp { id: *id },
            #[cfg(feature = "rdma")]
            MemoryRegistration::Rdma { mr, .. } => MemoryKey::Rdma {
                lkey: mr.lkey,
                rkey: mr.rkey,
            },
        }
    }

    /// Unregisters the memory from the associated device.
    ///
    /// For TCP, removes the ID from the registry.
    /// For RDMA, the `RawMemoryRegion` is dropped (handled by the enum
    /// variant being dropped), which calls `ibv_dereg_mr` automatically.
    pub fn unregister(&self, _mem: &AlignedMemory) {
        match self {
            MemoryRegistration::Tcp { device, id } => {
                if let Some(tcp) = as_tcp_device(device) {
                    tcp.unregister(*id);
                }
            }
            #[cfg(feature = "rdma")]
            MemoryRegistration::Rdma { .. } => {
                // RawMemoryRegion's Drop calls ibv_dereg_mr.
                // Nothing extra to do here — the variant will be dropped
                // when the MemoryRegistration is dropped.
            }
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
                .field("lkey", &mr.lkey)
                .field("rkey", &mr.rkey)
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
