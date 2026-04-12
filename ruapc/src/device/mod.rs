mod tcp_device;
pub use tcp_device::TcpDevice;

#[cfg(feature = "rdma")]
mod rdma_device;
#[cfg(feature = "rdma")]
pub use rdma_device::RdmaDevice;

use std::sync::Arc;

use crate::Result;
use crate::memory::MemoryRegistration;

/// A network device abstraction using enum dispatch.
///
/// Can be a real RDMA NIC or a virtual TCP device. Memory must be
/// registered on a device before it can participate in Remote Read/Write
/// operations through that device's transport.
#[derive(Debug)]
pub enum Device {
    Tcp(TcpDevice),
    #[cfg(feature = "rdma")]
    Rdma(RdmaDevice),
}

impl Device {
    /// Returns the unique index assigned by [`Devices::add`].
    ///
    /// This is a convenience wrapper so callers don't need to import
    /// `ruapc_bufpool::Device`.
    pub fn index(&self) -> usize {
        match self {
            Device::Tcp(d) => d.index(),
            #[cfg(feature = "rdma")]
            Device::Rdma(d) => d.index(),
        }
    }

    /// Reads data from a registered memory region using absolute address.
    pub fn read_memory(&self, id: u32, addr: u64, len: u64) -> Result<Vec<u8>> {
        match self {
            Device::Tcp(d) => d.read_memory(id, addr, len),
            #[cfg(feature = "rdma")]
            Device::Rdma(_) => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "read_memory not supported on RDMA device".into(),
            )),
        }
    }

    /// Writes data into a registered memory region using absolute address.
    pub fn write_memory(&self, id: u32, addr: u64, data: &[u8]) -> Result<()> {
        match self {
            Device::Tcp(d) => d.write_memory(id, addr, data),
            #[cfg(feature = "rdma")]
            Device::Rdma(_) => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "write_memory not supported on RDMA device".into(),
            )),
        }
    }
}

/// Implementation of the `ruapc_bufpool::Device` trait for the `Device` enum.
///
/// This connects the generic buffer pool machinery to ruapc's concrete
/// TCP and RDMA device types.
impl ruapc_bufpool::Device for Device {
    type Registration = MemoryRegistration;

    fn index(&self) -> usize {
        match self {
            Device::Tcp(d) => d.index(),
            #[cfg(feature = "rdma")]
            Device::Rdma(d) => d.index(),
        }
    }

    fn set_index(&mut self, idx: usize) {
        match self {
            Device::Tcp(d) => d.set_index(idx),
            #[cfg(feature = "rdma")]
            Device::Rdma(d) => d.set_index(idx),
        }
    }

    #[allow(unsafe_code)]
    fn register(
        self: &Arc<Self>,
        mem: &mut ruapc_bufpool::RegisteredMemory<Self::Registration>,
    ) -> std::io::Result<()> {
        let aligned = mem.aligned_memory();
        let ptr = aligned.as_ptr();
        let size = aligned.size();

        match self.as_ref() {
            Device::Tcp(tcp) => {
                let id = tcp.register(ptr as usize, size);
                mem.add_registration(MemoryRegistration::Tcp {
                    device: self.clone(),
                    id,
                });
                Ok(())
            }
            #[cfg(feature = "rdma")]
            Device::Rdma(rdma) => {
                let mr = unsafe {
                    ruapc_rdma::verbs::ibv_reg_mr(
                        rdma.pd_ptr(),
                        ptr as *mut _,
                        size,
                        ruapc_rdma::verbs::ACCESS_FLAGS as _,
                    )
                };
                if mr.is_null() {
                    return Err(std::io::Error::other("ibv_reg_mr failed"));
                }
                let raw_mr = unsafe { ruapc_rdma::RawMemoryRegion::from_raw(mr) };
                mem.add_registration(MemoryRegistration::Rdma {
                    device: self.clone(),
                    mr: raw_mr,
                });
                Ok(())
            }
        }
    }
}

/// Type alias for the device collection backed by `ruapc_bufpool`.
pub type Devices = ruapc_bufpool::Devices<Device>;

/// Extension methods for `Devices` providing convenient device-addition helpers.
pub trait DevicesExt {
    /// Adds a TCP device and returns a shared reference to it.
    fn add_tcp_device(&mut self) -> Arc<Device>;

    /// Adds an RDMA device and returns a shared reference to it.
    #[cfg(feature = "rdma")]
    fn add_rdma_device(&mut self, inner: Arc<ruapc_rdma::Device>) -> Arc<Device>;

    /// Finds the `Device` that wraps the given `ruapc_rdma::Device`.
    #[cfg(feature = "rdma")]
    fn find_by_rdma_device(&self, inner: &Arc<ruapc_rdma::Device>) -> Option<&Arc<Device>>;

    /// Collects the inner `ruapc_rdma::Device` arcs from all RDMA devices.
    #[cfg(feature = "rdma")]
    fn rdma_inner_devices(&self) -> ruapc_rdma::Devices;
}

impl DevicesExt for Devices {
    fn add_tcp_device(&mut self) -> Arc<Device> {
        self.add(Device::Tcp(TcpDevice::new(0)))
    }

    #[cfg(feature = "rdma")]
    fn add_rdma_device(&mut self, inner: Arc<ruapc_rdma::Device>) -> Arc<Device> {
        self.add(Device::Rdma(RdmaDevice::new(0, inner)))
    }

    #[cfg(feature = "rdma")]
    fn find_by_rdma_device(&self, inner: &Arc<ruapc_rdma::Device>) -> Option<&Arc<Device>> {
        self.iter().find(|d| match d.as_ref() {
            Device::Rdma(r) => Arc::ptr_eq(r.inner(), inner),
            _ => false,
        })
    }

    #[cfg(feature = "rdma")]
    fn rdma_inner_devices(&self) -> ruapc_rdma::Devices {
        let arcs = self
            .iter()
            .filter_map(|d| {
                if let Device::Rdma(r) = d.as_ref() {
                    Some(r.inner().clone())
                } else {
                    None
                }
            })
            .collect();
        ruapc_rdma::Devices::from_arcs(arcs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_devices_add_tcp() {
        let mut devices = Devices::new();
        let d0 = devices.add_tcp_device();
        let d1 = devices.add_tcp_device();
        assert_eq!(d0.index(), 0);
        assert_eq!(d1.index(), 1);
        assert_eq!(devices.len(), 2);
    }

    #[test]
    fn test_tcp_device_register_validate() {
        let dev = TcpDevice::new(0);
        let id = dev.register(0x1000, 4096);

        // Valid access.
        assert!(dev.validate_access(id, 0, 100).is_ok());
        assert!(dev.validate_access(id, 4000, 96).is_ok());

        // Out of bounds.
        assert!(dev.validate_access(id, 4000, 97).is_err());

        // Unknown ID.
        assert!(dev.validate_access(id + 1, 0, 1).is_err());

        // Unregister.
        dev.unregister(id);
        assert!(dev.validate_access(id, 0, 1).is_err());
    }
}
