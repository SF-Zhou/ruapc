mod tcp_device;
pub use tcp_device::TcpDevice;

#[cfg(feature = "rdma")]
mod rdma_device;
#[cfg(feature = "rdma")]
pub use rdma_device::RdmaDevice;

use std::sync::Arc;

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
    /// Returns the unique index assigned by [`Devices::add_device`].
    pub fn index(&self) -> usize {
        match self {
            Device::Tcp(d) => d.index(),
            #[cfg(feature = "rdma")]
            Device::Rdma(d) => d.index(),
        }
    }
}

/// A fixed collection of devices. Devices are assigned monotonically
/// increasing indices when added. The set must be finalized before
/// creating a `BufferPool`.
#[derive(Debug)]
pub struct Devices {
    devices: Vec<Arc<Device>>,
}

impl Devices {
    /// Creates an empty device collection.
    pub fn new() -> Self {
        Self {
            devices: Vec::new(),
        }
    }

    /// Adds a TCP device and returns a shared reference to it.
    pub fn add_tcp_device(&mut self) -> Arc<Device> {
        let index = self.devices.len();
        let device = Arc::new(Device::Tcp(TcpDevice::new(index)));
        self.devices.push(device.clone());
        device
    }

    /// Adds an RDMA device and returns a shared reference to it.
    #[cfg(feature = "rdma")]
    pub fn add_rdma_device(&mut self, inner: Arc<ruapc_rdma::Device>) -> Arc<Device> {
        let index = self.devices.len();
        let device = Arc::new(Device::Rdma(RdmaDevice::new(index, inner)));
        self.devices.push(device.clone());
        device
    }

    /// Returns the number of devices.
    pub fn len(&self) -> usize {
        self.devices.len()
    }

    /// Returns true if no devices have been added.
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty()
    }

    /// Returns an iterator over the devices.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<Device>> {
        self.devices.iter()
    }

    /// Returns the device at the given index.
    pub fn get(&self, index: usize) -> Option<&Arc<Device>> {
        self.devices.get(index)
    }
}

impl Default for Devices {
    fn default() -> Self {
        Self::new()
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
