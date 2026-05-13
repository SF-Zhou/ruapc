use ruapc_bufpool::Device as _;

use crate::Device;

#[derive(Debug)]
pub struct Devices {
    inner: Vec<Device>,
}

impl Devices {
    /// Creates a new `Devices` instance with a TCP device. RDMA devices can be added later using `add_rdma_device`.
    pub fn new() -> Self {
        let mut this = Self { inner: Vec::new() };
        this.add(Device::Tcp(crate::device::TcpDevice::new()));
        this
    }

    /// Returns a reference to the TCP device. The TCP device is always at index 0.
    pub fn tcp_device(&self) -> &Device {
        &self.inner[0]
    }

    #[cfg(feature = "rdma")]
    /// Returns a slice of RDMA devices. The TCP device is at index 0, so RDMA devices start from index 1.
    pub fn rdma_devices(&self) -> &[Device] {
        &self.inner[1..]
    }

    #[cfg(feature = "rdma")]
    pub fn add_rdma_device(&mut self, inner: ruapc_rdma_sys::ActiveDevice) {
        use crate::device::RdmaDevice;
        self.add(Device::Rdma(RdmaDevice::new(inner)))
    }

    fn add(&mut self, mut device: Device) {
        let index = self.inner.len();
        device.set_index(index);
        self.inner.push(device);
    }
}

impl ruapc_bufpool::Devices for Devices {
    type Device = Device;
    type Iter<'a> = std::slice::Iter<'a, Device>;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.inner.iter()
    }
}

impl Default for Devices {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<Devices> for Devices {
    fn as_ref(&self) -> &Devices {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::device::{Devices, TcpDevice};
    use ruapc_bufpool::{AlignedMemory, Device, Devices as _};
    use std::sync::Arc;

    #[test]
    fn test_devices_add_tcp() {
        let devices = Devices::new();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices.iter().next().unwrap().index(), 0);
    }

    #[test]
    fn test_devices_is_empty() {
        let devices = Devices::new();
        // Always has at least the TCP device, so not empty.
        assert!(!devices.is_empty());
    }

    #[test]
    fn test_devices_as_ref() {
        let devices = Devices::new();
        let r: &Devices = devices.as_ref();
        assert_eq!(r.len(), devices.len());
    }

    #[test]
    fn test_tcp_device_register_validate() {
        let dev = TcpDevice::new();
        let mem = Arc::new(AlignedMemory::new(4096).unwrap());
        let ptr = mem.as_ptr() as u64;
        let size = mem.size() as u64;
        let reg = dev.register(Arc::clone(&mem));
        let id = reg.id;

        // Valid access.
        assert!(dev.read_memory(id, ptr, 100).is_ok());
        assert!(dev.read_memory(id, ptr + size - 96, 96).is_ok());

        // Out of bounds.
        assert!(dev.read_memory(id, ptr + size - 96, 97).is_err());

        // Unknown ID.
        assert!(dev.read_memory(id + 1, ptr, 1).is_err());

        drop(reg);
        assert!(dev.read_memory(id, ptr, 1).is_err());
    }
}
