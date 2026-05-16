use std::sync::Arc;
use std::sync::atomic::Ordering;

use ruapc_bufpool::{AlignedMemory, DeviceIndex, Registration};
use ruapc_bufpool::{Device as _, TcpDevice};

#[cfg(feature = "rdma")]
use crate::rdma::RdmaDevice;

pub type Buffer = ruapc_bufpool::Buffer;
pub type BufferPool = ruapc_bufpool::BufferPool;

#[derive(Debug)]
pub struct Devices {
    tcp_device: TcpDevice,
    #[cfg(feature = "rdma")]
    rdma_devices: Vec<RdmaDevice>,
    #[allow(unused)]
    magic: u32,
}

static NEXT_MAGIC: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

impl Default for Devices {
    fn default() -> Self {
        let magic = NEXT_MAGIC.fetch_add(1, Ordering::Relaxed);
        let mut tcp_device = TcpDevice::default();
        tcp_device.set_index(DeviceIndex { magic, index: 0 });
        Self {
            tcp_device,
            #[cfg(feature = "rdma")]
            rdma_devices: Vec::new(),
            magic,
        }
    }
}

impl Devices {
    pub fn tcp_device(&self) -> &TcpDevice {
        &self.tcp_device
    }

    #[cfg(feature = "rdma")]
    pub fn rdma_devices(&self) -> &[RdmaDevice] {
        &self.rdma_devices
    }

    #[cfg(feature = "rdma")]
    pub fn add_rdma_device(&mut self, inner_dev: ruapc_rdma::ActiveDevice) {
        let index = (1 + self.rdma_devices.len()) as u32;
        let mut dev = RdmaDevice::new(inner_dev);
        dev.set_index(DeviceIndex {
            magic: self.magic,
            index,
        });
        self.rdma_devices.push(dev);
    }
}

impl ruapc_bufpool::Devices for Devices {
    fn len(&self) -> usize {
        #[cfg(feature = "rdma")]
        {
            1 + self.rdma_devices.len()
        }
        #[cfg(not(feature = "rdma"))]
        {
            1
        }
    }

    fn register(&self, mem: &Arc<AlignedMemory>) -> std::io::Result<Vec<Box<dyn Registration>>> {
        #[cfg(not(feature = "rdma"))]
        {
            Ok(vec![self.tcp_device.register(mem)?])
        }
        #[cfg(feature = "rdma")]
        {
            let mut regs = vec![self.tcp_device.register(mem)?];
            for rdma in &self.rdma_devices {
                regs.push(rdma.register(mem)?);
            }
            Ok(regs)
        }
    }
}
