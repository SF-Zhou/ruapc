use std::io::Result;
use std::sync::Arc;

use crate::{AlignedMemory, MemoryKey};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DeviceIndex {
    pub magic: u32,
    pub index: u32,
}

pub trait AsDeviceIndex {
    fn as_device_index(&self) -> DeviceIndex;
}

pub trait Registration: Send + Sync + std::fmt::Debug {
    fn memory_key(&self) -> MemoryKey;
}

pub trait Device: Send + Sync + std::fmt::Debug {
    fn index(&self) -> DeviceIndex;

    fn set_index(&mut self, idx: DeviceIndex);

    fn register(&self, mem: &Arc<AlignedMemory>) -> Result<Box<dyn Registration>>;
}

impl AsDeviceIndex for DeviceIndex {
    fn as_device_index(&self) -> DeviceIndex {
        *self
    }
}

impl<T: Device> AsDeviceIndex for T {
    fn as_device_index(&self) -> DeviceIndex {
        self.index()
    }
}
