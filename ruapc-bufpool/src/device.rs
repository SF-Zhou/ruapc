//! Device and registration traits for memory registration.

use std::io::Result;
use std::sync::Arc;

use crate::{AlignedMemory, MemoryKey};

/// An index identifying a device within a device collection.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DeviceIndex {
    /// Magic number identifying the device collection.
    pub magic: u32,
    /// Index within the collection.
    pub index: u32,
}

/// Trait for types that can provide a [`DeviceIndex`].
pub trait AsDeviceIndex {
    /// Returns the device index.
    fn as_device_index(&self) -> DeviceIndex;
}

/// Trait representing a device registration handle.
///
/// Implementations track whatever state the device needs (e.g. RDMA memory region keys).
pub trait Registration: Send + Sync + std::fmt::Debug {
    /// Returns the memory key for this registration.
    fn memory_key(&self) -> MemoryKey;
}

/// Trait representing a device that can register memory.
pub trait Device: Send + Sync + std::fmt::Debug {
    /// Returns the device index.
    fn index(&self) -> DeviceIndex;

    /// Sets the device index.
    fn set_index(&mut self, idx: DeviceIndex);

    /// Registers the given aligned memory region with this device.
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
