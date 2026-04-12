use std::io::Result;
use std::sync::Arc;

use crate::RegisteredMemory;

/// A device registration handle for a memory region.
///
/// When a `RegisteredMemory` is dropped, `unregister` is called on each
/// registration to clean up device-side state.
pub trait Registration: Send + Sync + std::fmt::Debug {
    /// Unregisters the memory from the device.
    ///
    /// `buf` is the byte slice of the underlying `AlignedMemory`,
    /// provided for implementations that need the address/length
    /// during teardown.
    fn unregister(&self, buf: &[u8]);
}

/// A device that can register memory regions for zero-copy I/O.
///
/// Implementations live outside `ruapc-bufpool` (e.g. in `ruapc` for
/// TCP and RDMA devices). The `Device` trait uses static dispatch via
/// generics rather than trait objects.
pub trait Device: Send + Sync + std::fmt::Debug {
    /// The registration handle returned by [`register`](Self::register).
    type Registration: Registration;

    /// Returns the unique index assigned to this device within a
    /// [`Devices`] collection.
    fn index(&self) -> usize;

    /// Sets the device index. Called by [`Devices::add`] when
    /// the device is added to the collection.
    fn set_index(&mut self, idx: usize);

    /// Registers this device's memory region and pushes the registration
    /// into `mem` on success. If registration fails, `mem` is left
    /// unchanged — already-registered devices remain valid for cleanup.
    fn register(self: &Arc<Self>, mem: &mut RegisteredMemory<Self::Registration>) -> Result<()>;
}

/// A fixed collection of devices.
///
/// Devices are assigned monotonically increasing indices when added.
/// The set must be finalized before creating a `BufferPool`.
#[derive(Debug)]
pub struct Devices<D: Device> {
    devices: Vec<Arc<D>>,
}

impl<D: Device> Devices<D> {
    /// Creates an empty device collection.
    pub fn new() -> Self {
        Self {
            devices: Vec::new(),
        }
    }

    /// Adds a device to the collection, assigning it the next index.
    ///
    /// Returns a shared reference to the device.
    pub fn add(&mut self, mut device: D) -> Arc<D> {
        let index = self.devices.len();
        device.set_index(index);
        let device = Arc::new(device);
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
    pub fn iter(&self) -> impl Iterator<Item = &Arc<D>> {
        self.devices.iter()
    }

    /// Returns the device at the given index.
    pub fn get(&self, index: usize) -> Option<&Arc<D>> {
        self.devices.get(index)
    }

    /// Returns a reference to the inner device Vec.
    pub fn as_slice(&self) -> &[Arc<D>] {
        &self.devices
    }
}

impl<D: Device> Default for Devices<D> {
    fn default() -> Self {
        Self::new()
    }
}
