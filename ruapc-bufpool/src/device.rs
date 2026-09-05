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

/// A device's audited memory-registration capability.
///
/// # Safety
///
/// Implementations may retain the supplied memory only to keep a registration
/// alive. They must not expose that memory to callers or access its bytes
/// independently of the pool's individual allocations. CPU and device accesses
/// must preserve each allocation's lifetime and shared/exclusive borrowing rules.
/// Dropping a registration must end its use of the region before returning.
///
/// This bound lets [`crate::DeviceSet`] combine devices without handing backing
/// memory to application-defined, safe device wrappers.
/// Implementing a registrar requires an explicit safety commitment:
///
/// ```compile_fail,E0200
/// use std::sync::Arc;
/// use ruapc_bufpool::{AlignedMemory, MemoryRegistrar, Registration};
///
/// #[derive(Debug)]
/// struct UncheckedRegistrar;
///
/// impl MemoryRegistrar for UncheckedRegistrar {
///     fn register(&self, _: &Arc<AlignedMemory>)
///         -> std::io::Result<Box<dyn Registration>>
///     {
///         unimplemented!()
///     }
/// }
/// ```
pub unsafe trait MemoryRegistrar: Send + Sync + std::fmt::Debug {
    /// Registers the given aligned memory region with this device.
    fn register(&self, mem: &Arc<AlignedMemory>) -> Result<Box<dyn Registration>>;
}

/// Device identity and a reference to its audited registration capability.
///
/// Application wrappers may implement this trait safely: they never receive
/// pool memory. [`crate::DeviceSet`] registers memory directly with the returned
/// [`MemoryRegistrar`], so wrappers cannot intercept the registered region.
pub trait Device: Send + Sync + std::fmt::Debug {
    /// The implementation responsible for registration and memory lifetime.
    type Registrar: MemoryRegistrar;

    /// Returns this device's registration capability.
    fn registrar(&self) -> &Self::Registrar;

    /// Returns the device index.
    fn index(&self) -> DeviceIndex;

    /// Sets the device index.
    fn set_index(&mut self, idx: DeviceIndex);
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
