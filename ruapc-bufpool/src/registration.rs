//! Memory registration capability and owned registration handles.

use std::io::Result;
use std::sync::Arc;

use crate::{AlignedMemory, MemoryKey};

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
/// [`crate::BufferPool`] calls registrars in their configured order and retains
/// them until all registrations have been dropped.
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
