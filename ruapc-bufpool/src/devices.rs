//! Device collection trait for registering memory on multiple devices.

use std::io::Result;
use std::sync::Arc;

use crate::{AlignedMemory, Registration};

/// Trait for a collection of devices that can register memory.
///
/// # Safety
///
/// The pool grants exclusive access to individual allocations through
/// [`crate::Buffer`]. Implementations may retain the supplied `Arc` to keep a
/// registration alive, but must not expose it to callers or access its bytes
/// independently of those allocations. Any CPU or device access must preserve
/// the allocation's lifetime and obey its shared/exclusive borrowing rules.
/// In particular, retaining the `Arc` does not authorize reading its slice while
/// a buffer can be mutated or returned to the pool.
///
/// Registration handle destruction must end any use of its region before it
/// returns; the pool releases backing memory after dropping all handles.
pub unsafe trait Devices: Send + Sync + std::fmt::Debug {
    /// Returns the number of devices.
    fn len(&self) -> usize;

    /// Returns `true` if there are no devices.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Registers the given aligned memory on all devices.
    fn register(&self, mem: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>>;
}

/// An empty device collection that performs no registrations.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyDevices;

// SAFETY: this collection neither retains nor accesses the supplied memory.
unsafe impl Devices for EmptyDevices {
    fn len(&self) -> usize {
        0
    }

    fn register(&self, _mem: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>> {
        Ok(Vec::new())
    }
}
