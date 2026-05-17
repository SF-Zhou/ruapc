//! Device collection trait for registering memory on multiple devices.

use std::io::Result;
use std::sync::Arc;

use crate::{AlignedMemory, Registration};

/// Trait for a collection of devices that can register memory.
pub trait Devices: Send + Sync + std::fmt::Debug {
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

impl Devices for EmptyDevices {
    fn len(&self) -> usize {
        0
    }

    fn register(&self, _mem: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>> {
        Ok(Vec::new())
    }
}
