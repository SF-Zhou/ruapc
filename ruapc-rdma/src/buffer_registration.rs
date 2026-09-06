//! Audited registration capability used by buffer-pool device collections.

use std::sync::Arc;

use ruapc_bufpool::{AlignedMemory, MemoryRegistrar, Registration};

use crate::ActiveDevice;

// SAFETY: MemoryRegion keeps the backing Arc private and never accesses its
// bytes. Its destructor deregisters the region before releasing that Arc.
// Registration itself only creates the hardware mapping; queue operations own
// the separate obligation to preserve each allocation's lifetime and access.
unsafe impl MemoryRegistrar for ActiveDevice {
    fn register(&self, memory: &Arc<AlignedMemory>) -> std::io::Result<Box<dyn Registration>> {
        let region = ActiveDevice::register(self, memory)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(Box::new(region))
    }
}
