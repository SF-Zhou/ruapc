use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::{Error, ErrorKind, Result};

/// A virtual TCP "device" for memory registration.
///
/// TCP doesn't have hardware memory registration like RDMA, so this
/// maintains a software registry mapping IDs to memory regions.
/// Remote Read/Write operations use reverse RPC, and this registry
/// provides the safety checks (ID validity, bounds checking).
pub struct TcpDevice {
    index: usize,
    registry: Mutex<HashMap<u32, MemoryRegion>>,
    next_id: AtomicU32,
}

#[derive(Debug, Clone, Copy)]
struct MemoryRegion {
    addr: usize,
    len: usize,
}

impl TcpDevice {
    pub(crate) fn new(index: usize) -> Self {
        Self {
            index,
            registry: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(1),
        }
    }

    /// Returns the device index assigned by `Devices::add_device`.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Registers a memory region and returns its assigned ID.
    pub(crate) fn register(&self, addr: usize, len: usize) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut registry = self.registry.lock().unwrap();
        registry.insert(id, MemoryRegion { addr, len });
        id
    }

    /// Unregisters a previously registered memory region.
    pub(crate) fn unregister(&self, id: u32) {
        let mut registry = self.registry.lock().unwrap();
        registry.remove(&id);
    }

    /// Validates that an access (id, offset, len) is within a registered region.
    pub fn validate_access(&self, id: u32, offset: u64, len: u64) -> Result<usize> {
        let registry = self.registry.lock().unwrap();
        let region = registry.get(&id).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("memory region ID {id} not registered"),
            )
        })?;

        let end = offset.checked_add(len).ok_or_else(|| {
            Error::new(ErrorKind::InvalidArgument, "offset + len overflow".into())
        })?;

        if end as usize > region.len {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "access out of bounds: offset={offset} len={len} exceeds region size={}",
                    region.len
                ),
            ));
        }

        Ok(region.addr + offset as usize)
    }
}

impl std::fmt::Debug for TcpDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpDevice")
            .field("index", &self.index)
            .finish()
    }
}
