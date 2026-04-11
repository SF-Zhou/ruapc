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

    /// Reads data from a registered memory region using absolute address.
    ///
    /// Validates that the range `[addr, addr+len)` is within the region
    /// registered under `id`, then copies data out.
    ///
    /// # Safety justification
    ///
    /// The memory was registered by the owning `Memory` struct which holds
    /// both the `AlignedMemory` allocation and the `Arc<Device>` keeping
    /// this `TcpDevice` alive. As long as the registration exists in the
    /// registry, the underlying memory is guaranteed to be valid.
    #[allow(unsafe_code)]
    pub fn read_memory(&self, id: u32, addr: u64, len: u64) -> Result<Vec<u8>> {
        self.validate_absolute_access(id, addr, len)?;
        // SAFETY: validate_absolute_access confirmed the region is registered
        // and the range [addr, addr+len) is within bounds.
        let data = unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) };
        Ok(data.to_vec())
    }

    /// Writes data into a registered memory region using absolute address.
    ///
    /// Validates that the range `[addr, addr+len)` is within the region
    /// registered under `id`, then copies data in.
    ///
    /// # Safety justification
    ///
    /// Same as `read_memory` — the registration guarantees the memory is valid.
    #[allow(unsafe_code)]
    pub fn write_memory(&self, id: u32, addr: u64, data: &[u8]) -> Result<()> {
        self.validate_absolute_access(id, addr, data.len() as u64)?;
        // SAFETY: validate_absolute_access confirmed the region is registered
        // and the range [addr, addr+len) is within bounds.
        let dest = unsafe { std::slice::from_raw_parts_mut(addr as *mut u8, data.len()) };
        dest.copy_from_slice(data);
        Ok(())
    }

    /// Validates that an absolute address range is within a registered region.
    fn validate_absolute_access(&self, id: u32, addr: u64, len: u64) -> Result<()> {
        let registry = self.registry.lock().unwrap();
        let region = registry.get(&id).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("memory region ID {id} not registered"),
            )
        })?;

        let region_start = region.addr as u64;
        let region_end = region_start + region.len as u64;
        let access_end = addr
            .checked_add(len)
            .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "addr + len overflow".into()))?;

        if addr < region_start || access_end > region_end {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "access out of bounds: addr={addr:#x} len={len} outside region [{region_start:#x}, {region_end:#x})"
                ),
            ));
        }

        Ok(())
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
