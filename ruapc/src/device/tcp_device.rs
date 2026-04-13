use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use ruapc_bufpool::AlignedMemory;

use crate::{Error, ErrorKind, Result};

/// A virtual TCP "device" for memory registration.
///
/// TCP doesn't have hardware memory registration like RDMA, so this
/// maintains a software registry mapping IDs to `Arc<AlignedMemory>`
/// regions. Remote Read/Write operations use reverse RPC, and this
/// registry provides the safety checks (ID validity, bounds checking).
pub struct TcpDevice {
    index: usize,
    registry: Mutex<HashMap<u32, Arc<AlignedMemory>>>,
    next_id: AtomicU32,
}

impl TcpDevice {
    /// Creates a new TCP device. The `index` is typically overwritten
    /// by [`Devices::add`](ruapc_bufpool::Devices::add) via `set_index`.
    pub fn new(index: usize) -> Self {
        Self {
            index,
            registry: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(1),
        }
    }

    /// Returns the device index assigned by `Devices::add`.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Sets the device index. Called by `Devices::add`.
    pub fn set_index(&mut self, idx: usize) {
        self.index = idx;
    }

    /// Registers a memory region and returns its assigned ID.
    pub(crate) fn register(&self, memory: Arc<AlignedMemory>) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut registry = self.registry.lock().unwrap();
        registry.insert(id, memory);
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
    /// registered under `id`, then copies data out via the `AlignedMemory`
    /// slice.
    pub fn read_memory(&self, id: u32, addr: u64, len: u64) -> Result<Vec<u8>> {
        let registry = self.registry.lock().unwrap();
        let mem = self.lookup(&registry, id)?;
        let offset = self.validate_absolute_access(mem, addr, len)?;
        let data = &mem.as_slice()[offset..offset + len as usize];
        Ok(data.to_vec())
    }

    /// Writes data into a registered memory region using absolute address.
    ///
    /// Validates that the range `[addr, addr+len)` is within the region
    /// registered under `id`, then copies data in via the `AlignedMemory`
    /// slice.
    pub fn write_memory(&self, id: u32, addr: u64, data: &[u8]) -> Result<()> {
        let registry = self.registry.lock().unwrap();
        let mem = self.lookup(&registry, id)?;
        let offset = self.validate_absolute_access(mem, addr, data.len() as u64)?;
        let dest = &mut mem.as_mut_slice()[offset..offset + data.len()];
        dest.copy_from_slice(data);
        Ok(())
    }

    /// Looks up the memory region by ID.
    fn lookup<'a>(
        &self,
        registry: &'a HashMap<u32, Arc<AlignedMemory>>,
        id: u32,
    ) -> Result<&'a Arc<AlignedMemory>> {
        registry.get(&id).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("memory region ID {id} not registered"),
            )
        })
    }

    /// Validates that an absolute address range is within the registered
    /// memory and returns the byte offset from the start of the allocation.
    fn validate_absolute_access(&self, mem: &AlignedMemory, addr: u64, len: u64) -> Result<usize> {
        let region_start = mem.as_ptr() as u64;
        let region_end = region_start + mem.size() as u64;
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

        Ok((addr - region_start) as usize)
    }

    /// Validates that an access (id, offset, len) is within a registered region.
    pub fn validate_access(&self, id: u32, offset: u64, len: u64) -> Result<usize> {
        let registry = self.registry.lock().unwrap();
        let mem = self.lookup(&registry, id)?;

        let end = offset.checked_add(len).ok_or_else(|| {
            Error::new(ErrorKind::InvalidArgument, "offset + len overflow".into())
        })?;

        if end as usize > mem.size() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "access out of bounds: offset={offset} len={len} exceeds region size={}",
                    mem.size()
                ),
            ));
        }

        Ok(mem.as_ptr() as usize + offset as usize)
    }
}

impl std::fmt::Debug for TcpDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpDevice")
            .field("index", &self.index)
            .finish()
    }
}
