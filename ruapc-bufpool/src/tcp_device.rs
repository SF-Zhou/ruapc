use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;

use crate::{AlignedMemory, Device, DeviceIndex, MemoryKey, Registration};

#[derive(Default)]
struct TcpRegistry {
    map: DashMap<u32, Arc<AlignedMemory>>,
    free_ids: Mutex<Vec<u32>>,
}

impl Debug for TcpRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpRegistry").finish()
    }
}

#[derive(Default)]
pub struct TcpDevice {
    index: DeviceIndex,
    registry: Arc<TcpRegistry>,
    next_id: AtomicU32,
}

#[derive(Debug)]
pub struct TcpMemoryRegistration {
    registry: Arc<TcpRegistry>,
    pub id: u32,
}

impl Drop for TcpMemoryRegistration {
    fn drop(&mut self) {
        self.registry.map.remove(&self.id);
        self.registry.free_ids.lock().unwrap().push(self.id);
    }
}

impl Registration for TcpMemoryRegistration {
    fn memory_key(&self) -> MemoryKey {
        MemoryKey {
            lkey: self.id,
            rkey: self.id,
        }
    }
}

impl Device for TcpDevice {
    fn index(&self) -> DeviceIndex {
        self.index
    }

    fn set_index(&mut self, idx: DeviceIndex) {
        self.index = idx;
    }

    fn register(&self, mem: &Arc<AlignedMemory>) -> std::io::Result<Box<dyn Registration>> {
        let id = self
            .registry
            .free_ids
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::Relaxed));
        self.registry.map.insert(id, Arc::clone(mem));
        Ok(Box::new(TcpMemoryRegistration {
            registry: self.registry.clone(),
            id,
        }))
    }
}

impl TcpDevice {
    pub fn read_memory(&self, id: u32, addr: u64, len: u64) -> std::io::Result<Vec<u8>> {
        let entry = self.registry.map.get(&id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("memory region ID {id} not registered"),
            )
        })?;
        let mem = entry.value();

        let region_start = mem.as_ptr() as u64;
        let region_end = region_start + mem.size() as u64;
        let access_end = addr.checked_add(len).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "addr + len overflow")
        })?;

        if addr < region_start || access_end > region_end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "access out of bounds: addr={addr:#x} len={len} outside region [{region_start:#x}, {region_end:#x})"
                ),
            ));
        }

        let offset = (addr - region_start) as usize;
        Ok(mem.as_slice()[offset..offset + len as usize].to_vec())
    }
}

impl Debug for TcpDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpDevice")
            .field("index", &self.index)
            .finish()
    }
}
