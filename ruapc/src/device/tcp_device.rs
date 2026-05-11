use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use ruapc_bufpool::AlignedMemory;

use crate::{Error, ErrorKind, Result};

pub struct TcpDevice {
    index: usize,
    registry: DashMap<u64, Arc<AlignedMemory>>,
    next_id: AtomicU64,
}

impl TcpDevice {
    pub fn new() -> Self {
        Self {
            index: 0,
            registry: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn set_index(&mut self, idx: usize) {
        self.index = idx;
    }

    /// Registers a memory region and returns its assigned ID.
    ///
    /// The `Arc<AlignedMemory>` is stored in the registry, keeping the
    /// underlying memory alive as long as the registration exists.
    pub(crate) fn register(&self, memory: Arc<AlignedMemory>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.registry.insert(id, memory);
        id
    }

    pub(crate) fn unregister(&self, id: u64) {
        self.registry.remove(&id);
    }

    /// Reads data from a registered memory region using absolute address.
    ///
    /// Validates that the range `[addr, addr+len)` falls within the region
    /// registered under `id`, then copies the data out.
    pub fn read_memory(&self, id: u64, addr: u64, len: u64) -> Result<Vec<u8>> {
        let entry = self.registry.get(&id).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("memory region ID {id} not registered"),
            )
        })?;
        let mem = entry.value();

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

        let offset = (addr - region_start) as usize;
        Ok(mem.as_slice()[offset..offset + len as usize].to_vec())
    }

    /// Writes data into a registered memory region using absolute address.
    ///
    /// Validates that the range `[addr, addr+len)` falls within the region
    /// registered under `id`, then copies the data in.
    pub fn write_memory(&self, id: u64, addr: u64, data: &[u8]) -> Result<()> {
        let entry = self.registry.get(&id).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("memory region ID {id} not registered"),
            )
        })?;
        let mem = entry.value();

        let region_start = mem.as_ptr() as u64;
        let region_end = region_start + mem.size() as u64;
        let len = data.len() as u64;
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

        let offset = (addr - region_start) as usize;
        mem.as_mut_slice()[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }
}

impl Default for TcpDevice {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TcpDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpDevice")
            .field("index", &self.index)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn make_mem(size: usize) -> Arc<AlignedMemory> {
        Arc::new(AlignedMemory::new(size).unwrap())
    }

    #[test]
    fn test_new_default_index() {
        let dev = TcpDevice::new();
        assert_eq!(dev.index(), 0);
    }

    #[test]
    fn test_set_index() {
        let mut dev = TcpDevice::new();
        dev.set_index(42);
        assert_eq!(dev.index(), 42);
    }

    #[test]
    fn test_register_assigns_sequential_ids() {
        let dev = TcpDevice::new();
        let m1 = make_mem(4096);
        let m2 = make_mem(4096);
        let id1 = dev.register(m1);
        let id2 = dev.register(m2);
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn test_unregister_removes_id() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let id = dev.register(mem);
        dev.unregister(id);
        assert!(dev.read_memory(id, ptr, 10).is_err());
    }

    #[test]
    fn test_read_memory_within_region() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        mem.as_mut_slice()[10..21].copy_from_slice(b"hello_world");
        let id = dev.register(mem);

        let data = dev.read_memory(id, ptr + 10, 5).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn test_read_memory_unknown_id() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        dev.register(mem);
        assert!(dev.read_memory(999, ptr, 10).is_err());
    }

    #[test]
    fn test_read_memory_addr_before_region() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let id = dev.register(mem);
        assert!(dev.read_memory(id, ptr - 1, 10).is_err());
    }

    #[test]
    fn test_read_memory_addr_past_region() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let size = mem.size();
        let id = dev.register(mem);
        assert!(dev.read_memory(id, ptr + size as u64 + 1, 1).is_err());
    }

    #[test]
    fn test_read_memory_len_exceeds_region() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let size = mem.size();
        let id = dev.register(mem);
        // addr within region but addr+len exceeds region end
        assert!(dev.read_memory(id, ptr + size as u64 - 10, 20).is_err());
    }

    #[test]
    fn test_read_memory_at_exact_bounds() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let size = mem.size() as u64;
        let id = dev.register(mem);
        // Read exactly the whole region
        let data = dev.read_memory(id, ptr, size).unwrap();
        assert_eq!(data.len(), size as usize);
        // Read zero bytes
        let data = dev.read_memory(id, ptr, 0).unwrap();
        assert!(data.is_empty());
        // Read exactly to the end
        let data = dev.read_memory(id, ptr + size - 10, 10).unwrap();
        assert_eq!(data.len(), 10);
    }

    #[test]
    fn test_write_memory_within_region() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let id = dev.register(Arc::clone(&mem));

        dev.write_memory(id, ptr + 5, b"hello").unwrap();
        assert_eq!(&mem.as_slice()[5..10], b"hello");
    }

    #[test]
    fn test_write_memory_unknown_id() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        dev.register(mem);
        assert!(dev.write_memory(999, ptr, b"x").is_err());
    }

    #[test]
    fn test_write_memory_out_of_bounds() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let size = mem.size();
        let id = dev.register(mem);
        assert!(
            dev.write_memory(id, ptr + size as u64 - 4, b"hello")
                .is_err()
        );
    }

    #[test]
    fn test_write_memory_before_region() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let id = dev.register(mem);
        assert!(dev.write_memory(id, ptr - 1, b"x").is_err());
    }

    #[test]
    fn test_read_write_roundtrip() {
        let dev = TcpDevice::new();
        let mem = make_mem(4096);
        let ptr = mem.as_ptr() as u64;
        let id = dev.register(mem);

        let test_data: Vec<u8> = (0..128).map(|i| i as u8).collect();
        dev.write_memory(id, ptr + 10, &test_data).unwrap();

        let read = dev
            .read_memory(id, ptr + 10, test_data.len() as u64)
            .unwrap();
        assert_eq!(read, test_data);
    }

    #[test]
    fn test_multiple_registrations() {
        let dev = TcpDevice::new();
        let m1 = make_mem(4096);
        let m2 = make_mem(4096);
        let p1 = m1.as_ptr() as u64;
        let p2 = m2.as_ptr() as u64;
        let id1 = dev.register(m1);
        let id2 = dev.register(m2);

        // Both IDs work independently
        assert!(dev.read_memory(id1, p1, 10).is_ok());
        assert!(dev.read_memory(id2, p2, 10).is_ok());

        // Unregister id1, id2 still works
        dev.unregister(id1);
        assert!(dev.read_memory(id1, p1, 10).is_err());
        assert!(dev.read_memory(id2, p2, 10).is_ok());
    }

    #[test]
    fn test_default_trait() {
        let dev = TcpDevice::default();
        assert_eq!(dev.index(), 0);
    }

    #[test]
    fn test_debug_format() {
        let dev = TcpDevice::new();
        let debug = format!("{:?}", dev);
        assert!(debug.contains("TcpDevice"));
        assert!(debug.contains("index"));
    }

    #[test]
    fn test_concurrent_register_and_read() {
        let dev = Arc::new(TcpDevice::new());
        let mut handles = vec![];

        for i in 0..4 {
            let dev = dev.clone();
            let handle = thread::spawn(move || {
                let mem = make_mem(4096);
                mem.as_mut_slice()[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                let ptr = mem.as_ptr() as u64;
                let id = dev.register(Arc::clone(&mem));
                let data = dev.read_memory(id, ptr, 8).unwrap();
                assert_eq!(&data[..8], &(i as u64).to_le_bytes());
                id
            });
            handles.push(handle);
        }

        for h in handles {
            let id = h.join().unwrap();
            dev.unregister(id);
        }
    }
}
