use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::{Error, ErrorKind, Result};

pub struct TcpDevice {
    index: usize,
    registry: DashMap<u64, (usize, usize)>,
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
    /// The caller guarantees `ptr` points to `size` bytes of valid
    /// memory that will not be freed until `unregister` is called.
    pub(crate) fn register(&self, ptr: *const u8, size: usize) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.registry.insert(id, (ptr as usize, size));
        id
    }

    pub(crate) fn unregister(&self, id: u64) {
        self.registry.remove(&id);
    }

    /// Reads data from a registered memory region using absolute address.
    ///
    /// Validates that the range `[addr, addr+len)` falls within the region
    /// registered under `id`, then copies data out via direct pointer access.
    #[allow(unsafe_code)]
    pub fn read_memory(&self, id: u64, addr: u64, len: u64) -> Result<Vec<u8>> {
        let entry = self.registry.get(&id).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("memory region ID {id} not registered"),
            )
        })?;
        let (ptr, size) = *entry;

        let region_start = ptr as u64;
        let region_end = region_start + size as u64;
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
        let mut data = vec![0u8; len as usize];
        unsafe {
            std::ptr::copy_nonoverlapping(
                (ptr as *const u8).add(offset),
                data.as_mut_ptr(),
                len as usize,
            );
        }
        Ok(data)
    }

    /// Writes data into a registered memory region using absolute address.
    ///
    /// Validates that the range `[addr, addr+len)` falls within the region
    /// registered under `id`, then copies data in via direct pointer access.
    #[allow(unsafe_code)]
    pub fn write_memory(&self, id: u64, addr: u64, data: &[u8]) -> Result<()> {
        let entry = self.registry.get(&id).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("memory region ID {id} not registered"),
            )
        })?;
        let (ptr, size) = *entry;

        let region_start = ptr as u64;
        let region_end = region_start + size as u64;
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
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), (ptr as *mut u8).add(offset), data.len());
        }
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
#[allow(unsafe_code)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn make_buf(size: usize) -> (Vec<u8>, *const u8) {
        let buf = vec![0u8; size];
        let ptr = buf.as_ptr();
        (buf, ptr)
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
        let (_b1, p1) = make_buf(100);
        let (_b2, p2) = make_buf(200);
        let id1 = dev.register(p1, _b1.len());
        let id2 = dev.register(p2, _b2.len());
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn test_unregister_removes_id() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        let id = dev.register(p, _b.len());
        dev.unregister(id);
        assert!(dev.read_memory(id, p as u64, 10).is_err());
    }

    #[test]
    fn test_read_memory_within_region() {
        let dev = TcpDevice::new();
        let mut buf = vec![0u8; 100];
        buf[10..21].copy_from_slice(b"hello_world");
        let ptr = buf.as_ptr();
        let id = dev.register(ptr, buf.len());

        let data = dev.read_memory(id, ptr as u64 + 10, 5).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn test_read_memory_unknown_id() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        dev.register(p, _b.len());
        assert!(dev.read_memory(999, 0, 10).is_err());
    }

    #[test]
    fn test_read_memory_addr_before_region() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        let id = dev.register(p, _b.len());
        assert!(dev.read_memory(id, p as u64 - 1, 10).is_err());
    }

    #[test]
    fn test_read_memory_addr_past_region() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        let id = dev.register(p, _b.len());
        assert!(dev.read_memory(id, p as u64 + 101, 1).is_err());
    }

    #[test]
    fn test_read_memory_len_exceeds_region() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        let id = dev.register(p, _b.len());
        // addr within region but addr+len exceeds region end
        assert!(dev.read_memory(id, p as u64 + 90, 20).is_err());
    }

    #[test]
    fn test_read_memory_at_exact_bounds() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        let id = dev.register(p, _b.len());
        // Read exactly the whole region
        let data = dev.read_memory(id, p as u64, 100).unwrap();
        assert_eq!(data.len(), 100);
        // Read zero bytes
        let data = dev.read_memory(id, p as u64, 0).unwrap();
        assert!(data.is_empty());
        // Read exactly to the end
        let data = dev.read_memory(id, p as u64 + 90, 10).unwrap();
        assert_eq!(data.len(), 10);
    }

    #[test]
    fn test_write_memory_within_region() {
        let dev = TcpDevice::new();
        let buf = vec![0u8; 100];
        let ptr = buf.as_ptr();
        let id = dev.register(ptr, buf.len());

        dev.write_memory(id, ptr as u64 + 5, b"hello").unwrap();
        assert_eq!(&buf[5..10], b"hello");
    }

    #[test]
    fn test_write_memory_unknown_id() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        dev.register(p, _b.len());
        assert!(dev.write_memory(999, p as u64, b"x").is_err());
    }

    #[test]
    fn test_write_memory_out_of_bounds() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        let id = dev.register(p, _b.len());
        assert!(dev.write_memory(id, p as u64 + 96, b"hello").is_err());
    }

    #[test]
    fn test_write_memory_before_region() {
        let dev = TcpDevice::new();
        let (_b, p) = make_buf(100);
        let id = dev.register(p, _b.len());
        assert!(dev.write_memory(id, p as u64 - 1, b"x").is_err());
    }

    #[test]
    fn test_read_write_roundtrip() {
        let dev = TcpDevice::new();
        let buf = vec![0u8; 256];
        let ptr = buf.as_ptr();
        let id = dev.register(ptr, buf.len());

        let test_data: Vec<u8> = (0..128).map(|i| i as u8).collect();
        dev.write_memory(id, ptr as u64 + 10, &test_data).unwrap();

        let read = dev
            .read_memory(id, ptr as u64 + 10, test_data.len() as u64)
            .unwrap();
        assert_eq!(read, test_data);
    }

    #[test]
    fn test_multiple_registrations() {
        let dev = TcpDevice::new();
        let (_b1, p1) = make_buf(100);
        let (_b2, p2) = make_buf(200);
        let id1 = dev.register(p1, _b1.len());
        let id2 = dev.register(p2, _b2.len());

        // Both IDs work independently
        assert!(dev.read_memory(id1, p1 as u64, 10).is_ok());
        assert!(dev.read_memory(id2, p2 as u64, 10).is_ok());

        // Unregister id1, id2 still works
        dev.unregister(id1);
        assert!(dev.read_memory(id1, p1 as u64, 10).is_err());
        assert!(dev.read_memory(id2, p2 as u64, 10).is_ok());
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
                let mut buf = vec![0u8; 64];
                buf[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                let ptr = buf.as_ptr() as u64;
                let id = dev.register(buf.as_ptr(), buf.len());
                let data = dev.read_memory(id, ptr, 8).unwrap();
                assert_eq!(&data[..8], &(i as u64).to_le_bytes());
                (id, buf)
            });
            handles.push(handle);
        }

        for h in handles {
            let (id, _buf) = h.join().unwrap();
            dev.unregister(id);
        }
    }
}
