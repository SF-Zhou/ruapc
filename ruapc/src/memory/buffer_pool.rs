use std::collections::VecDeque;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::device::{Device, Devices};
use crate::{Error, ErrorKind, Result};

use super::buffer::Buffer;
use super::memory_key::MemoryKey;
use super::registered::Memory;

/// A memory pool that manages large registered memory chunks and
/// hands out fixed-size [`Buffer`]s from them.
///
/// Each chunk is a large `AlignedMemory` (e.g. 256 MiB) registered on
/// all devices, then sliced into fixed-size blocks (e.g. 4 MiB).
/// Freed buffers are returned to an internal free list for reuse.
///
/// When the free list is empty and the memory limit hasn't been reached,
/// a new chunk is allocated. When the limit is hit, `allocate` returns
/// an error; `async_allocate` waits until a buffer is returned.
///
/// Inspired by `ruapc-bufpool`'s design (RAII buffer return, async
/// waiting via oneshot channels), simplified to fixed-size slab
/// allocation since all blocks are the same size.
pub struct BufferPool {
    devices: Arc<Devices>,
    inner: Mutex<PoolInner>,
    block_size: usize,
    chunk_size: usize,
    max_memory: usize,
}

struct PoolInner {
    memories: Vec<Memory>,
    free_list: VecDeque<FreeSlot>,
    allocated_memory: usize,
    waiters: VecDeque<oneshot::Sender<()>>,
}

#[derive(Debug, Clone, Copy)]
struct FreeSlot {
    memory_index: usize,
    block_index: usize,
}

impl BufferPool {
    /// Creates a new buffer pool.
    ///
    /// - `devices`: the set of devices to register memory on.
    /// - `block_size`: size of each allocated buffer (e.g. 4 MiB).
    /// - `chunk_size`: size of each bulk allocation (e.g. 256 MiB).
    ///   Must be a multiple of `block_size`.
    /// - `max_memory`: upper bound on total memory from the OS.
    ///   Use `0` for unlimited.
    pub fn new(
        devices: Arc<Devices>,
        block_size: usize,
        chunk_size: usize,
        max_memory: usize,
    ) -> Arc<Self> {
        assert!(block_size > 0, "block_size must be > 0");
        assert!(chunk_size >= block_size, "chunk_size must be >= block_size");
        assert!(
            chunk_size.is_multiple_of(block_size),
            "chunk_size must be a multiple of block_size"
        );
        Arc::new(Self {
            devices,
            inner: Mutex::new(PoolInner {
                memories: Vec::new(),
                free_list: VecDeque::new(),
                allocated_memory: 0,
                waiters: VecDeque::new(),
            }),
            block_size,
            chunk_size,
            max_memory,
        })
    }

    /// Allocates a buffer synchronously.
    ///
    /// If the free list is empty, tries to allocate a new chunk.
    /// Returns an error if the memory limit is reached.
    pub fn allocate(self: &Arc<Self>) -> Result<Buffer> {
        let mut inner = self.inner.lock().unwrap();
        self.allocate_inner(&mut inner)
    }

    /// Allocates a buffer, waiting asynchronously if none are available.
    ///
    /// If the free list is empty and the memory limit is reached,
    /// this method waits until another buffer is returned to the pool.
    pub async fn async_allocate(self: &Arc<Self>) -> Result<Buffer> {
        loop {
            let rx = {
                let mut inner = self.inner.lock().unwrap();
                match self.allocate_inner(&mut inner) {
                    Ok(buf) => return Ok(buf),
                    Err(_) => {
                        // Memory limit reached and free list empty — wait.
                        let (tx, rx) = oneshot::channel();
                        inner.waiters.push_back(tx);
                        rx
                    }
                }
            };
            // Wait outside the lock for a buffer to be returned.
            let _ = rx.await;
        }
    }

    fn allocate_inner(self: &Arc<Self>, inner: &mut PoolInner) -> Result<Buffer> {
        // Try to pop from the free list.
        if let Some(slot) = inner.free_list.pop_front() {
            return self.make_buffer(inner, slot);
        }

        // Free list empty — try to allocate a new chunk.
        if self.max_memory == 0 || inner.allocated_memory + self.chunk_size <= self.max_memory {
            self.allocate_chunk(inner)?;
            if let Some(slot) = inner.free_list.pop_front() {
                return self.make_buffer(inner, slot);
            }
        }

        Err(Error::new(
            ErrorKind::InvalidArgument,
            "buffer pool exhausted".into(),
        ))
    }

    fn allocate_chunk(&self, inner: &mut PoolInner) -> Result<()> {
        let mem = Memory::new(self.chunk_size, &self.devices)?;
        let memory_index = inner.memories.len();
        let blocks_per_chunk = self.chunk_size / self.block_size;

        // Note: Memory::new rounds up to alignment boundary, so actual
        // size may be larger. We use chunk_size for block counting.
        for block_index in 0..blocks_per_chunk {
            inner.free_list.push_back(FreeSlot {
                memory_index,
                block_index,
            });
        }

        inner.allocated_memory += mem.aligned_memory().size();
        inner.memories.push(mem);
        Ok(())
    }

    #[allow(unsafe_code)]
    fn make_buffer(self: &Arc<Self>, inner: &PoolInner, slot: FreeSlot) -> Result<Buffer> {
        let mem = &inner.memories[slot.memory_index];
        let base = mem.aligned_memory().as_ptr();
        let offset = slot.block_index * self.block_size;
        // SAFETY: offset is within the allocated memory region.
        let ptr = unsafe { NonNull::new_unchecked(base.add(offset) as *mut u8) };
        Ok(Buffer::new(
            Arc::clone(self),
            ptr,
            self.block_size,
            slot.memory_index,
            slot.block_index,
        ))
    }

    /// Returns a buffer to the free list. Called by `Buffer::drop`.
    pub(crate) fn return_buffer(&self, memory_index: usize, block_index: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.free_list.push_back(FreeSlot {
            memory_index,
            block_index,
        });
        // Wake one waiter, if any.
        if let Some(tx) = inner.waiters.pop_front() {
            let _ = tx.send(());
        }
    }

    /// Returns the `MemoryKey` for a specific memory chunk on a device.
    pub(crate) fn get_memory_key(&self, memory_index: usize, device: &Device) -> Result<MemoryKey> {
        let inner = self.inner.lock().unwrap();
        let mem = inner.memories.get(memory_index).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("invalid memory index {memory_index}"),
            )
        })?;
        mem.get_memory_key(device)
    }

    /// Returns the total memory allocated from the OS.
    pub fn allocated_memory(&self) -> usize {
        self.inner.lock().unwrap().allocated_memory
    }

    /// Returns the number of free buffers available.
    pub fn free_count(&self) -> usize {
        self.inner.lock().unwrap().free_list.len()
    }

    /// Returns the configured block size.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Returns the configured chunk size.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("BufferPool")
            .field("block_size", &self.block_size)
            .field("chunk_size", &self.chunk_size)
            .field("max_memory", &self.max_memory)
            .field("allocated_memory", &inner.allocated_memory)
            .field("free_count", &inner.free_list.len())
            .field("chunks", &inner.memories.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pool(block_size: usize, chunk_size: usize, max_memory: usize) -> Arc<BufferPool> {
        let mut devices = Devices::new();
        devices.add_tcp_device();
        BufferPool::new(Arc::new(devices), block_size, chunk_size, max_memory)
    }

    #[test]
    fn test_basic_allocate_and_return() {
        // 4 MiB blocks, 8 MiB chunks → 2 blocks per chunk.
        let pool = make_pool(4 * 1024 * 1024, 8 * 1024 * 1024, 0);
        assert_eq!(pool.free_count(), 0);

        let buf = pool.allocate().unwrap();
        assert_eq!(buf.len(), 4 * 1024 * 1024);
        // After first alloc, chunk was created with 2 blocks, 1 used.
        assert_eq!(pool.free_count(), 1);

        drop(buf);
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn test_memory_limit() {
        // chunk_size = block_size = 2MiB (minimum alignment), max_memory = 2MiB.
        let pool = make_pool(2 * 1024 * 1024, 2 * 1024 * 1024, 2 * 1024 * 1024);
        let buf = pool.allocate().unwrap();
        assert_eq!(buf.len(), 2 * 1024 * 1024);

        // Second allocation should fail (limit reached).
        assert!(pool.allocate().is_err());

        drop(buf);
        // After return, should be able to allocate again.
        assert!(pool.allocate().is_ok());
    }

    #[test]
    fn test_buffer_read_write() {
        let pool = make_pool(2 * 1024 * 1024, 2 * 1024 * 1024, 0);
        let mut buf = pool.allocate().unwrap();
        buf[0] = 0xAB;
        buf[1] = 0xCD;
        assert_eq!(buf[0], 0xAB);
        assert_eq!(buf[1], 0xCD);
    }

    #[test]
    fn test_buffer_remote_buffer_info() {
        let mut devices = Devices::new();
        let d0 = devices.add_tcp_device();
        let pool = BufferPool::new(Arc::new(devices), 2 * 1024 * 1024, 2 * 1024 * 1024, 0);
        let buf = pool.allocate().unwrap();
        let info = buf.remote_buffer_info(&d0).unwrap();
        assert_eq!(info.len, 2 * 1024 * 1024);
        assert_eq!(info.addr, buf.as_ptr() as u64);
        assert!(matches!(info.key, MemoryKey::Tcp { .. }));
    }

    #[tokio::test]
    async fn test_async_allocate_waits() {
        let pool = make_pool(2 * 1024 * 1024, 2 * 1024 * 1024, 2 * 1024 * 1024);
        let buf = pool.allocate().unwrap();

        let pool2 = pool.clone();
        let handle = tokio::spawn(async move { pool2.async_allocate().await.unwrap() });

        // Give the spawned task a moment to register as a waiter.
        tokio::task::yield_now().await;

        // Return the buffer — should wake the waiter.
        drop(buf);

        let buf2 = handle.await.unwrap();
        assert_eq!(buf2.len(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_multiple_chunks() {
        // 2 MiB blocks, 2 MiB chunks, unlimited.
        let pool = make_pool(2 * 1024 * 1024, 2 * 1024 * 1024, 0);
        let buf1 = pool.allocate().unwrap();
        let buf2 = pool.allocate().unwrap();
        // Two separate chunks should have been allocated.
        assert_eq!(pool.allocated_memory(), 2 * 2 * 1024 * 1024);
        assert_ne!(buf1.as_ptr(), buf2.as_ptr());
    }
}
