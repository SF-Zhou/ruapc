use std::collections::VecDeque;
use std::io::{Error, Result};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use aliasable::boxed::AliasableBox;
use tokio::sync::oneshot;

use crate::buffer::Buffer;
use crate::device::{Device, Devices};
use crate::memory::RegisteredMemory;

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
pub struct BufferPool<D: Device> {
    devices: Arc<Devices<D>>,
    inner: Mutex<PoolInner<D>>,
    block_size: usize,
    chunk_size: usize,
    max_memory: usize,
}

struct PoolInner<D: Device> {
    memories: Vec<AliasableBox<RegisteredMemory<D::Registration>>>,
    free_list: VecDeque<FreeSlot>,
    allocated_memory: usize,
    waiters: VecDeque<oneshot::Sender<()>>,
}

#[derive(Debug, Clone, Copy)]
struct FreeSlot {
    memory_index: usize,
    block_index: usize,
}

impl<D: Device> BufferPool<D> {
    /// Creates a new buffer pool.
    ///
    /// - `devices`: the set of devices to register memory on.
    /// - `block_size`: size of each allocated buffer (e.g. 4 MiB).
    /// - `chunk_size`: size of each bulk allocation (e.g. 256 MiB).
    ///   Must be a multiple of `block_size`.
    /// - `max_memory`: upper bound on total memory from the OS.
    ///   Use `0` for unlimited.
    pub fn new(
        devices: Arc<Devices<D>>,
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
    pub fn allocate(self: &Arc<Self>) -> Result<Buffer<D>> {
        let mut inner = self.inner.lock().unwrap();
        self.allocate_inner(&mut inner)
    }

    /// Allocates a buffer, waiting asynchronously if none are available.
    ///
    /// If the free list is empty and the memory limit is reached,
    /// this method waits until another buffer is returned to the pool.
    pub async fn async_allocate(self: &Arc<Self>) -> Result<Buffer<D>> {
        loop {
            let rx = {
                let mut inner = self.inner.lock().unwrap();
                match self.allocate_inner(&mut inner) {
                    Ok(buf) => return Ok(buf),
                    Err(_) => {
                        let (tx, rx) = oneshot::channel();
                        inner.waiters.push_back(tx);
                        rx
                    }
                }
            };
            let _ = rx.await;
        }
    }

    fn allocate_inner(self: &Arc<Self>, inner: &mut PoolInner<D>) -> Result<Buffer<D>> {
        if let Some(slot) = inner.free_list.pop_front() {
            return self.make_buffer(inner, slot);
        }

        if self.max_memory == 0 || inner.allocated_memory + self.chunk_size <= self.max_memory {
            self.allocate_chunk(inner)?;
            if let Some(slot) = inner.free_list.pop_front() {
                return self.make_buffer(inner, slot);
            }
        }

        Err(Error::other("buffer pool exhausted"))
    }

    fn allocate_chunk(&self, inner: &mut PoolInner<D>) -> Result<()> {
        let mem = RegisteredMemory::new(self.chunk_size, &self.devices)?;
        let memory_index = inner.memories.len();
        let blocks_per_chunk = self.chunk_size / self.block_size;

        for block_index in 0..blocks_per_chunk {
            inner.free_list.push_back(FreeSlot {
                memory_index,
                block_index,
            });
        }

        inner.allocated_memory += mem.aligned_memory().size();
        inner
            .memories
            .push(AliasableBox::from_unique(Box::new(mem)));
        Ok(())
    }

    #[allow(unsafe_code)]
    fn make_buffer(self: &Arc<Self>, inner: &PoolInner<D>, slot: FreeSlot) -> Result<Buffer<D>> {
        let mem: &RegisteredMemory<D::Registration> = &inner.memories[slot.memory_index];
        let base = mem.aligned_memory().as_ptr();
        let offset = slot.block_index * self.block_size;
        // SAFETY: offset is within the allocated memory region.
        let ptr = unsafe { NonNull::new_unchecked(base.add(offset) as *mut u8) };
        // SAFETY: The RegisteredMemory lives inside an AliasableBox in the
        // append-only Vec. The AliasableBox heap-allocates it so its address
        // is stable even when the Vec grows. The Buffer holds Arc<BufferPool>
        // which keeps the pool (and thus the PoolInner with all memories)
        // alive for the Buffer's lifetime.
        let memory = NonNull::from(mem);
        Ok(Buffer::new(
            Arc::clone(self),
            ptr,
            self.block_size,
            memory,
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
        if let Some(tx) = inner.waiters.pop_front() {
            let _ = tx.send(());
        }
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

    /// Returns a reference to the devices this pool is registered on.
    pub fn devices(&self) -> &Arc<Devices<D>> {
        &self.devices
    }
}

impl<D: Device> std::fmt::Debug for BufferPool<D> {
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
