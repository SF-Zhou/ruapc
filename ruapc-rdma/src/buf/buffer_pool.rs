//! RDMA Buffer Pool with buddy memory allocation.
//!
//! This module provides a sophisticated buffer pool implementation that:
//! - Uses 64MiB chunks as the base unit for RDMA memory registration
//! - Implements buddy memory allocation with O(1) operations using bitmaps
//! - Provides separate allocation mechanism for small buffers
//! - Supports both synchronous and asynchronous allocation

use crate::*;
use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;

/// Size of each RDMA registered chunk (64 MiB).
pub const CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Allocation levels for buddy allocator.
/// Level 0: 64MiB (largest), Level 1: 16MiB, Level 2: 4MiB, Level 3: 1MiB (smallest)
const LEVEL_SIZES: [usize; 4] = [
    64 * 1024 * 1024, // Level 0: 64MiB
    16 * 1024 * 1024, // Level 1: 16MiB
    4 * 1024 * 1024,  // Level 2: 4MiB
    1024 * 1024,      // Level 3: 1MiB
];

/// Number of blocks at each level.
const LEVEL_BLOCK_COUNTS: [usize; 4] = [1, 4, 16, 64];

/// Default small buffer threshold (64 KiB).
pub const DEFAULT_SMALL_BUFFER_SIZE: usize = 64 * 1024;

/// Determine the allocation level for a given size.
/// Returns the smallest level that can accommodate the requested size.
/// Level 0 = 64MiB, Level 3 = 1MiB
fn size_to_level(size: usize) -> Option<usize> {
    // Search from smallest (level 3) to largest (level 0)
    for level in (0..LEVEL_SIZES.len()).rev() {
        if size <= LEVEL_SIZES[level] {
            return Some(level);
        }
    }
    None
}

/// Get the size of a block at a given level.
#[inline]
fn level_to_size(level: usize) -> usize {
    LEVEL_SIZES[level]
}

/// A single 64MiB RDMA registered chunk with buddy allocation tracking.
///
/// Uses a pure bitmap approach for O(1) allocation and deallocation:
/// - `allocated_bitmap`: tracks blocks that are directly allocated to users
/// - `split_bitmap`: tracks blocks that have been split into smaller blocks
///
/// A block is "free" if it's neither allocated nor split.
/// A block is "available for allocation" if it's free AND its parent is split (or it's at level 0).
struct Chunk {
    /// The underlying registered buffer.
    buffer: RegisteredBuffer,
    /// Bitmap for tracking allocated blocks at each level.
    /// Level 0: 1 block (bit 0)
    /// Level 1: 4 blocks (bits 0-3)
    /// Level 2: 16 blocks (bits 0-15)
    /// Level 3: 64 blocks (bits 0-63)
    allocated_bitmap: [u64; 4],
    /// Bitmap for tracking which blocks are "split" (have children).
    split_bitmap: [u64; 4],
}

impl Chunk {
    fn new(buffer: RegisteredBuffer) -> Self {
        Self {
            buffer,
            allocated_bitmap: [0; 4],
            split_bitmap: [0; 4],
        }
    }

    /// Try to allocate a block at the given level.
    /// Returns the block index within this chunk if successful.
    ///
    /// Time complexity: O(1) amortized - uses trailing_zeros for fast free block lookup.
    fn allocate(&mut self, level: usize) -> Option<usize> {
        // Find a free block at this level.
        // A block is available if:
        // 1. It's not allocated
        // 2. It's not split
        // 3. Its parent is split (or we're at level 0)
        if let Some(block_idx) = self.find_free_block(level) {
            self.mark_allocated(level, block_idx);
            return Some(block_idx);
        }

        // No free block at this level, try to split from a higher level.
        if level > 0 {
            if let Some(parent_block) = self.allocate(level - 1) {
                // The parent was marked allocated, but we're splitting it.
                self.mark_free(level - 1, parent_block);
                self.mark_split(level - 1, parent_block);

                // Return the first child.
                let first_child = parent_block * 4;
                self.mark_allocated(level, first_child);
                return Some(first_child);
            }
        }

        None
    }

    /// Find a free block at the given level using bitmap operations.
    /// O(1) using trailing_zeros.
    fn find_free_block(&self, level: usize) -> Option<usize> {
        // A block is free if it's not allocated and not split.
        let free_mask = !(self.allocated_bitmap[level] | self.split_bitmap[level]);

        // Also need to check that parent is split (block is "available").
        let available_mask = if level == 0 {
            // At level 0, the only block is always available if free.
            free_mask & 1
        } else {
            // At other levels, a block is available if its parent is split.
            let parent_split = self.split_bitmap[level - 1];
            // Expand parent split bits to child positions.
            // Each parent bit controls 4 consecutive child bits.
            let mut expanded: u64 = 0;
            for parent_idx in 0..LEVEL_BLOCK_COUNTS[level - 1] {
                if (parent_split & (1 << parent_idx)) != 0 {
                    let child_base = parent_idx * 4;
                    expanded |= 0xF << child_base;
                }
            }
            free_mask & expanded
        };

        // Mask to valid block range.
        // Note: LEVEL_BLOCK_COUNTS[3] == 64, so we need to handle the edge case
        // where shifting by 64 would overflow.
        let valid_mask = if LEVEL_BLOCK_COUNTS[level] == 64 {
            u64::MAX
        } else {
            (1u64 << LEVEL_BLOCK_COUNTS[level]) - 1
        };
        let available = available_mask & valid_mask;

        if available != 0 {
            Some(available.trailing_zeros() as usize)
        } else {
            None
        }
    }

    /// Deallocate a block and try to merge with buddies.
    /// O(1) - all operations are bitmap manipulations.
    fn deallocate(&mut self, level: usize, block_idx: usize) {
        self.mark_free(level, block_idx);
        self.try_merge(level, block_idx);
    }

    /// Try to merge a freed block with its siblings.
    /// O(1) per level, O(log n) total for recursive merging up the tree.
    fn try_merge(&mut self, level: usize, block_idx: usize) {
        if level == 0 {
            // Already at the top level, nothing to merge.
            return;
        }

        // Check if all 4 siblings are free (not allocated and not split).
        let base = (block_idx / 4) * 4;
        let sibling_mask: u64 = 0xF << base;

        let allocated_siblings = self.allocated_bitmap[level] & sibling_mask;
        let split_siblings = self.split_bitmap[level] & sibling_mask;

        if allocated_siblings == 0 && split_siblings == 0 {
            // All 4 siblings are free, merge into parent.
            let parent_idx = block_idx / 4;
            self.clear_split(level - 1, parent_idx);

            // Recursively try to merge at the parent level.
            self.try_merge(level - 1, parent_idx);
        }
    }

    #[inline]
    fn mark_allocated(&mut self, level: usize, block_idx: usize) {
        self.allocated_bitmap[level] |= 1 << block_idx;
    }

    #[inline]
    fn mark_free(&mut self, level: usize, block_idx: usize) {
        self.allocated_bitmap[level] &= !(1 << block_idx);
    }

    #[inline]
    fn mark_split(&mut self, level: usize, block_idx: usize) {
        self.split_bitmap[level] |= 1 << block_idx;
    }

    #[inline]
    fn clear_split(&mut self, level: usize, block_idx: usize) {
        self.split_bitmap[level] &= !(1 << block_idx);
    }

    /// Get the memory offset and size for a block.
    fn block_info(&self, level: usize, block_idx: usize) -> (usize, usize) {
        let block_size = level_to_size(level);
        let offset = block_idx * block_size;
        (offset, block_size)
    }

    fn lkey(&self, device_index: usize) -> u32 {
        self.buffer.lkey(device_index)
    }

    fn rkey(&self, device_index: usize) -> u32 {
        self.buffer.rkey(device_index)
    }

    fn base_ptr(&self) -> *const u8 {
        self.buffer.as_ptr()
    }
}

/// Small buffer chunk for allocations smaller than small_buffer_size.
struct SmallBufferChunk {
    /// The underlying registered buffer.
    buffer: RegisteredBuffer,
    /// Free list of small buffer indices.
    free_list: VecDeque<usize>,
    /// Size of each small buffer.
    small_buffer_size: usize,
}

impl SmallBufferChunk {
    fn new(buffer: RegisteredBuffer, small_buffer_size: usize) -> Self {
        let buffer_count = CHUNK_SIZE / small_buffer_size;
        let mut free_list = VecDeque::with_capacity(buffer_count);
        for i in 0..buffer_count {
            free_list.push_back(i);
        }
        Self {
            buffer,
            free_list,
            small_buffer_size,
        }
    }

    fn allocate(&mut self) -> Option<usize> {
        self.free_list.pop_front()
    }

    fn deallocate(&mut self, idx: usize) {
        self.free_list.push_back(idx);
    }

    fn lkey(&self, device_index: usize) -> u32 {
        self.buffer.lkey(device_index)
    }

    fn rkey(&self, device_index: usize) -> u32 {
        self.buffer.rkey(device_index)
    }

    fn base_ptr(&self) -> *const u8 {
        self.buffer.as_ptr()
    }
}

/// Configuration for BufferPool.
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Maximum memory limit in bytes (0 means no limit).
    pub max_memory: usize,
    /// Small buffer size threshold.
    pub small_buffer_size: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_memory: 0,
            small_buffer_size: DEFAULT_SMALL_BUFFER_SIZE,
        }
    }
}

/// Internal state of the buffer pool.
struct BufferPoolInner {
    /// Buddy allocation chunks.
    chunks: Vec<Chunk>,
    /// Small buffer chunks.
    small_chunks: Vec<SmallBufferChunk>,
    /// Total allocated memory.
    allocated_memory: usize,
    /// Configuration.
    config: BufferPoolConfig,
    /// RDMA devices.
    devices: Devices,
}

/// A pool of RDMA buffers with buddy memory allocation.
///
/// This pool manages RDMA-registered memory with the following features:
/// - 64MiB chunk-based memory registration
/// - Buddy memory allocation with 4 levels: 64MiB, 16MiB, 4MiB, 1MiB
/// - Separate allocation mechanism for small buffers (< small_buffer_size)
/// - O(1) allocation and merge operations using bitmaps
/// - Both synchronous and asynchronous allocation interfaces
pub struct BufferPool {
    inner: Mutex<BufferPoolInner>,
    /// Notify for waking up waiters when memory becomes available.
    notify: Notify,
}

impl BufferPool {
    /// Creates a new buffer pool with the given devices and default configuration.
    pub fn new(devices: &Devices) -> Arc<Self> {
        Self::with_config(devices, BufferPoolConfig::default())
    }

    /// Creates a new buffer pool with the given devices and configuration.
    pub fn with_config(devices: &Devices, config: BufferPoolConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(BufferPoolInner {
                chunks: Vec::new(),
                small_chunks: Vec::new(),
                allocated_memory: 0,
                config,
                devices: devices.clone(),
            }),
            notify: Notify::new(),
        })
    }

    /// Allocates a buffer of at least the given size.
    ///
    /// Returns an error if allocation fails (e.g., out of memory).
    pub fn allocate(self: &Arc<Self>, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Err(Error::new(
                ErrorKind::AllocMemoryFailed,
                "cannot allocate zero-sized buffer".to_string(),
            ));
        }

        let mut inner = self.inner.lock().unwrap();

        // Check if this is a small buffer allocation.
        if size <= inner.config.small_buffer_size {
            return self.allocate_small_locked(&mut inner);
        }

        // Determine the allocation level.
        let level = size_to_level(size).ok_or_else(|| {
            Error::new(
                ErrorKind::AllocMemoryFailed,
                format!("requested size {} exceeds maximum block size", size),
            )
        })?;

        self.allocate_buddy_locked(&mut inner, level)
    }

    /// Asynchronously allocates a buffer of at least the given size.
    ///
    /// If the pool has reached its memory limit, this function will wait
    /// until memory becomes available.
    pub async fn async_allocate(self: &Arc<Self>, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Err(Error::new(
                ErrorKind::AllocMemoryFailed,
                "cannot allocate zero-sized buffer".to_string(),
            ));
        }

        loop {
            // Try to allocate.
            match self.allocate(size) {
                Ok(buffer) => return Ok(buffer),
                Err(e) if e.kind == ErrorKind::AllocMemoryFailed => {
                    // Check if we're at the memory limit.
                    let at_limit = {
                        let inner = self.inner.lock().unwrap();
                        inner.config.max_memory > 0
                            && inner.allocated_memory >= inner.config.max_memory
                    };

                    if at_limit {
                        // Wait for memory to become available.
                        self.notify.notified().await;
                    } else {
                        // Some other allocation failure.
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Allocate a small buffer (internal, called with lock held).
    fn allocate_small_locked(self: &Arc<Self>, inner: &mut BufferPoolInner) -> Result<Buffer> {
        // Try to find a chunk with free small buffers.
        for (chunk_idx, chunk) in inner.small_chunks.iter_mut().enumerate() {
            if let Some(buffer_idx) = chunk.allocate() {
                let offset = buffer_idx * chunk.small_buffer_size;
                return Ok(Buffer {
                    pool: Arc::downgrade(self),
                    chunk_index: chunk_idx,
                    block_index: buffer_idx,
                    level: usize::MAX, // Sentinel for small buffer.
                    offset,
                    capacity: chunk.small_buffer_size,
                    length: 0,
                    is_small: true,
                });
            }
        }

        // No free small buffers, allocate a new chunk.
        if inner.config.max_memory > 0
            && inner.allocated_memory + CHUNK_SIZE > inner.config.max_memory
        {
            return Err(ErrorKind::AllocMemoryFailed.into());
        }

        let buffer = RegisteredBuffer::create(&inner.devices, CHUNK_SIZE)?;
        let chunk_idx = inner.small_chunks.len();
        let mut chunk = SmallBufferChunk::new(buffer, inner.config.small_buffer_size);
        inner.allocated_memory += CHUNK_SIZE;

        let buffer_idx = chunk.allocate().unwrap();
        let offset = buffer_idx * chunk.small_buffer_size;
        let capacity = chunk.small_buffer_size;
        inner.small_chunks.push(chunk);

        Ok(Buffer {
            pool: Arc::downgrade(self),
            chunk_index: chunk_idx,
            block_index: buffer_idx,
            level: usize::MAX,
            offset,
            capacity,
            length: 0,
            is_small: true,
        })
    }

    /// Allocate a buddy buffer (internal, called with lock held).
    fn allocate_buddy_locked(
        self: &Arc<Self>,
        inner: &mut BufferPoolInner,
        level: usize,
    ) -> Result<Buffer> {
        // Try to allocate from existing chunks.
        for (chunk_idx, chunk) in inner.chunks.iter_mut().enumerate() {
            if let Some(block_idx) = chunk.allocate(level) {
                let (offset, capacity) = chunk.block_info(level, block_idx);
                return Ok(Buffer {
                    pool: Arc::downgrade(self),
                    chunk_index: chunk_idx,
                    block_index: block_idx,
                    level,
                    offset,
                    capacity,
                    length: 0,
                    is_small: false,
                });
            }
        }

        // No free blocks, allocate a new chunk.
        if inner.config.max_memory > 0
            && inner.allocated_memory + CHUNK_SIZE > inner.config.max_memory
        {
            return Err(ErrorKind::AllocMemoryFailed.into());
        }

        let buffer = RegisteredBuffer::create(&inner.devices, CHUNK_SIZE)?;
        let chunk_idx = inner.chunks.len();
        let mut chunk = Chunk::new(buffer);
        inner.allocated_memory += CHUNK_SIZE;

        let block_idx = chunk.allocate(level).unwrap();
        let (offset, capacity) = chunk.block_info(level, block_idx);
        inner.chunks.push(chunk);

        Ok(Buffer {
            pool: Arc::downgrade(self),
            chunk_index: chunk_idx,
            block_index: block_idx,
            level,
            offset,
            capacity,
            length: 0,
            is_small: false,
        })
    }

    /// Deallocate a buffer (called from Buffer::drop).
    fn deallocate(&self, buffer: &Buffer) {
        let mut inner = self.inner.lock().unwrap();

        if buffer.is_small {
            // Return small buffer to its chunk's free list.
            if let Some(chunk) = inner.small_chunks.get_mut(buffer.chunk_index) {
                chunk.deallocate(buffer.block_index);
            }
        } else {
            // Return buddy buffer and try to merge.
            if let Some(chunk) = inner.chunks.get_mut(buffer.chunk_index) {
                chunk.deallocate(buffer.level, buffer.block_index);
            }
        }

        // Notify any waiters that memory is available.
        self.notify.notify_waiters();
    }

    /// Returns the total allocated memory in bytes.
    pub fn allocated_memory(&self) -> usize {
        self.inner.lock().unwrap().allocated_memory
    }

    /// Returns the number of buddy chunks.
    pub fn chunk_count(&self) -> usize {
        self.inner.lock().unwrap().chunks.len()
    }

    /// Returns the number of small buffer chunks.
    pub fn small_chunk_count(&self) -> usize {
        self.inner.lock().unwrap().small_chunks.len()
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("BufferPool")
            .field("chunk_count", &inner.chunks.len())
            .field("small_chunk_count", &inner.small_chunks.len())
            .field("allocated_memory", &inner.allocated_memory)
            .field("config", &inner.config)
            .finish()
    }
}

/// A buffer allocated from the BufferPool.
///
/// The buffer is automatically returned to the pool when dropped.
pub struct Buffer {
    pool: Weak<BufferPool>,
    chunk_index: usize,
    block_index: usize,
    level: usize,
    offset: usize,
    capacity: usize,
    length: usize,
    is_small: bool,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            pool.deallocate(self);
        }
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let ptr = self.as_ptr();
        let len = self.length;
        unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len) }
    }
}

impl Buffer {
    /// Returns the local key for RDMA operations on the specified device.
    pub fn lkey(&self, device: &Device) -> u32 {
        if let Some(pool) = self.pool.upgrade() {
            let inner = pool.inner.lock().unwrap();
            if self.is_small {
                inner.small_chunks[self.chunk_index].lkey(device.index())
            } else {
                inner.chunks[self.chunk_index].lkey(device.index())
            }
        } else {
            0
        }
    }

    /// Returns the remote key for RDMA operations on the specified device.
    pub fn rkey(&self, device: &Device) -> u32 {
        if let Some(pool) = self.pool.upgrade() {
            let inner = pool.inner.lock().unwrap();
            if self.is_small {
                inner.small_chunks[self.chunk_index].rkey(device.index())
            } else {
                inner.chunks[self.chunk_index].rkey(device.index())
            }
        } else {
            0
        }
    }

    /// Returns the capacity of this buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current length of data in this buffer.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true if the buffer contains no data.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns a slice of the buffer contents.
    pub fn as_slice(&self) -> &[u8] {
        if let Some(pool) = self.pool.upgrade() {
            let inner = pool.inner.lock().unwrap();
            let base_ptr = if self.is_small {
                inner.small_chunks[self.chunk_index].base_ptr()
            } else {
                inner.chunks[self.chunk_index].base_ptr()
            };
            unsafe {
                let ptr = base_ptr.add(self.offset);
                std::slice::from_raw_parts(ptr, self.length)
            }
        } else {
            &[]
        }
    }

    /// Returns a pointer to the start of the buffer.
    pub fn as_ptr(&self) -> *const u8 {
        if let Some(pool) = self.pool.upgrade() {
            let inner = pool.inner.lock().unwrap();
            let base_ptr = if self.is_small {
                inner.small_chunks[self.chunk_index].base_ptr()
            } else {
                inner.chunks[self.chunk_index].base_ptr()
            };
            unsafe { base_ptr.add(self.offset) }
        } else {
            std::ptr::null()
        }
    }

    /// Sets the length of data in this buffer.
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity, "length exceeds capacity");
        self.length = len;
    }

    /// Extends the buffer from a slice.
    pub fn extend_from_slice(&mut self, buf: &[u8]) -> Result<()> {
        if buf.len() + self.length > self.capacity {
            return Err(Error::new(
                ErrorKind::InsufficientBuffer,
                format!(
                    "offset {} + length {} > capacity {}",
                    self.length,
                    buf.len(),
                    self.capacity
                ),
            ));
        }

        if let Some(pool) = self.pool.upgrade() {
            let inner = pool.inner.lock().unwrap();
            let base_ptr = if self.is_small {
                inner.small_chunks[self.chunk_index].base_ptr()
            } else {
                inner.chunks[self.chunk_index].base_ptr()
            };
            unsafe {
                let dst = base_ptr.add(self.offset + self.length) as *mut u8;
                std::ptr::copy_nonoverlapping(buf.as_ptr(), dst, buf.len());
            }
            drop(inner);
            self.length += buf.len();
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::AllocMemoryFailed,
                "buffer pool has been dropped".to_string(),
            ))
        }
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slice = self.as_slice();
        bytes::Bytes::from_static(unsafe {
            std::slice::from_raw_parts(slice.as_ptr(), slice.len())
        })
        .fmt(f)
    }
}

// Ensure Buffer is Send + Sync
unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_to_level() {
        // Level 3: 1MiB (smallest)
        assert_eq!(size_to_level(1), Some(3));
        assert_eq!(size_to_level(1024), Some(3));
        assert_eq!(size_to_level(1024 * 1024), Some(3));

        // Level 2: 4MiB
        assert_eq!(size_to_level(1024 * 1024 + 1), Some(2));
        assert_eq!(size_to_level(4 * 1024 * 1024), Some(2));

        // Level 1: 16MiB
        assert_eq!(size_to_level(4 * 1024 * 1024 + 1), Some(1));
        assert_eq!(size_to_level(16 * 1024 * 1024), Some(1));

        // Level 0: 64MiB
        assert_eq!(size_to_level(16 * 1024 * 1024 + 1), Some(0));
        assert_eq!(size_to_level(64 * 1024 * 1024), Some(0));

        // Too large
        assert_eq!(size_to_level(64 * 1024 * 1024 + 1), None);
    }

    #[test]
    fn test_level_to_size() {
        assert_eq!(level_to_size(0), 64 * 1024 * 1024);
        assert_eq!(level_to_size(1), 16 * 1024 * 1024);
        assert_eq!(level_to_size(2), 4 * 1024 * 1024);
        assert_eq!(level_to_size(3), 1024 * 1024);
    }

    #[test]
    fn test_buffer_pool_allocate() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        // Test 1MiB allocation.
        let buf1 = pool.allocate(1024 * 1024).unwrap();
        assert_eq!(buf1.capacity(), 1024 * 1024);
        assert_eq!(buf1.len(), 0);
        assert!(buf1.is_empty());

        // Test 4MiB allocation.
        let buf2 = pool.allocate(4 * 1024 * 1024).unwrap();
        assert_eq!(buf2.capacity(), 4 * 1024 * 1024);

        // Test 16MiB allocation.
        let buf3 = pool.allocate(16 * 1024 * 1024).unwrap();
        assert_eq!(buf3.capacity(), 16 * 1024 * 1024);

        // Test 64MiB allocation (needs new chunk).
        let buf4 = pool.allocate(64 * 1024 * 1024).unwrap();
        assert_eq!(buf4.capacity(), 64 * 1024 * 1024);

        assert_eq!(pool.chunk_count(), 2);
    }

    #[test]
    fn test_buffer_pool_small_buffer() {
        let devices = Devices::availables().unwrap();
        let config = BufferPoolConfig {
            max_memory: 0,
            small_buffer_size: 64 * 1024,
        };
        let pool = BufferPool::with_config(&devices, config);

        // Test small buffer allocation.
        let buf1 = pool.allocate(1024).unwrap();
        assert_eq!(buf1.capacity(), 64 * 1024);
        assert!(buf1.is_small);

        // More small buffers from same chunk.
        let buf2 = pool.allocate(32 * 1024).unwrap();
        assert_eq!(buf2.capacity(), 64 * 1024);
        assert!(buf2.is_small);

        assert_eq!(pool.small_chunk_count(), 1);
        assert_eq!(pool.chunk_count(), 0);
    }

    #[test]
    fn test_buffer_extend_from_slice() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        let mut buf = pool.allocate(1024 * 1024).unwrap();
        assert_eq!(buf.len(), 0);

        let data = [1u8; 100];
        buf.extend_from_slice(&data).unwrap();
        assert_eq!(buf.len(), 100);
        assert_eq!(&buf[..], &data);

        let more_data = [2u8; 50];
        buf.extend_from_slice(&more_data).unwrap();
        assert_eq!(buf.len(), 150);
        assert_eq!(&buf[100..], &more_data);
    }

    #[test]
    fn test_buffer_deallocation_and_reuse() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        // Allocate all 64 1MiB blocks from a chunk.
        let mut buffers: Vec<Buffer> = Vec::new();
        for _ in 0..64 {
            buffers.push(pool.allocate(1024 * 1024).unwrap());
        }
        assert_eq!(pool.chunk_count(), 1);

        // Drop all buffers.
        buffers.clear();

        // Allocate a 64MiB buffer - should be able to use the merged chunk.
        let big_buf = pool.allocate(64 * 1024 * 1024).unwrap();
        assert_eq!(big_buf.capacity(), 64 * 1024 * 1024);
        assert_eq!(pool.chunk_count(), 1); // Should still be 1 chunk.
    }

    #[test]
    fn test_buffer_buddy_split_and_merge() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        // Allocate a 4MiB buffer (level 2).
        // This should split: 64MiB -> 16MiB -> 4MiB
        let buf1 = pool.allocate(4 * 1024 * 1024).unwrap();
        assert_eq!(buf1.capacity(), 4 * 1024 * 1024);

        // Allocate 3 more 4MiB buffers (should use the remaining splits).
        let buf2 = pool.allocate(4 * 1024 * 1024).unwrap();
        let buf3 = pool.allocate(4 * 1024 * 1024).unwrap();
        let buf4 = pool.allocate(4 * 1024 * 1024).unwrap();

        // All 4 should still be from the same chunk.
        assert_eq!(pool.chunk_count(), 1);

        // Drop all 4 - they should merge back.
        drop(buf1);
        drop(buf2);
        drop(buf3);
        drop(buf4);

        // Now allocate a 16MiB buffer - should succeed from the merged block.
        let buf5 = pool.allocate(16 * 1024 * 1024).unwrap();
        assert_eq!(buf5.capacity(), 16 * 1024 * 1024);
        assert_eq!(pool.chunk_count(), 1);
    }

    #[test]
    fn test_memory_limit() {
        let devices = Devices::availables().unwrap();
        let config = BufferPoolConfig {
            max_memory: CHUNK_SIZE, // Only allow 1 chunk.
            small_buffer_size: DEFAULT_SMALL_BUFFER_SIZE,
        };
        let pool = BufferPool::with_config(&devices, config);

        // Allocate a 64MiB buffer.
        let buf1 = pool.allocate(64 * 1024 * 1024).unwrap();
        assert_eq!(pool.chunk_count(), 1);

        // Try to allocate another - should fail.
        let result = pool.allocate(64 * 1024 * 1024);
        assert!(result.is_err());

        // Drop the first buffer.
        drop(buf1);

        // Now allocation should succeed.
        let buf2 = pool.allocate(64 * 1024 * 1024).unwrap();
        assert_eq!(buf2.capacity(), 64 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_async_allocate() {
        let devices = Devices::availables().unwrap();
        let config = BufferPoolConfig {
            max_memory: CHUNK_SIZE, // Only allow 1 chunk.
            small_buffer_size: DEFAULT_SMALL_BUFFER_SIZE,
        };
        let pool = BufferPool::with_config(&devices, config);

        // Allocate a 64MiB buffer.
        let buf1 = pool.async_allocate(64 * 1024 * 1024).await.unwrap();
        assert_eq!(pool.chunk_count(), 1);

        // Spawn a task that will wait for memory.
        let pool_clone = pool.clone();
        let handle =
            tokio::spawn(async move { pool_clone.async_allocate(64 * 1024 * 1024).await.unwrap() });

        // Give the task time to start waiting.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Drop the first buffer to free memory.
        drop(buf1);

        // The spawned task should now complete.
        let buf2 = handle.await.unwrap();
        assert_eq!(buf2.capacity(), 64 * 1024 * 1024);
    }

    #[test]
    fn test_zero_size_allocation() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        let result = pool.allocate(0);
        assert!(result.is_err());
    }

    #[test]
    fn test_oversized_allocation() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        // Try to allocate more than 64MiB.
        let result = pool.allocate(65 * 1024 * 1024);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_chunks() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        // Allocate 3 x 64MiB buffers.
        let buf1 = pool.allocate(64 * 1024 * 1024).unwrap();
        let buf2 = pool.allocate(64 * 1024 * 1024).unwrap();
        let buf3 = pool.allocate(64 * 1024 * 1024).unwrap();

        assert_eq!(pool.chunk_count(), 3);
        assert_eq!(pool.allocated_memory(), 3 * CHUNK_SIZE);

        drop(buf1);
        drop(buf2);
        drop(buf3);
    }

    #[test]
    fn test_small_buffer_reuse() {
        let devices = Devices::availables().unwrap();
        let config = BufferPoolConfig {
            max_memory: 0,
            small_buffer_size: 64 * 1024,
        };
        let pool = BufferPool::with_config(&devices, config);

        // Allocate and drop several small buffers.
        for _ in 0..10 {
            let buf = pool.allocate(1024).unwrap();
            drop(buf);
        }

        // Should still have only 1 small chunk.
        assert_eq!(pool.small_chunk_count(), 1);

        // Allocate many small buffers to verify reuse.
        let mut buffers = Vec::new();
        for _ in 0..100 {
            buffers.push(pool.allocate(1024).unwrap());
        }

        // Should still have only 1 small chunk (64MiB / 64KiB = 1024 small buffers).
        assert_eq!(pool.small_chunk_count(), 1);
    }

    #[test]
    fn test_buffer_write_and_read() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        let mut buf = pool.allocate(1024 * 1024).unwrap();

        // Write some data.
        let data: Vec<u8> = (0..255u8).collect();
        buf.extend_from_slice(&data).unwrap();

        // Read it back.
        assert_eq!(&buf[..], &data[..]);

        // Modify in place.
        buf[0] = 99;
        assert_eq!(buf[0], 99);
    }

    #[test]
    fn test_buffer_set_len() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        let mut buf = pool.allocate(1024 * 1024).unwrap();
        assert_eq!(buf.len(), 0);

        buf.set_len(100);
        assert_eq!(buf.len(), 100);

        buf.set_len(0);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    #[should_panic(expected = "length exceeds capacity")]
    fn test_buffer_set_len_exceeds_capacity() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        let mut buf = pool.allocate(1024 * 1024).unwrap();
        buf.set_len(2 * 1024 * 1024); // This should panic.
    }

    #[test]
    fn test_buffer_extend_exceeds_capacity() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        let mut buf = pool.allocate(1024 * 1024).unwrap();
        let large_data = vec![0u8; 2 * 1024 * 1024];

        let result = buf.extend_from_slice(&large_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_allocation_pattern() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        // Allocate various sizes.
        let buf_1m = pool.allocate(1024 * 1024).unwrap();
        let buf_4m = pool.allocate(4 * 1024 * 1024).unwrap();
        let buf_16m = pool.allocate(16 * 1024 * 1024).unwrap();

        // Should all be from the same chunk.
        assert_eq!(pool.chunk_count(), 1);

        // Deallocate in different order.
        drop(buf_4m);
        drop(buf_1m);
        drop(buf_16m);

        // Allocate again - should reuse merged blocks.
        let buf_64m = pool.allocate(64 * 1024 * 1024).unwrap();
        assert_eq!(buf_64m.capacity(), 64 * 1024 * 1024);
        assert_eq!(pool.chunk_count(), 1);
    }

    #[test]
    fn test_allocation_after_partial_free() {
        let devices = Devices::availables().unwrap();
        let pool = BufferPool::new(&devices);

        // Allocate 4 x 16MiB buffers
        let buf1 = pool.allocate(16 * 1024 * 1024).unwrap();
        let buf2 = pool.allocate(16 * 1024 * 1024).unwrap();
        let buf3 = pool.allocate(16 * 1024 * 1024).unwrap();
        let buf4 = pool.allocate(16 * 1024 * 1024).unwrap();

        assert_eq!(pool.chunk_count(), 1);

        // Free buf2 and buf3 (middle ones)
        drop(buf2);
        drop(buf3);

        // Allocate 2 x 16MiB - should reuse freed blocks
        let buf5 = pool.allocate(16 * 1024 * 1024).unwrap();
        let buf6 = pool.allocate(16 * 1024 * 1024).unwrap();

        assert_eq!(pool.chunk_count(), 1);

        drop(buf1);
        drop(buf4);
        drop(buf5);
        drop(buf6);

        // Should be able to allocate 64MiB after all freed
        let buf7 = pool.allocate(64 * 1024 * 1024).unwrap();
        assert_eq!(buf7.capacity(), 64 * 1024 * 1024);
        assert_eq!(pool.chunk_count(), 1);
    }
}
