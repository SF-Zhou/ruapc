//! RDMA Buffer Pool with buddy memory allocation and small buffer optimization.
//!
//! This module provides a memory pool optimized for RDMA operations with:
//! - 64MiB RDMA-registered memory blocks
//! - Buddy memory allocation for sizes: 64MiB, 16MiB, 4MiB, 1MiB
//! - Separate small buffer mechanism for buffers < `small_buffer_size`
//! - Automatic buffer recycling on drop
//! - Memory limit enforcement with async waiting support

use crate::*;
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Condvar, Mutex},
};

/// Size of a single RDMA-registered memory block (64 MiB).
pub const RDMA_BLOCK_SIZE: usize = 64 * 1024 * 1024;

/// Default small buffer size threshold (64 KiB).
pub const DEFAULT_SMALL_BUFFER_SIZE: usize = 64 * 1024;

/// Allocation levels for buddy memory system (in bytes).
/// Level 0: 64 MiB, Level 1: 16 MiB, Level 2: 4 MiB, Level 3: 1 MiB
const BUDDY_LEVELS: [usize; 4] = [
    64 * 1024 * 1024, // 64 MiB
    16 * 1024 * 1024, // 16 MiB
    4 * 1024 * 1024,  // 4 MiB
    1024 * 1024,      // 1 MiB
];

/// Number of children per buddy level (4 = 64/16 = 16/4 = 4/1).
const BUDDY_CHILDREN: usize = 4;

/// Configuration for the buffer pool.
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Maximum memory usage in bytes (0 means unlimited).
    pub max_memory: usize,
    /// Threshold for small buffer allocation.
    pub small_buffer_size: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_memory: 0, // unlimited
            small_buffer_size: DEFAULT_SMALL_BUFFER_SIZE,
        }
    }
}

/// Metadata for a registered RDMA buffer.
struct RdmaBufferMeta {
    /// The registered buffer.
    buffer: RegisteredBuffer,
    /// Base pointer for fast access.
    base_ptr: *mut u8,
}

// Safety: The base_ptr is derived from RegisteredBuffer which is Send+Sync.
unsafe impl Send for RdmaBufferMeta {}
unsafe impl Sync for RdmaBufferMeta {}

/// Internal state for the buffer pool.
struct BufferPoolState {
    /// All registered RDMA buffers (each 64 MiB).
    rdma_buffers: Vec<RdmaBufferMeta>,
    /// Free lists for each buddy level (0=64MiB, 1=16MiB, 2=4MiB, 3=1MiB).
    /// Each entry is (buffer_index, offset within buffer).
    buddy_free_lists: [Vec<(usize, usize)>; 4],
    /// Free list for small buffers (offset, buffer_index).
    small_free_list: Vec<(usize, usize)>,
    /// Current total allocated memory.
    total_memory: usize,
    /// Maximum memory limit.
    max_memory: usize,
    /// Small buffer size threshold.
    small_buffer_size: usize,
}

/// A pool of RDMA buffers with buddy memory allocation.
///
/// Features:
/// - Uses 64MiB as the RDMA buffer registration unit
/// - Implements buddy memory allocation for 64MiB, 16MiB, 4MiB, 1MiB sizes
/// - Separate small buffer mechanism for buffers < `small_buffer_size`
/// - Automatic buffer recycling when `Buffer` is dropped
/// - Memory limit enforcement with async waiting
pub struct BufferPool {
    devices: Devices,
    state: Mutex<BufferPoolState>,
    available: Condvar,
}

/// Represents the allocation type for a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AllocationType {
    /// Buddy allocation at a specific level.
    Buddy(usize),
    /// Small buffer allocation.
    Small,
}

/// A buffer allocated from the pool.
///
/// When dropped, the buffer is automatically returned to the pool for reuse.
///
/// # Safety Invariants
///
/// - `base_ptr` is derived from the `RegisteredBuffer` at index `buffer_idx` in the pool
/// - The underlying `RegisteredBuffer` remains valid as long as the pool exists
/// - The `Buffer` holds an `Arc<BufferPool>` ensuring the pool outlives this buffer
/// - Concurrent access is safe because:
///   - Read/write operations use raw pointer arithmetic without locking (safe due to unique ownership)
///   - Metadata access (lkey/rkey) acquires the pool lock
///   - The buffer is exclusively owned after allocation until dropped
pub struct Buffer {
    pool: Arc<BufferPool>,
    /// Index of the RDMA buffer in the pool.
    buffer_idx: usize,
    /// Base pointer to the RDMA buffer data (cached for performance).
    /// This pointer remains valid as long as the BufferPool exists.
    base_ptr: *mut u8,
    /// Offset within the RDMA buffer.
    offset: usize,
    /// Capacity of this buffer.
    capacity: usize,
    /// Current length of valid data.
    length: usize,
    /// Type of allocation (for proper deallocation).
    allocation_type: AllocationType,
}

// Safety: Buffer contains a raw pointer but it's derived from RegisteredBuffer
// which is Send+Sync, and we ensure proper synchronization through BufferPool.
unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.pool.deallocate(self.buffer_idx, self.offset, self.allocation_type);
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
        unsafe { std::slice::from_raw_parts_mut(self.base_ptr.add(self.offset), self.length) }
    }
}

impl Buffer {
    /// Returns the local key for RDMA operations on the specified device.
    pub fn lkey(&self, device: &Device) -> u32 {
        let state = self.pool.state.lock().unwrap();
        state.rdma_buffers[self.buffer_idx].buffer.lkey(device.index())
    }

    /// Returns the remote key for RDMA operations on the specified device.
    pub fn rkey(&self, device: &Device) -> u32 {
        let state = self.pool.state.lock().unwrap();
        state.rdma_buffers[self.buffer_idx].buffer.rkey(device.index())
    }

    /// Returns the capacity of this buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current length of valid data in this buffer.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true if this buffer contains no data.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns the buffer as a slice.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.base_ptr.add(self.offset), self.length) }
    }

    /// Returns a pointer to the start of the buffer data.
    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.base_ptr.add(self.offset) }
    }

    /// Sets the length of valid data in this buffer.
    pub fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.capacity);
        self.length = len;
    }

    /// Extends the buffer with data from a slice.
    pub fn extend_from_slice(&mut self, buf: &[u8]) -> Result<()> {
        let cap = self.capacity();
        if buf.len() + self.len() <= cap {
            let dst = unsafe {
                std::slice::from_raw_parts_mut(self.base_ptr.add(self.offset + self.length), buf.len())
            };
            dst.copy_from_slice(buf);
            self.length += buf.len();
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InsufficientBuffer,
                format!(
                    "offset {} + length {} > capacity {}",
                    self.length,
                    buf.len(),
                    cap
                ),
            ))
        }
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("capacity", &self.capacity)
            .field("length", &self.length)
            .field("allocation_type", &self.allocation_type)
            .finish()
    }
}

impl BufferPool {
    /// Creates a new buffer pool with default configuration.
    ///
    /// This is a compatibility wrapper that creates a pool with unlimited memory.
    ///
    /// # Arguments
    /// * `block_size` - Ignored (uses fixed 64MiB blocks)
    /// * `block_count` - Ignored (dynamically allocates as needed)
    /// * `devices` - RDMA devices to register buffers with
    pub fn create(_block_size: usize, _block_count: usize, devices: &Devices) -> Result<Arc<Self>> {
        Self::with_config(devices, BufferPoolConfig::default())
    }

    /// Creates a new buffer pool with the specified configuration.
    ///
    /// # Arguments
    /// * `devices` - RDMA devices to register buffers with
    /// * `config` - Pool configuration including memory limits
    pub fn with_config(devices: &Devices, config: BufferPoolConfig) -> Result<Arc<Self>> {
        let state = BufferPoolState {
            rdma_buffers: Vec::new(),
            buddy_free_lists: [Vec::new(), Vec::new(), Vec::new(), Vec::new()],
            small_free_list: Vec::new(),
            total_memory: 0,
            max_memory: config.max_memory,
            small_buffer_size: config.small_buffer_size,
        };

        Ok(Arc::new(Self {
            devices: devices.clone(),
            state: Mutex::new(state),
            available: Condvar::new(),
        }))
    }

    /// Allocates a buffer of at least the specified size.
    ///
    /// For sizes <= `small_buffer_size`, uses the small buffer mechanism.
    /// For larger sizes, uses buddy memory allocation with levels:
    /// - 1 MiB for sizes up to 1 MiB
    /// - 4 MiB for sizes up to 4 MiB
    /// - 16 MiB for sizes up to 16 MiB
    /// - 64 MiB for sizes up to 64 MiB
    ///
    /// # Arguments
    /// * `size` - Minimum size of the buffer
    ///
    /// # Returns
    /// A buffer with at least the requested capacity, or an error if allocation fails.
    pub fn allocate_with_size(self: &Arc<Self>, size: usize) -> Result<Buffer> {
        let state = self.state.lock().unwrap();
        let small_buffer_size = state.small_buffer_size;
        drop(state);

        if size <= small_buffer_size {
            self.allocate_small()
        } else {
            self.allocate_buddy(size)
        }
    }

    /// Allocates a buffer using the default size (small buffer).
    ///
    /// This is a compatibility wrapper for the old API.
    pub fn allocate(self: &Arc<Self>) -> Result<Buffer> {
        let state = self.state.lock().unwrap();
        let small_buffer_size = state.small_buffer_size;
        drop(state);
        self.allocate_small_with_capacity(small_buffer_size)
    }

    /// Allocates a small buffer from the dedicated small buffer pool.
    fn allocate_small(self: &Arc<Self>) -> Result<Buffer> {
        let state = self.state.lock().unwrap();
        let small_buffer_size = state.small_buffer_size;
        drop(state);
        self.allocate_small_with_capacity(small_buffer_size)
    }

    /// Allocates a small buffer with the specified capacity.
    fn allocate_small_with_capacity(self: &Arc<Self>, capacity: usize) -> Result<Buffer> {
        let mut state = self.state.lock().unwrap();

        // Try to get from small buffer free list.
        if let Some((offset, buffer_idx)) = state.small_free_list.pop() {
            let base_ptr = state.rdma_buffers[buffer_idx].base_ptr;
            return Ok(Buffer {
                pool: self.clone(),
                buffer_idx,
                base_ptr,
                offset,
                capacity,
                length: 0,
                allocation_type: AllocationType::Small,
            });
        }

        // Need to allocate a new 64MiB block and split it into small buffers.
        let buffer_idx = self.allocate_new_rdma_buffer(&mut state)?;
        let base_ptr = state.rdma_buffers[buffer_idx].base_ptr;
        let num_small_buffers = RDMA_BLOCK_SIZE / capacity;

        // Add all small buffers to free list (except the first one we'll return).
        for i in 1..num_small_buffers {
            state.small_free_list.push((i * capacity, buffer_idx));
        }

        Ok(Buffer {
            pool: self.clone(),
            buffer_idx,
            base_ptr,
            offset: 0,
            capacity,
            length: 0,
            allocation_type: AllocationType::Small,
        })
    }

    /// Allocates a buffer using buddy memory allocation.
    fn allocate_buddy(self: &Arc<Self>, size: usize) -> Result<Buffer> {
        // Determine the appropriate level.
        let level = Self::size_to_level(size)?;
        let capacity = BUDDY_LEVELS[level];

        let mut state = self.state.lock().unwrap();

        // Try to allocate from this level or higher.
        if let Some((buffer_idx, offset)) = self.try_allocate_buddy_at_level(&mut state, level) {
            let base_ptr = state.rdma_buffers[buffer_idx].base_ptr;
            return Ok(Buffer {
                pool: self.clone(),
                buffer_idx,
                base_ptr,
                offset,
                capacity,
                length: 0,
                allocation_type: AllocationType::Buddy(level),
            });
        }

        // If level 0 (64 MiB), we need a new RDMA buffer.
        if level == 0 {
            let buffer_idx = self.allocate_new_rdma_buffer(&mut state)?;
            let base_ptr = state.rdma_buffers[buffer_idx].base_ptr;
            return Ok(Buffer {
                pool: self.clone(),
                buffer_idx,
                base_ptr,
                offset: 0,
                capacity: RDMA_BLOCK_SIZE,
                length: 0,
                allocation_type: AllocationType::Buddy(0),
            });
        }

        // Try to split from a higher level.
        for parent_level in (0..level).rev() {
            if let Some((buffer_idx, parent_offset)) =
                self.try_allocate_buddy_at_level(&mut state, parent_level)
            {
                // Split the parent block into children.
                let parent_size = BUDDY_LEVELS[parent_level];
                let child_size = BUDDY_LEVELS[parent_level + 1];
                let num_children = parent_size / child_size;

                // Recursively split down to the target level.
                let (final_buffer_idx, final_offset) = self.split_to_level(
                    &mut state,
                    buffer_idx,
                    parent_offset,
                    parent_level,
                    level,
                );

                // Add remaining children to free list.
                for i in 1..num_children {
                    let child_offset = parent_offset + i * child_size;
                    // Need to recursively split these as well if they're not at the target level.
                    if parent_level + 1 < level {
                        self.add_split_children_to_free_list(
                            &mut state,
                            buffer_idx,
                            child_offset,
                            parent_level + 1,
                            level,
                        );
                    } else {
                        state.buddy_free_lists[parent_level + 1].push((buffer_idx, child_offset));
                    }
                }

                let base_ptr = state.rdma_buffers[final_buffer_idx].base_ptr;
                return Ok(Buffer {
                    pool: self.clone(),
                    buffer_idx: final_buffer_idx,
                    base_ptr,
                    offset: final_offset,
                    capacity,
                    length: 0,
                    allocation_type: AllocationType::Buddy(level),
                });
            }
        }

        // Need to allocate a new 64 MiB block.
        let buffer_idx = self.allocate_new_rdma_buffer(&mut state)?;

        // Split down to the requested level.
        let (final_buffer_idx, final_offset) =
            self.split_to_level(&mut state, buffer_idx, 0, 0, level);

        // Add the remaining 64 MiB block's children to free lists.
        let child_size = BUDDY_LEVELS[1]; // 16 MiB
        for i in 1..BUDDY_CHILDREN {
            let child_offset = i * child_size;
            if level > 1 {
                self.add_split_children_to_free_list(&mut state, buffer_idx, child_offset, 1, level);
            } else {
                state.buddy_free_lists[1].push((buffer_idx, child_offset));
            }
        }

        let base_ptr = state.rdma_buffers[final_buffer_idx].base_ptr;
        Ok(Buffer {
            pool: self.clone(),
            buffer_idx: final_buffer_idx,
            base_ptr,
            offset: final_offset,
            capacity,
            length: 0,
            allocation_type: AllocationType::Buddy(level),
        })
    }

    /// Splits a block from `from_level` down to `to_level`, adding siblings to free lists.
    fn split_to_level(
        &self,
        state: &mut BufferPoolState,
        buffer_idx: usize,
        offset: usize,
        from_level: usize,
        to_level: usize,
    ) -> (usize, usize) {
        let current_offset = offset;
        for level in from_level..to_level {
            let child_size = BUDDY_LEVELS[level + 1];
            // Add siblings (all except the first) to the free list at level+1.
            for i in 1..BUDDY_CHILDREN {
                state.buddy_free_lists[level + 1].push((buffer_idx, current_offset + i * child_size));
            }
            // Continue with the first child.
        }
        (buffer_idx, current_offset)
    }

    /// Adds children of a split block to free lists, splitting down if needed.
    fn add_split_children_to_free_list(
        &self,
        state: &mut BufferPoolState,
        buffer_idx: usize,
        offset: usize,
        current_level: usize,
        target_level: usize,
    ) {
        if current_level >= target_level {
            state.buddy_free_lists[current_level].push((buffer_idx, offset));
            return;
        }

        let child_size = BUDDY_LEVELS[current_level + 1];
        for i in 0..BUDDY_CHILDREN {
            self.add_split_children_to_free_list(
                state,
                buffer_idx,
                offset + i * child_size,
                current_level + 1,
                target_level,
            );
        }
    }

    /// Tries to allocate from a specific buddy level's free list.
    fn try_allocate_buddy_at_level(
        &self,
        state: &mut BufferPoolState,
        level: usize,
    ) -> Option<(usize, usize)> {
        state.buddy_free_lists[level].pop()
    }

    /// Converts a size to the appropriate buddy level.
    fn size_to_level(size: usize) -> Result<usize> {
        if size > BUDDY_LEVELS[0] {
            return Err(Error::new(
                ErrorKind::InsufficientBuffer,
                format!(
                    "requested size {} exceeds maximum buffer size {}",
                    size, BUDDY_LEVELS[0]
                ),
            ));
        }

        // Find the smallest level that can accommodate the size.
        for (level, &level_size) in BUDDY_LEVELS.iter().enumerate().rev() {
            if size <= level_size {
                return Ok(level);
            }
        }

        // Should not reach here, but return largest level as fallback.
        Ok(0)
    }

    /// Allocates a new 64 MiB RDMA buffer.
    fn allocate_new_rdma_buffer(&self, state: &mut BufferPoolState) -> Result<usize> {
        // Check memory limit.
        if state.max_memory > 0 && state.total_memory + RDMA_BLOCK_SIZE > state.max_memory {
            return Err(Error::new(
                ErrorKind::AllocMemoryFailed,
                format!(
                    "memory limit exceeded: current {} + {} > max {}",
                    state.total_memory, RDMA_BLOCK_SIZE, state.max_memory
                ),
            ));
        }

        let buffer = RegisteredBuffer::create(&self.devices, RDMA_BLOCK_SIZE)?;
        let base_ptr = buffer.as_ptr() as *mut u8;
        let buffer_idx = state.rdma_buffers.len();
        state.rdma_buffers.push(RdmaBufferMeta { buffer, base_ptr });
        state.total_memory += RDMA_BLOCK_SIZE;

        Ok(buffer_idx)
    }

    /// Deallocates a buffer, returning it to the appropriate free list.
    fn deallocate(&self, buffer_idx: usize, offset: usize, allocation_type: AllocationType) {
        let mut state = self.state.lock().unwrap();

        match allocation_type {
            AllocationType::Small => {
                // Small buffers go directly to the small free list without merging.
                state.small_free_list.push((offset, buffer_idx));
            }
            AllocationType::Buddy(level) => {
                // Try to merge with buddies.
                self.deallocate_buddy(&mut state, buffer_idx, offset, level);
            }
        }

        // Notify any waiting allocators.
        drop(state);
        self.available.notify_all();
    }

    /// Deallocates a buddy buffer, attempting to merge with adjacent buddies.
    fn deallocate_buddy(
        &self,
        state: &mut BufferPoolState,
        buffer_idx: usize,
        offset: usize,
        level: usize,
    ) {
        let mut current_level = level;
        let mut current_offset = offset;

        while current_level > 0 {
            let block_size = BUDDY_LEVELS[current_level];
            let parent_size = BUDDY_LEVELS[current_level - 1];
            // Use bitwise AND for faster alignment calculation (parent_size is always power of 2).
            let parent_offset = current_offset & !(parent_size - 1);
            let buddy_offset_in_parent = current_offset - parent_offset;
            let buddy_index = buddy_offset_in_parent / block_size;

            // Check if all siblings are free and collect their indices.
            let mut all_siblings_free = true;
            let mut sibling_indices = Vec::with_capacity(BUDDY_CHILDREN - 1);

            for i in 0..BUDDY_CHILDREN {
                if i == buddy_index {
                    continue; // Skip the current block.
                }
                let sibling_offset = parent_offset + i * block_size;
                if let Some(idx) = state.buddy_free_lists[current_level]
                    .iter()
                    .position(|&(bi, off)| bi == buffer_idx && off == sibling_offset)
                {
                    sibling_indices.push(idx);
                } else {
                    all_siblings_free = false;
                    break;
                }
            }

            if all_siblings_free && sibling_indices.len() == BUDDY_CHILDREN - 1 {
                // Remove siblings from free list in reverse index order to avoid invalidation.
                sibling_indices.sort_unstable_by(|a, b| b.cmp(a));
                for idx in sibling_indices {
                    state.buddy_free_lists[current_level].swap_remove(idx);
                }

                // Move up to parent level.
                current_level -= 1;
                current_offset = parent_offset;
            } else {
                // Can't merge, add to current level's free list.
                state.buddy_free_lists[current_level].push((buffer_idx, current_offset));
                return;
            }
        }

        // Reached level 0 (64 MiB), add to level 0 free list.
        state.buddy_free_lists[0].push((buffer_idx, current_offset));
    }

    /// Waits for a buffer to become available (for async contexts).
    ///
    /// This method blocks until a buffer becomes available or the timeout expires.
    pub fn wait_for_buffer(&self, timeout: std::time::Duration) -> Result<()> {
        let state = self.state.lock().unwrap();
        let result = self
            .available
            .wait_timeout(state, timeout)
            .map_err(|_| Error::new(ErrorKind::AllocMemoryFailed, "lock poisoned".to_string()))?;

        if result.1.timed_out() {
            Err(Error::new(
                ErrorKind::AllocMemoryFailed,
                "timeout waiting for buffer".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Returns the current memory usage of the pool.
    pub fn memory_usage(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.total_memory
    }

    /// Returns the maximum memory limit of the pool.
    pub fn max_memory(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.max_memory
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().unwrap();
        f.debug_struct("BufferPool")
            .field("total_memory", &state.total_memory)
            .field("max_memory", &state.max_memory)
            .field("rdma_buffers", &state.rdma_buffers.len())
            .field("small_free_list", &state.small_free_list.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer() {
        let devices = Devices::availables().unwrap();
        let buffer_pool = BufferPool::create(1 << 20, 32, &devices).unwrap();

        let mut buf = buffer_pool.allocate().unwrap();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);

        const SLICE: [u8; 233] = [1u8; 233];
        buf.extend_from_slice(&SLICE).unwrap();
        assert_eq!(buf.len(), 233);
        assert_eq!(buf.deref(), SLICE);

        buf.deref_mut().fill(2);
        assert_eq!(buf.len(), 233);
        assert_eq!(buf.deref(), SLICE.map(|v| v * 2));
    }

    #[test]
    fn test_buddy_allocation() {
        let devices = Devices::availables().unwrap();
        let buffer_pool = BufferPool::with_config(&devices, BufferPoolConfig::default()).unwrap();

        // Test 1 MiB allocation.
        let buf1 = buffer_pool.allocate_with_size(512 * 1024).unwrap();
        assert_eq!(buf1.capacity(), 1024 * 1024);
        assert_eq!(buf1.allocation_type, AllocationType::Buddy(3));

        // Test 4 MiB allocation.
        let buf4 = buffer_pool.allocate_with_size(2 * 1024 * 1024).unwrap();
        assert_eq!(buf4.capacity(), 4 * 1024 * 1024);
        assert_eq!(buf4.allocation_type, AllocationType::Buddy(2));

        // Test 16 MiB allocation.
        let buf16 = buffer_pool.allocate_with_size(8 * 1024 * 1024).unwrap();
        assert_eq!(buf16.capacity(), 16 * 1024 * 1024);
        assert_eq!(buf16.allocation_type, AllocationType::Buddy(1));

        // Test 64 MiB allocation.
        let buf64 = buffer_pool.allocate_with_size(32 * 1024 * 1024).unwrap();
        assert_eq!(buf64.capacity(), 64 * 1024 * 1024);
        assert_eq!(buf64.allocation_type, AllocationType::Buddy(0));
    }

    #[test]
    fn test_small_buffer_allocation() {
        let devices = Devices::availables().unwrap();
        let buffer_pool = BufferPool::with_config(&devices, BufferPoolConfig::default()).unwrap();

        // Test small buffer allocation.
        let small_buf = buffer_pool.allocate_with_size(4096).unwrap();
        assert_eq!(small_buf.capacity(), DEFAULT_SMALL_BUFFER_SIZE);
        assert_eq!(small_buf.allocation_type, AllocationType::Small);

        // Verify memory was allocated.
        assert_eq!(buffer_pool.memory_usage(), RDMA_BLOCK_SIZE);
    }

    #[test]
    fn test_buffer_recycling() {
        let devices = Devices::availables().unwrap();
        let buffer_pool = BufferPool::with_config(&devices, BufferPoolConfig::default()).unwrap();

        // Allocate and drop a buffer.
        {
            let _buf = buffer_pool.allocate().unwrap();
        }

        // Memory should still be allocated (buffer is recycled, not freed).
        assert_eq!(buffer_pool.memory_usage(), RDMA_BLOCK_SIZE);

        // Allocating again should reuse the recycled buffer.
        let _buf2 = buffer_pool.allocate().unwrap();
        assert_eq!(buffer_pool.memory_usage(), RDMA_BLOCK_SIZE);
    }

    #[test]
    fn test_memory_limit() {
        let devices = Devices::availables().unwrap();
        let config = BufferPoolConfig {
            max_memory: RDMA_BLOCK_SIZE,
            small_buffer_size: DEFAULT_SMALL_BUFFER_SIZE,
        };
        let buffer_pool = BufferPool::with_config(&devices, config).unwrap();

        // First allocation should succeed.
        let buf1 = buffer_pool.allocate_with_size(32 * 1024 * 1024).unwrap();

        // Second allocation that would exceed limit should fail.
        let result = buffer_pool.allocate_with_size(64 * 1024 * 1024);
        assert!(result.is_err());

        // Drop first buffer.
        drop(buf1);

        // Now we should be able to allocate again.
        let _buf2 = buffer_pool.allocate_with_size(32 * 1024 * 1024).unwrap();
    }
}
