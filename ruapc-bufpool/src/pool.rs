//! Buffer pool implementation with buddy memory allocation.

use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use aliasable::boxed::AliasableBox;
use tokio::sync::oneshot;

use crate::AlignedMemory;
use crate::buddy::{BuddyBlock, NUM_LEVELS, NodeState, SIZE_64MIB, size_to_level};
use crate::buffer::Buffer;
use crate::devices::Devices;
use crate::intrusive_list::IntrusiveList;

/// Default maximum memory limit (256 MiB).
const DEFAULT_MAX_MEMORY: usize = 256 * 1024 * 1024;

/// Builder for creating a [`BufferPool`] with custom configuration.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use ruapc_bufpool::{BufferPoolBuilder, EmptyDevices};
///
/// let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
///     .max_memory(512 * 1024 * 1024)
///     .build();
/// ```
pub struct BufferPoolBuilder {
    max_memory: usize,
    devices: Arc<dyn Devices>,
}

impl BufferPoolBuilder {
    /// Creates a new builder with the given devices.
    ///
    /// Each new 64 MiB block will be registered with all devices.
    #[must_use]
    pub fn new(devices: Arc<dyn Devices>) -> Self {
        Self {
            max_memory: DEFAULT_MAX_MEMORY,
            devices,
        }
    }

    /// Sets the maximum memory limit for the pool.
    ///
    /// The limit should be a multiple of 64 MiB for optimal utilization.
    #[must_use]
    pub const fn max_memory(mut self, max_memory: usize) -> Self {
        self.max_memory = max_memory;
        self
    }

    /// Builds the buffer pool with the configured settings.
    #[must_use]
    pub fn build(self) -> Arc<BufferPool> {
        let inner = PoolInner {
            max_memory: self.max_memory,
            allocated_memory: 0,
            blocks: Vec::new(),
            free_lists: std::array::from_fn(|_| IntrusiveList::new()),
            waiting_lists: std::array::from_fn(|_| VecDeque::new()),
            min_waiting_level: None,
        };

        Arc::new(BufferPool {
            devices: self.devices,
            inner: Mutex::new(inner),
        })
    }
}

/// A high-performance memory pool using buddy memory allocation.
///
/// Manages memory in 64 MiB blocks and supports allocation of buffers
/// at four size levels: 1 MiB, 4 MiB, 16 MiB, and 64 MiB.
pub struct BufferPool {
    devices: Arc<dyn Devices>,
    inner: Mutex<PoolInner>,
}

impl BufferPool {
    /// Creates a new buffer pool with the given devices and default settings.
    #[must_use]
    pub fn new(devices: Arc<dyn Devices>) -> Arc<Self> {
        BufferPoolBuilder::new(devices).build()
    }

    /// Returns a buffer to the pool.
    ///
    /// Called automatically when a [`Buffer`] is dropped.
    pub(crate) fn return_buffer(
        self: &Arc<Self>,
        level: usize,
        index: usize,
        block: NonNull<BuddyBlock>,
    ) {
        let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
        inner.deallocate_buffer(level, index, block);
    }

    /// Allocates a buffer of at least the specified size.
    ///
    /// The returned buffer may be larger than requested, rounded up to the
    /// nearest allocation level (1 MiB, 4 MiB, 16 MiB, or 64 MiB).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `size` is 0 or exceeds 64 MiB
    /// - Memory limit has been reached
    /// - Underlying allocator fails
    pub fn allocate(self: &Arc<Self>, size: usize) -> Result<Buffer> {
        let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
        inner.allocate_sync(size, self)
    }

    /// Allocates a buffer asynchronously.
    ///
    /// If the memory limit has been reached, waits for other buffers to be freed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `size` is 0 or exceeds 64 MiB
    /// - Underlying allocator fails
    pub async fn async_allocate(self: &Arc<Self>, size: usize) -> Result<Buffer> {
        let level = size_to_level(size).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid size: {size} (must be 1-67108864 bytes)"),
            )
        })?;

        loop {
            let receiver = {
                let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");

                match inner.try_allocate(level, self) {
                    Ok(buffer) => return Ok(buffer),
                    Err(e) if e.kind() == ErrorKind::OutOfMemory => {
                        let (sender, receiver) = oneshot::channel();
                        inner.waiting_lists[level].push_back(sender);
                        inner.update_min_waiting_level_on_add(level);
                        receiver
                    }
                    Err(e) => return Err(e),
                }
            };

            // Wait for buffer to be available (lock is released)
            if let Ok(buffer) = receiver.await {
                return Ok(buffer);
            }
            // Sender was dropped without sending, retry allocation
        }
    }

    /// Returns the current amount of allocated memory in bytes.
    pub fn allocated_memory(&self) -> usize {
        self.inner
            .lock()
            .expect("BufferPool mutex poisoned")
            .allocated_memory
    }

    /// Returns the maximum memory limit in bytes.
    pub fn max_memory(&self) -> usize {
        self.inner
            .lock()
            .expect("BufferPool mutex poisoned")
            .max_memory
    }

    /// Returns the number of free buffers at each level.
    pub fn free_counts(&self) -> [usize; NUM_LEVELS] {
        let inner = self.inner.lock().expect("BufferPool mutex poisoned");
        std::array::from_fn(|i| inner.free_lists[i].len())
    }

    /// Returns the devices associated with this pool.
    pub fn devices(&self) -> &Arc<dyn Devices> {
        &self.devices
    }

    /// Returns the memory key for a given block and device.
    pub(crate) fn memory_key(
        &self,
        block_index: usize,
        device_index: &impl crate::AsDeviceIndex,
    ) -> Result<crate::MemoryKey> {
        let inner = self.inner.lock().expect("BufferPool mutex poisoned");
        let idx = device_index.as_device_index();
        inner
            .blocks
            .get(block_index)
            .and_then(|block| block.registrations.get(idx.index as usize))
            .map(|r| r.memory_key())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("memory not registered on device index {}", idx.index),
                )
            })
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().expect("BufferPool mutex poisoned");
        f.debug_struct("BufferPool")
            .field("allocated_memory", &inner.allocated_memory)
            .field("max_memory", &inner.max_memory)
            .field("blocks", &inner.blocks.len())
            .finish_non_exhaustive()
    }
}

/// Sender type for waiting list entries.
type WaitingSender = oneshot::Sender<Buffer>;

/// Internal pool state protected by the mutex.
struct PoolInner {
    max_memory: usize,
    allocated_memory: usize,
    /// All allocated buddy blocks. Each entry is wrapped in `AliasableBox` to
    /// opt out of the `noalias` guarantee that `Box` carries. This is required
    /// because `Buffer` and intrusive free-list nodes hold `NonNull<BuddyBlock>`
    /// pointers that alias the box contents. Without `AliasableBox`, the
    /// compiler could assume exclusive access through the `Box` and mis-optimize
    /// accesses through the raw pointers.
    blocks: Vec<AliasableBox<BuddyBlock>>,
    free_lists: [IntrusiveList<crate::buddy::FreeNodeData>; NUM_LEVELS],
    waiting_lists: [VecDeque<WaitingSender>; NUM_LEVELS],
    min_waiting_level: Option<usize>,
}

impl PoolInner {
    fn allocate_sync(&mut self, size: usize, pool: &Arc<BufferPool>) -> Result<Buffer> {
        let level = size_to_level(size).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid size: {size} (must be 1-67108864 bytes)"),
            )
        })?;

        self.try_allocate(level, pool)
    }

    fn try_allocate(&mut self, level: usize, pool: &Arc<BufferPool>) -> Result<Buffer> {
        if let Some(buffer) = self.try_allocate_from_free_lists(level, Arc::clone(pool)) {
            return Ok(buffer);
        }

        // Need to allocate a new 64 MiB block
        self.allocate_new_block(&pool.devices)?;

        // Try again - should succeed now
        self.try_allocate_from_free_lists(level, Arc::clone(pool))
            .ok_or_else(|| Error::other("allocation failed unexpectedly"))
    }

    fn try_allocate_from_free_lists(
        &mut self,
        level: usize,
        pool: Arc<BufferPool>,
    ) -> Option<Buffer> {
        for search_level in level..NUM_LEVELS {
            if !self.free_lists[search_level].is_empty() {
                return Some(self.allocate_at_level(search_level, level, pool));
            }
        }
        None
    }

    fn allocate_at_level(
        &mut self,
        from_level: usize,
        target_level: usize,
        pool: Arc<BufferPool>,
    ) -> Buffer {
        let node = self.free_lists[from_level].pop_front().unwrap();

        let (block, block_index, index_in_level) = unsafe {
            let node_ref = &*node.as_ptr();
            (
                node_ref.data.block,
                node_ref.data.block_index,
                node_ref.data.index_in_level,
            )
        };

        let block_ref = unsafe { &mut *block.as_ptr() };

        if from_level == target_level {
            block_ref.set_state(from_level, index_in_level, NodeState::Allocated);
            let ptr = block_ref.get_memory_addr(from_level, index_in_level);

            unsafe {
                Buffer::new(
                    NonNull::new(ptr).unwrap(),
                    from_level,
                    index_in_level,
                    block,
                    block_index,
                    pool,
                )
            }
        } else {
            self.split_and_allocate(
                block,
                block_index,
                from_level,
                index_in_level,
                target_level,
                pool,
            )
        }
    }

    fn split_and_allocate(
        &mut self,
        block: NonNull<BuddyBlock>,
        block_index: usize,
        from_level: usize,
        from_index: usize,
        target_level: usize,
        pool: Arc<BufferPool>,
    ) -> Buffer {
        let block_ref = unsafe { &mut *block.as_ptr() };

        let mut current_level = from_level;
        let mut current_index = from_index;

        while current_level > target_level {
            block_ref.set_state(current_level, current_index, NodeState::Split);

            let (child_level, first_child_index) =
                BuddyBlock::get_first_child(current_level, current_index).unwrap();

            // Add siblings 1-3 to free list (child 0 will be used or split further)
            for i in 1..4 {
                let child_index = first_child_index + i;
                block_ref.set_state(child_level, child_index, NodeState::Free);

                let node = block_ref.get_free_node_mut(child_level, child_index);
                unsafe {
                    self.free_lists[child_level].push_front(node);
                }
            }

            current_level = child_level;
            current_index = first_child_index;
        }

        block_ref.set_state(current_level, current_index, NodeState::Allocated);
        let ptr = block_ref.get_memory_addr(current_level, current_index);

        unsafe {
            Buffer::new(
                NonNull::new(ptr).unwrap(),
                current_level,
                current_index,
                block,
                block_index,
                pool,
            )
        }
    }

    fn allocate_new_block(&mut self, devices: &Arc<dyn Devices>) -> Result<()> {
        if self.allocated_memory + SIZE_64MIB > self.max_memory {
            return Err(Error::new(ErrorKind::OutOfMemory, "memory limit reached"));
        }

        // Allocate 64 MiB aligned memory.
        let aligned = Arc::new(AlignedMemory::new(SIZE_64MIB)?);

        // Register with all devices.
        let regs = devices.register(&aligned)?;

        let block_index = self.blocks.len();

        // Create buddy block with memory and registrations.
        let block = BuddyBlock::new(aligned, block_index, regs);

        // Wrap in AliasableBox to allow aliasing via NonNull<BuddyBlock> in
        // Buffer and free-list nodes without violating noalias semantics.
        let block = AliasableBox::from_unique(block);

        let block_ptr = NonNull::new(std::ptr::from_ref::<BuddyBlock>(&block).cast_mut()).unwrap();

        // Add root node to level 3 free list.
        unsafe {
            let node = (*block_ptr.as_ptr()).get_free_node_mut(3, 0);
            self.free_lists[3].push_front(node);
        }

        self.blocks.push(block);
        self.allocated_memory += SIZE_64MIB;

        Ok(())
    }

    pub(crate) fn deallocate_buffer(
        &mut self,
        level: usize,
        index: usize,
        block: NonNull<BuddyBlock>,
    ) {
        let block_ref = unsafe { &mut *block.as_ptr() };

        let (final_level, _final_index) = self.try_merge(block_ref, level, index);

        // Check if we should notify a waiting allocation
        if let Some(min_waiting) = self.min_waiting_level
            && final_level >= min_waiting
        {
            for wait_level in min_waiting..=final_level {
                if self.waiting_lists[wait_level].pop_front().is_some() {
                    // Sender is dropped here, signaling the waiter to retry
                    self.update_min_waiting_level_on_remove(wait_level);
                    return;
                }
            }
        }
    }

    fn try_merge(&mut self, block: &mut BuddyBlock, level: usize, index: usize) -> (usize, usize) {
        let mut current_level = level;
        let mut current_index = index;

        loop {
            if current_level >= 3 {
                block.set_state(current_level, current_index, NodeState::Free);
                let node = block.get_free_node_mut(current_level, current_index);
                unsafe {
                    self.free_lists[current_level].push_front(node);
                }
                return (current_level, current_index);
            }

            let siblings = BuddyBlock::get_siblings(current_index);

            let all_free = siblings.iter().all(|&idx| {
                idx == current_index || block.get_state(current_level, idx) == NodeState::Free
            });

            if !all_free {
                block.set_state(current_level, current_index, NodeState::Free);
                let node = block.get_free_node_mut(current_level, current_index);
                unsafe {
                    self.free_lists[current_level].push_front(node);
                }
                return (current_level, current_index);
            }

            // All siblings are free, remove them from free list and merge
            for &sibling_idx in &siblings {
                if sibling_idx != current_index {
                    let node = block.get_free_node_mut(current_level, sibling_idx);
                    unsafe {
                        self.free_lists[current_level].remove(node);
                    }
                }
                block.set_state(current_level, sibling_idx, NodeState::Allocated);
            }

            // Move up to parent
            let (parent_level, parent_index) =
                BuddyBlock::get_parent(current_level, current_index).unwrap();
            current_level = parent_level;
            current_index = parent_index;
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    fn update_min_waiting_level_on_add(&mut self, added_level: usize) {
        match self.min_waiting_level {
            None => self.min_waiting_level = Some(added_level),
            Some(min_level) if added_level < min_level => {
                self.min_waiting_level = Some(added_level);
            }
            _ => {}
        }
    }

    fn update_min_waiting_level_on_remove(&mut self, removed_level: usize) {
        if self.min_waiting_level == Some(removed_level)
            && self.waiting_lists[removed_level].is_empty()
        {
            self.min_waiting_level =
                (removed_level..NUM_LEVELS).find(|&level| !self.waiting_lists[level].is_empty());
        }
    }
}

// Note: PoolInner uses default drop. Each BuddyBlock's fields are dropped in
// declaration order: `registrations` is dropped before `memory`, ensuring device
// deregistration (e.g. ibv_dereg_mr) happens while the memory is still valid.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EmptyDevices;
    use crate::buddy::{LEVEL_SIZES, SIZE_1MIB};

    fn test_pool() -> Arc<BufferPool> {
        BufferPoolBuilder::new(Arc::new(EmptyDevices)).build()
    }

    fn test_pool_with_max(max: usize) -> Arc<BufferPool> {
        BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(max)
            .build()
    }

    #[test]
    fn test_pool_builder_defaults() {
        let pool = test_pool();
        drop(pool);
    }

    #[test]
    fn test_pool_builder_custom() {
        let pool = test_pool_with_max(128 * 1024 * 1024);
        drop(pool);
    }

    #[test]
    fn test_simple_allocation() {
        let pool = test_pool();
        let buffer = pool.allocate(SIZE_1MIB).unwrap();
        assert_eq!(buffer.len(), SIZE_1MIB);
    }

    #[test]
    fn test_allocation_sizes() {
        let pool = test_pool();

        let b1 = pool.allocate(1).unwrap();
        assert_eq!(b1.len(), SIZE_1MIB);

        let b2 = pool.allocate(SIZE_1MIB + 1).unwrap();
        assert_eq!(b2.len(), LEVEL_SIZES[1]);

        let b3 = pool.allocate(LEVEL_SIZES[1] + 1).unwrap();
        assert_eq!(b3.len(), LEVEL_SIZES[2]);

        let b4 = pool.allocate(LEVEL_SIZES[2] + 1).unwrap();
        assert_eq!(b4.len(), LEVEL_SIZES[3]);
    }

    #[test]
    fn test_allocation_reuse() {
        let pool = test_pool_with_max(SIZE_64MIB);

        let addr1 = {
            let buffer = pool.allocate(SIZE_1MIB).unwrap();
            buffer.as_ptr() as usize
        };

        let buffer2 = pool.allocate(SIZE_1MIB).unwrap();
        let addr2 = buffer2.as_ptr() as usize;

        assert!(addr1 > 0);
        assert!(addr2 > 0);
    }

    #[test]
    fn test_memory_limit_sync() {
        let pool = test_pool_with_max(SIZE_64MIB);

        let _b1 = pool.allocate(SIZE_64MIB).unwrap();

        let result = pool.allocate(SIZE_1MIB);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::OutOfMemory);
    }

    #[test]
    fn test_invalid_size() {
        let pool = test_pool();

        let result = pool.allocate(0);
        assert!(result.is_err());

        let result = pool.allocate(SIZE_64MIB + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_buddy_splitting() {
        let pool = test_pool_with_max(SIZE_64MIB);

        let buffers: Vec<_> = (0..64).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();

        assert_eq!(buffers.len(), 64);

        let base = buffers[0].as_ptr() as usize;
        for buf in &buffers {
            let addr = buf.as_ptr() as usize;
            assert!(addr >= base - SIZE_64MIB && addr < base + SIZE_64MIB);
            assert_eq!(buf.len(), SIZE_1MIB);
        }
    }

    #[test]
    fn test_buddy_merging() {
        let pool = test_pool_with_max(SIZE_64MIB);

        let b1 = pool.allocate(LEVEL_SIZES[2]).unwrap();
        let b2 = pool.allocate(LEVEL_SIZES[2]).unwrap();
        let b3 = pool.allocate(LEVEL_SIZES[2]).unwrap();
        let b4 = pool.allocate(LEVEL_SIZES[2]).unwrap();

        assert!(pool.allocate(SIZE_1MIB).is_err());

        drop(b1);
        drop(b2);
        drop(b3);
        drop(b4);

        let b5 = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(b5.len(), SIZE_64MIB);
    }

    #[tokio::test]
    async fn test_async_allocation() {
        let pool = test_pool();
        let buffer = pool.async_allocate(SIZE_1MIB).await.unwrap();
        assert_eq!(buffer.len(), SIZE_1MIB);
    }

    #[tokio::test]
    async fn test_async_allocation_waiting() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = test_pool_with_max(SIZE_64MIB);

        let buffer = pool.async_allocate(SIZE_64MIB).await.unwrap();

        let pool_clone = pool.clone();

        let handle = tokio::spawn(async move { pool_clone.async_allocate(SIZE_1MIB).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(buffer);

        let result = timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok());
        let buffer = result.unwrap().unwrap().unwrap();
        assert_eq!(buffer.len(), SIZE_1MIB);
    }

    #[tokio::test]
    async fn test_pool_stats() {
        let pool = test_pool_with_max(SIZE_64MIB * 2);

        assert_eq!(pool.allocated_memory(), 0);
        assert_eq!(pool.max_memory(), SIZE_64MIB * 2);

        let _buffer = pool.async_allocate(SIZE_1MIB).await.unwrap();
        assert_eq!(pool.allocated_memory(), SIZE_64MIB);
    }

    #[test]
    fn test_buffer_write_read() {
        let pool = test_pool();
        let mut buffer = pool.allocate(SIZE_1MIB).unwrap();

        for (i, byte) in buffer.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        for (i, byte) in buffer.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8);
        }
    }

    #[test]
    fn test_multiple_pools() {
        let pool1 = test_pool_with_max(SIZE_64MIB);
        let pool2 = test_pool_with_max(SIZE_64MIB);

        let b1 = pool1.allocate(SIZE_64MIB).unwrap();
        let b2 = pool2.allocate(SIZE_64MIB).unwrap();

        assert_eq!(b1.len(), SIZE_64MIB);
        assert_eq!(b2.len(), SIZE_64MIB);
    }

    #[test]
    fn test_clone_pool() {
        let pool = test_pool_with_max(SIZE_64MIB);

        let pool_clone = pool.clone();

        let b1 = pool.allocate(SIZE_64MIB).unwrap();

        let result = pool_clone.allocate(SIZE_1MIB);
        assert!(result.is_err());

        drop(b1);

        let _b2 = pool_clone.allocate(SIZE_1MIB).unwrap();
    }
}
