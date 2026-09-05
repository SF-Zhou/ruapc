//! Public pool facade and allocation orchestration.
//!
//! `allocator` owns the locked buddy state; `small` coordinates slab classes and
//! reclaimable thread caches. Device registration runs outside those locks.

use std::io::{Error, ErrorKind, Result};
use std::ptr::NonNull;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aliasable::boxed::AliasableBox;
use tokio::sync::oneshot;

use crate::AlignedMemory;
use crate::buddy::{BuddyBlock, NUM_LEVELS, SIZE_64MIB, size_to_level};
use crate::buffer::Buffer;
use crate::devices::Devices;
use crate::slab::{NUM_SLAB_CLASSES, SlabClass, size_to_class};
use crate::thread_cache::CacheShard;

mod allocator;
mod builder;
mod small;
#[cfg(test)]
mod tests;

use allocator::BuddyAllocator;
pub use builder::{BufferPoolBuilder, DEFAULT_BUFFER_POOL_MEMORY};

enum AllocationStep {
    Ready(Buffer),
    Grow,
    Wait(oneshot::Receiver<Buffer>),
}

/// A high-performance memory pool using buddy memory allocation.
///
/// Manages registered 64 MiB blocks. Small allocations use 16 KiB, 64 KiB,
/// and 256 KiB slabs; larger allocations use 1 MiB, 4 MiB, 16 MiB, and 64 MiB
/// buddy nodes. Freed buffers are reused without clearing their contents.
pub struct BufferPool {
    devices: Arc<dyn Devices>,
    max_memory: usize,
    starvation_timeout: Duration,
    slab_empty_watermark: usize,
    /// Whether the per-thread chunk cache is enabled.
    /// See [`BufferPoolBuilder::thread_cache`].
    thread_cache: bool,
    /// Hint that the buddy pool has pending demand (async waiters or an
    /// active reservation). Read lock-free by the slab layer to bypass the
    /// empty-slab watermark, so that cached slabs cannot stall waiters or
    /// the anti-starvation drain. Conservatively-true is harmless.
    has_demand: AtomicBool,
    /// Slab size classes for small allocations, each behind its own mutex
    /// so small-buffer traffic does not contend on the buddy pool's mutex.
    /// Lock ordering: a slab class lock may be taken before `inner`, never
    /// after.
    slab_classes: [Mutex<SlabClass>; NUM_SLAB_CLASSES],
    /// Per-thread cache shards registered by threads that use this pool.
    /// Lock ordering: this lock and shard locks are leaves — never acquire
    /// a slab class lock or `inner` while holding them.
    thread_shards: Mutex<Vec<Arc<CacheShard>>>,
    inner: Mutex<BuddyAllocator>,
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
        inner.deallocate_buffer(self, level, index, block);
    }

    /// Allocates a buffer of at least the specified size.
    ///
    /// The returned buffer may be larger than requested, rounded up to the
    /// nearest size class (16 KiB, 64 KiB, 256 KiB, 1 MiB, 4 MiB, 16 MiB, or
    /// 64 MiB). Sizes up to 256 KiB are served by the slab layer; larger
    /// sizes by the buddy allocator.
    ///
    /// If the pool needs to grow, the 64 MiB block creation and device
    /// registration (potentially milliseconds for RDMA) happen *outside*
    /// the pool mutex, so concurrent allocations and frees are not stalled.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `size` is 0 or exceeds 64 MiB
    /// - Memory limit has been reached
    /// - Underlying allocator fails
    pub fn allocate(self: &Arc<Self>, size: usize) -> Result<Buffer> {
        if let Some(class) = size_to_class(size) {
            if let Some(buffer) = self.try_take_chunk_fast(class) {
                return Ok(buffer);
            }
            let backing = self.allocate_buddy(0)?;
            return Ok(self.install_backing_and_take(class, backing));
        }

        let level = size_to_level(size).ok_or_else(|| invalid_size_error(size))?;
        self.allocate_buddy(level)
    }

    fn allocate_buddy(self: &Arc<Self>, level: usize) -> Result<Buffer> {
        {
            let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
            if let Some(buffer) = inner.try_allocate_local(level, self) {
                return Ok(buffer);
            }
        }
        match self.prepare_allocation(level, false)? {
            AllocationStep::Ready(buffer) => Ok(buffer),
            AllocationStep::Grow => self.grow_and_allocate(level),
            AllocationStep::Wait(_) => unreachable!("synchronous allocations never queue"),
        }
    }

    /// Allocates a buffer asynchronously.
    ///
    /// If the memory limit has been reached, waits for other buffers to be
    /// freed. Freed capacity is handed off directly: the freeing task
    /// allocates on behalf of the waiter and sends the buffer through the
    /// waiter's channel, so waiters cannot lose races against concurrent
    /// [`Self::allocate`] calls.
    ///
    /// Like [`Self::allocate`], pool growth (block creation and device
    /// registration) happens outside the pool mutex. Note that it still runs
    /// on the current thread and may block it for the duration of the device
    /// registration; this only happens when the pool actually grows.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `size` is 0 or exceeds 64 MiB
    /// - Underlying allocator fails
    pub async fn async_allocate(self: &Arc<Self>, size: usize) -> Result<Buffer> {
        if let Some(class) = size_to_class(size) {
            if let Some(buffer) = self.try_take_chunk_fast(class) {
                return Ok(buffer);
            }
            let backing = self.async_allocate_buddy(0).await?;
            return Ok(self.install_backing_and_take(class, backing));
        }

        let level = size_to_level(size).ok_or_else(|| invalid_size_error(size))?;
        self.async_allocate_buddy(level).await
    }

    async fn async_allocate_buddy(self: &Arc<Self>, level: usize) -> Result<Buffer> {
        loop {
            {
                let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
                if let Some(buffer) = inner.try_allocate_local(level, self) {
                    return Ok(buffer);
                }
            }
            match self.prepare_allocation(level, true)? {
                AllocationStep::Ready(buffer) => return Ok(buffer),
                AllocationStep::Grow => return self.grow_and_allocate(level),
                AllocationStep::Wait(receiver) => {
                    // Direct handoff wins the race against competing allocations.
                    if let Ok(buffer) = receiver.await {
                        return Ok(buffer);
                    }
                }
            }
        }
    }

    /// Shared slow transition: reclaim cached capacity, grow, then (if allowed)
    /// queue a waiter. The last capacity check and registration share a lock.
    fn prepare_allocation(self: &Arc<Self>, level: usize, wait: bool) -> Result<AllocationStep> {
        self.reclaim_cached_capacity();

        let receiver = {
            let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
            if let Some(buffer) = inner.try_allocate_local(level, self) {
                return Ok(AllocationStep::Ready(buffer));
            }
            match inner.try_reserve_block(self.max_memory) {
                Ok(()) => return Ok(AllocationStep::Grow),
                Err(error) if !wait => return Err(error),
                Err(_) => inner.wait_for(level, self),
            }
        };

        // A concurrent free either observes registered demand or is captured by
        // this sweep. Idle threads cannot strand cached chunks while we wait.
        self.reclaim_cached_capacity();
        Ok(AllocationStep::Wait(receiver))
    }

    /// Completes an already reserved growth without holding the pool lock during
    /// system allocation/device registration. The grower claims its buffer before
    /// handing remaining capacity to waiters queued during registration.
    fn grow_and_allocate(self: &Arc<Self>, level: usize) -> Result<Buffer> {
        let block = self.create_block_or_release()?;
        let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
        inner.install_block(block);
        let buffer = inner.try_allocate_local(level, self);
        inner.serve_waiters(self);
        buffer.ok_or_else(|| Error::other("fresh block did not satisfy reserved allocation"))
    }

    /// Creates and registers a new 64 MiB block. Requires a prior successful
    /// [`BuddyAllocator::try_reserve_block`]; releases the reservation on failure.
    ///
    /// This is deliberately *not* called with the pool mutex held: memory
    /// allocation and device registration (e.g. `ibv_reg_mr`) can take
    /// milliseconds and must not stall concurrent allocations and frees.
    fn create_block_or_release(&self) -> Result<AliasableBox<BuddyBlock>> {
        let create = || -> Result<AliasableBox<BuddyBlock>> {
            let aligned = Arc::new(AlignedMemory::new(SIZE_64MIB)?);
            let regs = self.devices.register(&aligned)?;
            Ok(BuddyBlock::new(aligned, regs))
        };
        create().inspect_err(|_| {
            self.inner
                .lock()
                .expect("BufferPool mutex poisoned")
                .allocated_memory -= SIZE_64MIB;
        })
    }

    /// Returns the current amount of budgeted memory in bytes.
    ///
    /// This includes installed 64 MiB blocks plus any reservations for
    /// blocks that are currently being created and registered.
    pub fn allocated_memory(&self) -> usize {
        self.inner
            .lock()
            .expect("BufferPool mutex poisoned")
            .allocated_memory
    }

    /// Returns the maximum memory limit in bytes.
    pub const fn max_memory(&self) -> usize {
        self.max_memory
    }

    /// Returns the number of free buffers at each level.
    pub fn free_counts(&self) -> [usize; NUM_LEVELS] {
        let inner = self.inner.lock().expect("BufferPool mutex poisoned");
        std::array::from_fn(|i| inner.free_lists[i].len())
    }

    /// Returns the number of deferred (pending-merge) buddy quads at each level.
    ///
    /// `pending_counts()[L]` is the number of split parents at level `L` whose
    /// 4 children are all free but whose merge has been deferred (lazy buddy).
    /// Level 0 is always 0.
    pub fn pending_counts(&self) -> [usize; NUM_LEVELS] {
        let inner = self.inner.lock().expect("BufferPool mutex poisoned");
        std::array::from_fn(|i| inner.pending_lists[i].len())
    }

    /// Returns the number of free chunks in each slab size class
    /// (16 KiB, 64 KiB, 256 KiB).
    pub fn slab_free_counts(&self) -> [usize; NUM_SLAB_CLASSES] {
        std::array::from_fn(|class| {
            self.slab_classes[class]
                .lock()
                .expect("SlabClass mutex poisoned")
                .free_chunks()
        })
    }

    /// Returns the devices associated with this pool.
    pub fn devices(&self) -> &Arc<dyn Devices> {
        &self.devices
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let blocks = self
            .inner
            .lock()
            .expect("BufferPool mutex poisoned")
            .blocks
            .len();
        f.debug_struct("BufferPool")
            .field("allocated_memory", &self.allocated_memory())
            .field("max_memory", &self.max_memory)
            .field("blocks", &blocks)
            .finish_non_exhaustive()
    }
}

fn invalid_size_error(size: usize) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        format!("invalid size: {size} (must be 1-67108864 bytes)"),
    )
}
