//! Buffer pool implementation with buddy memory allocation.

use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aliasable::boxed::AliasableBox;
use tokio::sync::oneshot;

use crate::AlignedMemory;
use crate::buddy::{
    BuddyBlock, FreeNode, LEVEL_SIZES, NODES_PER_LEVEL, NUM_LEVELS, NodeState, SIZE_64MIB,
    size_to_level,
};
use crate::buffer::Buffer;
use crate::devices::Devices;
use crate::intrusive_list::IntrusiveList;
use crate::slab::{NUM_SLAB_CLASSES, SlabClass, size_to_class};
use crate::thread_cache::{self, CacheShard, MAG_REFILL, RawChunk};

/// Default maximum memory limit (256 MiB).
const DEFAULT_MAX_MEMORY: usize = 256 * 1024 * 1024;

/// Default merge watermarks for each level (lazy buddy merging).
///
/// When a buffer is freed at level `L`, its buddy quad is only merged upward
/// if the free list at level `L` already holds at least `watermark[L]` nodes
/// (or a larger allocation is waiting). This avoids split/merge thrashing for
/// workloads that repeatedly allocate and free buffers of the same size.
///
/// The level-3 (64 MiB, root) entry is unused since roots are never merged.
const DEFAULT_MERGE_WATERMARKS: [usize; NUM_LEVELS] = [16, 8, 2, 0];

/// Default starvation timeout for large async waiters.
///
/// A waiter for a 4 MiB or larger buffer that has been queued longer than
/// this triggers a subtree reservation, protecting capacity from being
/// consumed by smaller allocations until the waiter can be satisfied.
const DEFAULT_STARVATION_TIMEOUT: Duration = Duration::from_millis(50);

/// Default number of empty slabs cached per slab size class.
///
/// Caching avoids refill/release thrashing for small-buffer churn; each
/// cached slab holds 1 MiB. Empty slabs beyond the watermark — or all of
/// them when the buddy pool has pending demand — are returned to the buddy
/// pool.
const DEFAULT_SLAB_EMPTY_WATERMARK: usize = 1;

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
    merge_watermarks: [usize; NUM_LEVELS],
    starvation_timeout: Duration,
    slab_empty_watermark: usize,
    thread_cache: bool,
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
            merge_watermarks: DEFAULT_MERGE_WATERMARKS,
            starvation_timeout: DEFAULT_STARVATION_TIMEOUT,
            slab_empty_watermark: DEFAULT_SLAB_EMPTY_WATERMARK,
            thread_cache: true,
        }
    }

    /// Enables or disables the per-thread chunk cache (enabled by default).
    ///
    /// When enabled, small-buffer (slab chunk) allocations and frees are
    /// served from per-thread cache shards; the shared per-class slab lock
    /// is only touched for batched refills and overflows. Threads that free
    /// chunks keep up to 2 MiB cached per slab class. The pool reclaims all
    /// cached chunks whenever it actually needs the memory (buddy
    /// allocation miss or a queued async waiter), and each thread flushes
    /// its shards on exit.
    ///
    /// Disable for exact accounting of free chunk counts (e.g. in tests
    /// asserting [`BufferPool::slab_free_counts`]).
    #[must_use]
    pub const fn thread_cache(mut self, enabled: bool) -> Self {
        self.thread_cache = enabled;
        self
    }

    /// Sets the number of empty slabs cached per slab size class.
    ///
    /// Small allocations (up to 256 KiB) are served from slabs: 1 MiB buddy
    /// leaves carved into fixed-size chunks. When all chunks of a slab are
    /// free, the slab is cached for reuse; empty slabs beyond this watermark
    /// are returned to the buddy pool. When the buddy pool has pending
    /// demand (async waiters or an anti-starvation reservation), empty slabs
    /// are always returned immediately, regardless of the watermark.
    #[must_use]
    pub const fn slab_empty_watermark(mut self, watermark: usize) -> Self {
        self.slab_empty_watermark = watermark;
        self
    }

    /// Sets the starvation timeout for large async waiters.
    ///
    /// Waiters are normally served smallest-request-first, so under
    /// sustained small-allocation pressure a large waiter could starve.
    /// When a waiter for a 4 MiB or larger buffer has been queued longer
    /// than this timeout, the pool reserves a 64 MiB-aligned subtree for it:
    /// free capacity inside the subtree is drained toward the waiter and
    /// protected from other allocations until the request is satisfied.
    /// At most one reservation is active at a time.
    ///
    /// Use `Duration::MAX` to disable starvation protection.
    #[must_use]
    pub const fn starvation_timeout(mut self, timeout: Duration) -> Self {
        self.starvation_timeout = timeout;
        self
    }

    /// Sets the maximum memory limit for the pool.
    ///
    /// The limit should be a multiple of 64 MiB for optimal utilization.
    #[must_use]
    pub const fn max_memory(mut self, max_memory: usize) -> Self {
        self.max_memory = max_memory;
        self
    }

    /// Sets the merge watermarks for lazy buddy merging (one per level).
    ///
    /// When a buffer is freed at level `L`, its buddy quad is merged upward
    /// only if the free list at level `L` already holds at least
    /// `watermarks[L]` nodes, or a larger allocation is currently waiting.
    /// Deferred merges are performed on demand when an allocation cannot be
    /// satisfied from the free lists (before growing the pool).
    ///
    /// Setting all watermarks to 0 restores eager merging.
    /// The level-3 entry is unused since root nodes are never merged.
    #[must_use]
    pub const fn merge_watermarks(mut self, watermarks: [usize; NUM_LEVELS]) -> Self {
        self.merge_watermarks = watermarks;
        self
    }

    /// Builds the buffer pool with the configured settings.
    #[must_use]
    pub fn build(self) -> Arc<BufferPool> {
        let inner = PoolInner {
            allocated_memory: 0,
            merge_watermarks: self.merge_watermarks,
            blocks: Vec::new(),
            free_lists: std::array::from_fn(|_| IntrusiveList::new()),
            pending_lists: std::array::from_fn(|_| IntrusiveList::new()),
            waiting_lists: std::array::from_fn(|_| VecDeque::new()),
            min_waiting_level: None,
            reservation: None,
        };

        Arc::new(BufferPool {
            devices: self.devices,
            max_memory: self.max_memory,
            starvation_timeout: self.starvation_timeout,
            slab_empty_watermark: self.slab_empty_watermark,
            thread_cache: self.thread_cache,
            has_demand: AtomicBool::new(false),
            slab_classes: std::array::from_fn(|class| Mutex::new(SlabClass::new(class))),
            thread_shards: Mutex::new(Vec::new()),
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
        inner.deallocate_buffer(self, level, index, block);
    }

    /// Allocates a buffer of at least the specified size.
    ///
    /// The returned buffer may be larger than requested, rounded up to the
    /// nearest size class (64 KiB, 256 KiB, 1 MiB, 4 MiB, 16 MiB, or
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

    /// Allocates a buddy buffer at the given level (synchronous).
    fn allocate_buddy(self: &Arc<Self>, level: usize) -> Result<Buffer> {
        {
            let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
            if let Some(buffer) = inner.try_allocate_local(level, self) {
                return Ok(buffer);
            }
        }

        // Cached chunks keep their slabs non-empty; reclaiming them from
        // all threads lets the empty-slab reclamation below make progress.
        self.flush_thread_cache();

        // The slab layer may be caching empty slabs; releasing them can
        // satisfy this allocation without growing the pool.
        self.reclaim_empty_slabs();

        {
            let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
            if let Some(buffer) = inner.try_allocate_local(level, self) {
                return Ok(buffer);
            }
            // Grow the pool: reserve budget under the lock, then create and
            // register the block without holding the pool mutex.
            inner.try_reserve_block(self.max_memory)?;
        }

        let block = self.create_block_or_release()?;

        let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
        inner.install_block(block);
        // Cannot fail: the freshly installed block is allocated under the
        // same lock acquisition that installed it. The grower is served
        // before any waiters: it paid the budget reservation for this block.
        let buffer = inner
            .try_allocate_local(level, self)
            .ok_or_else(|| Error::other("allocation failed unexpectedly"));
        // Hand any remaining capacity of the new block to queued waiters
        // (they registered while the block was being created and registered).
        inner.serve_waiters(self);
        buffer
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

    /// Allocates a buddy buffer at the given level, waiting for capacity if
    /// the memory limit has been reached.
    async fn async_allocate_buddy(self: &Arc<Self>, level: usize) -> Result<Buffer> {
        loop {
            enum Step {
                Grow,
                Wait(oneshot::Receiver<Buffer>),
            }

            {
                let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
                if let Some(buffer) = inner.try_allocate_local(level, self) {
                    return Ok(buffer);
                }
            }

            // Cached chunks keep their slabs non-empty; reclaiming them
            // from all threads lets the empty-slab reclamation below make
            // progress.
            self.flush_thread_cache();

            // The slab layer may be caching empty slabs; releasing them can
            // satisfy this allocation without growing or waiting.
            self.reclaim_empty_slabs();

            let step = {
                let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");

                if let Some(buffer) = inner.try_allocate_local(level, self) {
                    return Ok(buffer);
                }

                if inner.try_reserve_block(self.max_memory).is_ok() {
                    Step::Grow
                } else {
                    let (sender, receiver) = oneshot::channel();
                    inner.waiting_lists[level].push_back(Waiter {
                        since: Instant::now(),
                        sender,
                    });
                    inner.update_min_waiting_level_on_add(level);
                    // Hint the slab layer that cached empty slabs must be
                    // released as soon as they appear. SeqCst pairs with the
                    // load in `return_chunk`: a concurrent free either sees
                    // the demand (and bypasses/flushes its cache shard), or
                    // its cached push is visible to the reclaim sweep below.
                    self.has_demand.store(true, Ordering::SeqCst);
                    Step::Wait(receiver)
                }
            };

            if matches!(step, Step::Wait(_)) {
                // Reclaim chunks cached by all threads before parking: a
                // cached chunk on an idle thread must not stall this waiter.
                self.flush_thread_cache();
                self.reclaim_empty_slabs();
            }

            match step {
                Step::Grow => {
                    let block = self.create_block_or_release()?;
                    let mut inner = self.inner.lock().expect("BufferPool mutex poisoned");
                    inner.install_block(block);
                    let buffer = inner.try_allocate_local(level, self);
                    inner.serve_waiters(self);
                    if let Some(buffer) = buffer {
                        return Ok(buffer);
                    }
                    // Unreachable in practice (install + allocate happen under
                    // one lock acquisition); retry defensively.
                }
                Step::Wait(receiver) => {
                    // Wait for a handed-off buffer (the lock is released
                    // while waiting).
                    if let Ok(buffer) = receiver.await {
                        return Ok(buffer);
                    }
                    // Sender was dropped without sending (e.g. pool
                    // teardown); retry allocation defensively.
                }
            }
        }
    }

    /// Creates and registers a new 64 MiB block. Requires a prior successful
    /// [`PoolInner::try_reserve_block`]; releases the reservation on failure.
    ///
    /// This is deliberately *not* called with the pool mutex held: memory
    /// allocation and device registration (e.g. `ibv_reg_mr`) can take
    /// milliseconds and must not stall concurrent allocations and frees.
    fn create_block_or_release(&self) -> Result<Box<BuddyBlock>> {
        let create = || -> Result<Box<BuddyBlock>> {
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
    /// (64 KiB, 256 KiB).
    pub fn slab_free_counts(&self) -> [usize; NUM_SLAB_CLASSES] {
        std::array::from_fn(|class| {
            self.slab_classes[class]
                .lock()
                .expect("SlabClass mutex poisoned")
                .free_chunks()
        })
    }

    /// Takes a chunk from an existing slab of the given class, if available.
    fn try_take_chunk(self: &Arc<Self>, class: usize) -> Option<Buffer> {
        let (ptr, index, block) = self.slab_classes[class]
            .lock()
            .expect("SlabClass mutex poisoned")
            .alloc()?;
        Some(unsafe { Buffer::new_chunk(ptr, class, index, block, Arc::clone(self)) })
    }

    /// Takes a chunk, preferring the current thread's cache shard.
    ///
    /// On a shard miss, refills it with a batch of chunks taken from the
    /// shared slab layer under a single lock acquisition.
    fn try_take_chunk_fast(self: &Arc<Self>, class: usize) -> Option<Buffer> {
        if !self.thread_cache {
            return self.try_take_chunk(class);
        }
        let Some(shard) = thread_cache::shard_for(self) else {
            return self.try_take_chunk(class);
        };
        let chunk = match shard.pop(class) {
            Some(chunk) => chunk,
            None => {
                let mut batch = Vec::with_capacity(MAG_REFILL[class] + 1);
                self.take_chunk_batch(class, MAG_REFILL[class] + 1, &mut batch);
                let chunk = batch.pop()?;
                shard.store_batch(class, batch);
                chunk
            }
        };
        Some(unsafe {
            Buffer::new_chunk(chunk.ptr, class, chunk.index, chunk.block, Arc::clone(self))
        })
    }

    /// Takes up to `n` chunks from the slab layer under one lock acquisition.
    fn take_chunk_batch(&self, class: usize, n: usize, out: &mut Vec<RawChunk>) {
        let mut slab_class = self.slab_classes[class]
            .lock()
            .expect("SlabClass mutex poisoned");
        for _ in 0..n {
            match slab_class.alloc() {
                Some((ptr, index, block)) => out.push(RawChunk { ptr, index, block }),
                None => break,
            }
        }
    }

    /// Installs a fresh 1 MiB backing buffer as a slab of the given class
    /// and takes the first chunk from it.
    fn install_backing_and_take(self: &Arc<Self>, class: usize, backing: Buffer) -> Buffer {
        let (ptr, index, block) = {
            let mut slab_class = self.slab_classes[class]
                .lock()
                .expect("SlabClass mutex poisoned");
            slab_class.insert_backing(backing);
            slab_class
                .alloc()
                .expect("fresh slab must have free chunks")
        };
        unsafe { Buffer::new_chunk(ptr, class, index, block, Arc::clone(self)) }
    }

    /// Returns a chunk to the pool.
    ///
    /// Called automatically when a slab-chunk [`Buffer`] is dropped. The
    /// chunk is cached in the current thread's shard when possible;
    /// otherwise (cache disabled, shard overflow, buddy demand or thread
    /// destruction) it goes back to the shared slab layer.
    pub(crate) fn return_chunk(
        self: &Arc<Self>,
        class: usize,
        ptr: NonNull<u8>,
        index: usize,
        block: NonNull<BuddyBlock>,
    ) {
        let chunk = RawChunk { ptr, index, block };
        if self.thread_cache
            && !self.has_demand.load(Ordering::SeqCst)
            && let Some(shard) = thread_cache::shard_for(self)
        {
            if let Some(overflow) = shard.push(class, chunk) {
                self.return_chunks_direct(class, overflow);
            }
            // Close the race against a waiter registering between the
            // demand check above and the push: if demand appeared, flush
            // the shard we just pushed to. Either this load sees the
            // (SeqCst) demand store, or the waiter's reclaim sweep sees
            // our push — the chunk cannot be stranded.
            if self.has_demand.load(Ordering::SeqCst) {
                self.flush_shard(&shard);
            }
            return;
        }
        self.return_chunks_direct(class, [chunk]);
    }

    /// Returns chunks to their slabs under one lock acquisition.
    ///
    /// If a slab becomes fully free and exceeds the empty-slab watermark —
    /// or the buddy pool has pending demand — the slab's backing buffer is
    /// released to the buddy pool (outside the class lock; its `Drop`
    /// re-enters the pool through the regular buddy free path).
    pub(crate) fn return_chunks_direct(
        &self,
        class: usize,
        chunks: impl IntoIterator<Item = RawChunk>,
    ) {
        let mut released = Vec::new();
        {
            let max_empty = if self.has_demand.load(Ordering::SeqCst) {
                0
            } else {
                self.slab_empty_watermark
            };
            let mut slab_class = self.slab_classes[class]
                .lock()
                .expect("SlabClass mutex poisoned");
            for chunk in chunks {
                if let Some(backing) =
                    slab_class.free(chunk.ptr.as_ptr() as usize, chunk.index, max_empty)
                {
                    released.push(backing);
                }
            }
        }
        drop(released);
    }

    /// Registers a thread's cache shard (called via TLS on first use).
    pub(crate) fn register_shard(&self, shard: Arc<CacheShard>) {
        self.thread_shards
            .lock()
            .expect("thread_shards mutex poisoned")
            .push(shard);
    }

    /// Flushes and unregisters a thread's cache shard (thread exit).
    pub(crate) fn release_shard(&self, shard: &Arc<CacheShard>) {
        self.flush_shard(shard);
        self.thread_shards
            .lock()
            .expect("thread_shards mutex poisoned")
            .retain(|s| !Arc::ptr_eq(s, shard));
    }

    /// Returns all chunks cached in `shard` to the slab layer.
    fn flush_shard(&self, shard: &CacheShard) {
        for (class, mag) in shard.drain_all().into_iter().enumerate() {
            if !mag.is_empty() {
                self.return_chunks_direct(class, mag);
            }
        }
    }

    /// Reclaims cached chunks from all threads' cache shards.
    ///
    /// Called when the pool actually needs memory: a buddy allocation miss
    /// or a newly queued async waiter. Guarantees that cached capacity can
    /// never strand an allocation, no matter which thread it is cached on.
    fn flush_thread_cache(&self) {
        if !self.thread_cache {
            return;
        }
        let shards: Vec<Arc<CacheShard>> = self
            .thread_shards
            .lock()
            .expect("thread_shards mutex poisoned")
            .clone();
        for shard in shards {
            self.flush_shard(&shard);
        }
    }

    /// Releases all cached empty slabs back to the buddy pool.
    /// Returns `true` if any slab was released.
    fn reclaim_empty_slabs(&self) -> bool {
        let mut any = false;
        for class in &self.slab_classes {
            let empties = class
                .lock()
                .expect("SlabClass mutex poisoned")
                .drain_empty();
            any |= !empties.is_empty();
            // Dropping the backing buffers takes the buddy pool mutex; the
            // class lock has already been released.
            drop(empties);
        }
        any
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

/// A queued asynchronous allocation waiting for capacity.
struct Waiter {
    /// When the waiter was queued; used for starvation detection.
    since: Instant,
    /// Channel used to hand a buffer directly to the waiter.
    sender: oneshot::Sender<Buffer>,
}

/// An anti-starvation reservation for a large waiter.
///
/// Created when a waiter for level >= 1 has been queued longer than the
/// configured starvation timeout. At most one reservation exists at a time.
/// The reserved waiter is removed from the waiting lists (its buffer arrives
/// through this reservation, or through the priority claim in
/// [`PoolInner::serve_waiters`]).
struct Reservation {
    /// Channel of the starving waiter.
    sender: oneshot::Sender<Buffer>,
    /// Requested allocation level.
    level: usize,
    /// The subtree being drained for this waiter, if one has been chosen.
    /// `None` ("floating") when no drainable subtree exists yet; the waiter
    /// is then served through the priority claim on public capacity, and an
    /// upgrade to a subtree is retried on subsequent frees.
    subtree: Option<ReservedSubtree>,
}

/// A buddy subtree reserved for a starving waiter.
///
/// Free nodes inside the subtree are *absorbed*: unlinked from the free
/// lists (invisible to regular allocation) while keeping their `Free` state
/// as an "absorbed" marker. Frees inside the subtree are intercepted before
/// the regular merge path. Since live buffers inside are eventually dropped
/// and absorbed capacity can never be re-allocated, `collected` grows
/// monotonically until the subtree is whole — this is the progress
/// guarantee that makes starvation impossible.
struct ReservedSubtree {
    /// The block containing the reserved subtree.
    block: NonNull<BuddyBlock>,
    /// Index of the subtree root within the reservation level.
    root_index: usize,
    /// Bytes absorbed so far; the reservation completes when this reaches
    /// `LEVEL_SIZES[level]`.
    collected: usize,
}

// SAFETY: the NonNull<BuddyBlock> is only dereferenced while holding the
// pool mutex, and the block is owned by (and outlives) the pool.
unsafe impl Send for ReservedSubtree {}

/// Internal pool state protected by the mutex.
struct PoolInner {
    /// Total memory budgeted, in bytes: installed blocks plus any 64 MiB
    /// reservations for blocks currently being created and registered
    /// outside the lock.
    allocated_memory: usize,
    /// Per-level merge watermarks for lazy buddy merging.
    /// See [`BufferPoolBuilder::merge_watermarks`].
    merge_watermarks: [usize; NUM_LEVELS],
    /// All allocated buddy blocks. Each entry is wrapped in `AliasableBox` to
    /// opt out of the `noalias` guarantee that `Box` carries. This is required
    /// because `Buffer` and intrusive free-list nodes hold `NonNull<BuddyBlock>`
    /// pointers that alias the box contents. Without `AliasableBox`, the
    /// compiler could assume exclusive access through the `Box` and mis-optimize
    /// accesses through the raw pointers.
    blocks: Vec<AliasableBox<BuddyBlock>>,
    free_lists: [IntrusiveList<crate::buddy::FreeNodeData>; NUM_LEVELS],
    /// Pending-merge lists for lazy buddy merging, one per level.
    ///
    /// `pending_lists[L]` links parent nodes at level `L` that are in the
    /// [`NodeState::SplitPending`] state: split, with all 4 children free,
    /// but whose merge has been deferred. A `Split` parent's intrusive node
    /// is otherwise unused, so it is reused here at no extra memory cost.
    ///
    /// Invariant: a parent is on `pending_lists[L]` (state `SplitPending`)
    /// if and only if all 4 of its children are free. This makes on-demand
    /// coalescing O(number of merges performed), independent of block count.
    /// Level 0 is unused (leaf nodes have no children).
    pending_lists: [IntrusiveList<crate::buddy::FreeNodeData>; NUM_LEVELS],
    waiting_lists: [VecDeque<Waiter>; NUM_LEVELS],
    min_waiting_level: Option<usize>,
    /// Active anti-starvation reservation, if any. See [`Reservation`].
    reservation: Option<Reservation>,
}

impl PoolInner {
    /// Pushes a node onto the free list at `level`.
    ///
    /// # Safety
    ///
    /// Same contract as [`IntrusiveList::push_front`].
    unsafe fn push_free(&mut self, level: usize, node: NonNull<FreeNode>) {
        unsafe {
            self.free_lists[level].push_front(node);
        }
    }

    /// Pops a node from the free list at `level`.
    fn pop_free(&mut self, level: usize) -> Option<NonNull<FreeNode>> {
        self.free_lists[level].pop_front()
    }

    /// Removes a node from the free list at `level`.
    ///
    /// # Safety
    ///
    /// Same contract as [`IntrusiveList::remove`].
    unsafe fn remove_free(&mut self, level: usize, node: NonNull<FreeNode>) {
        unsafe {
            self.free_lists[level].remove(node);
        }
    }

    /// Reserves a 64 MiB slot of the memory budget for a new block.
    ///
    /// # Errors
    ///
    /// Returns `OutOfMemory` if the reservation would exceed the limit.
    fn try_reserve_block(&mut self, max_memory: usize) -> Result<()> {
        if self.allocated_memory + SIZE_64MIB > max_memory {
            return Err(Error::new(ErrorKind::OutOfMemory, "memory limit reached"));
        }
        self.allocated_memory += SIZE_64MIB;
        Ok(())
    }

    /// Attempts to allocate from local state: free lists first, then
    /// demand-driven coalescing of deferred quads. Does not grow the pool.
    fn try_allocate_local(&mut self, level: usize, pool: &Arc<BufferPool>) -> Option<Buffer> {
        if let Some(buffer) = self.try_allocate_from_free_lists(level, pool) {
            return Some(buffer);
        }

        // Demand-driven coalescing: merging is deferred on free (lazy buddy),
        // so free quads at lower levels may satisfy this allocation once merged.
        if self.coalesce_pending(level) {
            return self.try_allocate_from_free_lists(level, pool);
        }

        // A cancelled reserved waiter may be sitting on absorbed capacity
        // with no future free to notice the cancellation; reclaim it here so
        // allocation misses cannot strand memory indefinitely.
        if self.reclaim_cancelled_reservation() {
            return self.try_allocate_from_free_lists(level, pool);
        }

        None
    }

    /// Releases the reservation if its waiter has been cancelled.
    /// Returns `true` if a reservation was reclaimed.
    fn reclaim_cancelled_reservation(&mut self) -> bool {
        let Some(res) = &self.reservation else {
            return false;
        };
        if !res.sender.is_closed() {
            return false;
        }
        let res = self.reservation.take().unwrap();
        if let Some(sub) = &res.subtree {
            self.release_reservation_subtree(res.level, sub);
        }
        true
    }

    fn try_allocate_from_free_lists(
        &mut self,
        level: usize,
        pool: &Arc<BufferPool>,
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
        pool: &Arc<BufferPool>,
    ) -> Buffer {
        let node = self.pop_free(from_level).unwrap();

        let block = unsafe { (*node.as_ptr()).data.block };
        let index_in_level = unsafe { (*block.as_ptr()).node_index_in_level(node, from_level) };

        let block_ref = unsafe { &mut *block.as_ptr() };

        // Taking this node breaks its (previously complete) buddy quad, so
        // its parent must leave the pending-merge list, if it was on it.
        self.demote_pending_parent(block_ref, from_level, index_in_level);

        if from_level == target_level {
            block_ref.set_state(from_level, index_in_level, NodeState::Allocated);
            let ptr = block_ref.get_memory_addr(from_level, index_in_level);

            unsafe {
                Buffer::new(
                    NonNull::new(ptr).unwrap(),
                    from_level,
                    index_in_level,
                    block,
                    Arc::clone(pool),
                )
            }
        } else {
            self.split_and_allocate(block, from_level, index_in_level, target_level, pool)
        }
    }

    fn split_and_allocate(
        &mut self,
        block: NonNull<BuddyBlock>,
        from_level: usize,
        from_index: usize,
        target_level: usize,
        pool: &Arc<BufferPool>,
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
                    self.push_free(child_level, node);
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
                Arc::clone(pool),
            )
        }
    }

    /// Installs a freshly created block into the pool.
    ///
    /// The memory budget was already reserved by
    /// [`PoolInner::try_reserve_block`] before the block was created; the
    /// caller is expected to allocate its own buffer and then call
    /// [`PoolInner::serve_waiters`] under the same lock acquisition.
    fn install_block(&mut self, block: Box<BuddyBlock>) {
        // Wrap in AliasableBox to allow aliasing via NonNull<BuddyBlock> in
        // Buffer and free-list nodes without violating noalias semantics.
        let block = AliasableBox::from_unique(block);

        let block_ptr = NonNull::new(std::ptr::from_ref::<BuddyBlock>(&block).cast_mut()).unwrap();

        // Add root node to level 3 free list.
        unsafe {
            let node = (*block_ptr.as_ptr()).get_free_node_mut(3, 0);
            self.push_free(3, node);
        }

        self.blocks.push(block);
    }

    pub(crate) fn deallocate_buffer(
        &mut self,
        pool: &Arc<BufferPool>,
        level: usize,
        index: usize,
        block: NonNull<BuddyBlock>,
    ) {
        if !self.try_absorb_into_reservation(pool, level, index, block) {
            let block_ref = unsafe { &mut *block.as_ptr() };
            self.try_merge(block_ref, level, index);
            // Starvation check runs before serving so that a freshly
            // activated reservation can claim this free ahead of smaller
            // waiters.
            self.check_starvation(pool);
        }
        self.serve_waiters(pool);
    }

    /// Hands freed capacity directly to queued waiters.
    ///
    /// A starving reserved waiter (if any) gets first claim on public
    /// capacity at its level. Then, for each waiter (smallest requested size
    /// first), allocates a buffer on its behalf and sends it through the
    /// waiter's channel. This eliminates the wake-then-retry race where
    /// concurrent `allocate()` callers could steal the capacity from a woken
    /// waiter, and satisfies as many waiters as the freed capacity allows in
    /// a single pass (no under-notification).
    ///
    /// Smallest-first is a deliberate policy: it maximizes the number of
    /// satisfied waiters. Large waiters are protected from starvation by the
    /// reservation mechanism (see [`Reservation`]).
    fn serve_waiters(&mut self, pool: &Arc<BufferPool>) {
        self.try_complete_reservation_from_lists(pool);

        while let Some(wait_level) = self.min_waiting_level {
            let Some(buffer) = self.try_allocate_local(wait_level, pool) else {
                break;
            };

            let waiter = self.waiting_lists[wait_level]
                .pop_front()
                .expect("waiting list at min_waiting_level must be non-empty");
            self.update_min_waiting_level_on_remove(wait_level);

            if let Err(buffer) = waiter.sender.send(buffer) {
                // The receiver was dropped (waiter cancelled). Reclaim the
                // buffer without running Buffer::drop, which would deadlock
                // by re-locking the pool mutex we are already holding.
                let (level, index, block) = buffer.into_raw_parts();
                let block_ref = unsafe { &mut *block.as_ptr() };
                self.try_merge(block_ref, level, index);
                // Keep serving: the reclaimed capacity may satisfy the next
                // waiter in line.
            }
        }

        // Refresh the demand hint for the slab layer and thread caches.
        pool.has_demand.store(
            self.min_waiting_level.is_some() || self.reservation.is_some(),
            Ordering::SeqCst,
        );
    }

    /// Returns `true` if `(level, index)` lies strictly inside the subtree
    /// rooted at `root_index` of `res_level`.
    const fn subtree_contains(
        res_level: usize,
        root_index: usize,
        level: usize,
        index: usize,
    ) -> bool {
        level < res_level && (index >> (2 * (res_level - level))) == root_index
    }

    /// Intercepts a free that falls inside the reserved subtree.
    ///
    /// The freed node is absorbed: its state is set to `Free` but it is NOT
    /// pushed onto the free lists, making it invisible to regular allocation.
    /// Returns `false` if there is no matching reservation (the caller then
    /// runs the regular merge path).
    fn try_absorb_into_reservation(
        &mut self,
        pool: &Arc<BufferPool>,
        level: usize,
        index: usize,
        block: NonNull<BuddyBlock>,
    ) -> bool {
        let Some(res) = &mut self.reservation else {
            return false;
        };
        let Some(sub) = &mut res.subtree else {
            return false;
        };
        if sub.block != block || !Self::subtree_contains(res.level, sub.root_index, level, index) {
            return false;
        }

        if res.sender.is_closed() {
            // The reserved waiter was cancelled: mark this node absorbed as
            // well, then release the whole subtree in one pass. (Freeing the
            // node separately first would double-process it during release.)
            let res = self.reservation.take().unwrap();
            let block_ref = unsafe { &mut *block.as_ptr() };
            block_ref.set_state(level, index, NodeState::Free);
            self.release_reservation_subtree(res.level, &res.subtree.unwrap());
            return true;
        }

        let block_ref = unsafe { &mut *block.as_ptr() };
        debug_assert_eq!(block_ref.get_state(level, index), NodeState::Allocated);
        block_ref.set_state(level, index, NodeState::Free); // absorbed marker
        sub.collected += LEVEL_SIZES[level];
        self.try_finalize_reservation(pool);
        true
    }

    /// Checks for starving large waiters and manages reservation activation.
    ///
    /// If a reservation is already active but floating (no subtree), retries
    /// the subtree upgrade. Otherwise, if the oldest waiter at some level of
    /// at least 1 (largest level first) has been queued longer than the
    /// starvation timeout, removes it from the waiting lists and activates a
    /// reservation for it.
    fn check_starvation(&mut self, pool: &Arc<BufferPool>) {
        if let Some(res) = &self.reservation {
            if res.subtree.is_some() {
                return;
            }
            // Try to upgrade a floating reservation to a drainable subtree.
            let level = res.level;
            if let Some(subtree) = self.reserve_subtree(level) {
                self.reservation.as_mut().unwrap().subtree = Some(subtree);
                self.try_finalize_reservation(pool);
            }
            return;
        }

        if self.min_waiting_level.is_none() {
            return;
        }
        let now = Instant::now();
        // Largest level first: big requests are the starvation victims.
        for level in (1..NUM_LEVELS).rev() {
            let Some(waiter) = self.waiting_lists[level].front() else {
                continue;
            };
            if now.duration_since(waiter.since) < pool.starvation_timeout {
                continue;
            }

            let waiter = self.waiting_lists[level].pop_front().unwrap();
            self.update_min_waiting_level_on_remove(level);
            let subtree = self.reserve_subtree(level);
            self.reservation = Some(Reservation {
                sender: waiter.sender,
                level,
                subtree,
            });
            self.try_finalize_reservation(pool);
            return;
        }
    }

    /// Chooses and claims the best subtree for a reservation at `level`:
    /// the `Split` node at that level with the most free bytes inside
    /// (maximum head start). Absorbs its current free nodes.
    ///
    /// Returns `None` if no `Split` node exists at that level, in which case
    /// capacity can only arrive as whole nodes of >= `level`, which the
    /// priority claim in [`Self::serve_waiters`] picks up directly.
    fn reserve_subtree(&mut self, level: usize) -> Option<ReservedSubtree> {
        let mut best: Option<(NonNull<BuddyBlock>, usize, usize)> = None;

        for i in 0..self.blocks.len() {
            let block_ptr =
                NonNull::new(std::ptr::from_ref::<BuddyBlock>(&self.blocks[i]).cast_mut()).unwrap();
            let block_ref = unsafe { &*block_ptr.as_ptr() };

            for root_index in 0..NODES_PER_LEVEL[level] {
                if block_ref.get_state(level, root_index) != NodeState::Split {
                    continue;
                }
                let mut free_bytes = 0;
                for (child_level, &child_size) in LEVEL_SIZES.iter().enumerate().take(level) {
                    let first = root_index << (2 * (level - child_level));
                    let count = 1usize << (2 * (level - child_level));
                    for index in first..first + count {
                        if block_ref.get_state(child_level, index) == NodeState::Free {
                            free_bytes += child_size;
                        }
                    }
                }
                if best.is_none_or(|(_, _, bytes)| free_bytes > bytes) {
                    best = Some((block_ptr, root_index, free_bytes));
                }
            }
        }

        let (block, root_index, _) = best?;
        let collected = self.absorb_subtree_free_nodes(block, level, root_index);
        Some(ReservedSubtree {
            block,
            root_index,
            collected,
        })
    }

    /// Absorbs all currently free nodes inside the subtree: they are removed
    /// from the free lists (and their parents from the pending lists) but
    /// keep their `Free` state as the "absorbed" marker. Returns the number
    /// of bytes absorbed.
    fn absorb_subtree_free_nodes(
        &mut self,
        block: NonNull<BuddyBlock>,
        res_level: usize,
        root_index: usize,
    ) -> usize {
        let block_ref = unsafe { &mut *block.as_ptr() };
        let mut collected = 0;

        for (level, &size) in LEVEL_SIZES.iter().enumerate().take(res_level) {
            let first = root_index << (2 * (res_level - level));
            let count = 1usize << (2 * (res_level - level));
            for index in first..first + count {
                if block_ref.get_state(level, index) == NodeState::Free {
                    self.demote_pending_parent(block_ref, level, index);
                    let node = block_ref.get_free_node_mut(level, index);
                    unsafe {
                        self.remove_free(level, node);
                    }
                    collected += size;
                }
            }
        }

        collected
    }

    /// Completes the reservation if its subtree has been fully collected:
    /// the whole subtree is handed to the waiter as a single buffer.
    fn try_finalize_reservation(&mut self, pool: &Arc<BufferPool>) {
        let Some(res) = &self.reservation else {
            return;
        };
        let Some(sub) = &res.subtree else {
            return;
        };
        debug_assert!(sub.collected <= LEVEL_SIZES[res.level]);
        if sub.collected < LEVEL_SIZES[res.level] {
            return;
        }

        let res = self.reservation.take().unwrap();
        let sub = res.subtree.unwrap();
        let block_ref = unsafe { &mut *sub.block.as_ptr() };

        // Restore the state invariant for an allocated node: all strict
        // descendants must read Allocated (clears absorbed markers and stale
        // Split states alike).
        for level in 0..res.level {
            let first = sub.root_index << (2 * (res.level - level));
            let count = 1usize << (2 * (res.level - level));
            for index in first..first + count {
                block_ref.set_state(level, index, NodeState::Allocated);
            }
        }
        block_ref.set_state(res.level, sub.root_index, NodeState::Allocated);

        let ptr = block_ref.get_memory_addr(res.level, sub.root_index);
        let buffer = unsafe {
            Buffer::new(
                NonNull::new(ptr).unwrap(),
                res.level,
                sub.root_index,
                sub.block,
                Arc::clone(pool),
            )
        };
        if let Err(buffer) = res.sender.send(buffer) {
            // Cancelled at the finish line: reclaim the whole node.
            let (level, index, block) = buffer.into_raw_parts();
            let block_ref = unsafe { &mut *block.as_ptr() };
            self.try_merge(block_ref, level, index);
        }
    }

    /// Priority claim for the reserved waiter: if public capacity at its
    /// level exists (or can be coalesced), complete the reservation from it
    /// and release any partially drained subtree back to the pool.
    ///
    /// Also detects a cancelled reserved waiter and releases its subtree.
    fn try_complete_reservation_from_lists(&mut self, pool: &Arc<BufferPool>) {
        if self.reclaim_cancelled_reservation() {
            return;
        }
        let Some(res) = &self.reservation else {
            return;
        };

        let level = res.level;
        let Some(buffer) = self.try_allocate_local(level, pool) else {
            return;
        };

        let res = self.reservation.take().unwrap();
        if let Some(sub) = &res.subtree {
            self.release_reservation_subtree(res.level, sub);
        }
        if let Err(buffer) = res.sender.send(buffer) {
            let (level, index, block) = buffer.into_raw_parts();
            let block_ref = unsafe { &mut *block.as_ptr() };
            self.try_merge(block_ref, level, index);
        }
    }

    /// Returns all absorbed capacity of a released reservation to the pool.
    ///
    /// Absorbed markers must all be cleared to `Allocated` *before* any node
    /// is re-freed: `try_merge` inspects sibling states, and an absorbed
    /// sibling still marked `Free` is not on any list, so merging with it
    /// would corrupt the free lists.
    fn release_reservation_subtree(&mut self, res_level: usize, sub: &ReservedSubtree) {
        let block_ref = unsafe { &mut *sub.block.as_ptr() };

        let mut absorbed = Vec::new();
        for level in 0..res_level {
            let first = sub.root_index << (2 * (res_level - level));
            let count = 1usize << (2 * (res_level - level));
            for index in first..first + count {
                if block_ref.get_state(level, index) == NodeState::Free {
                    block_ref.set_state(level, index, NodeState::Allocated);
                    absorbed.push((level, index));
                }
            }
        }

        for (level, index) in absorbed {
            let block_ref = unsafe { &mut *sub.block.as_ptr() };
            self.try_merge(block_ref, level, index);
        }
    }

    /// Returns `true` if a node freed at `level` should be merged upward.
    ///
    /// Lazy buddy policy: merge only if the free list at this level already
    /// holds enough nodes (watermark reached), or a larger allocation is
    /// currently waiting (demand-driven). Otherwise keep the node at its
    /// level so that same-size reallocation avoids a split/merge round trip.
    fn should_merge(&self, level: usize) -> bool {
        if self.free_lists[level].len() >= self.merge_watermarks[level] {
            return true;
        }
        ((level + 1)..NUM_LEVELS).any(|l| !self.waiting_lists[l].is_empty())
    }

    fn try_merge(&mut self, block: &mut BuddyBlock, level: usize, index: usize) {
        let mut current_level = level;
        let mut current_index = index;

        loop {
            if current_level >= 3 {
                block.set_state(current_level, current_index, NodeState::Free);
                let node = block.get_free_node_mut(current_level, current_index);
                unsafe {
                    self.push_free(current_level, node);
                }
                return;
            }

            let siblings = BuddyBlock::get_siblings(current_index);

            // Note: `current_index` itself is not yet marked Free, so its
            // parent cannot already be SplitPending at this point.
            let quad_complete = siblings.iter().all(|&idx| {
                idx == current_index || block.get_state(current_level, idx) == NodeState::Free
            });

            if quad_complete && self.should_merge(current_level) {
                // Eager merge: remove siblings from free list and ascend.
                for &sibling_idx in &siblings {
                    if sibling_idx != current_index {
                        let node = block.get_free_node_mut(current_level, sibling_idx);
                        unsafe {
                            self.remove_free(current_level, node);
                        }
                    }
                    block.set_state(current_level, sibling_idx, NodeState::Allocated);
                }

                let (parent_level, parent_index) =
                    BuddyBlock::get_parent(current_level, current_index).unwrap();
                current_level = parent_level;
                current_index = parent_index;
                continue;
            }

            // Stop here: the freed node stays at this level.
            block.set_state(current_level, current_index, NodeState::Free);
            let node = block.get_free_node_mut(current_level, current_index);
            unsafe {
                self.push_free(current_level, node);
            }

            if quad_complete {
                // Deferred merge (lazy buddy): record the parent on the
                // pending-merge list so coalescing later is O(1) per quad.
                let (parent_level, parent_index) =
                    BuddyBlock::get_parent(current_level, current_index).unwrap();
                debug_assert_eq!(
                    block.get_state(parent_level, parent_index),
                    NodeState::Split
                );
                block.set_state(parent_level, parent_index, NodeState::SplitPending);
                let parent_node = block.get_free_node_mut(parent_level, parent_index);
                unsafe {
                    self.pending_lists[parent_level].push_front(parent_node);
                }
            }

            return;
        }
    }

    /// Removes the parent of `(level, index)` from the pending-merge list if
    /// it was there. Must be called whenever a free node is taken out of its
    /// free list for allocation, since that breaks the complete buddy quad.
    fn demote_pending_parent(&mut self, block: &mut BuddyBlock, level: usize, index: usize) {
        if let Some((parent_level, parent_index)) = BuddyBlock::get_parent(level, index)
            && block.get_state(parent_level, parent_index) == NodeState::SplitPending
        {
            let parent_node = block.get_free_node_mut(parent_level, parent_index);
            unsafe {
                self.pending_lists[parent_level].remove(parent_node);
            }
            block.set_state(parent_level, parent_index, NodeState::Split);
        }
    }

    /// Merges deferred buddy quads from the pending-merge lists, bottom-up,
    /// until a free node at `target_level` is produced (or nothing is left).
    ///
    /// This is the demand-driven counterpart of lazy merging in [`Self::try_merge`]:
    /// it is invoked when an allocation cannot be satisfied from the free
    /// lists, before growing the pool with a new 64 MiB block.
    ///
    /// Complexity is O(number of merges actually performed) — independent of
    /// the number of blocks — since complete quads are tracked incrementally
    /// on the pending lists. Each merge is amortized against the O(1) work
    /// that deferred it, and the loop exits as soon as the target level gains
    /// a free node. Returns `true` if at least one merge was performed.
    fn coalesce_pending(&mut self, target_level: usize) -> bool {
        let mut merged_any = false;

        // Bottom-up: a merge at level L may complete a quad at level L + 1,
        // which is then picked up by a later iteration. Merging above
        // `target_level` cannot help the current allocation, so skip it.
        'levels: for parent_level in 1..=target_level {
            loop {
                if !self.free_lists[target_level].is_empty() {
                    break 'levels; // Target satisfied, keep the rest deferred.
                }
                let Some(parent_node) = self.pending_lists[parent_level].pop_front() else {
                    break;
                };

                let block = unsafe { (*parent_node.as_ptr()).data.block };
                // SAFETY: the block outlives its nodes and we hold the pool mutex.
                let block_ref = unsafe { &mut *block.as_ptr() };
                let parent_index = block_ref.node_index_in_level(parent_node, parent_level);
                debug_assert_eq!(
                    block_ref.get_state(parent_level, parent_index),
                    NodeState::SplitPending
                );

                // Merge the quad: all 4 children are free by invariant.
                let (child_level, first_child) =
                    BuddyBlock::get_first_child(parent_level, parent_index).unwrap();
                for k in 0..4 {
                    debug_assert_eq!(
                        block_ref.get_state(child_level, first_child + k),
                        NodeState::Free
                    );
                    let node = block_ref.get_free_node_mut(child_level, first_child + k);
                    unsafe {
                        self.remove_free(child_level, node);
                    }
                    block_ref.set_state(child_level, first_child + k, NodeState::Allocated);
                }

                block_ref.set_state(parent_level, parent_index, NodeState::Free);
                let node = block_ref.get_free_node_mut(parent_level, parent_index);
                unsafe {
                    self.push_free(parent_level, node);
                }
                merged_any = true;

                // Cascade: the merged node may complete its own quad.
                if parent_level < 3 {
                    let siblings = BuddyBlock::get_siblings(parent_index);
                    let quad_complete = siblings
                        .iter()
                        .all(|&idx| block_ref.get_state(parent_level, idx) == NodeState::Free);
                    if quad_complete {
                        let (gp_level, gp_index) =
                            BuddyBlock::get_parent(parent_level, parent_index).unwrap();
                        debug_assert_eq!(block_ref.get_state(gp_level, gp_index), NodeState::Split);
                        block_ref.set_state(gp_level, gp_index, NodeState::SplitPending);
                        let gp_node = block_ref.get_free_node_mut(gp_level, gp_index);
                        unsafe {
                            self.pending_lists[gp_level].push_front(gp_node);
                        }
                    }
                }
            }
        }

        merged_any
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

        // Small sizes are served by the slab layer.
        let s1 = pool.allocate(1).unwrap();
        assert_eq!(s1.len(), 16 * 1024);

        let s1b = pool.allocate(16 * 1024 + 1).unwrap();
        assert_eq!(s1b.len(), 64 * 1024);

        let s2 = pool.allocate(64 * 1024).unwrap();
        assert_eq!(s2.len(), 64 * 1024);

        let s3 = pool.allocate(64 * 1024 + 1).unwrap();
        assert_eq!(s3.len(), 256 * 1024);

        let s4 = pool.allocate(256 * 1024).unwrap();
        assert_eq!(s4.len(), 256 * 1024);

        // Larger sizes go to the buddy allocator.
        let b1 = pool.allocate(256 * 1024 + 1).unwrap();
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
    fn test_lazy_merge_keeps_nodes_below_watermark() {
        // High watermarks: merging on free is always deferred.
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .merge_watermarks([64, 16, 4, 0])
            .build();

        let buffer = pool.allocate(SIZE_1MIB).unwrap();
        // Splitting 64MiB down to 1MiB leaves 3 free nodes at each level.
        assert_eq!(pool.free_counts(), [3, 3, 3, 0]);
        assert_eq!(pool.pending_counts(), [0, 0, 0, 0]);

        drop(buffer);
        // Lazy merge: the freed node stays at level 0 instead of merging
        // back up to the root; its parent is tracked as a pending quad.
        assert_eq!(pool.free_counts(), [4, 3, 3, 0]);
        assert_eq!(pool.pending_counts(), [0, 1, 0, 0]);

        // Same-size reallocation is served without any split, and the
        // broken quad leaves the pending list.
        let _buffer = pool.allocate(SIZE_1MIB).unwrap();
        assert_eq!(pool.free_counts(), [3, 3, 3, 0]);
        assert_eq!(pool.pending_counts(), [0, 0, 0, 0]);
    }

    #[test]
    fn test_zero_watermarks_restore_eager_merge() {
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .merge_watermarks([0, 0, 0, 0])
            .build();

        let buffer = pool.allocate(SIZE_1MIB).unwrap();
        assert_eq!(pool.free_counts(), [3, 3, 3, 0]);

        drop(buffer);
        // Eager merge: everything coalesces back to the 64MiB root.
        assert_eq!(pool.free_counts(), [0, 0, 0, 1]);
    }

    #[test]
    fn test_demand_driven_coalescing_on_alloc() {
        // Max memory allows two blocks, so this also verifies that deferred
        // quads are coalesced and reused instead of growing the pool.
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB * 2)
            .merge_watermarks([64, 16, 4, 0])
            .build();

        let buffer = pool.allocate(SIZE_1MIB).unwrap();
        drop(buffer);
        assert_eq!(pool.free_counts(), [4, 3, 3, 0]);
        assert_eq!(pool.pending_counts(), [0, 1, 0, 0]);
        assert_eq!(pool.allocated_memory(), SIZE_64MIB);

        // No free 64MiB node exists; demand-driven coalescing must rebuild
        // the root from the pending quads (cascading up all three levels)
        // rather than allocating a second block.
        let big = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(big.len(), SIZE_64MIB);
        assert_eq!(pool.allocated_memory(), SIZE_64MIB);
        assert_eq!(pool.free_counts(), [0, 0, 0, 0]);
        assert_eq!(pool.pending_counts(), [0, 0, 0, 0]);
    }

    #[test]
    fn test_coalescing_is_minimal_and_on_demand() {
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .merge_watermarks([64, 16, 4, 0])
            .build();

        // Occupy four full level-1 quads with 1MiB buffers and drain level 2,
        // so that only deferred level-0 nodes remain after dropping.
        let small: Vec<_> = (0..16).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
        let _large: Vec<_> = (0..3)
            .map(|_| pool.allocate(LEVEL_SIZES[2]).unwrap())
            .collect();
        assert_eq!(pool.free_counts(), [0, 0, 0, 0]);

        drop(small);
        // Lazy merge keeps all 16 freed nodes at level 0; the 4 complete
        // quads are tracked on the pending list.
        assert_eq!(pool.free_counts(), [16, 0, 0, 0]);
        assert_eq!(pool.pending_counts(), [0, 4, 0, 0]);

        // A 4MiB allocation cannot be served directly; demand-driven
        // coalescing merges exactly ONE pending quad (early exit) and keeps
        // the rest deferred at level 0 for future small allocations.
        let b = pool.allocate(LEVEL_SIZES[1]).unwrap();
        assert_eq!(b.len(), LEVEL_SIZES[1]);
        assert_eq!(pool.free_counts(), [12, 0, 0, 0]);
        assert_eq!(pool.pending_counts(), [0, 3, 0, 0]);
        assert_eq!(pool.allocated_memory(), SIZE_64MIB);

        // Each further 4MiB demand consumes exactly one more pending quad.
        let b2 = pool.allocate(LEVEL_SIZES[1]).unwrap();
        assert_eq!(b2.len(), LEVEL_SIZES[1]);
        assert_eq!(pool.free_counts(), [8, 0, 0, 0]);
        assert_eq!(pool.pending_counts(), [0, 2, 0, 0]);
    }

    #[tokio::test]
    async fn test_waiter_triggers_merge_on_free() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .merge_watermarks([64, 16, 4, 0])
            .build();

        let buffers: Vec<_> = (0..4)
            .map(|_| pool.allocate(LEVEL_SIZES[2]).unwrap())
            .collect();

        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move { pool_clone.async_allocate(SIZE_64MIB).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Freeing with a 64MiB waiter present must force merging despite the
        // high watermarks (demand-driven path in should_merge).
        drop(buffers);

        let result = timeout(Duration::from_secs(1), handle).await;
        let buffer = result.unwrap().unwrap().unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
    }

    #[test]
    fn test_lazy_merge_randomized_stress() {
        // Random alloc/free mix across all levels; debug_asserts in
        // try_merge/coalesce_pending/demote_pending_parent verify the
        // pending-list invariant on every transition.
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB * 4)
            .merge_watermarks([4, 2, 1, 0])
            .build();

        let mut held: Vec<Buffer> = Vec::new();
        let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (rng >> 33) as usize
        };

        for _ in 0..10_000 {
            let r = next();
            if r % 100 < 60 || held.is_empty() {
                let level = [0, 0, 0, 1, 1, 2, 3][next() % 7];
                if let Ok(buffer) = pool.allocate(LEVEL_SIZES[level]) {
                    held.push(buffer);
                }
            } else {
                held.swap_remove(next() % held.len());
            }
        }

        drop(held);
        // Full drain: everything must be reachable again via coalescing.
        let all: Vec<_> = (0..4).map(|_| pool.allocate(SIZE_64MIB).unwrap()).collect();
        assert_eq!(all.len(), 4);
        assert_eq!(pool.free_counts(), [0, 0, 0, 0]);
        assert_eq!(pool.pending_counts(), [0, 0, 0, 0]);
    }

    #[test]
    fn test_concurrent_allocation_and_growth() {
        // Many threads allocate and free concurrently, forcing concurrent
        // pool growth (block creation happens outside the pool mutex).
        let pool = test_pool_with_max(SIZE_64MIB * 16);
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let pool = pool.clone();
                std::thread::spawn(move || {
                    let mut held = Vec::new();
                    for i in 0..500 {
                        let level = (t + i) % 3;
                        match pool.allocate(LEVEL_SIZES[level]) {
                            Ok(buffer) => held.push(buffer),
                            Err(e) => assert_eq!(e.kind(), ErrorKind::OutOfMemory),
                        }
                        if i % 3 == 0 {
                            held.clear();
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        // Budget must never be exceeded, even with racing growers.
        assert!(pool.allocated_memory() <= pool.max_memory());

        // With everything freed, the pool must be fully recoverable.
        let blocks = pool.allocated_memory() / SIZE_64MIB;
        let all: Vec<_> = (0..blocks)
            .map(|_| pool.allocate(SIZE_64MIB).unwrap())
            .collect();
        assert_eq!(all.len(), blocks);
    }

    #[tokio::test]
    async fn test_handoff_serves_multiple_waiters_from_one_free() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = test_pool_with_max(SIZE_64MIB);
        let held = pool.allocate(SIZE_64MIB).unwrap();

        // Queue 4 waiters for 16MiB each; the pool is exhausted.
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let pool = pool.clone();
                tokio::spawn(async move { pool.async_allocate(LEVEL_SIZES[2]).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;

        // A single free must hand capacity to ALL of them (the old
        // notify-one design would wake only one and strand the rest).
        drop(held);

        for handle in handles {
            let buffer = timeout(Duration::from_secs(1), handle)
                .await
                .expect("waiter starved: free did not serve all waiters")
                .unwrap()
                .unwrap();
            assert_eq!(buffer.len(), LEVEL_SIZES[2]);
        }
    }

    #[tokio::test]
    async fn test_handoff_cancelled_waiter_returns_buffer() {
        use std::time::Duration;

        let pool = test_pool_with_max(SIZE_64MIB);
        let held = pool.allocate(SIZE_64MIB).unwrap();

        // Register a waiter, then cancel it before capacity arrives.
        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(SIZE_1MIB).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        waiter.abort();
        tokio::time::sleep(Duration::from_millis(20)).await;

        // The free hands a buffer to the cancelled waiter; the failed send
        // must reclaim it into the pool instead of leaking it.
        drop(held);

        // The full 64MiB must be recoverable again.
        let buffer = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
    }

    #[tokio::test]
    async fn test_handoff_mixed_sizes_smallest_first() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = test_pool_with_max(SIZE_64MIB);
        let held = pool.allocate(SIZE_64MIB).unwrap();

        // One large and several small waiters; one 64MiB free fits them all.
        let big = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(LEVEL_SIZES[2]).await })
        };
        let smalls: Vec<_> = (0..3)
            .map(|_| {
                let pool = pool.clone();
                tokio::spawn(async move { pool.async_allocate(SIZE_1MIB).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(held);

        for handle in smalls.into_iter().chain(std::iter::once(big)) {
            let buffer = timeout(Duration::from_secs(1), handle)
                .await
                .expect("waiter starved")
                .unwrap()
                .unwrap();
            assert!(!buffer.is_empty());
        }
    }

    #[tokio::test]
    async fn test_starving_waiter_reservation_drains_and_blocks_theft() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .starvation_timeout(Duration::from_millis(10))
            .build();

        // Fill the pool with 64 small buffers, then queue a 64MiB waiter.
        let mut held: Vec<_> = (0..64).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The first free after the timeout activates the reservation.
        held.pop();
        // From now on, freed capacity is absorbed by the reservation:
        // fast-path allocations must NOT be able to steal it. (Without the
        // reservation this allocate would succeed and the waiter would
        // starve for as long as the churn continues.)
        assert!(pool.allocate(SIZE_1MIB).is_err());

        // Drain the rest; every free flows to the reserved waiter.
        held.clear();
        let buffer = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("64MiB waiter starved despite reservation")
            .unwrap()
            .unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
    }

    #[tokio::test]
    async fn test_starving_waiter_priority_over_later_small_waiters() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .starvation_timeout(Duration::from_millis(10))
            .build();

        let held = pool.allocate(SIZE_64MIB).unwrap();

        // A large waiter queues first and ages past the timeout...
        let big = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;

        // ...then small waiters arrive. Under plain smallest-first they
        // would carve up the freed 64MiB and strand the large waiter.
        let smalls: Vec<_> = (0..3)
            .map(|_| {
                let pool = pool.clone();
                tokio::spawn(async move { pool.async_allocate(SIZE_1MIB).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(held);

        // The aged large waiter must win the freed 64MiB.
        let big_buffer = timeout(Duration::from_secs(1), big)
            .await
            .expect("aged 64MiB waiter lost to later small waiters")
            .unwrap()
            .unwrap();
        assert_eq!(big_buffer.len(), SIZE_64MIB);

        // Releasing it then serves the small waiters normally.
        drop(big_buffer);
        for handle in smalls {
            let buffer = timeout(Duration::from_secs(1), handle)
                .await
                .expect("small waiter starved")
                .unwrap()
                .unwrap();
            assert_eq!(buffer.len(), SIZE_1MIB);
        }
    }

    #[tokio::test]
    async fn test_cancelled_reserved_waiter_releases_absorbed_capacity() {
        use std::time::Duration;

        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .starvation_timeout(Duration::from_millis(10))
            .build();

        let mut held: Vec<_> = (0..64).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Activate the reservation and drain part of the pool into it.
        for _ in 0..16 {
            held.pop();
        }
        assert!(pool.allocate(SIZE_1MIB).is_err());

        // Cancel the reserved waiter; the next free detects the cancellation
        // and releases all absorbed capacity back to the pool.
        waiter.abort();
        tokio::time::sleep(Duration::from_millis(20)).await;
        held.pop();

        let reclaimed = pool.allocate(SIZE_1MIB);
        assert!(
            reclaimed.is_ok(),
            "absorbed capacity leaked after waiter cancellation"
        );
        drop(reclaimed);

        // Full recovery: everything must merge back into one 64MiB node.
        held.clear();
        let buffer = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_starvation_reservation_randomized_stress() {
        use std::time::Duration;

        // Small pool + tiny starvation timeout + aggressive request timeouts:
        // exercises reservation activation, drain, priority completion and
        // cancellation-release concurrently. The debug_asserts in the merge,
        // pending and reservation paths check the invariants throughout.
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB * 2)
            .starvation_timeout(Duration::from_millis(5))
            .build();

        let tasks: Vec<_> = (0..8u64)
            .map(|t| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    let mut rng: u64 = t * 7919 + 12345;
                    let mut next = move || {
                        rng = rng
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        (rng >> 33) as usize
                    };
                    const SIZES: [usize; 9] = [
                        64 * 1024,
                        64 * 1024,
                        256 * 1024,
                        LEVEL_SIZES[0],
                        LEVEL_SIZES[0],
                        LEVEL_SIZES[1],
                        LEVEL_SIZES[1],
                        LEVEL_SIZES[2],
                        LEVEL_SIZES[3],
                    ];
                    for _ in 0..200 {
                        let size = SIZES[next() % SIZES.len()];
                        let wait_ms = (next() % 25) as u64;
                        let result = tokio::time::timeout(
                            Duration::from_millis(wait_ms),
                            pool.async_allocate(size),
                        )
                        .await;
                        if let Ok(Ok(buffer)) = result {
                            if next() % 3 == 0 {
                                tokio::task::yield_now().await;
                            }
                            drop(buffer);
                        }
                        // Timed-out requests exercise waiter/reservation
                        // cancellation.
                    }
                })
            })
            .collect();

        for task in tasks {
            task.await.unwrap();
        }

        // Full drain: no capacity may be stranded in reservations or lists.
        let a = pool.allocate(SIZE_64MIB).unwrap();
        let b = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(a.len() + b.len(), 2 * SIZE_64MIB);
    }

    #[test]
    fn test_chunks_share_slab() {
        const KIB64: usize = 64 * 1024;
        // Exact slab accounting: keep chunks out of the per-thread cache.
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .thread_cache(false)
            .build();

        // 16 x 64KiB fit in one slab (one 1MiB buddy leaf).
        let chunks: Vec<_> = (0..16).map(|_| pool.allocate(KIB64).unwrap()).collect();
        assert_eq!(pool.allocated_memory(), SIZE_64MIB);

        let base = chunks[0].as_ptr() as usize & !(SIZE_1MIB - 1);
        let mut addrs: Vec<usize> = chunks.iter().map(|c| c.as_ptr() as usize).collect();
        addrs.sort_unstable();
        addrs.dedup();
        assert_eq!(addrs.len(), 16, "chunks must not overlap");
        for &addr in &addrs {
            assert_eq!(addr & !(SIZE_1MIB - 1), base, "chunks must share one slab");
            assert_eq!(addr % KIB64, 0, "chunks must be 64KiB-aligned");
        }
        assert_eq!(pool.slab_free_counts(), [0, 0, 0]);

        // The 17th chunk opens a second slab.
        let extra = pool.allocate(KIB64).unwrap();
        assert_ne!(extra.as_ptr() as usize & !(SIZE_1MIB - 1), base);
        assert_eq!(pool.slab_free_counts(), [0, 15, 0]);
    }

    #[test]
    fn test_slab_release_beyond_watermark() {
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .slab_empty_watermark(0)
            .thread_cache(false)
            .build();

        let chunk = pool.allocate(64 * 1024).unwrap();
        drop(chunk);

        // Watermark 0: the empty slab returns to the buddy pool at once,
        // and the whole 64MiB is recoverable.
        assert_eq!(pool.slab_free_counts(), [0, 0, 0]);
        let buffer = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
    }

    #[test]
    fn test_slab_watermark_caches_empty_slab() {
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .slab_empty_watermark(1)
            .thread_cache(false)
            .build();

        let chunk = pool.allocate(64 * 1024).unwrap();
        let buddy_free = pool.free_counts();
        drop(chunk);

        // The empty slab stays cached: buddy free lists are untouched and
        // all 16 chunks are available for reuse without a refill.
        assert_eq!(pool.slab_free_counts(), [0, 16, 0]);
        assert_eq!(pool.free_counts(), buddy_free);

        let chunk = pool.allocate(64 * 1024).unwrap();
        assert_eq!(pool.slab_free_counts(), [0, 15, 0]);
        assert_eq!(pool.free_counts(), buddy_free);
        drop(chunk);
    }

    #[test]
    fn test_buddy_miss_reclaims_cached_slabs() {
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .slab_empty_watermark(8)
            .thread_cache(false)
            .build();

        let chunk = pool.allocate(64 * 1024).unwrap();
        drop(chunk);
        assert_eq!(pool.slab_free_counts(), [0, 16, 0]);

        // The 64MiB allocation misses the buddy free lists; the reclaim
        // hook must release the cached empty slab instead of failing.
        let buffer = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
        assert_eq!(pool.slab_free_counts(), [0, 0, 0]);
    }

    #[tokio::test]
    async fn test_demand_releases_cached_slab_for_waiter() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .max_memory(SIZE_64MIB)
            .slab_empty_watermark(8)
            .build();

        // 63 x 1MiB + one slab (16 x 64KiB) fill the whole pool.
        let bufs: Vec<_> = (0..63).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
        let chunks: Vec<_> = (0..16).map(|_| pool.allocate(64 * 1024).unwrap()).collect();

        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(bufs);
        // The last MiB is held by the slab. Freeing its chunks must release
        // the (now empty) slab immediately — demand overrides the watermark —
        // so the waiter can complete.
        drop(chunks);

        let buffer = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter starved: cached empty slab was not released")
            .unwrap()
            .unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
    }

    #[tokio::test]
    async fn test_async_small_allocation_waits_and_completes() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = test_pool_with_max(SIZE_64MIB);
        let held = pool.allocate(SIZE_64MIB).unwrap();

        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(64 * 1024).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(held);

        let buffer = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("small waiter starved")
            .unwrap()
            .unwrap();
        assert_eq!(buffer.len(), 64 * 1024);
    }

    #[test]
    fn test_chunk_write_read_and_drop() {
        let pool = test_pool();
        let mut chunk = pool.allocate(100).unwrap();
        assert_eq!(chunk.capacity(), 16 * 1024);

        chunk.set_len(0);
        chunk.extend_from_slice(&[0xAB; 128]).unwrap();
        assert_eq!(chunk.len(), 128);
        assert!(chunk.iter().all(|&b| b == 0xAB));

        // Overflowing the (now smaller) capacity must fail cleanly.
        chunk.set_len(16 * 1024);
        assert!(chunk.extend_from_slice(&[0u8; 1]).is_err());
    }

    #[test]
    fn test_stats_consistency() {
        let pool = test_pool_with_max(SIZE_64MIB);
        assert_eq!(pool.allocated_memory(), 0);
        assert_eq!(pool.free_counts(), [0, 0, 0, 0]);

        let buffer = pool.allocate(SIZE_1MIB).unwrap();
        assert_eq!(pool.allocated_memory(), SIZE_64MIB);
        // Atomic counts must mirror the free lists exactly when quiescent.
        assert_eq!(pool.free_counts(), [3, 3, 3, 0]);

        drop(buffer);
        let counts = pool.free_counts();
        let free_bytes: usize = counts
            .iter()
            .zip(LEVEL_SIZES.iter())
            .map(|(c, s)| c * s)
            .sum();
        assert_eq!(free_bytes, SIZE_64MIB);
    }

    #[test]
    fn test_thread_cache_roundtrip() {
        let pool = test_pool_with_max(SIZE_64MIB);

        // Freed chunks go to the thread cache and come back on allocation.
        let addr = {
            let chunk = pool.allocate(64 * 1024).unwrap();
            chunk.as_ptr() as usize
        };
        let chunk = pool.allocate(64 * 1024).unwrap();
        assert_eq!(chunk.as_ptr() as usize, addr, "must reuse the cached chunk");
        drop(chunk);

        // Overflow: freeing more chunks than the magazine holds must not
        // lose any (all chunks remain allocatable).
        let chunks: Vec<_> = (0..64).map(|_| pool.allocate(64 * 1024).unwrap()).collect();
        drop(chunks);
        let chunks: Vec<_> = (0..64).map(|_| pool.allocate(64 * 1024).unwrap()).collect();
        assert_eq!(chunks.len(), 64);
    }

    #[test]
    fn test_thread_cache_flushes_on_buddy_miss() {
        // A chunk cached on this thread keeps its slab non-empty; a buddy
        // allocation of the whole pool must flush the cache to succeed.
        let pool = test_pool_with_max(SIZE_64MIB);
        let chunk = pool.allocate(64 * 1024).unwrap();
        drop(chunk); // now cached in this thread's magazine

        let buffer = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
    }

    #[test]
    fn test_thread_cache_cross_thread_free() {
        // Chunks allocated here, freed on another thread (its cache), then
        // the pool must still be fully recoverable from this thread.
        let pool = test_pool_with_max(SIZE_64MIB);
        let chunks: Vec<_> = (0..16).map(|_| pool.allocate(64 * 1024).unwrap()).collect();

        let pool2 = Arc::clone(&pool);
        std::thread::spawn(move || drop(chunks)).join().unwrap();
        drop(pool2);

        // The other thread exited: its cache was flushed on thread exit.
        let buffer = pool.allocate(SIZE_64MIB).unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
    }

    #[tokio::test]
    async fn test_thread_cache_demand_flush_unblocks_waiter() {
        use std::time::Duration;
        use tokio::time::timeout;

        let pool = test_pool_with_max(SIZE_64MIB);
        // Fill the pool: 63 x 1MiB + one full slab.
        let bufs: Vec<_> = (0..63).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
        let chunks: Vec<_> = (0..16).map(|_| pool.allocate(64 * 1024).unwrap()).collect();

        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(bufs);
        // Demand is signalled: these frees must bypass the thread cache so
        // the slab can be released to the waiter.
        drop(chunks);

        let buffer = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter starved: thread cache held the last slab")
            .unwrap()
            .unwrap();
        assert_eq!(buffer.len(), SIZE_64MIB);
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
