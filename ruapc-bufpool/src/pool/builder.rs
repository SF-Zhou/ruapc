//! Pool configuration and construction.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::buddy::NUM_LEVELS;
use crate::devices::Devices;
use crate::slab::SlabClass;

use super::{BuddyAllocator, BufferPool};

/// Default maximum memory limit (256 MiB).
pub const DEFAULT_BUFFER_POOL_MEMORY: usize = 256 * 1024 * 1024;

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
            max_memory: DEFAULT_BUFFER_POOL_MEMORY,
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
        let inner = BuddyAllocator::new(self.merge_watermarks);

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
