//! Per-thread magazine cache for slab chunks.
//!
//! Slab chunk allocation and free are the hottest pool operations in an RPC
//! workload (one allocation and one free per message sent and received, from
//! many threads at once). A per-thread *cache shard* batches this traffic
//! against the shared per-class slab mutex: pops and pushes hit the owning
//! thread's shard (an otherwise-uncontended mutex), and only refill and
//! overflow transfers touch the shared slab lock, amortizing one contended
//! acquisition over [`MAG_REFILL`] operations.
//!
//! Shards are registered with their pool, so the pool can reclaim cached
//! chunks from *all* threads when it actually needs the memory (buddy
//! allocation miss, or an async waiter appearing). This rules out capacity
//! being stranded in the cache of an idle thread. See
//! [`crate::BufferPool`] for the reclaim call sites and the memory-ordering
//! argument that closes the free-versus-reclaim race.
//!
//! Thread exit flushes and unregisters the thread's shards. A live TLS
//! entry holds a `Weak` reference to its pool, which keeps the `Arc`
//! allocation (not the pool itself) alive, so pointer identity cannot be
//! spoofed by an address reuse.

use std::cell::RefCell;
use std::sync::{Arc, Mutex, Weak};

use crate::BufferPool;
use crate::slab::{NUM_SLAB_CLASSES, RawChunk};

/// Maximum cached chunks per slab class (16 KiB, 64 KiB, 256 KiB): at most
/// 2 MiB per class per thread (1 MiB for the 16 KiB class).
pub(crate) const MAG_CAP: [usize; NUM_SLAB_CLASSES] = [64, 32, 8];

/// Number of chunks transferred from the shared slab layer on a refill
/// (half a magazine, so a refill is not immediately undone by frees).
pub(crate) const MAG_REFILL: [usize; NUM_SLAB_CLASSES] = [32, 16, 4];

/// One thread's cached chunks for one pool.
///
/// The mutex is effectively uncontended: pushes and pops come only from the
/// owning thread; other threads touch it only to reclaim (drain) chunks,
/// which is rare.
#[derive(Default)]
pub(crate) struct CacheShard {
    mags: Mutex<[Vec<RawChunk>; NUM_SLAB_CLASSES]>,
}

impl CacheShard {
    /// Pops a cached chunk.
    pub(crate) fn pop(&self, class: usize) -> Option<RawChunk> {
        self.mags.lock().expect("CacheShard mutex poisoned")[class].pop()
    }

    /// Pushes a freed chunk. When the magazine exceeds its capacity, drains
    /// the older half and returns it for the caller to give back to the
    /// pool.
    pub(crate) fn push(&self, class: usize, chunk: RawChunk) -> Option<Vec<RawChunk>> {
        let mut mags = self.mags.lock().expect("CacheShard mutex poisoned");
        let mag = &mut mags[class];
        mag.push(chunk);
        if mag.len() > MAG_CAP[class] {
            // Keep the most recently used half; hand the rest back.
            let keep = mag.split_off(mag.len() - MAG_CAP[class] / 2);
            Some(std::mem::replace(mag, keep))
        } else {
            None
        }
    }

    /// Stores a refill batch taken from the shared slab layer.
    pub(crate) fn store_batch(&self, class: usize, chunks: Vec<RawChunk>) {
        if chunks.is_empty() {
            return;
        }
        let mut mags = self.mags.lock().expect("CacheShard mutex poisoned");
        debug_assert!(mags[class].len() + chunks.len() <= MAG_CAP[class]);
        mags[class].extend(chunks);
    }

    /// Removes and returns all cached chunks (all classes).
    pub(crate) fn drain_all(&self) -> [Vec<RawChunk>; NUM_SLAB_CLASSES] {
        let mut mags = self.mags.lock().expect("CacheShard mutex poisoned");
        std::array::from_fn(|class| std::mem::take(&mut mags[class]))
    }
}

/// A thread's registered shard for one pool.
struct Entry {
    /// Weak pool handle: identifies the pool and keeps its `Arc` allocation
    /// (though not the pool value) alive, making the pointer identity in
    /// [`shard_for`] ABA-safe. Upgraded on thread exit to flush the shard.
    pool: Weak<BufferPool>,
    shard: Arc<CacheShard>,
}

#[derive(Default)]
struct Registry {
    entries: Vec<Entry>,
}

impl Drop for Registry {
    fn drop(&mut self) {
        for entry in self.entries.drain(..) {
            if let Some(pool) = entry.pool.upgrade() {
                pool.release_shard(&entry.shard);
            }
        }
    }
}

thread_local! {
    static REGISTRY: RefCell<Registry> = RefCell::new(Registry::default());
}

/// Borrows an existing shard for a single magazine operation. The callback must
/// not allocate from, return buffers to, or reclaim capacity from the pool:
/// those operations may reenter TLS. No reference escapes this borrow.
fn with_existing_shard<T>(
    pool: &Arc<BufferPool>,
    operation: impl FnOnce(&CacheShard) -> T,
) -> Option<T> {
    REGISTRY
        .try_with(|registry| {
            let registry = registry.borrow();
            registry
                .entries
                .iter()
                .find(|entry| std::ptr::eq(entry.pool.as_ptr(), Arc::as_ptr(pool)))
                .map(|entry| operation(&entry.shard))
        })
        .ok()
        .flatten()
}

/// Takes a chunk without cloning the shard's Arc on a cache hit.
pub(crate) fn pop_cached(pool: &Arc<BufferPool>, class: usize) -> Option<RawChunk> {
    with_existing_shard(pool, |shard| shard.pop(class)).flatten()
}

/// Pushes through a borrowed shard, registering one on the first return from
/// this thread. Overflow is handed back only after releasing the TLS borrow.
/// During thread destruction the untouched token returns to the caller.
pub(crate) fn push_cached(
    pool: &Arc<BufferPool>,
    class: usize,
    chunk: RawChunk,
) -> Result<Option<Vec<RawChunk>>, RawChunk> {
    let mut chunk = Some(chunk);
    if let Some(overflow) =
        with_existing_shard(pool, |shard| shard.push(class, chunk.take().unwrap()))
    {
        return Ok(overflow);
    }
    let chunk = chunk.unwrap();
    match shard_for(pool) {
        Some(shard) => Ok(shard.push(class, chunk)),
        None => Err(chunk),
    }
}

/// Returns the current thread's cache shard for `pool`, registering a new
/// one if needed. Returns `None` during thread destruction.
pub(crate) fn shard_for(pool: &Arc<BufferPool>) -> Option<Arc<CacheShard>> {
    REGISTRY
        .try_with(|r| {
            let mut reg = r.borrow_mut();
            if let Some(entry) = reg
                .entries
                .iter()
                .find(|e| std::ptr::eq(e.pool.as_ptr(), Arc::as_ptr(pool)))
            {
                return Arc::clone(&entry.shard);
            }
            // Drop entries of dead pools (their chunks point into freed
            // blocks and must simply be forgotten).
            reg.entries.retain(|e| e.pool.strong_count() > 0);

            let shard = Arc::new(CacheShard::default());
            pool.register_shard(Arc::clone(&shard));
            reg.entries.push(Entry {
                pool: Arc::downgrade(pool),
                shard: Arc::clone(&shard),
            });
            shard
        })
        .ok()
}
