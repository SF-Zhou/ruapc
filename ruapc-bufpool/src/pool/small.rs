//! Slab and thread-cache coordination. Cache locks are released before slab
//! locks, and slab locks are released before returning backing to the buddy pool.

use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::buddy::BuddyBlock;
use crate::buffer::Buffer;
use crate::slab::RawChunk;
use crate::thread_cache::{self, CacheShard, MAG_REFILL};

use super::BufferPool;

impl BufferPool {
    /// Reclaim magazines first: cached chunks otherwise keep their slabs busy.
    pub(super) fn reclaim_cached_capacity(self: &Arc<Self>) {
        self.flush_thread_cache();
        self.reclaim_empty_slabs();
    }

    /// Takes a chunk from an existing slab of the given class, if available.
    pub(super) fn try_take_chunk(self: &Arc<Self>, class: usize) -> Option<Buffer> {
        let chunk = self.slab_classes[class]
            .lock()
            .expect("SlabClass mutex poisoned")
            .alloc()?;
        Some(chunk.into_buffer(class, self))
    }

    /// Takes a chunk, preferring the current thread's cache shard.
    ///
    /// On a shard miss, refills it with a batch of chunks taken from the
    /// shared slab layer under a single lock acquisition.
    pub(super) fn try_take_chunk_fast(self: &Arc<Self>, class: usize) -> Option<Buffer> {
        if !self.thread_cache {
            return self.try_take_chunk(class);
        }
        if let Some(chunk) = thread_cache::pop_cached(self, class) {
            return Some(chunk.into_buffer(class, self));
        }
        let Some(shard) = thread_cache::shard_for(self) else {
            return self.try_take_chunk(class);
        };
        // Only this thread replenishes its shard; other threads can drain it.
        // A cache miss therefore stays empty until this refill. The TLS borrow
        // ended before taking any slab or pool locks.
        let mut batch = Vec::with_capacity(MAG_REFILL[class] + 1);
        self.take_chunk_batch(class, MAG_REFILL[class] + 1, &mut batch);
        let chunk = batch.pop()?;
        shard.store_batch(class, batch);
        Some(chunk.into_buffer(class, self))
    }

    /// Takes up to `n` chunks from the slab layer under one lock acquisition.
    pub(super) fn take_chunk_batch(&self, class: usize, n: usize, out: &mut Vec<RawChunk>) {
        let mut slab_class = self.slab_classes[class]
            .lock()
            .expect("SlabClass mutex poisoned");
        for _ in 0..n {
            match slab_class.alloc() {
                Some(chunk) => out.push(chunk),
                None => break,
            }
        }
    }

    /// Installs a fresh 1 MiB backing buffer as a slab of the given class
    /// and takes the first chunk from it.
    pub(super) fn install_backing_and_take(
        self: &Arc<Self>,
        class: usize,
        backing: Buffer,
    ) -> Buffer {
        let chunk = {
            let mut slab_class = self.slab_classes[class]
                .lock()
                .expect("SlabClass mutex poisoned");
            slab_class.insert_backing(backing);
            slab_class
                .alloc()
                .expect("fresh slab must have free chunks")
        };
        chunk.into_buffer(class, self)
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
        let mut chunk = RawChunk { ptr, index, block };
        if self.thread_cache && !self.has_demand.load(Ordering::SeqCst) {
            match thread_cache::push_cached(self, class, chunk) {
                Ok(overflow) => {
                    if let Some(overflow) = overflow {
                        self.return_chunks_direct(class, overflow);
                    }
                    // Either this load sees newly registered demand, or that
                    // waiter's reclaim sweep sees our push. Acquire an owned
                    // shard only when reclamation is actually needed; every
                    // callback into the pool runs after releasing the TLS borrow.
                    if self.has_demand.load(Ordering::SeqCst)
                        && let Some(shard) = thread_cache::shard_for(self)
                    {
                        self.flush_shard(&shard);
                    }
                    return;
                }
                Err(returned) => chunk = returned,
            }
        }
        self.return_chunks_direct(class, [chunk]);
    }

    /// Returns chunks to their slabs under one lock acquisition.
    ///
    /// If a slab becomes fully free and exceeds the empty-slab watermark —
    /// or the buddy pool has pending demand — the slab's backing buffer is
    /// released to the buddy pool (outside the class lock; the explicit return
    /// uses the regular buddy free path).
    pub(crate) fn return_chunks_direct(
        self: &Arc<Self>,
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
        for backing in released {
            backing.release(self);
        }
    }

    /// Registers a thread's cache shard (called via TLS on first use).
    pub(crate) fn register_shard(&self, shard: Arc<CacheShard>) {
        self.thread_shards
            .lock()
            .expect("thread_shards mutex poisoned")
            .push(shard);
    }

    /// Flushes and unregisters a thread's cache shard (thread exit).
    pub(crate) fn release_shard(self: &Arc<Self>, shard: &Arc<CacheShard>) {
        self.flush_shard(shard);
        self.thread_shards
            .lock()
            .expect("thread_shards mutex poisoned")
            .retain(|s| !Arc::ptr_eq(s, shard));
    }

    /// Returns all chunks cached in `shard` to the slab layer.
    pub(super) fn flush_shard(self: &Arc<Self>, shard: &CacheShard) {
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
    pub(super) fn flush_thread_cache(self: &Arc<Self>) {
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
    pub(super) fn reclaim_empty_slabs(self: &Arc<Self>) -> bool {
        let mut any = false;
        for class in &self.slab_classes {
            let empties = class
                .lock()
                .expect("SlabClass mutex poisoned")
                .drain_empty();
            any |= !empties.is_empty();
            // Returning backing takes the buddy mutex, after the class lock.
            for backing in empties {
                backing.release(self);
            }
        }
        any
    }
}
