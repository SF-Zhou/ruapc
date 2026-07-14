//! Slab layer for small buffer classes (64 KiB and 256 KiB).
//!
//! Small allocations are served from *slabs*: 1 MiB leaves taken from the
//! buddy pool and carved into fixed-size chunks. Each size class keeps its
//! own slabs, free-chunk bitmaps and an availability stack, protected by a
//! per-class mutex at the [`crate::BufferPool`] facade — small-allocation
//! traffic therefore does not contend on the buddy pool's global mutex.
//!
//! Empty slabs (all chunks free) are cached up to a configurable watermark
//! and returned to the buddy pool beyond it, immediately when the buddy pool
//! has pending demand (async waiters or an anti-starvation reservation), or
//! in bulk when a buddy allocation misses. Returning a slab is simply
//! dropping its backing [`Buffer`], which flows through the regular buddy
//! free path (merging, waiter handoff and reservation absorption included).

use std::collections::HashMap;
use std::ptr::NonNull;

use crate::buddy::{BuddyBlock, SIZE_1MIB};
use crate::buffer::Buffer;

/// Fast non-cryptographic hasher for the slab map: keys are trusted
/// 1 MiB-aligned addresses, so SipHash's DoS resistance is unnecessary.
type SlabMap = HashMap<usize, Slab, foldhash::fast::FixedState>;

/// Size of the buddy leaf backing one slab.
pub(crate) const SLAB_BACKING_SIZE: usize = SIZE_1MIB;

/// Number of slab size classes.
pub(crate) const NUM_SLAB_CLASSES: usize = 2;

/// Chunk sizes for each slab class.
pub(crate) const SLAB_CLASS_SIZES: [usize; NUM_SLAB_CLASSES] = [64 * 1024, 256 * 1024];

/// Returns the slab class for a request size, or `None` if the size is 0 or
/// belongs to the buddy allocator (> 256 KiB).
pub(crate) fn size_to_class(size: usize) -> Option<usize> {
    if size == 0 {
        return None;
    }
    SLAB_CLASS_SIZES.iter().position(|&s| size <= s)
}

/// A single slab: a 1 MiB buddy leaf carved into equal chunks.
struct Slab {
    /// Backing buffer from the buddy pool. Dropping it returns the memory
    /// through the regular buddy free path.
    backing: Buffer,
    /// Bitmask of free chunks (bit `i` set = chunk `i` is free).
    free_mask: u32,
    /// Whether this slab's address is currently on the `available` stack.
    in_available: bool,
}

/// State of one slab size class. Locking is done by the caller.
pub(crate) struct SlabClass {
    chunk_size: usize,
    /// Mask with one bit set per chunk (the "fully free" pattern).
    full_mask: u32,
    /// All slabs of this class, keyed by the 1 MiB-aligned base address.
    slabs: SlabMap,
    /// LIFO stack of slab base addresses that may have free chunks.
    /// Entries can be stale (slab became full or was released); they are
    /// validated and skipped lazily on allocation. Each slab transition
    /// pushes at most one entry, so staleness is bounded.
    available: Vec<usize>,
    /// Number of fully-free slabs currently cached.
    empty_count: usize,
}

impl SlabClass {
    pub(crate) fn new(class: usize) -> Self {
        let chunk_size = SLAB_CLASS_SIZES[class];
        let chunks = SLAB_BACKING_SIZE / chunk_size;
        debug_assert!(chunks <= 32);
        Self {
            chunk_size,
            full_mask: ((1u64 << chunks) - 1) as u32,
            slabs: SlabMap::default(),
            available: Vec::new(),
            empty_count: 0,
        }
    }

    /// Takes a free chunk from any available slab.
    ///
    /// Returns the chunk pointer, its index within the slab, and the
    /// `BuddyBlock` containing it (for device registration lookups).
    pub(crate) fn alloc(&mut self) -> Option<(NonNull<u8>, usize, NonNull<BuddyBlock>)> {
        loop {
            let &addr = self.available.last()?;
            let Some(slab) = self.slabs.get_mut(&addr) else {
                // Stale entry: the slab was released.
                self.available.pop();
                continue;
            };
            if slab.free_mask == 0 {
                // Stale entry: the slab became full.
                slab.in_available = false;
                self.available.pop();
                continue;
            }

            if slab.free_mask == self.full_mask {
                self.empty_count -= 1;
            }
            let index = slab.free_mask.trailing_zeros() as usize;
            slab.free_mask &= !(1u32 << index);
            if slab.free_mask == 0 {
                slab.in_available = false;
                self.available.pop();
            }

            let ptr = unsafe {
                NonNull::new_unchecked(
                    slab.backing
                        .as_ptr()
                        .cast_mut()
                        .add(index * self.chunk_size),
                )
            };
            return Some((ptr, index, slab.backing.block_ptr()));
        }
    }

    /// Adds a fresh slab backed by a 1 MiB buddy buffer.
    pub(crate) fn insert_backing(&mut self, backing: Buffer) {
        let addr = backing.as_ptr() as usize;
        debug_assert_eq!(
            addr & (SLAB_BACKING_SIZE - 1),
            0,
            "slab backing must be 1MiB-aligned"
        );
        debug_assert_eq!(backing.capacity(), SLAB_BACKING_SIZE);
        let prev = self.slabs.insert(
            addr,
            Slab {
                backing,
                free_mask: self.full_mask,
                in_available: true,
            },
        );
        debug_assert!(prev.is_none(), "duplicate slab backing");
        self.available.push(addr);
        self.empty_count += 1;
    }

    /// Returns a chunk to its slab (located via address masking).
    ///
    /// If this makes the slab fully free and more than `max_empty` empty
    /// slabs would be cached, the slab is removed and its backing buffer is
    /// returned so the caller can release it to the buddy pool (outside the
    /// class lock).
    pub(crate) fn free(
        &mut self,
        chunk_addr: usize,
        index: usize,
        max_empty: usize,
    ) -> Option<Buffer> {
        let base = chunk_addr & !(SLAB_BACKING_SIZE - 1);
        let slab = self
            .slabs
            .get_mut(&base)
            .expect("chunk freed into unknown slab");
        debug_assert_eq!((chunk_addr - base) / self.chunk_size, index);
        debug_assert_eq!(slab.free_mask & (1u32 << index), 0, "double free of chunk");

        slab.free_mask |= 1u32 << index;
        if !slab.in_available {
            slab.in_available = true;
            self.available.push(base);
        }

        if slab.free_mask == self.full_mask {
            if self.empty_count >= max_empty {
                let slab = self.slabs.remove(&base).unwrap();
                return Some(slab.backing);
            }
            self.empty_count += 1;
        }
        None
    }

    /// Removes all cached empty slabs, returning their backing buffers so
    /// the caller can release them to the buddy pool (outside the class
    /// lock).
    pub(crate) fn drain_empty(&mut self) -> Vec<Buffer> {
        if self.empty_count == 0 {
            return Vec::new();
        }
        let full = self.full_mask;
        let empties: Vec<usize> = self
            .slabs
            .iter()
            .filter(|(_, slab)| slab.free_mask == full)
            .map(|(&addr, _)| addr)
            .collect();
        debug_assert_eq!(empties.len(), self.empty_count);
        self.empty_count = 0;
        empties
            .into_iter()
            .map(|addr| self.slabs.remove(&addr).unwrap().backing)
            .collect()
    }

    /// Total number of free chunks across all slabs of this class.
    pub(crate) fn free_chunks(&self) -> usize {
        self.slabs
            .values()
            .map(|slab| slab.free_mask.count_ones() as usize)
            .sum()
    }
}
