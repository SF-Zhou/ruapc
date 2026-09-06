//! Lock-free slot array for in-flight work request buffers
//!
//! Replaces a `Mutex<HashMap>` on the hot path. Buffers of posted work
//! requests are stored in a fixed-size power-of-two slot array indexed by
//! `id % capacity`.
//!
//! ## Why this is safe without a lock
//!
//! - The owning work queue supplies non-repeating IDs. This table stores
//!   buffers independently of ID allocation and hardware posting order.
//! - Each slot is guarded by an atomic tag acting as a tiny state machine:
//!   `EMPTY -> WRITING -> id + TAG_BASE -> EMPTY`.
//! - Reusing an array index requires an empty slot; completing a work request
//!   requires an exact ID match. Delayed completions cannot take a newer
//!   request's buffer even when both IDs map to the same array index.
//!
//! Queue depth alone does not prevent collisions: failed posts and bufferless
//! work requests leave gaps in the ID sequence. Insertion returns the untouched
//! buffer on contention instead of waiting for a completion that might need to
//! run on the posting thread itself.

use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicU64, Ordering},
};

use super::queue_pair::WrBuffers;

/// Slot is free and may be claimed by a poster.
const EMPTY: u64 = 0;
/// Slot is being written to or drained; transient state.
const WRITING: u64 = 1;
/// Occupied slots store `id + TAG_BASE`. IDs are bounded by
/// [`crate::WRID`], so this never collides with `EMPTY`/`WRITING`.
const TAG_BASE: u64 = 2;

struct Slot {
    tag: AtomicU64,
    buffer: UnsafeCell<Option<WrBuffers>>,
}

/// SAFETY: access to `buffer` is serialized by the `tag` state machine:
/// only the thread that moved the tag into `WRITING` may touch the cell,
/// and tag transitions use acquire/release ordering.
unsafe impl Sync for Slot {}

/// Fixed-capacity lock-free storage for in-flight work request buffers.
pub(crate) struct WrSlots {
    slots: Box<[Slot]>,
    mask: u64,
}

impl WrSlots {
    /// Creates a slot array for a work queue of the given depth.
    ///
    /// Capacity is `2 * depth` rounded up to a power of two: the extra
    /// margin accommodates completions that have been polled from the CQ but
    /// whose buffers have not been taken yet. Collisions remain possible and
    /// are reported by [`Self::insert`].
    pub fn new(depth: u32) -> Self {
        let cap = (depth.max(1) as usize)
            .saturating_mul(2)
            .next_power_of_two();
        let slots = (0..cap)
            .map(|_| Slot {
                tag: AtomicU64::new(EMPTY),
                buffer: UnsafeCell::new(None),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            slots,
            mask: (cap - 1) as u64,
        }
    }

    /// Stores the buffer of an about-to-be-posted work request.
    ///
    /// Returns ownership immediately if another request or completion still
    /// occupies the selected slot. No allocation or waiting occurs here.
    pub fn insert(&self, id: u64, buffer: WrBuffers) -> Result<(), WrBuffers> {
        let slot = &self.slots[(id & self.mask) as usize];
        if slot
            .tag
            .compare_exchange(EMPTY, WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(buffer);
        }
        // SAFETY: we own the slot while its tag is `WRITING`.
        unsafe { *slot.buffer.get() = Some(buffer) };
        slot.tag.store(id.wrapping_add(TAG_BASE), Ordering::Release);
        Ok(())
    }

    /// Takes the buffer of a completed (or failed-to-post) work request.
    ///
    /// Returns `None` if no buffer was stored for this ID (e.g. buffer-less
    /// immediate-only sends).
    pub fn take(&self, id: u64) -> Option<WrBuffers> {
        let slot = &self.slots[(id & self.mask) as usize];
        let tag = id.wrapping_add(TAG_BASE);
        if slot
            .tag
            .compare_exchange(tag, WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // SAFETY: we own the slot while its tag is `WRITING`.
            let buffer = unsafe { (*slot.buffer.get()).take() };
            slot.tag.store(EMPTY, Ordering::Release);
            buffer
        } else {
            None
        }
    }
}

impl std::fmt::Debug for WrSlots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrSlots")
            .field("capacity", &self.slots.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ruapc_bufpool::{BufferPoolBuilder, EmptyDevices};

    use super::*;

    fn pool() -> Arc<ruapc_bufpool::BufferPool> {
        BufferPoolBuilder::new(Arc::new(EmptyDevices)).build()
    }

    #[test]
    fn test_insert_take_roundtrip() {
        let pool = pool();
        let slots = WrSlots::new(4);
        for id in 0..64 {
            let buf = pool.allocate(1024).unwrap();
            slots.insert(id, buf.into()).unwrap();
            assert!(slots.take(id).is_some());
            // Double take returns None.
            assert!(slots.take(id).is_none());
        }
    }

    #[test]
    fn test_take_without_insert() {
        let slots = WrSlots::new(4);
        assert!(slots.take(0).is_none());
    }

    #[test]
    fn test_wrap_around_reuses_slots() {
        let pool = pool();
        let slots = WrSlots::new(2); // capacity 4
        // Keep up to 2 in flight while IDs wrap around the array many times.
        let mut in_flight = std::collections::VecDeque::new();
        for id in 0..1000 {
            slots.insert(id, pool.allocate(64).unwrap().into()).unwrap();
            in_flight.push_back(id);
            if in_flight.len() == 2 {
                let id = in_flight.pop_front().unwrap();
                assert!(slots.take(id).is_some(), "buffer missing for id {id}");
            }
        }
    }

    #[test]
    fn collision_returns_buffer_without_disturbing_the_live_request() {
        let pool = pool();
        let slots = WrSlots::new(2); // capacity 4
        let live = pool.allocate(64).unwrap();
        let live_address = live.as_ptr();
        slots.insert(0, live.into()).unwrap();

        // Failed posts or bufferless WRs can skip IDs 1..4 while ID 0 remains
        // live. An in-flight count below queue depth does not prevent this.
        let next = pool.allocate(64).unwrap();
        let next_address = next.as_ptr();
        let returned = slots.insert(4, next.into()).unwrap_err();
        assert!(slots.take(4).is_none());
        let live = slots.take(0).unwrap().into_single().unwrap();
        assert_eq!(live.as_ptr(), live_address);

        slots.insert(4, returned).unwrap();
        assert!(slots.take(0).is_none(), "stale ID took a newer buffer");
        let next = slots.take(4).unwrap().into_single().unwrap();
        assert_eq!(next.as_ptr(), next_address);
    }

    #[test]
    fn test_concurrent_post_and_complete() {
        let pool = pool();
        let slots = Arc::new(WrSlots::new(64));
        let next_id = Arc::new(AtomicU64::new(0));
        let (tx, rx) = std::sync::mpsc::channel::<u64>();

        // 4 poster threads, 1 completer thread (mirrors real usage: many
        // senders, one event loop).
        let mut posters = vec![];
        for _ in 0..4 {
            let slots = Arc::clone(&slots);
            let pool = Arc::clone(&pool);
            let next_id = Arc::clone(&next_id);
            let tx = tx.clone();
            posters.push(std::thread::spawn(move || {
                for _ in 0..10_000 {
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    let mut buffer = pool.allocate(64).unwrap().into();
                    while let Err(returned) = slots.insert(id, buffer) {
                        buffer = returned;
                        std::thread::yield_now();
                    }
                    tx.send(id).unwrap();
                }
            }));
        }
        drop(tx);

        let completer = std::thread::spawn(move || {
            let mut taken = 0usize;
            while let Ok(id) = rx.recv() {
                assert!(slots.take(id).is_some(), "buffer missing for id {id}");
                taken += 1;
            }
            taken
        });

        for t in posters {
            t.join().unwrap();
        }
        assert_eq!(completer.join().unwrap(), 40_000);
    }
}
