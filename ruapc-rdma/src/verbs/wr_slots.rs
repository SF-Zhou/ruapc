//! Lock-free slot array for in-flight work request buffers
//!
//! Replaces a `Mutex<HashMap>` on the hot path. Buffers of posted work
//! requests are stored in a fixed-size power-of-two slot array indexed by
//! `id % capacity`.
//!
//! ## Why this is safe without a lock
//!
//! - IDs are allocated from a per-direction monotonic counter, so two
//!   concurrent posters never target the same slot while both IDs are
//!   in flight (in-flight count is bounded by the queue depth, and the
//!   array capacity is at least twice the queue depth).
//! - RC queue pairs complete work requests **in order** per direction, and
//!   the provider only frees a hardware queue slot once its completion has
//!   been polled. Therefore, by the time an ID wraps around onto a slot, the
//!   previous occupant has already completed; the only remaining window is
//!   between `ibv_poll_cq` and [`WrSlots::take`], which is covered by the 2x
//!   capacity margin plus a bounded spin in [`WrSlots::insert`].
//! - Each slot is guarded by an atomic tag acting as a tiny state machine:
//!   `EMPTY -> WRITING -> id + TAG_BASE -> EMPTY`.

use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicU64, Ordering},
};

use ruapc_bufpool::Buffer;

/// Slot is free and may be claimed by a poster.
const EMPTY: u64 = 0;
/// Slot is being written to or drained; transient state.
const WRITING: u64 = 1;
/// Occupied slots store `id + TAG_BASE`. IDs are bounded to 62 bits by
/// [`crate::WRID`], so this never collides with `EMPTY`/`WRITING`.
const TAG_BASE: u64 = 2;

struct Slot {
    tag: AtomicU64,
    buffer: UnsafeCell<Option<Buffer>>,
}

/// SAFETY: access to `buffer` is serialized by the `tag` state machine:
/// only the thread that moved the tag into `WRITING` may touch the cell,
/// and tag transitions use acquire/release ordering.
unsafe impl Sync for Slot {}

/// Fixed-capacity lock-free storage for in-flight work request buffers.
pub(crate) struct WrSlots {
    slots: Box<[Slot]>,
    mask: u64,
    next_id: AtomicU64,
}

impl WrSlots {
    /// Creates a slot array for a work queue of the given depth.
    ///
    /// Capacity is `2 * depth` rounded up to a power of two: the extra
    /// margin covers completions that have been polled from the CQ but whose
    /// buffers have not been taken yet.
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
            next_id: AtomicU64::new(0),
        }
    }

    /// Allocates the next monotonic work request ID.
    #[inline]
    pub fn alloc_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Stores the buffer of an about-to-be-posted work request.
    ///
    /// May briefly spin if the slot's previous occupant has been polled from
    /// the CQ but not yet taken by the completion handler.
    pub fn insert(&self, id: u64, buffer: Buffer) {
        let slot = &self.slots[(id & self.mask) as usize];
        let mut spins = 0u32;
        while slot
            .tag
            .compare_exchange_weak(EMPTY, WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spins += 1;
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
        // SAFETY: we own the slot while its tag is `WRITING`.
        unsafe { *slot.buffer.get() = Some(buffer) };
        slot.tag.store(id.wrapping_add(TAG_BASE), Ordering::Release);
    }

    /// Takes the buffer of a completed (or failed-to-post) work request.
    ///
    /// Returns `None` if no buffer was stored for this ID (e.g. buffer-less
    /// immediate-only sends).
    pub fn take(&self, id: u64) -> Option<Buffer> {
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

impl WrSlots {
    /// Takes and drops every stored buffer, returning how many were
    /// reclaimed.
    ///
    /// Intended for teardown after the owning QP has transitioned to the
    /// error state: buffers of unsignaled work requests that completed
    /// successfully never produce a CQE and must be reclaimed explicitly.
    pub fn reclaim_all(&self) -> usize {
        let mut reclaimed = 0;
        for slot in &self.slots {
            let tag = slot.tag.load(Ordering::Acquire);
            if tag >= TAG_BASE
                && slot
                    .tag
                    .compare_exchange(tag, WRITING, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                // SAFETY: we own the slot while its tag is `WRITING`.
                let buffer = unsafe { (*slot.buffer.get()).take() };
                slot.tag.store(EMPTY, Ordering::Release);
                if buffer.is_some() {
                    reclaimed += 1;
                }
            }
        }
        reclaimed
    }
}

impl std::fmt::Debug for WrSlots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrSlots")
            .field("capacity", &self.slots.len())
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
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
        for _ in 0..64 {
            let id = slots.alloc_id();
            let buf = pool.allocate(1024).unwrap();
            slots.insert(id, buf);
            assert!(slots.take(id).is_some());
            // Double take returns None.
            assert!(slots.take(id).is_none());
        }
    }

    #[test]
    fn test_take_without_insert() {
        let slots = WrSlots::new(4);
        let id = slots.alloc_id();
        assert!(slots.take(id).is_none());
    }

    #[test]
    fn test_wrap_around_reuses_slots() {
        let pool = pool();
        let slots = WrSlots::new(2); // capacity 4
        // Keep up to 2 in flight while IDs wrap around the array many times.
        let mut in_flight = std::collections::VecDeque::new();
        for _ in 0..1000 {
            let id = slots.alloc_id();
            slots.insert(id, pool.allocate(64).unwrap());
            in_flight.push_back(id);
            if in_flight.len() == 2 {
                let id = in_flight.pop_front().unwrap();
                assert!(slots.take(id).is_some(), "buffer missing for id {id}");
            }
        }
    }

    #[test]
    fn test_concurrent_post_and_complete() {
        let pool = pool();
        let slots = Arc::new(WrSlots::new(64));
        let (tx, rx) = std::sync::mpsc::channel::<u64>();

        // 4 poster threads, 1 completer thread (mirrors real usage: many
        // senders, one event loop).
        let mut posters = vec![];
        for _ in 0..4 {
            let slots = Arc::clone(&slots);
            let pool = Arc::clone(&pool);
            let tx = tx.clone();
            posters.push(std::thread::spawn(move || {
                for _ in 0..10_000 {
                    let id = slots.alloc_id();
                    slots.insert(id, pool.allocate(64).unwrap());
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
