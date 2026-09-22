//! Direct waiter handoff and bounded subtree reservations for starvation
//! protection. Called only with the parent allocator's mutex held.

use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tokio::sync::oneshot;

use crate::BufferPool;
use crate::buddy::{BuddyBlock, LEVEL_SIZES, NODES_PER_LEVEL, NUM_LEVELS, NodeState};
use crate::buffer::Buffer;

use super::BuddyAllocator;

/// A queued asynchronous allocation waiting for capacity.
pub(super) struct Waiter {
    /// When the waiter was queued; used for starvation detection.
    pub(super) since: Instant,
    /// Channel used to hand a buffer directly to the waiter.
    pub(super) sender: oneshot::Sender<Buffer>,
}

/// An anti-starvation reservation for a large waiter.
///
/// Created when a waiter for level >= 1 has been queued longer than the
/// configured starvation timeout. At most one reservation exists at a time.
/// The reserved waiter is removed from the waiting lists (its buffer arrives
/// through this reservation, or through the priority claim in
/// [`BuddyAllocator::serve_waiters`]).
pub(super) struct Reservation {
    /// Channel of the starving waiter.
    pub(super) sender: oneshot::Sender<Buffer>,
    /// Requested allocation level.
    pub(super) level: usize,
    /// The subtree being drained for this waiter, if one has been chosen.
    /// `None` ("floating") when no drainable subtree exists yet; the waiter
    /// is then served through the priority claim on public capacity, and an
    /// upgrade to a subtree is retried on subsequent frees.
    pub(super) subtree: Option<ReservedSubtree>,
}

/// A buddy subtree reserved for a starving waiter.
///
/// Free nodes inside the subtree are *absorbed*: unlinked from the free
/// lists (invisible to regular allocation) while keeping their `Free` state
/// as an "absorbed" marker. Frees inside the subtree are intercepted before
/// the regular merge path. Absorbed capacity cannot be reallocated, so
/// `collected` grows monotonically. The reservation can complete once all
/// live buffers inside the subtree have been returned.
pub(super) struct ReservedSubtree {
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

impl BuddyAllocator {
    /// Releases the reservation if its waiter has been cancelled.
    /// Returns `true` if a reservation was reclaimed.
    pub(super) fn reclaim_cancelled_reservation(&mut self) -> bool {
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
    pub(in crate::pool) fn serve_waiters(&mut self, pool: &Arc<BufferPool>) {
        self.try_complete_reservation_from_lists(pool);

        while let Some(wait_level) = self.min_waiting_level {
            let Some(buffer) = self.try_allocate_local(wait_level, pool) else {
                break;
            };

            let waiter = self.waiting_lists[wait_level]
                .pop_front()
                .expect("waiting list at min_waiting_level must be non-empty");
            self.update_min_waiting_level_on_remove(wait_level);

            self.deliver(waiter.sender, buffer);
        }

        // Every writer of this hint holds the allocator mutex. Skip unchanged
        // values: repeatedly exchanging false on ordinary frees needlessly
        // transfers another cache line between contending threads. Transitions
        // remain SeqCst for the return_chunk/reclaim race described in small.rs.
        let demand = self.min_waiting_level.is_some() || self.reservation.is_some();
        if pool.has_demand.load(Ordering::Relaxed) != demand {
            pool.has_demand.store(demand, Ordering::SeqCst);
        }
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
    pub(super) fn try_absorb_into_reservation(
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
            let state = unsafe { BuddyBlock::allocator_state(block) };
            state.set_state(level, index, NodeState::Free);
            self.release_reservation_subtree(res.level, &res.subtree.unwrap());
            return true;
        }

        let state = unsafe { BuddyBlock::allocator_state(block) };
        debug_assert_eq!(state.get_state(level, index), NodeState::Allocated);
        state.set_state(level, index, NodeState::Free); // absorbed marker
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
    pub(super) fn check_starvation(&mut self, pool: &Arc<BufferPool>) {
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
    pub(super) fn reserve_subtree(&mut self, level: usize) -> Option<ReservedSubtree> {
        let mut best: Option<(NonNull<BuddyBlock>, usize, usize)> = None;

        for i in 0..self.blocks.len() {
            let block_ptr =
                NonNull::new(std::ptr::from_ref::<BuddyBlock>(&self.blocks[i]).cast_mut()).unwrap();
            let state = unsafe { BuddyBlock::allocator_state(block_ptr) };

            for root_index in 0..NODES_PER_LEVEL[level] {
                if state.get_state(level, root_index) != NodeState::Split {
                    continue;
                }
                let mut free_bytes = 0;
                for (child_level, &child_size) in LEVEL_SIZES.iter().enumerate().take(level) {
                    let first = root_index << (2 * (level - child_level));
                    let count = 1usize << (2 * (level - child_level));
                    for index in first..first + count {
                        if state.get_state(child_level, index) == NodeState::Free {
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
    pub(super) fn absorb_subtree_free_nodes(
        &mut self,
        block: NonNull<BuddyBlock>,
        res_level: usize,
        root_index: usize,
    ) -> usize {
        let state = unsafe { BuddyBlock::allocator_state(block) };
        let mut collected = 0;

        for (level, &size) in LEVEL_SIZES.iter().enumerate().take(res_level) {
            let first = root_index << (2 * (res_level - level));
            let count = 1usize << (2 * (res_level - level));
            for index in first..first + count {
                if state.get_state(level, index) == NodeState::Free {
                    self.demote_pending_parent(state, level, index);
                    let node = state.get_free_node_mut(level, index);
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
    pub(super) fn try_finalize_reservation(&mut self, pool: &Arc<BufferPool>) {
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
        let state = unsafe { BuddyBlock::allocator_state(sub.block) };

        // Restore the state invariant for an allocated node: all strict
        // descendants must read Allocated (clears absorbed markers and stale
        // Split states alike).
        for level in 0..res.level {
            let first = sub.root_index << (2 * (res.level - level));
            let count = 1usize << (2 * (res.level - level));
            for index in first..first + count {
                state.set_state(level, index, NodeState::Allocated);
            }
        }
        state.set_state(res.level, sub.root_index, NodeState::Allocated);

        let ptr = unsafe {
            sub.block
                .as_ref()
                .get_memory_addr(res.level, sub.root_index)
        };
        let buffer = unsafe {
            Buffer::new(
                NonNull::new(ptr).unwrap(),
                res.level,
                sub.root_index,
                sub.block,
                Arc::clone(pool),
            )
        };
        self.deliver(res.sender, buffer);
    }

    /// Priority claim for the reserved waiter: if public capacity at its
    /// level exists (or can be coalesced), complete the reservation from it
    /// and release any partially drained subtree back to the pool.
    ///
    /// Also detects a cancelled reserved waiter and releases its subtree.
    pub(super) fn try_complete_reservation_from_lists(&mut self, pool: &Arc<BufferPool>) {
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
        self.deliver(res.sender, buffer);
    }

    /// Returns all absorbed capacity of a released reservation to the pool.
    ///
    /// Absorbed markers must all be cleared to `Allocated` *before* any node
    /// is re-freed: `try_merge` inspects sibling states, and an absorbed
    /// sibling still marked `Free` is not on any list, so merging with it
    /// would corrupt the free lists.
    pub(super) fn release_reservation_subtree(&mut self, res_level: usize, sub: &ReservedSubtree) {
        let state = unsafe { BuddyBlock::allocator_state(sub.block) };

        let mut absorbed = Vec::new();
        for level in 0..res_level {
            let first = sub.root_index << (2 * (res_level - level));
            let count = 1usize << (2 * (res_level - level));
            for index in first..first + count {
                if state.get_state(level, index) == NodeState::Free {
                    state.set_state(level, index, NodeState::Allocated);
                    absorbed.push((level, index));
                }
            }
        }

        for (level, index) in absorbed {
            let state = unsafe { BuddyBlock::allocator_state(sub.block) };
            self.try_merge(state, level, index);
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub(super) fn update_min_waiting_level_on_add(&mut self, added_level: usize) {
        match self.min_waiting_level {
            None => self.min_waiting_level = Some(added_level),
            Some(min_level) if added_level < min_level => {
                self.min_waiting_level = Some(added_level);
            }
            _ => {}
        }
    }

    pub(super) fn update_min_waiting_level_on_remove(&mut self, removed_level: usize) {
        if self.min_waiting_level == Some(removed_level)
            && self.waiting_lists[removed_level].is_empty()
        {
            self.min_waiting_level =
                (removed_level..NUM_LEVELS).find(|&level| !self.waiting_lists[level].is_empty());
        }
    }
    /// Handoff can race with cancellation. Reclaim under the existing lock:
    /// dropping Buffer here would recursively lock the allocator and deadlock.
    fn deliver(&mut self, sender: oneshot::Sender<Buffer>, buffer: Buffer) {
        if let Err(buffer) = sender.send(buffer) {
            let (level, index, block) = buffer.into_raw_parts();
            // SAFETY: the live buffer retained the block; the pool lock gives
            // exclusive access to its allocation state.
            self.try_merge(unsafe { BuddyBlock::allocator_state(block) }, level, index);
        }
    }
}
