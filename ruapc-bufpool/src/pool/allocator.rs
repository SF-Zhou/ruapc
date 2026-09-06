//! Buddy allocation state. Every operation requires the owning pool's mutex.
//! Free-list membership, node states, waiting requests and reservations change
//! together under that lock; no buffer may be dropped while it is held.

use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use aliasable::boxed::AliasableBox;
use tokio::sync::oneshot;

use crate::buddy::{BuddyBlock, BuddyState, FreeNode, NUM_LEVELS, NodeState, SIZE_64MIB};
use crate::buffer::Buffer;
use crate::intrusive_list::IntrusiveList;

use super::BufferPool;

mod waiters;
use waiters::{Reservation, Waiter};

/// Internal pool state protected by the mutex.
pub(super) struct BuddyAllocator {
    /// Total memory budgeted, in bytes: installed blocks plus any 64 MiB
    /// reservations for blocks currently being created and registered
    /// outside the lock.
    pub(super) allocated_memory: usize,
    /// Per-level merge watermarks for lazy buddy merging.
    /// See [`super::BufferPoolBuilder::merge_watermarks`].
    merge_watermarks: [usize; NUM_LEVELS],
    /// All allocated buddy blocks. Each entry is wrapped in `AliasableBox` to
    /// opt out of the `noalias` guarantee that `Box` carries. This is required
    /// because `Buffer` and intrusive free-list nodes hold `NonNull<BuddyBlock>`
    /// pointers that alias the box contents. Without `AliasableBox`, the
    /// compiler could assume exclusive access through the `Box` and mis-optimize
    /// accesses through the raw pointers.
    pub(super) blocks: Vec<AliasableBox<BuddyBlock>>,
    pub(super) free_lists: [IntrusiveList<crate::buddy::FreeNodeData>; NUM_LEVELS],
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
    pub(super) pending_lists: [IntrusiveList<crate::buddy::FreeNodeData>; NUM_LEVELS],
    waiting_lists: [VecDeque<Waiter>; NUM_LEVELS],
    min_waiting_level: Option<usize>,
    /// Active anti-starvation reservation, if any. See [`Reservation`].
    reservation: Option<Reservation>,
}

impl BuddyAllocator {
    pub(super) fn new(merge_watermarks: [usize; NUM_LEVELS]) -> Self {
        Self {
            allocated_memory: 0,
            merge_watermarks,
            blocks: Vec::new(),
            free_lists: std::array::from_fn(|_| IntrusiveList::new()),
            pending_lists: std::array::from_fn(|_| IntrusiveList::new()),
            waiting_lists: std::array::from_fn(|_| VecDeque::new()),
            min_waiting_level: None,
            reservation: None,
        }
    }

    /// Registers demand while the allocation mutex is held, preventing a free
    /// from racing between the capacity check and registration.
    pub(super) fn wait_for(
        &mut self,
        level: usize,
        pool: &BufferPool,
    ) -> oneshot::Receiver<Buffer> {
        let (sender, receiver) = oneshot::channel();
        self.waiting_lists[level].push_back(Waiter {
            since: Instant::now(),
            sender,
        });
        self.update_min_waiting_level_on_add(level);
        // Paired with return_chunk's checks and the cache reclaim sweep.
        pool.has_demand.store(true, Ordering::SeqCst);
        receiver
    }

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
    pub(super) fn try_reserve_block(&mut self, max_memory: usize) -> Result<()> {
        if max_memory.saturating_sub(self.allocated_memory) < SIZE_64MIB {
            return Err(Error::new(ErrorKind::OutOfMemory, "memory limit reached"));
        }
        self.allocated_memory += SIZE_64MIB;
        Ok(())
    }

    /// Attempts to allocate from local state: free lists first, then
    /// demand-driven coalescing of deferred quads. Does not grow the pool.
    pub(super) fn try_allocate_local(
        &mut self,
        level: usize,
        pool: &Arc<BufferPool>,
    ) -> Option<Buffer> {
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
        // SAFETY: the block is installed and this allocator holds the pool mutex.
        let state = unsafe { BuddyBlock::allocator_state(block) };
        let index_in_level = state.node_index_in_level(node, from_level);

        // Taking this node breaks its (previously complete) buddy quad, so
        // its parent must leave the pending-merge list, if it was on it.
        self.demote_pending_parent(state, from_level, index_in_level);

        if from_level == target_level {
            state.set_state(from_level, index_in_level, NodeState::Allocated);
            let ptr = unsafe { block.as_ref().get_memory_addr(from_level, index_in_level) };

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
        let state = unsafe { BuddyBlock::allocator_state(block) };

        let mut current_level = from_level;
        let mut current_index = from_index;

        while current_level > target_level {
            state.set_state(current_level, current_index, NodeState::Split);

            let (child_level, first_child_index) =
                BuddyState::get_first_child(current_level, current_index).unwrap();

            // Add siblings 1-3 to free list (child 0 will be used or split further)
            for i in 1..4 {
                let child_index = first_child_index + i;
                state.set_state(child_level, child_index, NodeState::Free);

                let node = state.get_free_node_mut(child_level, child_index);
                unsafe {
                    self.push_free(child_level, node);
                }
            }

            current_level = child_level;
            current_index = first_child_index;
        }

        state.set_state(current_level, current_index, NodeState::Allocated);
        let ptr = unsafe { block.as_ref().get_memory_addr(current_level, current_index) };

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
    /// [`BuddyAllocator::try_reserve_block`] before the block was created; the
    /// caller is expected to allocate its own buffer and then call
    /// [`BuddyAllocator::serve_waiters`] under the same lock acquisition.
    pub(super) fn install_block(&mut self, block: AliasableBox<BuddyBlock>) {
        let block_ptr = NonNull::new(std::ptr::from_ref::<BuddyBlock>(&block).cast_mut()).unwrap();

        // Add root node to level 3 free list.
        unsafe {
            let node = BuddyBlock::allocator_state(block_ptr).get_free_node_mut(3, 0);
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
        if self.min_waiting_level.is_none() && self.reservation.is_none() {
            // Ordinary frees need only the buddy state. Avoid walking waiter
            // policy and touching its cache lines when no request is queued.
            self.try_merge(unsafe { BuddyBlock::allocator_state(block) }, level, index);
            if pool.has_demand.load(Ordering::Relaxed) {
                pool.has_demand.store(false, Ordering::SeqCst);
            }
            return;
        }
        if !self.try_absorb_into_reservation(pool, level, index, block) {
            let state = unsafe { BuddyBlock::allocator_state(block) };
            self.try_merge(state, level, index);
            // Starvation check runs before serving so that a freshly
            // activated reservation can claim this free ahead of smaller
            // waiters.
            self.check_starvation(pool);
        }
        self.serve_waiters(pool);
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

    fn try_merge(&mut self, state: &BuddyState, level: usize, index: usize) {
        let mut current_level = level;
        let mut current_index = index;

        loop {
            if current_level >= 3 {
                state.set_state(current_level, current_index, NodeState::Free);
                let node = state.get_free_node_mut(current_level, current_index);
                unsafe {
                    self.push_free(current_level, node);
                }
                return;
            }

            let siblings = BuddyState::get_siblings(current_index);

            // Note: `current_index` itself is not yet marked Free, so its
            // parent cannot already be SplitPending at this point.
            let quad_complete = siblings.iter().all(|&idx| {
                idx == current_index || state.get_state(current_level, idx) == NodeState::Free
            });

            if quad_complete && self.should_merge(current_level) {
                // Eager merge: remove siblings from free list and ascend.
                for &sibling_idx in &siblings {
                    if sibling_idx != current_index {
                        let node = state.get_free_node_mut(current_level, sibling_idx);
                        unsafe {
                            self.remove_free(current_level, node);
                        }
                    }
                    state.set_state(current_level, sibling_idx, NodeState::Allocated);
                }

                let (parent_level, parent_index) =
                    BuddyState::get_parent(current_level, current_index).unwrap();
                current_level = parent_level;
                current_index = parent_index;
                continue;
            }

            // Stop here: the freed node stays at this level.
            state.set_state(current_level, current_index, NodeState::Free);
            let node = state.get_free_node_mut(current_level, current_index);
            unsafe {
                self.push_free(current_level, node);
            }

            if quad_complete {
                // Deferred merge (lazy buddy): record the parent on the
                // pending-merge list so coalescing later is O(1) per quad.
                let (parent_level, parent_index) =
                    BuddyState::get_parent(current_level, current_index).unwrap();
                self.defer_merge(state, parent_level, parent_index);
            }

            return;
        }
    }

    /// A complete free quad is recorded exactly once, on its parent's node.
    fn defer_merge(&mut self, state: &BuddyState, level: usize, index: usize) {
        debug_assert_eq!(state.get_state(level, index), NodeState::Split);
        state.set_state(level, index, NodeState::SplitPending);
        let node = state.get_free_node_mut(level, index);
        // SAFETY: Split nodes belong to neither list; all children are free.
        unsafe {
            self.pending_lists[level].push_front(node);
        }
    }

    /// Removes the parent of `(level, index)` from the pending-merge list if
    /// it was there. Must be called whenever a free node is taken out of its
    /// free list for allocation, since that breaks the complete buddy quad.
    fn demote_pending_parent(&mut self, state: &BuddyState, level: usize, index: usize) {
        if let Some((parent_level, parent_index)) = BuddyState::get_parent(level, index)
            && state.get_state(parent_level, parent_index) == NodeState::SplitPending
        {
            let parent_node = state.get_free_node_mut(parent_level, parent_index);
            unsafe {
                self.pending_lists[parent_level].remove(parent_node);
            }
            state.set_state(parent_level, parent_index, NodeState::Split);
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
                let state = unsafe { BuddyBlock::allocator_state(block) };
                let parent_index = state.node_index_in_level(parent_node, parent_level);
                debug_assert_eq!(
                    state.get_state(parent_level, parent_index),
                    NodeState::SplitPending
                );

                // Merge the quad: all 4 children are free by invariant.
                let (child_level, first_child) =
                    BuddyState::get_first_child(parent_level, parent_index).unwrap();
                for k in 0..4 {
                    debug_assert_eq!(
                        state.get_state(child_level, first_child + k),
                        NodeState::Free
                    );
                    let node = state.get_free_node_mut(child_level, first_child + k);
                    unsafe {
                        self.remove_free(child_level, node);
                    }
                    state.set_state(child_level, first_child + k, NodeState::Allocated);
                }

                state.set_state(parent_level, parent_index, NodeState::Free);
                let node = state.get_free_node_mut(parent_level, parent_index);
                unsafe {
                    self.push_free(parent_level, node);
                }
                merged_any = true;

                // Cascade: the merged node may complete its own quad.
                if parent_level < 3 {
                    let siblings = BuddyState::get_siblings(parent_index);
                    let quad_complete = siblings
                        .iter()
                        .all(|&idx| state.get_state(parent_level, idx) == NodeState::Free);
                    if quad_complete {
                        let (gp_level, gp_index) =
                            BuddyState::get_parent(parent_level, parent_index).unwrap();
                        self.defer_merge(state, gp_level, gp_index);
                    }
                }
            }
        }

        merged_any
    }
}

// Note: BuddyAllocator uses default drop. Each BuddyBlock's fields are dropped in
// declaration order: `registrations` is dropped before `memory`, ensuring device
// deregistration (e.g. ibv_dereg_mr) happens while the memory is still valid.
