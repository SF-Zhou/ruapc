//! Buddy memory allocation implementation.
//!
//! This module implements the buddy memory allocation algorithm with 4 levels:
//! - Level 0: 1MiB (64 blocks per 64MiB)
//! - Level 1: 4MiB (16 blocks per 64MiB)
//! - Level 2: 16MiB (4 blocks per 64MiB)
//! - Level 3: 64MiB (1 block per 64MiB, the root)

use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::Arc;

use aliasable::boxed::AliasableBox;

use crate::AlignedMemory;
use crate::device::Registration;
use crate::intrusive_list::IntrusiveNode;

/// Size constants for each level.
pub const SIZE_1MIB: usize = 1024 * 1024;
pub const SIZE_4MIB: usize = 4 * SIZE_1MIB;
pub const SIZE_16MIB: usize = 4 * SIZE_4MIB;
pub const SIZE_64MIB: usize = 4 * SIZE_16MIB;

/// Number of levels in the buddy allocator.
pub const NUM_LEVELS: usize = 4;

/// Sizes for each level (indexed by level).
pub const LEVEL_SIZES: [usize; NUM_LEVELS] = [SIZE_1MIB, SIZE_4MIB, SIZE_16MIB, SIZE_64MIB];

/// Number of nodes at each level within a 64MiB block.
pub const NODES_PER_LEVEL: [usize; NUM_LEVELS] = [64, 16, 4, 1];

/// Total number of nodes in the state array: 64 + 16 + 4 + 1 = 85
pub const TOTAL_STATE_NODES: usize = 85;

#[allow(clippy::manual_div_ceil)]
pub const STATE_ARRAY_BYTES: usize = (TOTAL_STATE_NODES * 2).div_ceil(8);

/// Starting index in the state array for each level.
pub const LEVEL_STATE_OFFSETS: [usize; NUM_LEVELS] = [0, 64, 80, 84];

/// State of a node in the buddy tree.
///
/// Each node uses 2 bits (00=Allocated, 01=Free, 10=Split, 11=SplitPending).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Default)]
pub enum NodeState {
    /// The node is allocated and in use.
    #[default]
    Allocated = 0,
    /// The node is free and available for allocation.
    Free = 1,
    /// The node has been split into smaller children.
    Split = 2,
    /// The node is split and all 4 children are free, but merging has been
    /// deferred (lazy buddy). The node's intrusive list node is linked into
    /// the pool's pending-merge list for its level.
    SplitPending = 3,
}

impl NodeState {
    pub const fn as_bits(self) -> u8 {
        self as u8
    }

    pub const fn from_bits(bits: u8) -> Self {
        match bits {
            0 => Self::Allocated,
            1 => Self::Free,
            2 => Self::Split,
            3 => Self::SplitPending,
            _ => panic!("invalid bits"),
        }
    }
}

/// Data stored in each free list node.
#[derive(Debug)]
pub struct FreeNodeData {
    /// Pointer to the parent `BuddyBlock`.
    pub block: NonNull<BuddyBlock>,
}

// SAFETY: the back-pointer refers to a block retained by the pool; allocation
// state and intrusive links are accessed only under that pool's mutex.
unsafe impl Send for FreeNodeData {}
// SAFETY: sharing the back-pointer does not dereference it. Mutation of the
// pointed allocation state still requires the owning pool's mutex.
unsafe impl Sync for FreeNodeData {}

/// A free list node that can be inserted into an intrusive list.
pub type FreeNode = IntrusiveNode<FreeNodeData>;

/// A 64MiB buddy block that manages memory allocation at all levels.
///
/// Each block owns its aligned memory (via `Arc<AlignedMemory>`) and holds
/// device registrations for that memory region.
#[repr(C)]
pub struct BuddyBlock {
    /// Immutable registrations are dropped before their backing memory.
    pub registrations: Vec<Box<dyn Registration>>,
    pub memory: Arc<AlignedMemory>,
    state: BuddyState,
}

/// Mutable allocation metadata, accessed only under the owning pool's mutex.
///
/// Interior mutability keeps the region's immutable registration data readable
/// while its allocation tree changes. Nodes also remain behind UnsafeCell so
/// borrowing state never invalidates raw pointers retained by intrusive lists.
#[repr(C)]
pub(super) struct BuddyState {
    nodes: UnsafeCell<[FreeNode; TOTAL_STATE_NODES]>,
    states: UnsafeCell<[u8; STATE_ARRAY_BYTES]>,
}

impl BuddyBlock {
    /// Creates a block before publishing it to the pool or any buffers.
    pub fn new(
        memory: Arc<AlignedMemory>,
        registrations: Vec<Box<dyn Registration>>,
    ) -> AliasableBox<Self> {
        let block = AliasableBox::from_unique(Box::new(Self {
            registrations,
            memory,
            state: BuddyState {
                nodes: UnsafeCell::new(std::array::from_fn(|_| {
                    FreeNode::new(FreeNodeData {
                        block: NonNull::dangling(),
                    })
                })),
                states: UnsafeCell::new([0; STATE_ARRAY_BYTES]),
            },
        }));
        // Establish aliasable ownership before creating back-pointers. Moving a
        // unique Box later must never invalidate pointers stored in its nodes.
        let block_ptr = NonNull::from(&*block);
        // SAFETY: this new block has not escaped; its node array is exclusive.
        for node in unsafe { &mut *block.state.nodes.get() } {
            node.data.block = block_ptr;
        }
        block.state.set_state(3, 0, NodeState::Free);
        block
    }

    /// Borrows only the allocation metadata, leaving registrations immutable.
    ///
    /// # Safety
    ///
    /// `block` must belong to the locked pool and outlive the returned borrow.
    /// Its pool mutex must remain locked for every use of the returned state
    /// and of any node pointers obtained through it. No whole-block mutable
    /// reference may be created after publishing the block.
    pub(super) unsafe fn allocator_state<'a>(block: NonNull<Self>) -> &'a BuddyState {
        // SAFETY: the caller retains the block and serializes its allocator.
        // This projects the field without creating an exclusive block borrow.
        unsafe { &(*block.as_ptr()).state }
    }

    /// Gets the memory address for a node at the given level and index.
    pub fn get_memory_addr(&self, level: usize, index: usize) -> *mut u8 {
        let offset = index * LEVEL_SIZES[level];
        // Forming a pointer is safe even for an invalid node; Buffer::new's
        // caller must establish that its allocation is in bounds before use.
        self.memory.as_mut_ptr().wrapping_add(offset)
    }
}

impl BuddyState {
    /// Gets a mutable pointer to the free node for the given level and index.
    pub fn get_free_node_mut(&self, level: usize, index: usize) -> NonNull<FreeNode> {
        let flat_index = LEVEL_STATE_OFFSETS[level] + index;
        assert!(flat_index < TOTAL_STATE_NODES);
        // SAFETY: the validated index lies in this block's node array. Derive
        // raw pointers directly so later state borrows preserve linked nodes.
        unsafe { NonNull::new_unchecked(self.nodes.get().cast::<FreeNode>().add(flat_index)) }
    }

    /// Computes the index within a level from a node pointer.
    ///
    /// Given a node that was popped from `free_lists[level]`, returns its
    /// index within that level by computing its offset in the flat `nodes` array.
    pub fn node_index_in_level(&self, node: NonNull<FreeNode>, level: usize) -> usize {
        let base = self.nodes.get().cast::<FreeNode>() as usize;
        let node_addr = node.as_ptr() as usize;
        let flat_index = (node_addr - base) / std::mem::size_of::<FreeNode>();
        debug_assert!(flat_index < TOTAL_STATE_NODES);
        flat_index - LEVEL_STATE_OFFSETS[level]
    }

    /// Gets the state array index for a node at the given level and index.
    pub const fn state_index(level: usize, index: usize) -> usize {
        LEVEL_STATE_OFFSETS[level] + index
    }

    /// Gets the state of a node.
    pub const fn get_state(&self, level: usize, index: usize) -> NodeState {
        let idx = Self::state_index(level, index);
        let byte_idx = idx / 4;
        let bit_offset = (idx % 4) * 2;
        // SAFETY: the pool mutex serializes state reads and writes.
        let bits = unsafe { (*self.states.get())[byte_idx] };
        let mask = 0b11 << bit_offset;
        NodeState::from_bits((bits & mask) >> bit_offset)
    }

    /// Sets the state of a node.
    pub const fn set_state(&self, level: usize, index: usize, state: NodeState) {
        let idx = Self::state_index(level, index);
        let byte_idx = idx / 4;
        let bit_offset = (idx % 4) * 2;
        // SAFETY: the pool mutex serializes state reads and writes.
        let bits = unsafe { (*self.states.get())[byte_idx] };
        let mask = !(0b11 << bit_offset);
        let new_bits = (bits & mask) | (state.as_bits() << bit_offset);
        // SAFETY: only this byte is written; node storage is borrowed separately.
        unsafe { (*self.states.get())[byte_idx] = new_bits };
    }

    /// Gets the parent level and index for a node.
    /// Returns `None` for level 3 (root) nodes.
    pub const fn get_parent(level: usize, index: usize) -> Option<(usize, usize)> {
        if level >= 3 {
            None
        } else {
            Some((level + 1, index / 4))
        }
    }

    /// Gets the sibling indices for a node (all 4 siblings including itself).
    pub const fn get_siblings(index: usize) -> [usize; 4] {
        let base = (index / 4) * 4;
        [base, base + 1, base + 2, base + 3]
    }

    /// Gets the first child index for a node.
    /// Returns `None` for level 0 (leaf) nodes.
    pub const fn get_first_child(level: usize, index: usize) -> Option<(usize, usize)> {
        if level == 0 {
            None
        } else {
            Some((level - 1, index * 4))
        }
    }
}

// SAFETY: BuddyBlock can be sent between threads. The FreeNode fields contain
// raw pointers that are only accessed while holding the pool's mutex lock.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for BuddyBlock {}
// SAFETY: registrations/memory are immutable; allocation state is reachable
// only through allocator_state, whose contract requires the owning pool mutex.
unsafe impl Sync for BuddyBlock {}

/// Calculates the allocation level for a given size.
/// Returns `None` if the size is 0 or exceeds 64MiB.
#[inline]
pub const fn size_to_level(size: usize) -> Option<usize> {
    if size == 0 {
        return None;
    }
    if size <= SIZE_1MIB {
        Some(0)
    } else if size <= SIZE_4MIB {
        Some(1)
    } else if size <= SIZE_16MIB {
        Some(2)
    } else if size <= SIZE_64MIB {
        Some(3)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_to_level() {
        assert_eq!(size_to_level(0), None);
        assert_eq!(size_to_level(1), Some(0));
        assert_eq!(size_to_level(SIZE_1MIB), Some(0));
        assert_eq!(size_to_level(SIZE_1MIB + 1), Some(1));
        assert_eq!(size_to_level(SIZE_4MIB), Some(1));
        assert_eq!(size_to_level(SIZE_4MIB + 1), Some(2));
        assert_eq!(size_to_level(SIZE_16MIB), Some(2));
        assert_eq!(size_to_level(SIZE_16MIB + 1), Some(3));
        assert_eq!(size_to_level(SIZE_64MIB), Some(3));
        assert_eq!(size_to_level(SIZE_64MIB + 1), None);
    }

    #[test]
    fn test_state_index() {
        assert_eq!(BuddyState::state_index(0, 0), 0);
        assert_eq!(BuddyState::state_index(0, 63), 63);
        assert_eq!(BuddyState::state_index(1, 0), 64);
        assert_eq!(BuddyState::state_index(1, 15), 79);
        assert_eq!(BuddyState::state_index(2, 0), 80);
        assert_eq!(BuddyState::state_index(2, 3), 83);
        assert_eq!(BuddyState::state_index(3, 0), 84);
    }

    #[test]
    fn test_get_parent() {
        assert_eq!(BuddyState::get_parent(0, 0), Some((1, 0)));
        assert_eq!(BuddyState::get_parent(0, 3), Some((1, 0)));
        assert_eq!(BuddyState::get_parent(0, 4), Some((1, 1)));
        assert_eq!(BuddyState::get_parent(0, 63), Some((1, 15)));
        assert_eq!(BuddyState::get_parent(1, 0), Some((2, 0)));
        assert_eq!(BuddyState::get_parent(1, 15), Some((2, 3)));
        assert_eq!(BuddyState::get_parent(2, 0), Some((3, 0)));
        assert_eq!(BuddyState::get_parent(2, 3), Some((3, 0)));
        assert_eq!(BuddyState::get_parent(3, 0), None);
    }

    #[test]
    fn test_get_siblings() {
        assert_eq!(BuddyState::get_siblings(0), [0, 1, 2, 3]);
        assert_eq!(BuddyState::get_siblings(2), [0, 1, 2, 3]);
        assert_eq!(BuddyState::get_siblings(4), [4, 5, 6, 7]);
        assert_eq!(BuddyState::get_siblings(5), [4, 5, 6, 7]);
    }

    #[test]
    fn test_get_first_child() {
        assert_eq!(BuddyState::get_first_child(0, 0), None);
        assert_eq!(BuddyState::get_first_child(1, 0), Some((0, 0)));
        assert_eq!(BuddyState::get_first_child(1, 1), Some((0, 4)));
        assert_eq!(BuddyState::get_first_child(2, 0), Some((1, 0)));
        assert_eq!(BuddyState::get_first_child(3, 0), Some((2, 0)));
    }

    #[test]
    fn test_buddy_block_creation() {
        let mem = Arc::new(crate::AlignedMemory::new(SIZE_64MIB).unwrap());
        let block = BuddyBlock::new(mem, Vec::new());

        // Root is free
        assert_eq!(block.state.get_state(3, 0), NodeState::Free);

        // All other nodes are allocated (default)
        for i in 0..64 {
            assert_eq!(block.state.get_state(0, i), NodeState::Allocated);
        }
        for i in 0..16 {
            assert_eq!(block.state.get_state(1, i), NodeState::Allocated);
        }
        for i in 0..4 {
            assert_eq!(block.state.get_state(2, i), NodeState::Allocated);
        }
    }

    #[test]
    fn test_memory_address_calculation() {
        let mem = Arc::new(crate::AlignedMemory::new(SIZE_64MIB).unwrap());
        let base = mem.as_mut_ptr();
        let block = BuddyBlock::new(mem, Vec::new());

        assert_eq!(block.get_memory_addr(3, 0), base);
        assert_eq!(block.get_memory_addr(2, 0), base);
        assert_eq!(block.get_memory_addr(2, 1), unsafe { base.add(SIZE_16MIB) });
        assert_eq!(block.get_memory_addr(1, 0), base);
        assert_eq!(block.get_memory_addr(1, 4), unsafe { base.add(SIZE_16MIB) });
        assert_eq!(block.get_memory_addr(0, 0), base);
        assert_eq!(block.get_memory_addr(0, 1), unsafe { base.add(SIZE_1MIB) });
    }
}
