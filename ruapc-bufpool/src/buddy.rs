//! Buddy memory allocation implementation.
//!
//! This module implements the buddy memory allocation algorithm with 6 levels:
//! - Level 0: 64KiB (1024 blocks per 64MiB)
//! - Level 1: 256KiB (256 blocks per 64MiB)
//! - Level 2: 1MiB (64 blocks per 64MiB)
//! - Level 3: 4MiB (16 blocks per 64MiB)
//! - Level 4: 16MiB (4 blocks per 64MiB)
//! - Level 5: 64MiB (1 block per 64MiB, the root)

use std::ptr::NonNull;
use std::sync::Arc;

use crate::AlignedMemory;
use crate::device::Registration;
use crate::intrusive_list::IntrusiveNode;

/// Size constants for each level.
pub const SIZE_64KIB: usize = 64 * 1024;
pub const SIZE_256KIB: usize = 4 * SIZE_64KIB;
pub const SIZE_1MIB: usize = 4 * SIZE_256KIB;
pub const SIZE_4MIB: usize = 4 * SIZE_1MIB;
pub const SIZE_16MIB: usize = 4 * SIZE_4MIB;
pub const SIZE_64MIB: usize = 4 * SIZE_16MIB;

/// Number of levels in the buddy allocator.
pub const NUM_LEVELS: usize = 6;
pub const ROOT_LEVEL: usize = NUM_LEVELS - 1;

/// Sizes for each level (indexed by level).
pub const LEVEL_SIZES: [usize; NUM_LEVELS] = [
    SIZE_64KIB,
    SIZE_256KIB,
    SIZE_1MIB,
    SIZE_4MIB,
    SIZE_16MIB,
    SIZE_64MIB,
];

/// Number of nodes at each level within a 64MiB block.
pub const NODES_PER_LEVEL: [usize; NUM_LEVELS] = [1024, 256, 64, 16, 4, 1];

/// Total number of nodes in the state array: 1024 + 256 + 64 + 16 + 4 + 1 = 1365
pub const TOTAL_STATE_NODES: usize = 1365;

#[allow(clippy::manual_div_ceil)]
pub const STATE_ARRAY_BYTES: usize = (TOTAL_STATE_NODES * 2).div_ceil(8);

/// Starting index in the state array for each level.
pub const LEVEL_STATE_OFFSETS: [usize; NUM_LEVELS] = [0, 1024, 1280, 1344, 1360, 1364];

/// State of a node in the buddy tree.
///
/// Each node uses 2 bits (00=Allocated, 01=Free, 10=Split).
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

// SAFETY: FreeNodeData only contains NonNull which is Send if the pointed type is Send
unsafe impl Send for FreeNodeData {}
// SAFETY: FreeNodeData only contains NonNull which is Sync if the pointed type is Sync
unsafe impl Sync for FreeNodeData {}

/// A free list node that can be inserted into an intrusive list.
pub type FreeNode = IntrusiveNode<FreeNodeData>;

/// A 64MiB buddy block that manages memory allocation at all levels.
///
/// Each block owns its aligned memory (via `Arc<AlignedMemory>`) and holds
/// device registrations for that memory region.
pub struct BuddyBlock {
    /// The aligned memory backing this block. Shared with device registrations
    /// (e.g. TcpDevice's registry map keeps an `Arc` clone).
    pub memory: Arc<AlignedMemory>,

    /// Device registrations for this block's memory region.
    pub registrations: Vec<Box<dyn Registration>>,

    /// Bit-packed allocation state for all nodes in the buddy tree.
    pub states: [u8; STATE_ARRAY_BYTES],

    /// Free list nodes for all levels, laid out as a flat array:
    /// [level0: 64 nodes][level1: 16 nodes][level2: 4 nodes][level3: 1 node]
    /// Use `LEVEL_STATE_OFFSETS[level] + index` to compute the flat index.
    pub nodes: [FreeNode; TOTAL_STATE_NODES],
}

impl BuddyBlock {
    /// Creates a new `BuddyBlock` managing the given aligned memory.
    ///
    /// `registrations` are the device registrations for this memory region.
    pub fn new(memory: Arc<AlignedMemory>, registrations: Vec<Box<dyn Registration>>) -> Box<Self> {
        let mut block = Box::new(Self {
            memory,
            registrations,
            states: [0u8; STATE_ARRAY_BYTES],
            nodes: std::array::from_fn(|_| {
                FreeNode::new(FreeNodeData {
                    block: NonNull::dangling(),
                })
            }),
        });

        let block_ptr = NonNull::new(std::ptr::from_mut::<Self>(block.as_mut())).unwrap();

        // Initialize all free list nodes with correct back-pointer
        for node in block.nodes.iter_mut() {
            node.data = FreeNodeData { block: block_ptr };
        }

        // Initialize all states to Allocated (0x00) - already zeroed
        // Set root node to Free
        block.set_state(ROOT_LEVEL, 0, NodeState::Free);

        block
    }

    /// Gets a mutable pointer to the free node for the given level and index.
    pub fn get_free_node_mut(&mut self, level: usize, index: usize) -> NonNull<FreeNode> {
        let flat_index = LEVEL_STATE_OFFSETS[level] + index;
        NonNull::new(&mut self.nodes[flat_index]).unwrap()
    }

    /// Computes the index within a level from a node pointer.
    ///
    /// Given a node that was popped from `free_lists[level]`, returns its
    /// index within that level by computing its offset in the flat `nodes` array.
    pub fn node_index_in_level(&self, node: NonNull<FreeNode>, level: usize) -> usize {
        let base = self.nodes.as_ptr() as usize;
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
        let bits = self.states[byte_idx];
        let mask = 0b11 << bit_offset;
        NodeState::from_bits((bits & mask) >> bit_offset)
    }

    /// Sets the state of a node.
    pub const fn set_state(&mut self, level: usize, index: usize, state: NodeState) {
        let idx = Self::state_index(level, index);
        let byte_idx = idx / 4;
        let bit_offset = (idx % 4) * 2;
        let bits = self.states[byte_idx];
        let mask = !(0b11 << bit_offset);
        let new_bits = (bits & mask) | (state.as_bits() << bit_offset);
        self.states[byte_idx] = new_bits;
    }

    /// Gets the memory address for a node at the given level and index.
    pub fn get_memory_addr(&self, level: usize, index: usize) -> *mut u8 {
        let offset = index * LEVEL_SIZES[level];
        unsafe { self.memory.as_mut_ptr().add(offset) }
    }

    /// Gets the parent level and index for a node.
    /// Returns `None` for root level nodes.
    pub const fn get_parent(level: usize, index: usize) -> Option<(usize, usize)> {
        if level >= ROOT_LEVEL {
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
unsafe impl Sync for BuddyBlock {}

/// Calculates the allocation level for a given size.
/// Returns `None` if the size is 0 or exceeds 64MiB.
#[inline]
pub const fn size_to_level(size: usize) -> Option<usize> {
    if size == 0 {
        return None;
    }
    if size <= SIZE_64KIB {
        Some(0)
    } else if size <= SIZE_256KIB {
        Some(1)
    } else if size <= SIZE_1MIB {
        Some(2)
    } else if size <= SIZE_4MIB {
        Some(3)
    } else if size <= SIZE_16MIB {
        Some(4)
    } else if size <= SIZE_64MIB {
        Some(5)
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
        assert_eq!(size_to_level(SIZE_64KIB), Some(0));
        assert_eq!(size_to_level(SIZE_64KIB + 1), Some(1));
        assert_eq!(size_to_level(SIZE_256KIB), Some(1));
        assert_eq!(size_to_level(SIZE_256KIB + 1), Some(2));
        assert_eq!(size_to_level(SIZE_1MIB), Some(2));
        assert_eq!(size_to_level(SIZE_1MIB + 1), Some(3));
        assert_eq!(size_to_level(SIZE_4MIB), Some(3));
        assert_eq!(size_to_level(SIZE_4MIB + 1), Some(4));
        assert_eq!(size_to_level(SIZE_16MIB), Some(4));
        assert_eq!(size_to_level(SIZE_16MIB + 1), Some(5));
        assert_eq!(size_to_level(SIZE_64MIB), Some(5));
        assert_eq!(size_to_level(SIZE_64MIB + 1), None);
    }

    #[test]
    fn test_state_index() {
        assert_eq!(BuddyBlock::state_index(0, 0), 0);
        assert_eq!(BuddyBlock::state_index(0, 1023), 1023);
        assert_eq!(BuddyBlock::state_index(1, 0), 1024);
        assert_eq!(BuddyBlock::state_index(1, 255), 1279);
        assert_eq!(BuddyBlock::state_index(2, 0), 1280);
        assert_eq!(BuddyBlock::state_index(2, 63), 1343);
        assert_eq!(BuddyBlock::state_index(3, 0), 1344);
        assert_eq!(BuddyBlock::state_index(3, 15), 1359);
        assert_eq!(BuddyBlock::state_index(4, 0), 1360);
        assert_eq!(BuddyBlock::state_index(4, 3), 1363);
        assert_eq!(BuddyBlock::state_index(5, 0), 1364);
    }

    #[test]
    fn test_get_parent() {
        assert_eq!(BuddyBlock::get_parent(0, 0), Some((1, 0)));
        assert_eq!(BuddyBlock::get_parent(0, 3), Some((1, 0)));
        assert_eq!(BuddyBlock::get_parent(0, 4), Some((1, 1)));
        assert_eq!(BuddyBlock::get_parent(0, 63), Some((1, 15)));
        assert_eq!(BuddyBlock::get_parent(1, 0), Some((2, 0)));
        assert_eq!(BuddyBlock::get_parent(1, 255), Some((2, 63)));
        assert_eq!(BuddyBlock::get_parent(2, 0), Some((3, 0)));
        assert_eq!(BuddyBlock::get_parent(2, 63), Some((3, 15)));
        assert_eq!(BuddyBlock::get_parent(3, 0), Some((4, 0)));
        assert_eq!(BuddyBlock::get_parent(4, 0), Some((5, 0)));
        assert_eq!(BuddyBlock::get_parent(5, 0), None);
    }

    #[test]
    fn test_get_siblings() {
        assert_eq!(BuddyBlock::get_siblings(0), [0, 1, 2, 3]);
        assert_eq!(BuddyBlock::get_siblings(2), [0, 1, 2, 3]);
        assert_eq!(BuddyBlock::get_siblings(4), [4, 5, 6, 7]);
        assert_eq!(BuddyBlock::get_siblings(5), [4, 5, 6, 7]);
    }

    #[test]
    fn test_get_first_child() {
        assert_eq!(BuddyBlock::get_first_child(0, 0), None);
        assert_eq!(BuddyBlock::get_first_child(1, 0), Some((0, 0)));
        assert_eq!(BuddyBlock::get_first_child(1, 1), Some((0, 4)));
        assert_eq!(BuddyBlock::get_first_child(2, 0), Some((1, 0)));
        assert_eq!(BuddyBlock::get_first_child(3, 0), Some((2, 0)));
        assert_eq!(BuddyBlock::get_first_child(4, 0), Some((3, 0)));
        assert_eq!(BuddyBlock::get_first_child(5, 0), Some((4, 0)));
    }

    #[test]
    fn test_buddy_block_creation() {
        let mem = Arc::new(crate::AlignedMemory::new(SIZE_64MIB).unwrap());
        let block = BuddyBlock::new(mem, Vec::new());

        // Root is free
        assert_eq!(block.get_state(ROOT_LEVEL, 0), NodeState::Free);

        // All other nodes are allocated (default)
        for level in 0..ROOT_LEVEL {
            for i in 0..NODES_PER_LEVEL[level] {
                assert_eq!(block.get_state(level, i), NodeState::Allocated);
            }
        }
    }

    #[test]
    fn test_memory_address_calculation() {
        let mem = Arc::new(crate::AlignedMemory::new(SIZE_64MIB).unwrap());
        let base = mem.as_mut_ptr();
        let block = BuddyBlock::new(mem, Vec::new());

        assert_eq!(block.get_memory_addr(5, 0), base);
        assert_eq!(block.get_memory_addr(4, 0), base);
        assert_eq!(block.get_memory_addr(4, 1), unsafe { base.add(SIZE_16MIB) });
        assert_eq!(block.get_memory_addr(3, 0), base);
        assert_eq!(block.get_memory_addr(3, 4), unsafe { base.add(SIZE_16MIB) });
        assert_eq!(block.get_memory_addr(2, 0), base);
        assert_eq!(block.get_memory_addr(2, 1), unsafe { base.add(SIZE_1MIB) });
        assert_eq!(block.get_memory_addr(0, 0), base);
        assert_eq!(block.get_memory_addr(0, 1), unsafe { base.add(SIZE_64KIB) });
    }
}
