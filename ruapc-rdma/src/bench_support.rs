//! Internal measurement interfaces, available only with `bench-internals`.
//!
//! These components own only buffers explicitly passed to them. They cannot
//! access a QP's ownership tables or issue completion proofs, so measurements
//! never grant authority to reclaim buffers posted to a real device.

use crate::{CompletionCursor, WRID, WrBuffers};

pub use crate::verbs::read_bench_support as read;

/// An independently owned instance of the production buffer table.
/// Public callers must not alias the tag state with an out-of-range sequence.
pub struct WrSlots(crate::verbs::WrSlots);

impl WrSlots {
    pub fn new(depth: u32) -> Self {
        Self(crate::verbs::WrSlots::new(depth))
    }

    #[inline]
    pub fn insert(&self, id: u64, buffer: WrBuffers) -> Result<(), WrBuffers> {
        assert!(id <= WRID::MAX_SEQUENCE, "sequence out of range");
        self.0.insert(id, buffer)
    }

    #[inline]
    pub fn take(&self, id: u64) -> Option<WrBuffers> {
        assert!(id <= WRID::MAX_SEQUENCE, "sequence out of range");
        self.0.take(id)
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn slot_bytes() -> usize {
        crate::verbs::WrSlots::slot_bytes()
    }
}

/// Advances the production sweep cursor without accessing a QP or buffers.
#[inline]
pub fn sweep(
    cursor: &mut CompletionCursor,
    first_sequence: u64,
    completed: u64,
) -> std::ops::Range<u64> {
    assert!(completed <= WRID::MAX_SEQUENCE, "sequence out of range");
    cursor.sweep_to(first_sequence, completed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "sequence out of range")]
    fn take_rejects_sequences_that_alias_the_writing_tag() {
        WrSlots::new(1).take(u64::MAX);
    }

    #[test]
    #[should_panic(expected = "sequence out of range")]
    fn insert_rejects_sequences_that_alias_reserved_tags() {
        let pool =
            ruapc_bufpool::BufferPoolBuilder::new(std::sync::Arc::new(ruapc_bufpool::EmptyDevices))
                .build();
        WrSlots::new(1)
            .insert(u64::MAX - 1, pool.allocate(64).unwrap().into())
            .unwrap();
    }
}
