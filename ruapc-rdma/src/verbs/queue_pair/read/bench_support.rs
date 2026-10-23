//! Feature-gated bridge for the device-free READ benchmarks.
//!
//! These wrappers create their own ownership state. They cannot borrow batches
//! from a `QueuePair` or `ReadPosting`, expose prepared addresses, or manufacture
//! a completion token. There is no path from an in-flight safe READ to these
//! manually settled batches.

use super::*;

/// Keeps the production prepared descriptors alive until the benchmark drops
/// them, without exposing addresses or providing any way to post them.
pub struct PreparedPlan {
    _reads: Vec<PreparedRead>,
}

/// Runs production validation with a synthetic key, without device registration.
#[inline]
pub fn prepare(
    buffers: &[Buffer],
    requests: Vec<ReadRequest>,
    gather_limit: usize,
    lkey: u32,
) -> Result<PreparedPlan> {
    super::prepare(buffers, requests, gather_limit, |_| Ok(lkey))
        .map(|reads| PreparedPlan { _reads: reads })
}

/// A manually settled batch created exclusively for CPU bookkeeping benchmarks.
#[derive(Clone)]
pub struct Batch(Arc<ReadBatch>);

impl Batch {
    #[inline]
    pub fn new(count: usize, buffers: Vec<Buffer>) -> (Self, ReadReceiver) {
        let (batch, receiver) = ReadBatch::new(count, buffers, None, None);
        (Self(batch), receiver)
    }

    #[inline]
    pub fn complete_one(&self, success: bool) {
        self.0.complete_one(success);
    }

    #[inline]
    pub fn abort_unposted(&self, count: usize) {
        self.0.abort_unposted(count);
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Relaxed)
    }
}

/// An isolated instance of the production READ ledger. It is never attached to
/// a QP and can contain only benchmark-created batches.
pub struct Ledger(ReadState);

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

impl Ledger {
    pub fn new() -> Self {
        Self(ReadState::new())
    }

    /// Records a sequence and reports whether the entry was vacant.
    #[inline]
    pub fn insert(&self, sequence: u64, batch: &Batch) -> bool {
        assert!(sequence <= WRID::MAX_SEQUENCE, "sequence out of range");
        self.0
            .batches
            .insert(WRID::new(crate::WRType::Read, sequence), batch.0.clone())
            .is_none()
    }

    /// Simulates successful completion for a previously recorded sequence.
    #[inline]
    pub fn complete(&self, sequence: u64) -> bool {
        assert!(sequence <= WRID::MAX_SEQUENCE, "sequence out of range");
        if let Some((_, batch)) = self
            .0
            .batches
            .remove(&WRID::new(crate::WRType::Read, sequence))
        {
            batch.complete_one(true);
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.batches.is_empty()
    }
}
