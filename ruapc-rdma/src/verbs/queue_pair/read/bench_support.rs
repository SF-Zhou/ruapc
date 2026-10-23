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
    _sges: Vec<ReadSge>,
}

/// Runs production validation with a synthetic key, without device registration.
#[inline]
pub fn prepare(
    buffers: &[Buffer],
    requests: Vec<ReadRequest>,
    gather_limit: usize,
    lkey: u32,
) -> Result<PreparedPlan> {
    super::prepare(buffers, requests, gather_limit, |_| Ok(lkey)).map(|(reads, sges)| {
        PreparedPlan {
            _reads: reads,
            _sges: sges,
        }
    })
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

    /// Records a provider-accepted WR, mirroring the per-WR update the posting
    /// cursor performs after each successful post.
    #[inline]
    pub fn mark_accepted(&self, accepted: usize) {
        self.0.mark_accepted(accepted);
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
        Self::with_depth(64)
    }

    /// Creates a ledger whose table holds `2 * depth` slots (rounded up to a
    /// power of two). A depth of one forces collisions into the fallback map.
    pub fn with_depth(depth: u32) -> Self {
        Self(ReadState::new(depth))
    }

    /// Marks a batch as having an accepted WR, enabling the pending-list path
    /// exercised by real READ deadlines and failure scans.
    #[inline]
    pub fn register(&self, batch: &Batch) {
        self.0.register_pending(&batch.0);
    }

    /// Number of inserts that had to use the collision fallback map.
    #[inline]
    pub fn spills(&self) -> usize {
        self.0.spills()
    }

    /// Records a sequence and reports whether the entry was vacant.
    #[inline]
    pub fn insert(&self, sequence: u64, batch: &Batch) -> bool {
        assert!(sequence <= WRID::MAX_SEQUENCE, "sequence out of range");
        self.0.insert(sequence, batch.0.clone())
    }

    /// Simulates successful completion for a previously recorded sequence.
    #[inline]
    pub fn complete(&self, sequence: u64) -> bool {
        assert!(sequence <= WRID::MAX_SEQUENCE, "sequence out of range");
        if let Some(batch) = self.0.remove(sequence) {
            batch.complete_one(true);
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
