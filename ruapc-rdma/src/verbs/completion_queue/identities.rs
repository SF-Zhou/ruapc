//! CQ-local QPN leases and non-repeating per-QP work request sequences.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use super::CompletionQueue;
use crate::{ErrorKind, Result, WRID, WRType};

/// One incarnation of a hardware QP number in a particular CQ.
///
/// QPN reuse starts above all sequences allocated by earlier occupants,
/// including failed posts. A retained completion from an earlier QP cannot
/// therefore release a replacement QP's memory, even after QPN reuse.
/// This descriptor alone is not completion evidence: completing work also
/// requires a token issued by the originating CQ.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CompletionIdentity {
    first_sequence: u64,
    qp_num: u32,
}

impl CompletionIdentity {
    #[inline]
    pub fn qp_num(self) -> u32 {
        self.qp_num
    }

    /// First sequence belonging to this incarnation of the QPN.
    #[inline]
    pub fn first_sequence(self) -> u64 {
        self.first_sequence
    }

    /// Rejects another QPN or a sequence from an earlier incarnation.
    /// This does not establish that the work was posted or completed.
    #[inline]
    pub fn contains(self, qp_num: u32, wrid: WRID) -> bool {
        qp_num == self.qp_num && wrid.sequence() >= self.first_sequence
    }

    #[inline]
    pub(crate) fn encode(self, kind: WRType, sequence: u64) -> WRID {
        debug_assert!(sequence >= self.first_sequence);
        WRID::new(kind, sequence)
    }
}

/// CQ-local identity bookkeeping, sampled under the setup-only registry lock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QpRegistryStats {
    /// QPNs currently held by a QP or by a creation/destruction transaction.
    pub active: usize,
    /// Sequence floor for future registrations, advanced when leases retire.
    pub next_sequence_floor: u64,
}

/// Only QP creation/destruction and explicit introspection take the CQ lock.
/// A shared retired floor remembers old identities without storing old QPNs.
#[derive(Default)]
pub(super) struct IdentityRegistry {
    live: HashMap<u32, u64>,
    retired_floor: u64,
}

impl IdentityRegistry {
    pub(super) fn register(&mut self, qp_num: u32) -> Result<CompletionIdentity> {
        // A provider can reuse a QPN after destroy_qp returns but before its
        // old lease is dropped. Do not publish an identity using an old floor.
        if self.live.contains_key(&qp_num) {
            return Err(ErrorKind::CompletionIdentityInUse.into());
        }
        if self.retired_floor > WRID::MAX_SEQUENCE {
            return Err(ErrorKind::WorkRequestIdsExhausted.into());
        }
        self.live.insert(qp_num, self.retired_floor);
        Ok(CompletionIdentity {
            qp_num,
            first_sequence: self.retired_floor,
        })
    }

    fn release(&mut self, identity: CompletionIdentity, send_next: u64, recv_next: u64) {
        assert_eq!(
            self.live.get(&identity.qp_num),
            Some(&identity.first_sequence)
        );
        // Even an unused QP receives a distinct incarnation. Neither failed
        // posts nor a partially completed creation transaction roll IDs back.
        self.retired_floor = self
            .retired_floor
            .max(send_next)
            .max(recv_next)
            .max(identity.first_sequence + 1);
        // Advance the floor before making this QPN available, under one lock.
        self.live.remove(&identity.qp_num);
    }

    pub(super) fn stats(&self) -> QpRegistryStats {
        QpRegistryStats {
            active: self.live.len(),
            next_sequence_floor: self.retired_floor,
        }
    }
}

/// Held until the provider QP is destroyed and every posting borrow is gone.
pub(crate) struct IdentityLease {
    cq: Arc<CompletionQueue>,
    identity: CompletionIdentity,
    /// This guard covers allocation, ownership registration and provider post,
    /// preserving SQ order without another atomic counter.
    send_next: Mutex<u64>,
    recv_next: AtomicU64,
}

/// Reserves one SQ sequence and serializes posting until the guard is dropped.
#[derive(Debug)]
#[must_use = "hold the guard until the entire SQ posting transaction finishes"]
pub(crate) struct SendSequenceGuard<'a> {
    _next: MutexGuard<'a, u64>,
    sequence: u64,
}

impl SendSequenceGuard<'_> {
    #[inline]
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl IdentityLease {
    pub(super) fn new(cq: Arc<CompletionQueue>, identity: CompletionIdentity) -> Self {
        Self {
            cq,
            identity,
            send_next: Mutex::new(identity.first_sequence),
            recv_next: AtomicU64::new(identity.first_sequence),
        }
    }

    #[inline]
    pub(crate) fn cq(&self) -> &Arc<CompletionQueue> {
        &self.cq
    }

    #[inline]
    pub(crate) fn identity(&self) -> CompletionIdentity {
        self.identity
    }

    #[inline]
    pub(crate) fn lock_send(&self) -> Result<SendSequenceGuard<'_>> {
        lock_send_sequence(&self.send_next)
    }

    #[inline]
    pub(crate) fn alloc_recv(&self) -> Result<u64> {
        allocate_sequence(&self.recv_next)
    }
}

#[inline]
fn lock_send_sequence(next: &Mutex<u64>) -> Result<SendSequenceGuard<'_>> {
    let mut next = next.lock().unwrap();
    if *next > WRID::MAX_SEQUENCE {
        return Err(ErrorKind::WorkRequestIdsExhausted.into());
    }
    let sequence = *next;
    *next += 1;
    Ok(SendSequenceGuard {
        _next: next,
        sequence,
    })
}

#[inline]
fn allocate_sequence(next: &AtomicU64) -> Result<u64> {
    next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        (value <= WRID::MAX_SEQUENCE).then(|| value + 1)
    })
    .map_err(|_| ErrorKind::WorkRequestIdsExhausted.into())
}

impl Drop for IdentityLease {
    fn drop(&mut self) {
        // Destruction has stopped provider access. A panicking poster still
        // consumed its sequence; preserve that watermark despite poisoning.
        let send_next = *self
            .send_next
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.cq.identities.lock().unwrap().release(
            self.identity,
            send_next,
            *self.recv_next.get_mut(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn qpn_reuse_starts_above_both_directions_and_failed_posts() {
        let mut registry = IdentityRegistry::default();
        let old = registry.register(42).unwrap();
        registry.release(old, 4, 12);
        let current = registry.register(42).unwrap();
        assert_eq!(current.first_sequence(), 12);
        for kind in [
            WRType::Recv,
            WRType::SendData,
            WRType::SendImm,
            WRType::Read,
        ] {
            for sequence in 0..12 {
                assert!(!current.contains(42, old.encode(kind, sequence)));
            }
            assert!(current.contains(42, current.encode(kind, 12)));
        }
        assert!(!current.contains(43, current.encode(WRType::Recv, 12)));
        assert_eq!(
            registry.stats(),
            QpRegistryStats {
                active: 1,
                next_sequence_floor: 12
            }
        );
    }

    #[test]
    fn unused_incarnations_advance_without_growing_the_registry() {
        let mut registry = IdentityRegistry::default();
        for sequence in 0..10_000 {
            let identity = registry.register(42).unwrap();
            assert_eq!(identity.first_sequence(), sequence);
            registry.release(identity, sequence, sequence);
        }
        assert_eq!(
            registry.stats(),
            QpRegistryStats {
                active: 0,
                next_sequence_floor: 10_000
            }
        );
    }

    #[test]
    fn retirement_order_never_lowers_the_shared_floor() {
        let mut registry = IdentityRegistry::default();
        let a = registry.register(42).unwrap();
        let b = registry.register(43).unwrap();
        assert_eq!(a.first_sequence(), b.first_sequence());
        registry.release(b, 200, 100);
        let c = registry.register(44).unwrap();
        assert_eq!(c.first_sequence(), 200);
        // A live QP retains its original counter; a later drop with a smaller
        // next sequence must not undo the floor published by another QP.
        registry.release(a, 50, 60);
        assert_eq!(registry.stats().next_sequence_floor, 200);
        let replacement = registry.register(42).unwrap();
        assert_eq!(replacement.first_sequence(), 200);
        assert!(!replacement.contains(42, a.encode(WRType::Recv, 59)));
        registry.release(c, 205, 200);
        registry.release(replacement, 200, 200);
        assert_eq!(
            registry.stats(),
            QpRegistryStats {
                active: 0,
                next_sequence_floor: 205
            }
        );
    }

    #[test]
    fn retired_qpns_do_not_accumulate_registry_entries() {
        let mut registry = IdentityRegistry::default();
        for qp_num in 0..10_000 {
            let identity = registry.register(qp_num).unwrap();
            registry.release(
                identity,
                identity.first_sequence(),
                identity.first_sequence(),
            );
            assert!(registry.live.is_empty());
        }
        assert_eq!(
            registry.stats(),
            QpRegistryStats {
                active: 0,
                next_sequence_floor: 10_000
            }
        );
    }

    #[test]
    fn qpn_lease_blocks_reuse_until_the_old_watermark_is_returned() {
        let mut registry = IdentityRegistry::default();
        let old = registry.register(42).unwrap();
        // A new provider QP already has QPN 42, while the destroyed old QP's
        // Rust fields still hold its lease. Never preallocate the new floor.
        assert_eq!(
            registry.register(42).unwrap_err().kind,
            ErrorKind::CompletionIdentityInUse
        );
        assert_eq!(
            registry.stats(),
            QpRegistryStats {
                active: 1,
                next_sequence_floor: 0
            }
        );
        registry.release(old, 999, 2);
        let next = registry.register(42).unwrap();
        assert_eq!(next.first_sequence(), 999);
        assert!(!next.contains(42, old.encode(WRType::SendData, 998)));
    }

    #[test]
    fn concurrent_qpn_reuse_cannot_observe_a_preallocated_floor() {
        let registry = Mutex::new(IdentityRegistry::default());
        let old = registry.lock().unwrap().register(42).unwrap();
        std::thread::scope(|scope| {
            let (attempted, wait) = std::sync::mpsc::channel();
            let registry = &registry;
            let contender = scope.spawn(move || {
                let error = registry.lock().unwrap().register(42).unwrap_err();
                attempted.send(error.kind).unwrap();
            });
            assert_eq!(wait.recv().unwrap(), ErrorKind::CompletionIdentityInUse);
            registry.lock().unwrap().release(old, 10_000, 10);
            contender.join().unwrap();
        });
        assert_eq!(
            registry
                .lock()
                .unwrap()
                .register(42)
                .unwrap()
                .first_sequence(),
            10_000
        );
    }

    #[test]
    fn registry_has_no_cq_capacity_or_dense_qpn_requirement() {
        let mut registry = IdentityRegistry::default();
        for qp_num in (0..65_537).chain([u32::MAX]) {
            assert_eq!(registry.register(qp_num).unwrap().qp_num(), qp_num);
        }
        assert_eq!(
            registry.stats(),
            QpRegistryStats {
                active: 65_538,
                next_sequence_floor: 0
            }
        );
        assert_eq!(
            registry.register(42).unwrap_err().kind,
            ErrorKind::CompletionIdentityInUse
        );
    }

    #[test]
    fn exhaustion_preserves_the_qpn_watermark_without_wrapping() {
        let mut registry = IdentityRegistry::default();
        let old = registry.register(42).unwrap();
        registry.release(old, WRID::MAX_SEQUENCE, 0);
        let last = registry.register(42).unwrap();
        assert_eq!(last.first_sequence(), WRID::MAX_SEQUENCE);
        registry.release(last, WRID::MAX_SEQUENCE + 1, WRID::MAX_SEQUENCE);
        for _ in 0..10 {
            assert_eq!(
                registry.register(42).unwrap_err().kind,
                ErrorKind::WorkRequestIdsExhausted
            );
        }
        assert_eq!(
            registry.register(43).unwrap_err().kind,
            ErrorKind::WorkRequestIdsExhausted
        );
        assert_eq!(
            registry.stats(),
            QpRegistryStats {
                active: 0,
                next_sequence_floor: WRID::MAX_SEQUENCE + 1
            }
        );
    }

    #[test]
    fn concurrent_sequences_are_unique_at_the_exhaustion_boundary() {
        let counter = AtomicU64::new(WRID::MAX_SEQUENCE - 999);
        let sequences = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..200)
                            .filter_map(|_| allocate_sequence(&counter).ok())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            threads
                .into_iter()
                .flat_map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(sequences.len(), 1000);
        assert_eq!(sequences.into_iter().collect::<HashSet<_>>().len(), 1000);
        assert_eq!(counter.load(Ordering::Relaxed), WRID::MAX_SEQUENCE + 1);
        let invalid = AtomicU64::new(u64::MAX);
        assert!(allocate_sequence(&invalid).is_err());
        assert_eq!(invalid.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn send_sequence_exhaustion_never_wraps() {
        let next = Mutex::new(WRID::MAX_SEQUENCE);
        assert_eq!(
            lock_send_sequence(&next).unwrap().sequence(),
            WRID::MAX_SEQUENCE
        );
        for _ in 0..10 {
            assert_eq!(
                lock_send_sequence(&next).unwrap_err().kind,
                ErrorKind::WorkRequestIdsExhausted
            );
            assert_eq!(*next.lock().unwrap(), WRID::MAX_SEQUENCE + 1);
        }
        let invalid = Mutex::new(u64::MAX);
        assert!(lock_send_sequence(&invalid).is_err());
        assert_eq!(*invalid.lock().unwrap(), u64::MAX);
    }

    #[test]
    fn send_guard_serializes_the_entire_posting_transaction() {
        let next = Mutex::new(0);
        let first = lock_send_sequence(&next).unwrap();
        assert_eq!(first.sequence(), 0);
        std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::channel();
            let next = &next;
            let poster = scope.spawn(move || {
                assert!(matches!(
                    next.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ));
                tx.send(()).unwrap();
                lock_send_sequence(next).unwrap().sequence()
            });
            rx.recv().unwrap();
            drop(first);
            assert_eq!(poster.join().unwrap(), 1);
        });
        assert_eq!(*next.lock().unwrap(), 2);
    }

    #[test]
    fn poisoned_send_transaction_preserves_watermark_on_drop() {
        let dev = crate::test_utils::open_device();
        let cq = CompletionQueue::create(dev.context(), 16, None).unwrap();
        let lease = cq.register_qp(42).unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let guard = lease.lock_send().unwrap();
                assert_eq!(guard.sequence(), 0);
                panic!("simulate panic after reserving an SQ sequence");
            }))
            .is_err()
        );
        assert!(lease.send_next.is_poisoned());
        drop(lease);
        let replacement = cq.register_qp(42).unwrap();
        assert_eq!(replacement.identity().first_sequence(), 1);
        assert_eq!(replacement.lock_send().unwrap().sequence(), 1);
    }
}
