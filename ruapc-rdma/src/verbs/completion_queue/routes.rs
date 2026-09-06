//! CQ-local route ownership and non-repeating work request sequences.

use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};

use super::CompletionQueue;
use crate::{ErrorKind, Result, WRID};

/// Identifies one QP's use of a CQ route slot.
///
/// A slot can be reused after QP destruction. Its next occupant starts above
/// every sequence allocated by earlier occupants, including work requests that
/// were never posted. The floor therefore rejects stale completions without
/// consuming separate generation bits or retaining a record for every QP.
///
/// Routes are local to a CQ. Matching a route alone is not completion evidence;
/// [`super::super::QueuePair::complete`] also requires a token from the correct
/// CQ and checks the provider's QP number before releasing memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CompletionRoute {
    slot: u16,
    first_sequence: u64,
}

impl CompletionRoute {
    /// Returns the CQ-local slot as an array index.
    #[inline]
    pub fn slot(self) -> usize {
        usize::from(self.slot)
    }

    /// First sequence that can belong to this occupant of the slot.
    #[inline]
    pub fn first_sequence(self) -> u64 {
        self.first_sequence
    }

    /// Whether the WRID addresses this slot and is not from an older occupant.
    /// This does not establish that the work request was posted or completed.
    #[inline]
    pub fn contains(self, wrid: WRID) -> bool {
        wrid.get_slot() == self.slot() && wrid.get_id() >= self.first_sequence
    }
}

/// Only QP construction and destruction take the allocator's enclosing lock.
/// Normally grows with peak simultaneous QPs; exhausted slots remain retired.
#[derive(Default)]
pub(super) struct RouteAllocator {
    next_sequences: Vec<u64>,
    free: Vec<u16>,
}

impl RouteAllocator {
    pub(super) fn allocate(&mut self) -> Result<CompletionRoute> {
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                if self.next_sequences.len() > usize::from(WRID::SLOT_MAX) {
                    return Err(ErrorKind::CompletionRoutesExhausted.into());
                }
                let slot = self.next_sequences.len() as u16;
                self.next_sequences.push(0);
                slot
            }
        };
        let first_sequence = self.next_sequences[usize::from(slot)];
        debug_assert!(first_sequence <= WRID::ID_MASK);
        Ok(CompletionRoute {
            slot,
            first_sequence,
        })
    }

    fn release(&mut self, route: CompletionRoute, send_next: u64, recv_next: u64) {
        // Reserve one sequence even for a QP that never allocated a WRID. This
        // gives every lease a distinct identity for poller registration/removal.
        let next = send_next.max(recv_next).max(route.first_sequence + 1);
        self.next_sequences[route.slot()] = next;
        if next <= WRID::ID_MASK {
            self.free.push(route.slot);
        }
        // A slot that consumed its final sequence is permanently retired.
    }
}

/// Holds a slot until its QP has been destroyed and all posting access is gone.
/// The CQ owner prevents destruction of the allocator before the lease returns.
pub(crate) struct RouteLease {
    cq: Arc<CompletionQueue>,
    route: CompletionRoute,
    /// Sequence allocation and the entire SQ posting transaction share this
    /// lock, preserving hardware post order without a second atomic counter.
    send_next: Mutex<u64>,
    recv_next: AtomicU64,
}

/// Reserves one SQ sequence and serializes posting until the guard is dropped.
/// Hold it across ownership registration, provider posting and failure rollback.
#[derive(Debug)]
#[must_use = "the guard must be held until the SQ posting transaction finishes"]
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

impl RouteLease {
    pub(super) fn new(cq: Arc<CompletionQueue>, route: CompletionRoute) -> Self {
        Self {
            cq,
            route,
            send_next: Mutex::new(route.first_sequence),
            recv_next: AtomicU64::new(route.first_sequence),
        }
    }

    pub(crate) fn cq(&self) -> &Arc<CompletionQueue> {
        &self.cq
    }

    pub(crate) fn route(&self) -> CompletionRoute {
        self.route
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
    if *next > WRID::ID_MASK {
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
        (value <= WRID::ID_MASK).then(|| value + 1)
    })
    .map_err(|_| ErrorKind::WorkRequestIdsExhausted.into())
}

impl Drop for RouteLease {
    fn drop(&mut self) {
        // QP destruction has already stopped provider access and no posting
        // borrow remains. A panicking poster still consumed its sequence;
        // poisoning must not lose that watermark or interrupt resource cleanup.
        let send_next = *self
            .send_next
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.cq
            .routes
            .lock()
            .unwrap()
            .release(self.route, send_next, *self.recv_next.get_mut());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn reuse_starts_after_both_directions_and_rejects_old_wrids() {
        let mut allocator = RouteAllocator::default();
        let old = allocator.allocate().unwrap();
        allocator.release(old, 4, 12);
        let current = allocator.allocate().unwrap();
        assert_eq!(current.slot(), old.slot());
        assert_eq!(current.first_sequence(), 12);
        for sequence in 0..12 {
            assert!(!current.contains(WRID::recv(old.slot, sequence)));
            assert!(!current.contains(WRID::send_data(old.slot, sequence)));
            assert!(!current.contains(WRID::send_imm(old.slot, sequence)));
            assert!(!current.contains(WRID::read(old.slot, sequence)));
        }
        assert!(current.contains(WRID::read(current.slot, 12)));
        assert!(!current.contains(WRID::read(current.slot + 1, 12)));
    }

    #[test]
    fn unused_leases_still_have_distinct_incarnations() {
        let mut allocator = RouteAllocator::default();
        // Reuse outlives the previous 8-bit, 256-generation slot limit.
        for sequence in 0..10_000 {
            let route = allocator.allocate().unwrap();
            assert_eq!(route.slot(), 0);
            assert_eq!(route.first_sequence(), sequence);
            allocator.release(route, sequence, sequence);
        }
        assert_eq!(allocator.next_sequences.len(), 1);
    }

    #[test]
    fn routes_are_exclusive_and_reusable_at_capacity() {
        let mut allocator = RouteAllocator::default();
        let routes: Vec<_> = (0..=WRID::SLOT_MAX)
            .map(|slot| {
                let route = allocator.allocate().unwrap();
                assert_eq!(route.slot(), usize::from(slot));
                route
            })
            .collect();
        assert_eq!(
            allocator.allocate().unwrap_err().kind,
            ErrorKind::CompletionRoutesExhausted
        );
        allocator.release(routes[42], 10, 20);
        let reused = allocator.allocate().unwrap();
        assert_eq!(reused.slot(), 42);
        assert_eq!(reused.first_sequence(), 20);
        assert!(allocator.allocate().is_err());
    }

    #[test]
    fn exhausted_sequences_never_wrap_and_retire_the_route() {
        let counter = AtomicU64::new(WRID::ID_MASK);
        assert_eq!(allocate_sequence(&counter).unwrap(), WRID::ID_MASK);
        for _ in 0..10 {
            assert_eq!(
                allocate_sequence(&counter).unwrap_err().kind,
                ErrorKind::WorkRequestIdsExhausted
            );
            assert_eq!(counter.load(Ordering::Relaxed), WRID::ID_MASK + 1);
        }
        let counter = AtomicU64::new(u64::MAX);
        assert!(allocate_sequence(&counter).is_err());
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);

        let mut allocator = RouteAllocator::default();
        let old = allocator.allocate().unwrap();
        allocator.release(old, WRID::ID_MASK, WRID::ID_MASK);
        let last = allocator.allocate().unwrap();
        assert_eq!(last.slot(), old.slot());
        assert_eq!(last.first_sequence(), WRID::ID_MASK);
        allocator.release(last, WRID::ID_MASK + 1, WRID::ID_MASK);
        let fresh = allocator.allocate().unwrap();
        assert_ne!(fresh.slot(), old.slot());
        assert_eq!(fresh.first_sequence(), 0);
    }

    #[test]
    fn concurrent_route_allocation_is_exclusive() {
        let allocator = Mutex::new(RouteAllocator::default());
        let routes = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..100)
                            .map(|_| allocator.lock().unwrap().allocate().unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            threads
                .into_iter()
                .flat_map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(routes.len(), 800);
        assert_eq!(
            routes
                .iter()
                .map(|route| route.slot())
                .collect::<HashSet<_>>()
                .len(),
            routes.len()
        );
    }

    #[test]
    fn concurrent_sequences_are_unique_including_the_exhaustion_boundary() {
        let counter = AtomicU64::new(WRID::ID_MASK - 999);
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
        assert_eq!(counter.load(Ordering::Relaxed), WRID::ID_MASK + 1);
    }

    #[test]
    fn send_sequence_exhaustion_never_wraps() {
        let next = Mutex::new(WRID::ID_MASK);
        assert_eq!(lock_send_sequence(&next).unwrap().sequence(), WRID::ID_MASK);
        for _ in 0..10 {
            assert_eq!(
                lock_send_sequence(&next).unwrap_err().kind,
                ErrorKind::WorkRequestIdsExhausted
            );
            assert_eq!(*next.lock().unwrap(), WRID::ID_MASK + 1);
        }
        let next = Mutex::new(u64::MAX);
        assert!(lock_send_sequence(&next).is_err());
        assert_eq!(*next.lock().unwrap(), u64::MAX);
    }

    #[test]
    fn send_sequence_guard_excludes_posters_until_transaction_finishes() {
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
    fn poisoned_send_transaction_preserves_watermark_on_lease_drop() {
        let dev = crate::test_utils::open_device();
        let cq = CompletionQueue::create(dev.context(), 16, None).unwrap();
        let lease = cq.allocate_route().unwrap();
        let first = lease.route();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let guard = lease.lock_send().unwrap();
                assert_eq!(guard.sequence(), first.first_sequence());
                panic!("simulate a panic after SQ sequence allocation");
            }))
            .is_err()
        );
        assert!(lease.send_next.is_poisoned());
        drop(lease);

        let replacement = cq.allocate_route().unwrap();
        assert_eq!(replacement.route().slot(), first.slot());
        assert_eq!(
            replacement.route().first_sequence(),
            first.first_sequence() + 1
        );
        assert_eq!(
            replacement.lock_send().unwrap().sequence(),
            first.first_sequence() + 1
        );
    }
}
