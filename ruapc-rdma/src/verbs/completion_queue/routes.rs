//! CQ-local route ownership and non-repeating work request sequences.

use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};

use super::CompletionQueue;
use crate::{ErrorKind, Result, WRID, WRType};

/// Immutable for the CQ's complete lifetime, including retained CQ tokens.
/// The provider's positive `c_int` CQ capacity requires at most 31 slot bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct WrIdLayout {
    sequence_mask: u64,
    slot_mask: u64,
    capacity: u32,
    sequence_bits: u32,
}

impl WrIdLayout {
    pub(super) fn new(capacity: u32) -> Self {
        assert!(capacity > 0 && capacity <= i32::MAX as u32);
        let slot_bits = u32::BITS - (capacity - 1).leading_zeros();
        let sequence_bits = WRID::TYPE_SHIFT - slot_bits;
        let sequence_mask = (1 << sequence_bits) - 1;
        Self {
            sequence_mask,
            slot_mask: WRID::PAYLOAD_MASK ^ sequence_mask,
            capacity,
            sequence_bits,
        }
    }

    pub(super) fn capacity(self) -> u32 {
        self.capacity
    }

    pub(super) fn sequence_bits(self) -> u32 {
        self.sequence_bits
    }

    #[inline]
    pub(super) fn slot(self, wrid: WRID) -> usize {
        ((wrid.raw() & self.slot_mask) >> self.sequence_bits) as usize
    }

    #[inline]
    pub(super) fn sequence(self, wrid: WRID) -> u64 {
        wrid.raw() & self.sequence_mask
    }
}

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
    first_sequence: u64,
    /// Pre-shifted slot bits; posting does not need a variable shift.
    prefix: u64,
    layout: WrIdLayout,
}

impl CompletionRoute {
    /// Returns the CQ-local slot as an array index.
    #[inline]
    pub fn slot(self) -> usize {
        (self.prefix >> self.layout.sequence_bits) as usize
    }

    /// First sequence that can belong to this occupant of the slot.
    #[inline]
    pub fn first_sequence(self) -> u64 {
        self.first_sequence
    }

    /// Sequence width fixed when the originating CQ was created.
    pub fn sequence_bits(self) -> u32 {
        self.layout.sequence_bits
    }

    /// Last sequence that can be allocated in this CQ's layout.
    #[inline]
    pub fn max_sequence(self) -> u64 {
        self.layout.sequence_mask
    }

    /// Whether the WRID addresses this slot and is not from an older occupant.
    /// This does not establish that the work request was posted or completed.
    #[inline]
    pub fn contains(self, wrid: WRID) -> bool {
        wrid.raw() & self.layout.slot_mask == self.prefix
            && self.layout.sequence(wrid) >= self.first_sequence
    }

    #[inline]
    pub(crate) fn encode(self, kind: WRType, sequence: u64) -> WRID {
        debug_assert!(sequence <= self.layout.sequence_mask);
        WRID::new(kind, self.prefix | sequence)
    }
}

/// Only QP construction and destruction take the allocator's enclosing lock.
/// Normally grows with peak simultaneous QPs; exhausted slots remain retired.
pub(super) struct RouteAllocator {
    layout: WrIdLayout,
    next_sequences: Vec<u64>,
    free: Vec<u32>,
}

impl RouteAllocator {
    pub(super) fn new(layout: WrIdLayout) -> Self {
        Self {
            layout,
            next_sequences: Vec::new(),
            free: Vec::new(),
        }
    }

    pub(super) fn allocate(&mut self) -> Result<CompletionRoute> {
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                if self.next_sequences.len() >= self.layout.capacity as usize {
                    return Err(ErrorKind::CompletionRoutesExhausted.into());
                }
                let slot = self.next_sequences.len() as u32;
                self.next_sequences.push(0);
                slot
            }
        };
        let first_sequence = self.next_sequences[slot as usize];
        debug_assert!(first_sequence <= self.layout.sequence_mask);
        Ok(CompletionRoute {
            first_sequence,
            prefix: u64::from(slot) << self.layout.sequence_bits,
            layout: self.layout,
        })
    }

    fn release(&mut self, route: CompletionRoute, send_next: u64, recv_next: u64) {
        // Reserve one sequence even for a QP that never allocated a WRID. This
        // gives every lease a distinct identity for poller registration/removal.
        let next = send_next.max(recv_next).max(route.first_sequence + 1);
        self.next_sequences[route.slot()] = next;
        if next <= self.layout.sequence_mask {
            self.free.push(route.slot() as u32);
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

    #[inline]
    pub(crate) fn cq(&self) -> &Arc<CompletionQueue> {
        &self.cq
    }

    #[inline]
    pub(crate) fn route(&self) -> CompletionRoute {
        self.route
    }

    #[inline]
    pub(crate) fn lock_send(&self) -> Result<SendSequenceGuard<'_>> {
        lock_send_sequence(&self.send_next, self.route.max_sequence())
    }

    #[inline]
    pub(crate) fn alloc_recv(&self) -> Result<u64> {
        allocate_sequence(&self.recv_next, self.route.max_sequence())
    }
}

#[inline]
fn lock_send_sequence(next: &Mutex<u64>, max_sequence: u64) -> Result<SendSequenceGuard<'_>> {
    let mut next = next.lock().unwrap();
    if *next > max_sequence {
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
fn allocate_sequence(next: &AtomicU64, max_sequence: u64) -> Result<u64> {
    next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        (value <= max_sequence).then(|| value + 1)
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
    fn layout_roundtrips_every_type_at_capacity_and_sequence_boundaries() {
        for (capacity, slot_bits) in [
            (1, 0),
            (2, 1),
            (3, 2),
            (16_384, 14),
            (16_385, 15),
            (65_536, 16),
            (65_537, 17),
            (i32::MAX as u32, 31),
        ] {
            let layout = WrIdLayout::new(capacity);
            assert_eq!(layout.capacity(), capacity);
            assert_eq!(layout.sequence_bits(), 62 - slot_bits);
            for slot in [0, capacity / 2, capacity - 1] {
                let route = CompletionRoute {
                    first_sequence: 0,
                    prefix: u64::from(slot) << layout.sequence_bits,
                    layout,
                };
                assert_eq!(route.slot(), slot as usize);
                for kind in [
                    WRType::Recv,
                    WRType::SendData,
                    WRType::SendImm,
                    WRType::Read,
                ] {
                    for sequence in [0, 1, route.max_sequence()] {
                        let wrid = route.encode(kind, sequence);
                        assert_eq!(wrid.get_type(), kind);
                        assert_eq!(layout.slot(wrid), slot as usize);
                        assert_eq!(layout.sequence(wrid), sequence);
                        assert!(route.contains(wrid));
                    }
                }
            }
        }
    }

    #[test]
    fn exhausted_routes_retire_for_each_layout_width() {
        for capacity in [1, 2, 16_385, 65_537, i32::MAX as u32] {
            let layout = WrIdLayout::new(capacity);
            let mut allocator = RouteAllocator::new(layout);
            let route = allocator.allocate().unwrap();
            let max_sequence = route.max_sequence();
            let recv = AtomicU64::new(max_sequence);
            let send = Mutex::new(max_sequence);
            assert_eq!(
                allocate_sequence(&recv, max_sequence).unwrap(),
                max_sequence
            );
            assert_eq!(
                lock_send_sequence(&send, max_sequence).unwrap().sequence(),
                max_sequence
            );
            assert!(allocate_sequence(&recv, max_sequence).is_err());
            assert!(lock_send_sequence(&send, max_sequence).is_err());
            allocator.release(route, *send.lock().unwrap(), recv.load(Ordering::Relaxed));
            if capacity == 1 {
                assert_eq!(
                    allocator.allocate().unwrap_err().kind,
                    ErrorKind::CompletionRoutesExhausted
                );
            } else {
                let replacement = allocator.allocate().unwrap();
                assert_eq!(replacement.slot(), 1);
                assert!(!replacement.contains(route.encode(WRType::Recv, max_sequence)));
            }
        }
    }

    #[test]
    fn reuse_starts_after_both_directions_and_rejects_old_wrids() {
        let mut allocator = RouteAllocator::new(WrIdLayout::new(65_537));
        let old = allocator.allocate().unwrap();
        allocator.release(old, 4, 12);
        let current = allocator.allocate().unwrap();
        assert_eq!(current.slot(), old.slot());
        assert_eq!(current.first_sequence(), 12);
        for sequence in 0..12 {
            assert!(!current.contains(old.encode(WRType::Recv, sequence)));
            assert!(!current.contains(old.encode(WRType::SendData, sequence)));
            assert!(!current.contains(old.encode(WRType::SendImm, sequence)));
            assert!(!current.contains(old.encode(WRType::Read, sequence)));
        }
        assert!(current.contains(current.encode(WRType::Read, 12)));
        let other = allocator.allocate().unwrap();
        assert!(!current.contains(other.encode(WRType::Read, 12)));
    }

    #[test]
    fn unused_leases_still_have_distinct_incarnations() {
        let mut allocator = RouteAllocator::new(WrIdLayout::new(65_537));
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
        let mut allocator = RouteAllocator::new(WrIdLayout::new(65_537));
        let routes: Vec<_> = (0..65_537u32)
            .map(|slot| {
                let route = allocator.allocate().unwrap();
                assert_eq!(route.slot(), slot as usize);
                route
            })
            .collect();
        // Both prior fixed-width boundaries are crossed without aliasing.
        assert_ne!(routes[16_383], routes[16_384]);
        assert_ne!(routes[65_535], routes[65_536]);
        assert_eq!(routes.last().unwrap().slot(), 65_536);
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
        let max_sequence = WrIdLayout::new(65_537).sequence_mask;
        let counter = AtomicU64::new(max_sequence);
        assert_eq!(
            allocate_sequence(&counter, max_sequence).unwrap(),
            max_sequence
        );
        for _ in 0..10 {
            assert_eq!(
                allocate_sequence(&counter, max_sequence).unwrap_err().kind,
                ErrorKind::WorkRequestIdsExhausted
            );
            assert_eq!(counter.load(Ordering::Relaxed), max_sequence + 1);
        }
        let counter = AtomicU64::new(u64::MAX);
        assert!(allocate_sequence(&counter, max_sequence).is_err());
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);

        let mut allocator = RouteAllocator::new(WrIdLayout::new(65_537));
        let old = allocator.allocate().unwrap();
        allocator.release(old, max_sequence, max_sequence);
        let last = allocator.allocate().unwrap();
        assert_eq!(last.slot(), old.slot());
        assert_eq!(last.first_sequence(), max_sequence);
        allocator.release(last, max_sequence + 1, max_sequence);
        let fresh = allocator.allocate().unwrap();
        assert_ne!(fresh.slot(), old.slot());
        assert_eq!(fresh.first_sequence(), 0);
    }

    #[test]
    fn concurrent_route_allocation_is_exclusive() {
        let allocator = Mutex::new(RouteAllocator::new(WrIdLayout::new(65_537)));
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
        let max_sequence = WrIdLayout::new(i32::MAX as u32).sequence_mask;
        let counter = AtomicU64::new(max_sequence - 999);
        let sequences = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..200)
                            .filter_map(|_| allocate_sequence(&counter, max_sequence).ok())
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
        assert_eq!(counter.load(Ordering::Relaxed), max_sequence + 1);
    }

    #[test]
    fn send_sequence_exhaustion_never_wraps() {
        let max_sequence = WrIdLayout::new(i32::MAX as u32).sequence_mask;
        let next = Mutex::new(max_sequence);
        assert_eq!(
            lock_send_sequence(&next, max_sequence).unwrap().sequence(),
            max_sequence
        );
        for _ in 0..10 {
            assert_eq!(
                lock_send_sequence(&next, max_sequence).unwrap_err().kind,
                ErrorKind::WorkRequestIdsExhausted
            );
            assert_eq!(*next.lock().unwrap(), max_sequence + 1);
        }
        let next = Mutex::new(u64::MAX);
        assert!(lock_send_sequence(&next, max_sequence).is_err());
        assert_eq!(*next.lock().unwrap(), u64::MAX);
    }

    #[test]
    fn send_sequence_guard_excludes_posters_until_transaction_finishes() {
        let max_sequence = WrIdLayout::new(16).sequence_mask;
        let next = Mutex::new(0);
        let first = lock_send_sequence(&next, max_sequence).unwrap();
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
                lock_send_sequence(next, max_sequence).unwrap().sequence()
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
