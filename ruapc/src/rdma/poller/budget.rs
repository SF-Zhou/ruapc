//! CQ admission is bounded by software credits, including error/flush CQEs
//! for unsignaled sends. Queue depths alone do not bound unpolled CQEs: a
//! provider can recycle a WQE before its CQE is consumed.
//!
//! Each connection reserves R receive credits + W data credits + A ACK
//! credits. READs share a per-NIC semaphore H, so a CQ reserves only
//! min(H, sum of its connections' READ limits). While a CQ is running, credits
//! return only after completion processing. A stopped CQ closes all admission
//! before QP destruction refunds unpolled READ credits to the NIC; those can
//! serve other CQs, each with its own H reserve. No CQ-wide counter is touched
//! per WR.

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use super::flow::FlowControl;
use crate::rdma::RdmaConnectionConfig;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Demand {
    base: u64,
    reads: u64,
    connections: u64,
}

impl Demand {
    pub(super) fn connection(config: &RdmaConnectionConfig) -> Self {
        let window = (config.recv_queue_len / 2).max(1);
        Self {
            base: u64::from(config.recv_queue_len)
                + u64::from(window)
                + u64::from(FlowControl::ack_limit(window)),
            reads: u64::from((config.qp.max_send_wr / 2).max(1)),
            connections: 1,
        }
    }

    fn add(&mut self, other: Self) {
        self.base += other.base;
        self.reads += other.reads;
        self.connections += other.connections;
    }

    fn subtract(&mut self, other: Self) {
        self.base -= other.base;
        self.reads -= other.reads;
        self.connections -= other.connections;
    }

    pub(super) fn is_empty(self) -> bool {
        self.connections == 0
    }
}

#[derive(Debug, Default)]
struct Ledger {
    used: Demand,
    retired: Demand,
}

#[derive(Debug)]
pub(super) struct CqBudget {
    capacity: u32,
    read_limit: u32,
    ledger: Mutex<Ledger>,
    has_retired: AtomicBool,
}

impl CqBudget {
    pub(super) fn new(capacity: u32, read_limit: u32) -> Self {
        Self {
            capacity,
            read_limit,
            ledger: Mutex::default(),
            has_retired: AtomicBool::new(false),
        }
    }

    fn entries(&self, demand: Demand) -> u64 {
        demand.base + demand.reads.min(u64::from(self.read_limit))
    }

    pub(super) fn snapshot(&self) -> (u32, u64) {
        let ledger = self.ledger.lock().unwrap();
        (self.entries(ledger.used) as u32, ledger.used.connections)
    }

    pub(super) fn reserve(&self, demand: Demand) -> bool {
        let mut ledger = self.ledger.lock().unwrap();
        let mut next = ledger.used;
        next.add(demand);
        if self.entries(next) > u64::from(self.capacity) {
            return false;
        }
        ledger.used = next;
        true
    }

    /// Called only after the reservation's QP has been destroyed. Some
    /// providers leave its CQEs in the CQ, so capacity is not free yet.
    pub(super) fn retire(&self, demand: Demand) {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.retired.add(demand);
        self.has_retired.store(true, Ordering::Release);
    }

    /// Take this snapshot BEFORE polling. An empty CQ observed before a
    /// concurrent retirement is not evidence that that QP's CQEs are gone.
    pub(super) fn take_retired(&self) -> Demand {
        if !self.has_retired.load(Ordering::Acquire) {
            return Demand::default();
        }
        let mut ledger = self.ledger.lock().unwrap();
        self.has_retired.store(false, Ordering::Release);
        std::mem::take(&mut ledger.retired)
    }

    pub(super) fn release_drained(&self, retired: Demand) {
        if !retired.is_empty() {
            self.ledger.lock().unwrap().used.subtract(retired);
        }
    }
}

/// Carries retirement evidence across bounded CQ drains. A short batch or a
/// scheduling yield is not proof that a destroyed QP's remaining CQEs are gone.
#[derive(Default)]
pub(super) struct CqDrain {
    retired: Demand,
    batches: usize,
}

impl CqDrain {
    /// At most 1024 completions with the poller's 64-entry batch before
    /// returning to shutdown checks, READ deadlines and connection maintenance.
    pub(super) const MAX_BATCHES: usize = 16;

    pub(super) fn begin(&mut self, budget: &CqBudget) {
        // Include new retirements only before the poll whose result can
        // authorize their release. Earlier, undrained snapshots stay held.
        self.retired.add(budget.take_retired());
        self.batches = 0;
    }

    /// Records a poll result and returns whether this drain should continue.
    pub(super) fn polled(&mut self, budget: &CqBudget, count: usize, capacity: usize) -> bool {
        self.batches += 1;
        if count == 0 {
            budget.release_drained(std::mem::take(&mut self.retired));
            return false;
        }
        count == capacity && self.batches < Self::MAX_BATCHES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demand(base: u64, reads: u64) -> Demand {
        Demand {
            base,
            reads,
            connections: 1,
        }
    }

    #[test]
    fn default_capacity_scales_with_credits_and_shared_reads() {
        let budget = CqBudget::new(65_536, 32);
        let config = crate::rdma::RdmaConnectionConfig {
            qp: crate::rdma::RdmaQueuePairConfig::default(),
            recv_queue_len: 8,
            max_msg_size: 262_144,
            traffic_class: 0,
        };
        let demand = Demand::connection(&config);
        assert_eq!(demand.base, 16);
        for _ in 0..4094 {
            assert!(budget.reserve(demand));
        }
        assert_eq!(budget.snapshot(), (65_536, 4094));
        assert!(!budget.reserve(demand));
    }

    #[test]
    fn read_reserve_follows_connected_capacity_up_to_nic_limit() {
        let budget = CqBudget::new(100, 32);
        for expected in [10, 20, 30, 40, 42] {
            assert!(budget.reserve(demand(2, 8)));
            assert_eq!(budget.snapshot().0, expected);
        }
        budget.retire(demand(2, 8));
        let retired = budget.take_retired();
        assert_eq!(budget.snapshot().0, 42);
        budget.release_drained(retired);
        assert_eq!(budget.snapshot().0, 40);
    }

    #[test]
    fn later_retirement_requires_a_new_drain() {
        let budget = CqBudget::new(16, 0);
        let demand = demand(8, 0);
        assert!(budget.reserve(demand));
        assert!(budget.reserve(demand));
        budget.retire(demand);
        let snapshot = budget.take_retired();
        budget.retire(demand); // Racing the poll that returns zero.
        assert!(!budget.reserve(demand));
        budget.release_drained(snapshot);
        assert_eq!(budget.snapshot(), (8, 1));
        assert!(budget.reserve(demand));
        assert!(!budget.reserve(demand));
        budget.release_drained(budget.take_retired());
        assert_eq!(budget.snapshot(), (8, 1));
    }

    #[test]
    fn concurrent_admission_cannot_overbook() {
        let budget = CqBudget::new(512, 32);
        let admitted = std::sync::atomic::AtomicU32::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..100 {
                        if budget.reserve(demand(16, 32)) {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(admitted.load(Ordering::Relaxed), 30);
        assert_eq!(budget.snapshot(), (512, 30));
    }

    #[test]
    fn continuous_short_batches_yield_without_releasing_retirement() {
        let budget = CqBudget::new(32, 0);
        let retired = demand(32, 0);
        assert!(budget.reserve(retired));
        budget.retire(retired);
        let mut drain = CqDrain::default();
        // Each READ completion can make a permit available to a new poster;
        // even a small queue can produce short, nonempty batches indefinitely.
        for _ in 0..10_000 {
            drain.begin(&budget);
            assert!(!drain.polled(&budget, 16, 64), "maintenance must run");
            assert_eq!(budget.snapshot(), (32, 1));
            assert!(!budget.reserve(retired));
        }
        drain.begin(&budget);
        assert!(!drain.polled(&budget, 0, 64));
        assert_eq!(budget.snapshot(), (0, 0));
        assert!(budget.reserve(retired));
    }

    #[test]
    fn continuous_full_batches_have_a_scheduling_bound() {
        let budget = CqBudget::new(16, 0);
        let retired = demand(16, 0);
        assert!(budget.reserve(retired));
        budget.retire(retired);
        let mut drain = CqDrain::default();
        for _ in 0..3 {
            drain.begin(&budget);
            for _ in 1..CqDrain::MAX_BATCHES {
                assert!(drain.polled(&budget, 64, 64));
            }
            assert!(!drain.polled(&budget, 64, 64), "maintenance must run");
            assert_eq!(budget.snapshot(), (16, 1));
        }
        drain.begin(&budget);
        assert!(!drain.polled(&budget, 0, 64));
        assert_eq!(budget.snapshot(), (0, 0));
    }

    #[test]
    fn yielding_merges_retirements_but_an_empty_poll_cannot_release_later_ones() {
        let budget = CqBudget::new(24, 0);
        let retired = demand(8, 0);
        for _ in 0..3 {
            assert!(budget.reserve(retired));
        }
        let mut drain = CqDrain::default();
        budget.retire(retired);
        drain.begin(&budget);
        assert!(!drain.polled(&budget, 1, 64));
        budget.retire(retired);
        drain.begin(&budget); // Both earlier retirements are now covered.
        budget.retire(retired); // Races the poll; not covered by its snapshot.
        assert!(!drain.polled(&budget, 0, 64));
        assert_eq!(budget.snapshot(), (8, 1));
        drain.begin(&budget);
        assert!(!drain.polled(&budget, 1, 64));
        assert_eq!(budget.snapshot(), (8, 1));
        drain.begin(&budget);
        assert!(!drain.polled(&budget, 0, 64));
        assert_eq!(budget.snapshot(), (0, 0));
        drain.begin(&budget);
        assert!(!drain.polled(&budget, 0, 64)); // No double refund.
        assert_eq!(budget.snapshot(), (0, 0));
    }
}
