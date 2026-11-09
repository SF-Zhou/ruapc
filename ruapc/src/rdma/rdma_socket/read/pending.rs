//! READ failure policy. The verbs crate alone owns and settles DMA work.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use ruapc_rdma::{ReadFailure, ReadHandle, ReadObservation};

use super::ReadDeadline;

/// Bound weak metadata even when idle connections skip deadline sweeps.
const PRUNE_INTERVAL: usize = 64;

type Notifications = Vec<(ReadObservation, Option<ReadFailure>)>;

#[derive(Debug, Default)]
pub(in super::super) struct PendingReads(Mutex<Inner>);

#[derive(Debug, Default)]
struct Inner {
    entries: Vec<PendingRead>,
    /// Cached empty: neither completed batches nor DMA holds survive a scan.
    scan: Notifications,
    registrations: usize,
}

#[derive(Debug)]
struct PendingRead {
    handle: ReadHandle,
    deadlines: Deadlines,
}

#[derive(Debug)]
struct Deadlines {
    deadline: Option<ReadDeadline>,
    failed_at: Option<Instant>,
    close_reported: bool,
}

impl Deadlines {
    fn expire(
        &mut self,
        now: Instant,
        grace: Duration,
        recorded: Option<ReadFailure>,
    ) -> (Option<ReadFailure>, bool) {
        let failure = recorded
            .or_else(|| ReadDeadline::expired(self.deadline, now).map(|deadline| deadline.reason));
        if failure.is_some() && self.failed_at.is_none() {
            self.failed_at = Some(ReadDeadline::failure_start(self.deadline, now));
        }
        let close = !self.close_reported
            && self
                .failed_at
                .is_some_and(|at| now.saturating_duration_since(at) >= grace);
        self.close_reported |= close;
        (failure, close)
    }
}

impl PendingReads {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn register(&self, handle: ReadHandle, deadline: Option<ReadDeadline>) {
        let mut inner = self.lock();
        inner.registrations += 1;
        let scan = if inner.registrations >= PRUNE_INTERVAL {
            inner.registrations = 0;
            Some(Self::collect(&mut inner, |_, _| None))
        } else {
            None
        };
        inner.entries.push(PendingRead {
            handle,
            deadlines: Deadlines {
                deadline,
                failed_at: None,
                close_reported: false,
            },
        });
        drop(inner);
        if let Some(scan) = scan {
            self.restore(scan);
        }
    }

    /// Upgrade under the registry lock but defer every strong-reference drop
    /// until after unlocking. An observation can become the final owner during
    /// concurrent completion or QP destruction and then wake the receiver.
    fn collect(
        inner: &mut Inner,
        mut failure: impl FnMut(&mut PendingRead, &ReadObservation) -> Option<ReadFailure>,
    ) -> Notifications {
        let mut scan = std::mem::take(&mut inner.scan);
        debug_assert!(scan.is_empty(), "cached scan retained a READ observation");
        inner.entries.retain_mut(|entry| {
            let Some(observation) = entry.handle.observe() else {
                return false;
            };
            let pending = observation.is_pending();
            let reason = if pending {
                failure(entry, &observation)
            } else {
                None
            };
            scan.push((observation, reason));
            pending
        });
        scan
    }

    fn restore(&self, mut scan: Notifications) {
        // Wakers and Buffer destructors must run outside the registry lock.
        scan.clear();
        if scan.capacity() != 0 {
            let mut inner = self.lock();
            if scan.capacity() > inner.scan.capacity() {
                inner.scan = scan;
            }
        }
    }

    pub(super) fn fail(&self) {
        let scan = Self::collect(&mut self.lock(), |_, _| Some(ReadFailure::ConnectionClosed));
        for (observation, reason) in &scan {
            if let Some(reason) = reason {
                observation.fail(*reason);
            }
        }
        self.restore(scan);
    }

    pub(super) fn expire(&self, now: Instant, grace: Duration) -> bool {
        let mut expired = false;
        let scan = Self::collect(&mut self.lock(), |entry, observation| {
            // Remaining includes admission-blocked work. A deadline can imply
            // a QP fault only while the provider still has an accepted READ.
            if observation.has_in_flight() {
                let (reason, close) = entry.deadlines.expire(now, grace, observation.failure());
                expired |= close;
                reason
            } else {
                None
            }
        });
        for (observation, reason) in &scan {
            if let Some(reason) = reason {
                observation.fail(*reason);
            }
        }
        self.restore(scan);
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Wake, Waker},
        time::Duration,
    };

    use ruapc_rdma::{ReadCompletion, ReadPosting, ReadRequest, ReadSegment};

    use crate::{BufferPool, rdma::test_utils::make_rdma_devices};

    struct Fixture {
        qp: ruapc_rdma::QueuePair,
        pool: Arc<BufferPool>,
    }

    impl Fixture {
        fn new() -> Self {
            let devices = make_rdma_devices();
            let device = &devices[0];
            let cq = ruapc_rdma::CompletionQueue::create(device.context(), 16, None).unwrap();
            let qp = ruapc_rdma::QueuePair::create(
                device.pd(),
                &cq,
                &mut ruapc_rdma::ibv_qp_init_attr {
                    qp_type: ruapc_rdma::ibv_qp_type::IBV_QPT_RC,
                    cap: ruapc_rdma::ibv_qp_cap {
                        max_send_wr: 4,
                        max_recv_wr: 4,
                        max_send_sge: 1,
                        max_recv_sge: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                device.index(),
            )
            .unwrap();
            Self {
                qp,
                pool: ruapc_bufpool::BufferPoolBuilder::new(
                    devices.iter().map(|device| device.registrar()).collect(),
                )
                .build(),
            }
        }

        fn plan(&self) -> (ReadPosting, ReadCompletion) {
            let mut buffer = self.pool.allocate(64).unwrap();
            buffer.set_len(64);
            self.qp
                .prepare_reads(
                    vec![buffer.into()],
                    vec![ReadRequest {
                        remote_addr: 1024,
                        rkey: 7,
                        segments: vec![ReadSegment {
                            buffer: 0,
                            offset: 0,
                            len: 64,
                        }],
                    }],
                )
                .unwrap()
        }
    }

    #[test]
    fn deadline_failure_waits_for_grace_before_closing() {
        let now = Instant::now();
        let grace = Duration::from_millis(100);
        for (read, request, reason) in [
            (
                now + Duration::from_secs(1),
                now,
                ReadFailure::RequestTimeout,
            ),
            (now, now + Duration::from_secs(1), ReadFailure::Timeout),
            (now, now, ReadFailure::RequestTimeout),
        ] {
            let mut deadlines = Deadlines {
                deadline: ReadDeadline::earliest(Some(read), Some(request)),
                failed_at: None,
                close_reported: false,
            };
            assert_eq!(deadlines.expire(now, grace, None), (Some(reason), false));
            assert_eq!(
                deadlines.expire(now + grace, grace, Some(reason)),
                (Some(reason), true)
            );
            assert_eq!(
                deadlines.expire(now + grace, grace, Some(reason)),
                (Some(reason), false)
            );
        }
    }

    #[test]
    fn cancelled_read_has_a_grace_even_without_deadlines() {
        let now = Instant::now();
        let grace = Duration::from_millis(100);
        let mut deadlines = Deadlines {
            deadline: None,
            failed_at: None,
            close_reported: false,
        };
        assert_eq!(deadlines.expire(now, grace, None), (None, false));
        assert_eq!(
            deadlines.expire(now, grace, Some(ReadFailure::Cancelled)),
            (Some(ReadFailure::Cancelled), false)
        );
        assert_eq!(
            deadlines.expire(now + grace, grace, Some(ReadFailure::Cancelled)),
            (Some(ReadFailure::Cancelled), true)
        );
    }

    #[test]
    fn recorded_failure_keeps_its_reason_and_the_original_cleanup_clock() {
        let start = Instant::now();
        let grace = Duration::from_millis(100);
        for (at, observed, reason, expected_start) in [
            // A failure first observed after an expired deadline uses that
            // earlier clock, without replacing the recorded completion error.
            (start, start + grace / 2, ReadFailure::Completion, start),
            // Cancellation can precede the budget. Later sweeps must not move
            // cleanup to the future READ deadline or restart the grace.
            (
                start + Duration::from_secs(1),
                start,
                ReadFailure::Cancelled,
                start,
            ),
        ] {
            let mut deadlines = Deadlines {
                deadline: ReadDeadline::earliest(Some(at), None),
                failed_at: None,
                close_reported: false,
            };
            assert_eq!(
                deadlines.expire(observed, grace, Some(reason)),
                (Some(reason), false)
            );
            assert_eq!(deadlines.failed_at, Some(expected_start));
            assert_eq!(
                deadlines.expire(expected_start + grace, grace, Some(reason)),
                (Some(reason), true)
            );
            assert_eq!(deadlines.failed_at, Some(expected_start));
            assert_eq!(
                deadlines.expire(expected_start + grace * 2, grace, Some(reason)),
                (Some(reason), false)
            );
        }
    }

    #[test]
    fn unposted_work_cannot_trigger_watchdog_closure() {
        let fixture = Fixture::new();
        let (posting, mut completion) = fixture.plan();
        let pending = PendingReads::default();
        let now = Instant::now();
        pending.register(
            posting.handle(),
            ReadDeadline::earliest(Some(now), Some(now)),
        );
        assert!(!pending.expire(now, Duration::ZERO));
        assert!(completion.try_recv().is_none());
        posting.cancel();
        let outcome = completion
            .try_recv()
            .expect("unposted cancellation settles immediately");
        assert_eq!(outcome.result, Err(ReadFailure::Cancelled));
        assert_eq!(outcome.buffers.len(), 1);
        assert!(!pending.expire(now, Duration::ZERO));
        assert!(pending.lock().entries.is_empty());
    }

    #[test]
    fn completed_batches_are_pruned_without_deadline_scans() {
        let fixture = Fixture::new();
        let pending = PendingReads::default();
        for _ in 0..10_000 {
            let (posting, completion) = fixture.qp.prepare_reads(Vec::new(), Vec::new()).unwrap();
            pending.register(posting.handle(), None);
            drop(posting);
            drop(completion);
        }
        assert!(pending.lock().entries.len() <= PRUNE_INTERVAL);
        assert!(pending.lock().scan.is_empty());
    }

    #[test]
    fn scan_cache_keeps_capacity_without_retaining_batches() {
        let fixture = Fixture::new();
        let pending = PendingReads::default();
        let mut plans = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..4 {
            let (posting, completion) = fixture.plan();
            let handle = posting.handle();
            pending.register(handle.clone(), None);
            plans.push((posting, completion));
            handles.push(handle);
        }
        pending.fail();
        assert!(pending.lock().scan.capacity() >= 4);
        assert!(pending.lock().scan.is_empty());
        drop(plans);
        assert!(handles.iter().all(|handle| handle.observe().is_none()));
        assert!(!pending.expire(Instant::now(), Duration::ZERO));
        assert!(pending.lock().entries.is_empty());
        assert!(pending.lock().scan.capacity() >= 4);
    }

    #[test]
    fn poisoned_lock_does_not_prevent_registration_or_failure() {
        let fixture = Fixture::new();
        let pending = Arc::new(PendingReads::default());
        let poisoned = pending.clone();
        std::thread::spawn(move || {
            let _guard = poisoned.0.lock().unwrap();
            panic!("poison READ registry");
        })
        .join()
        .unwrap_err();
        let (posting, mut completion) = fixture.plan();
        pending.register(posting.handle(), None);
        pending.fail();
        assert!(completion.try_recv().is_none());
        drop(posting);
        assert_eq!(
            completion.try_recv().unwrap().result,
            Err(ReadFailure::ConnectionClosed)
        );
    }

    #[test]
    fn failure_wakes_the_task_outside_the_registry_lock() {
        struct LockProbe(Arc<PendingReads>);
        impl Wake for LockProbe {
            fn wake(self: Arc<Self>) {
                let _guard = self
                    .0
                    .0
                    .try_lock()
                    .expect("waker ran under READ registry lock");
            }
        }
        let fixture = Fixture::new();
        let pending = Arc::new(PendingReads::default());
        let (posting, mut completion) = fixture.plan();
        pending.register(posting.handle(), None);
        let waker = Waker::from(Arc::new(LockProbe(pending.clone())));
        let mut context = Context::from_waker(&waker);
        assert!(Pin::new(&mut completion).poll(&mut context).is_pending());
        pending.fail();
        assert!(completion.try_recv().is_none());
        drop(posting);
        assert_eq!(
            completion.try_recv().unwrap().result,
            Err(ReadFailure::ConnectionClosed)
        );
    }
}
