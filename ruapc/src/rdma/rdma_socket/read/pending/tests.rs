use super::*;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Wake, Waker},
    time::Duration,
};

use ruapc_bufpool::Device as _;
use ruapc_rdma::{ReadCompletion, ReadPosting, ReadRequest, ReadSegment};

use crate::{BufferPool, rdma::test_utils::make_rdma_devices};

struct Fixture {
    qp: ruapc_rdma::QueuePair,
    pool: Arc<BufferPool>,
}

impl Fixture {
    fn new() -> Self {
        let devices = make_rdma_devices();
        let device = &devices.devices()[0];
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
            pool: ruapc_bufpool::BufferPoolBuilder::new(devices).build(),
        }
    }

    fn plan(&self) -> (ReadPosting, ReadCompletion) {
        let mut buffer = self.pool.allocate(64).unwrap();
        buffer.set_len(64);
        self.qp
            .prepare_reads(
                vec![buffer],
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
fn delayed_sweep_reports_the_earlier_deadline_and_closes_only_once() {
    let now = Instant::now();
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
            read: Some(read),
            request: Some(request),
            watchdog_reported: false,
        };
        let sweep = now + Duration::from_secs(2);
        assert_eq!(deadlines.expire(sweep), (Some(reason), true));
        assert_eq!(deadlines.expire(sweep), (Some(reason), false));
    }
}

#[test]
fn request_expiry_does_not_disarm_the_read_watchdog() {
    let request = Instant::now();
    let read = request + Duration::from_secs(1);
    let mut deadlines = Deadlines {
        read: Some(read),
        request: Some(request),
        watchdog_reported: false,
    };
    assert_eq!(
        deadlines.expire(request),
        (Some(ReadFailure::RequestTimeout), false)
    );
    assert_eq!(
        deadlines.expire(read),
        (Some(ReadFailure::RequestTimeout), true)
    );
    assert_eq!(
        deadlines.expire(read),
        (Some(ReadFailure::RequestTimeout), false)
    );
    deadlines.read = None;
    deadlines.watchdog_reported = false;
    assert_eq!(
        deadlines.expire(read),
        (Some(ReadFailure::RequestTimeout), false)
    );
}

#[test]
fn unposted_work_cannot_trigger_watchdog_closure() {
    let fixture = Fixture::new();
    let (posting, mut completion) = fixture.plan();
    let pending = PendingReads::default();
    let now = Instant::now();
    pending.register(posting.handle(), Some(now), Some(now));
    assert!(!pending.expire(now));
    assert!(completion.try_recv().is_none());
    assert!(posting.cancel().is_some());
    assert!(!pending.expire(now));
    assert!(pending.lock().entries.is_empty());
}

#[test]
fn completed_batches_are_pruned_without_deadline_scans() {
    let fixture = Fixture::new();
    let pending = PendingReads::default();
    for _ in 0..10_000 {
        let (posting, completion) = fixture.qp.prepare_reads(Vec::new(), Vec::new()).unwrap();
        pending.register(posting.handle(), None, None);
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
        pending.register(handle.clone(), None, None);
        plans.push((posting, completion));
        handles.push(handle);
    }
    pending.fail();
    assert!(pending.lock().scan.capacity() >= 4);
    assert!(pending.lock().scan.is_empty());
    drop(plans);
    assert!(handles.iter().all(|handle| handle.observe().is_none()));
    assert!(!pending.expire(Instant::now()));
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
    pending.register(posting.handle(), None, None);
    pending.fail();
    assert_eq!(
        completion.try_recv().unwrap().unwrap_err(),
        ReadFailure::ConnectionClosed
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
    pending.register(posting.handle(), None, None);
    let waker = Waker::from(Arc::new(LockProbe(pending.clone())));
    let mut context = Context::from_waker(&waker);
    assert!(Pin::new(&mut completion).poll(&mut context).is_pending());
    pending.fail();
    assert_eq!(
        completion.try_recv().unwrap().unwrap_err(),
        ReadFailure::ConnectionClosed
    );
}
