use super::*;

use std::sync::{atomic::AtomicUsize, mpsc};

use ruapc_bufpool::Device as _;
use ruapc_rdma::{QueuePair, ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_state};

use crate::rdma::{
    ConnCountGuard, RdmaBandwidthLimiter, RdmaNicInfo, RdmaPathInfo, RdmaSocketConfig,
};

/// The shared CQ length must be clamped to the device's `max_cqe`:
/// drivers reject larger requests with EINVAL (e.g. the rxe soft-RoCE
/// driver caps `max_cqe` at 32767, below the default `device_cq_len`).
#[tokio::test]
async fn test_cq_len_clamped_to_device_max() {
    let device = crate::rdma::test_utils::open_rdma_device();
    let config = PollerConfig {
        cq_len: u32::MAX,
        read_limit: 1,
        spin_us: 0,
        dispatch_workers: 1,
    };
    let poller = DevicePoller::start(
        device.context(),
        "cq-clamp-test",
        config,
        Dispatcher::start(config.dispatch_workers),
    )
    .expect("CQ creation must succeed with a clamped length");
    let max_cqe = device.context().query_device().unwrap().max_cqe;
    assert!(poller.cq_capacity <= u32::try_from(max_cqe.max(1)).unwrap_or(u32::MAX));
    assert!(poller.cq_capacity > 0);
}

#[test]
fn test_ring_reservation_accounting() {
    let total = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (a, after_a) = RingReservation::add(&total, 16);
    assert_eq!(after_a, 16);
    let (b, after_b) = RingReservation::add(&total, 32);
    assert_eq!(after_b, 48);
    drop(a);
    assert_eq!(total.load(Ordering::Acquire), 32);
    drop(b);
    assert_eq!(total.load(Ordering::Acquire), 0);
}

#[test]
fn wake_during_maintenance_survives_draining_the_pipe() {
    let (writer, mut reader) = UnixStream::pair().unwrap();
    writer.set_nonblocking(true).unwrap();
    reader.set_nonblocking(true).unwrap();
    let requested = Arc::new(AtomicBool::new(false));
    let waker = PollerWaker(Arc::new(writer), requested.clone());
    waker.wake();
    assert!(requested.swap(false, Ordering::AcqRel));
    // The first maintenance pass is in progress when a different sender
    // publishes work. Consuming both pipe bytes must preserve its new hint.
    waker.clone().wake();
    let mut bytes = [0; 2];
    reader.read_exact(&mut bytes).unwrap();
    assert!(requested.swap(false, Ordering::AcqRel));
    assert!(!requested.load(Ordering::Acquire));
}

#[tokio::test]
async fn shard_admission_tries_other_queues_and_reclaims_failed_setup() {
    let device = crate::rdma::test_utils::open_rdma_device();
    let config = PollerConfig {
        cq_len: 63,
        read_limit: 32,
        spin_us: 0,
        dispatch_workers: 1,
    };
    let dispatcher = Dispatcher::start(1);
    let mut shards = DeviceShards::default();
    for index in 0..2 {
        shards.shards.push(Arc::new(
            DevicePoller::start(
                device.context(),
                &format!("admission-test-{index}"),
                config,
                dispatcher.clone(),
            )
            .unwrap(),
        ));
    }
    // Give the less loaded shard an unsaturated READ reserve. The larger
    // READ demand below can fit only on the other shard, whose shared READ
    // reserve is already paid. Lowest current usage alone is insufficient.
    let connection = |recv_queue_len, max_send_wr| crate::rdma::RdmaConnectionConfig {
        recv_queue_len,
        max_msg_size: 1024,
        traffic_class: 0,
        qp: crate::rdma::RdmaQueuePairConfig {
            max_send_wr,
            ..Default::default()
        },
    };
    assert_eq!(shards.shards[0].cq_capacity, 63);
    let a = shards.shards[0]
        .reserve(Demand::connection(&connection(8, 2)))
        .unwrap();
    let b = shards.shards[1]
        .reserve(Demand::connection(&connection(2, 64)))
        .unwrap();
    assert_eq!(shards.shards[0].shared.budget.snapshot().0, 17);
    assert_eq!(shards.shards[1].shared.budget.snapshot().0, 37);
    let (chosen, c) = shards
        .reserve(Demand::connection(&connection(8, 64)))
        .unwrap();
    assert!(Arc::ptr_eq(&chosen, &shards.shards[1]));
    assert_eq!(chosen.shared.budget.snapshot().0, 53);
    assert!(
        shards
            .reserve(Demand::connection(&connection(8, 64)))
            .is_err()
    );
    // These reservations model failed setup before QP creation. Their wake
    // causes even idle shards to drain and make the credits available again.
    drop((a, b, c));
    tokio::time::timeout(Duration::from_secs(5), async {
        while shards
            .shards
            .iter()
            .any(|p| p.shared.budget.snapshot().1 != 0)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("retired setup capacity must be reclaimed");
    assert!(
        shards
            .reserve(Demand::connection(&connection(8, 64)))
            .is_ok()
    );
}

#[tokio::test]
async fn failed_new_shard_does_not_hide_existing_capacity() {
    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pollers = DevicePollers::default();
    let config = PollerConfig {
        cq_len: 63,
        read_limit: 1,
        spin_us: 0,
        dispatch_workers: 1,
    };
    let connection = crate::rdma::RdmaConnectionConfig {
        recv_queue_len: 2,
        max_msg_size: 1024,
        traffic_class: 0,
        qp: crate::rdma::RdmaQueuePairConfig::default(),
    };
    let (first, a) = pollers.reserve(device, config, 2, &connection).unwrap();
    // Bypass public config validation to deterministically fail only the new
    // CQ creation; the existing shard can still satisfy this reservation.
    let (second, b) = pollers
        .reserve(
            device,
            PollerConfig {
                cq_len: 0,
                ..config
            },
            2,
            &connection,
        )
        .unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(pollers.report().len(), 1);
    assert_eq!(pollers.report()[0].connections, 2);
    drop((a, b));
}

/// The test controls CQ consumption itself so it can stop a registrar after
/// posting but before publication, without racing an independently running loop.
fn manual_poller(context: &Arc<ruapc_rdma::Context>) -> (DevicePoller, PollLoop) {
    let channel = CompChannel::create(context).unwrap();
    let cq = CompletionQueue::create(context, 16, Some(&channel)).unwrap();
    let shared = Arc::new(PollerShared {
        inner: Mutex::new(SharedInner::default()),
        budget: CqBudget::new(cq.capacity(), 1),
        has_incoming: AtomicBool::new(false),
        shutdown: AtomicBool::new(false),
    });
    let (wake_tx, wake_rx) = UnixStream::pair().unwrap();
    wake_tx.set_nonblocking(true).unwrap();
    wake_rx.set_nonblocking(true).unwrap();
    let maintenance_requested = Arc::new(AtomicBool::new(false));
    let poller = DevicePoller {
        cq: cq.clone(),
        shared: shared.clone(),
        waker: PollerWaker(Arc::new(wake_tx), maintenance_requested.clone()),
        thread: None,
        cq_capacity: cq.capacity(),
    };
    let poll_loop = PollLoop {
        cq,
        comp_channel: channel,
        wake_rx,
        shared,
        dispatcher: Dispatcher::start(1),
        spin: Duration::ZERO,
        conns: HashMap::default(),
        dirty_qps: Vec::new(),
        maintenance_requested,
        unack_cq_events: 0,
    };
    (poller, poll_loop)
}

/// An admitted QP with no posted WRs lets maintenance tests control software
/// credits without inventing CQEs or requiring a connected peer.
fn register_idle_socket(
    poller: &DevicePoller,
    device: &crate::rdma::RdmaDevice,
    buffer_pool: &Arc<crate::BufferPool>,
    state: &Arc<State>,
) -> Arc<RdmaSocket> {
    let connection = crate::rdma::RdmaConnectionConfig {
        recv_queue_len: 2,
        max_msg_size: 1024,
        traffic_class: 0,
        qp: crate::rdma::RdmaQueuePairConfig {
            max_send_wr: 4,
            max_recv_wr: 4,
            max_send_sge: 1,
            max_recv_sge: 1,
        },
    };
    let reservation = poller.reserve(Demand::connection(&connection)).unwrap();
    let mut attrs = ruapc_rdma::ibv_qp_init_attr {
        qp_type: ruapc_rdma::ibv_qp_type::IBV_QPT_RC,
        cap: ruapc_rdma::ibv_qp_cap {
            max_send_wr: 4,
            max_recv_wr: 4,
            max_send_sge: 1,
            max_recv_sge: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let qp = QueuePair::create(
        device.pd(),
        poller.cq(),
        poller.cq(),
        &mut attrs,
        device.index(),
    )
    .unwrap();
    let nic = RdmaNicInfo {
        device: device.info().name.clone(),
        port_num: device.info().ports[0].port_num,
        gid_index: 0,
        ip: None,
    };
    let (pending_sender, pending_receiver) = tokio::sync::mpsc::channel(4);
    let socket = Arc::new(RdmaSocket::new(
        reservation.bind(qp),
        buffer_pool.clone(),
        pending_sender,
        poller.waker(),
        RdmaSocketConfig {
            max_msg_size: 1024,
            send_window: 1,
            path: RdmaPathInfo {
                local: nic.clone(),
                remote: nic.clone(),
                same_connectivity_domain: true,
            },
            read_timeout: None,
            read_permits: Arc::new(tokio::sync::Semaphore::new(1)),
            bandwidth_limiter: Arc::new(RdmaBandwidthLimiter::new(
                nic.device,
                nic.port_num,
                0,
                Duration::ZERO,
                Duration::ZERO,
            )),
            sq_read_cap: 2,
        },
    ));
    poller
        .register(
            RegisterConn {
                socket: socket.clone(),
                state: state.clone(),
                pending_receiver,
                recv_submitted: 0,
                recv_buf_size: 1024,
                send_window: 1,
                msg_aggregation: false,
                supervisor_guard: crate::TaskSupervisor::create().start_async_task(),
                ring_reservation: RingReservation::add(&Arc::new(AtomicUsize::new(0)), 0).0,
                conn_count_guard: ConnCountGuard::acquire(&Arc::new(vec![AtomicUsize::new(0)]), 0),
            },
            || Ok(()),
        )
        .unwrap();
    socket
}

fn mark_dirty(poll_loop: &mut PollLoop, qp_num: u32) {
    PollLoop::mark_dirty(
        poll_loop.conns.get_mut(&qp_num).unwrap(),
        &mut poll_loop.dirty_qps,
    );
}

#[tokio::test]
async fn dirty_maintenance_is_bounded_and_survives_replacement() {
    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let (poller, mut poll_loop) = manual_poller(device.context());
    let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let first = register_idle_socket(&poller, device, &buffer_pool, &state);
    let second = register_idle_socket(&poller, device, &buffer_pool, &state);
    let first_qp = first.queue_pair.qp_num();
    let second_qp = second.queue_pair.qp_num();
    assert!(poll_loop.drain_incoming(false));
    assert_eq!(poll_loop.dirty_qps.len(), 2);
    poll_loop.maintain_dirty_connections(Instant::now(), false);
    assert!(poll_loop.dirty_qps.is_empty());

    // A pending send on an unrelated connection must stay untouched when
    // only the first connection has completion work. Exhaust its window
    // first so a later full scan can drain the message without posting it.
    assert!(matches!(
        second.state.try_acquire(),
        crate::rdma::SendPermit::Granted { .. }
    ));
    second
        .pending_sender
        .try_send(buffer_pool.allocate(64).unwrap())
        .unwrap();
    mark_dirty(&mut poll_loop, first_qp);
    mark_dirty(&mut poll_loop, first_qp);
    assert_eq!(poll_loop.dirty_qps, [first_qp]);
    poll_loop.maintain_dirty_connections(Instant::now(), false);
    assert!(poll_loop.conns[&second_qp].pending_sends.is_empty());
    poll_loop.maintain_connections(Instant::now(), false, false);
    assert_eq!(poll_loop.conns[&second_qp].pending_sends.len(), 1);
    assert!(
        poll_loop.dirty_qps.is_empty(),
        "a full window must not force retry polling"
    );

    // A receive allocation deficit stays listed once across both kinds of
    // maintenance, including more CQEs before the next timed retry.
    poll_loop.conns.get_mut(&first_qp).unwrap().recv_deficit = 1;
    mark_dirty(&mut poll_loop, first_qp);
    poll_loop.maintain_dirty_connections(Instant::now(), false);
    mark_dirty(&mut poll_loop, first_qp);
    assert_eq!(poll_loop.dirty_qps, [first_qp]);
    poll_loop.maintain_connections(Instant::now(), false, false);
    assert_eq!(poll_loop.dirty_qps, [first_qp]);
    poll_loop.conns.get_mut(&first_qp).unwrap().recv_deficit = 0;

    first.set_error();
    poll_loop.maintain_dirty_connections(Instant::now(), false);
    assert!(!poll_loop.conns.contains_key(&first_qp));
    assert!(poll_loop.dirty_qps.is_empty());
    drop(first);
    poll_loop
        .drain_completions(poller.cq(), &mut CompletionBatch::new(), &mut Vec::new())
        .unwrap();
    let replacement = register_idle_socket(&poller, device, &buffer_pool, &state);
    let replacement_qp = replacement.queue_pair.qp_num();
    assert!(poll_loop.drain_incoming(false));
    assert_eq!(poll_loop.dirty_qps, [replacement_qp]);
    poll_loop.maintain_dirty_connections(Instant::now(), false);
    assert!(!poll_loop.conns[&replacement_qp].dirty);
    assert!(poll_loop.dirty_qps.is_empty());
    poll_loop.shutdown_cleanup();
}

#[tokio::test]
async fn identical_wrids_are_routed_by_their_provider_qp_numbers() {
    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let (poller, mut poll_loop) = manual_poller(device.context());
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let sockets = [
        register_idle_socket(&poller, device, &pool, &state),
        register_idle_socket(&poller, device, &pool, &state),
    ];
    assert_ne!(
        sockets[0].queue_pair.qp_num(),
        sockets[1].queue_pair.qp_num()
    );
    assert_eq!(
        sockets[0].queue_pair.recv_identity().first_sequence(),
        sockets[1].queue_pair.recv_identity().first_sequence()
    );

    for socket in &sockets {
        let mut init = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_INIT,
            port_num: device.info().ports[0].port_num,
            ..Default::default()
        };
        let mask = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        socket.queue_pair.modify(&mut init, mask.0 as _).unwrap();
        socket.queue_pair.recv(pool.allocate(64).unwrap()).unwrap();
        socket.set_error();
    }
    // This manual poller has consumed nothing yet. Account for each real
    // receive before publishing the connections to its local registry.
    for incoming in &mut poller.shared.inner.lock().unwrap().incoming {
        incoming.recv_submitted = 1;
    }
    assert!(poll_loop.drain_incoming(false));
    let mut seen = std::collections::HashSet::new();
    let mut wrid = None;
    let mut storage = CompletionBatch::<8>::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen.len() < sockets.len() {
        for completion in poller.cq().poll_batch(&mut storage).unwrap() {
            assert!(completion.info().is_recv());
            assert_eq!(
                completion.info().status,
                ruapc_rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR
            );
            if let Some(first) = wrid {
                assert_eq!(completion.info().wr_id, first);
            } else {
                wrid = Some(completion.info().wr_id);
            }
            assert!(seen.insert(completion.qp_num()));
            poll_loop.dispatch(completion, &mut Vec::new());
        }
        assert!(
            Instant::now() < deadline,
            "both receive flushes must arrive"
        );
        std::thread::yield_now();
    }
    for socket in &sockets {
        assert!(
            poll_loop
                .conns
                .get_mut(&socket.queue_pair.qp_num())
                .unwrap()
                .ready_to_remove()
        );
    }
    poll_loop.maintain_dirty_connections(Instant::now(), false);
    assert!(poll_loop.conns.is_empty());
    assert!(poll_loop.dirty_qps.is_empty());
    poll_loop.shutdown_cleanup();
}

#[tokio::test]
async fn early_flush_completion_waits_for_registration_and_settles_receive() {
    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let (poller, mut poll_loop) = manual_poller(device.context());
    let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let mut attrs = ruapc_rdma::ibv_qp_init_attr {
        qp_type: ruapc_rdma::ibv_qp_type::IBV_QPT_RC,
        cap: ruapc_rdma::ibv_qp_cap {
            max_send_wr: 4,
            max_recv_wr: 4,
            max_send_sge: 1,
            max_recv_sge: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let qp = QueuePair::create(
        device.pd(),
        poller.cq(),
        poller.cq(),
        &mut attrs,
        device.index(),
    )
    .unwrap();
    let port = device.info().ports[0].port_num;
    let mut init = ibv_qp_attr {
        qp_state: ibv_qp_state::IBV_QPS_INIT,
        port_num: port,
        ..Default::default()
    };
    let mask = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_PORT
        | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
        | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
    qp.modify(&mut init, mask.0 as _).unwrap();

    let nic = RdmaNicInfo {
        device: device.info().name.clone(),
        port_num: port,
        gid_index: 0,
        ip: None,
    };
    let (pending_sender, pending_receiver) = tokio::sync::mpsc::channel(1);
    let socket = Arc::new(RdmaSocket::new(
        poller
            .reserve(Demand::connection(&crate::rdma::RdmaConnectionConfig {
                qp: crate::rdma::RdmaQueuePairConfig::default(),
                recv_queue_len: 1,
                max_msg_size: 1024,
                traffic_class: 0,
            }))
            .unwrap()
            .bind(qp),
        buffer_pool.clone(),
        pending_sender,
        poller.waker(),
        RdmaSocketConfig {
            max_msg_size: 1024,
            send_window: 2,
            path: RdmaPathInfo {
                local: nic.clone(),
                remote: nic,
                same_connectivity_domain: true,
            },
            read_timeout: None,
            read_permits: Arc::new(tokio::sync::Semaphore::new(1)),
            bandwidth_limiter: Arc::new(RdmaBandwidthLimiter::new(
                device.info().name.clone(),
                port,
                0,
                Duration::ZERO,
                Duration::ZERO,
            )),
            sq_read_cap: 1,
        },
    ));
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let supervisor = crate::TaskSupervisor::create();
    let counts = Arc::new(vec![AtomicUsize::new(0)]);
    let ring_total = Arc::new(AtomicUsize::new(0));
    let registration = RegisterConn {
        socket: socket.clone(),
        state,
        pending_receiver,
        recv_submitted: 1,
        recv_buf_size: 1024,
        send_window: 2,
        msg_aggregation: false,
        supervisor_guard: supervisor.start_async_task(),
        ring_reservation: RingReservation::add(&ring_total, 1024).0,
        conn_count_guard: ConnCountGuard::acquire(&counts, 0),
    };
    let buffer = buffer_pool.allocate(1024).unwrap();
    let cq = poller.cq().clone();
    let (posted_tx, posted_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (polled_tx, polled_rx) = mpsc::channel();
    let (dispatched_tx, dispatched_rx) = mpsc::channel();
    let timeout = Duration::from_secs(5);

    std::thread::scope(|scope| {
        let poller = &poller;
        let socket = &socket;
        let registrar = scope.spawn(move || {
            poller.register(registration, || {
                socket.queue_pair.recv(buffer)?;
                // INIT -> ERR flushes a real posted receive without needing a
                // peer or routable network. The socket is closed for teardown.
                socket.set_error();
                posted_tx
                    .send(())
                    .map_err(|error| Error::new(ErrorKind::ConnectionClosed, error.to_string()))?;
                resume_rx
                    .recv_timeout(timeout)
                    .map_err(|error| Error::new(ErrorKind::Timeout, error.to_string()))?;
                Ok(())
            })
        });
        posted_rx.recv_timeout(timeout).unwrap();
        assert!(!poller.shared.has_incoming.load(Ordering::Acquire));
        assert!(poller.shared.inner.try_lock().is_err());

        let router = scope.spawn(|| {
            let mut storage = CompletionBatch::<1>::new();
            let mut batch = DispatchBatch::new();
            let deadline = Instant::now() + timeout;
            loop {
                if let Some(completion) = cq.poll_batch(&mut storage).unwrap().next() {
                    assert!(completion.info().is_recv());
                    assert_eq!(
                        completion.info().status,
                        ruapc_rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR
                    );
                    polled_tx.send(()).unwrap();
                    poll_loop.dispatch(completion, &mut batch);
                    dispatched_tx.send(()).unwrap();
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "receive did not produce a flush CQE"
                );
                std::thread::yield_now();
            }
        });
        polled_rx.recv_timeout(timeout).unwrap();
        let early_dispatch = dispatched_rx.recv_timeout(Duration::from_millis(50));
        // Always release the registrar before asserting, including when a
        // regression lets the CQE escape through the empty-inbox fast path.
        resume_tx.send(()).unwrap();
        registrar.join().unwrap().unwrap();
        router.join().unwrap();
        assert!(matches!(
            early_dispatch,
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
    });

    let conn = poll_loop
        .conns
        .get_mut(&socket.queue_pair.qp_num())
        .expect("the early CQE must find the newly published connection");
    assert!(
        conn.ready_to_remove(),
        "the flush CQE must settle the receive ledger"
    );
    poll_loop.shutdown_cleanup();
    // Removing poller state cannot release CQ capacity while another socket
    // owner may still submit work. Destruction plus a later drain does.
    assert_eq!(poller.shared.budget.snapshot().1, 1);
    drop(socket);
    assert_eq!(poller.shared.budget.snapshot().1, 1);
    poll_loop
        .drain_completions(&cq, &mut CompletionBatch::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(poller.shared.budget.snapshot(), (0, 0));
    assert_eq!(counts[0].load(Ordering::Acquire), 0);
    assert_eq!(ring_total.load(Ordering::Acquire), 0);
}
