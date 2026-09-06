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

/// The test controls CQ consumption itself so it can stop a registrar after
/// posting but before publication, without racing an independently running loop.
fn manual_poller(context: &Arc<ruapc_rdma::Context>) -> (DevicePoller, PollLoop) {
    let channel = CompChannel::create(context).unwrap();
    let cq = CompletionQueue::create(context, 16, Some(&channel)).unwrap();
    let shared = Arc::new(PollerShared {
        inner: Mutex::new(SharedInner::default()),
        has_incoming: AtomicBool::new(false),
        shutdown: AtomicBool::new(false),
    });
    let (wake_tx, wake_rx) = UnixStream::pair().unwrap();
    wake_tx.set_nonblocking(true).unwrap();
    wake_rx.set_nonblocking(true).unwrap();
    let poller = DevicePoller {
        cq: cq.clone(),
        shared: shared.clone(),
        waker: PollerWaker(Arc::new(wake_tx)),
        thread: None,
        wr_budget: Arc::new(AtomicU32::new(0)),
        cq_capacity: 16,
    };
    let poll_loop = PollLoop {
        cq,
        comp_channel: channel,
        wake_rx,
        shared,
        dispatcher: Dispatcher::start(1),
        spin: Duration::ZERO,
        conns: Vec::new(),
        unack_cq_events: 0,
    };
    (poller, poll_loop)
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
        qp,
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
    let reservation = poller.reserve(8).unwrap();
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
            poller.register(reservation, registration, || {
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

    let conn = poll_loop.conns[socket.queue_pair.send_route().slot()]
        .as_mut()
        .expect("the early CQE must find the newly published connection");
    assert!(
        conn.ready_to_remove(),
        "the flush CQE must settle the receive ledger"
    );
    poll_loop.shutdown_cleanup();
    assert_eq!(poller.wr_budget.load(Ordering::Acquire), 0);
    assert_eq!(counts[0].load(Ordering::Acquire), 0);
    assert_eq!(ring_total.load(Ordering::Acquire), 0);
}
