use super::*;

use std::sync::{atomic::AtomicUsize, mpsc};

use ruapc_bufpool::Device as _;
use ruapc_rdma::{QueuePair, ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_state};

use crate::{
    MsgFlags, MsgMeta, SocketTrait,
    rdma::{ConnCountGuard, RdmaBandwidthLimiter, RdmaNicInfo, RdmaPathInfo, RdmaSocketConfig},
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
        max_large_msg_size: 0,
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
        max_large_msg_size: 0,
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
        drain: CqDrain::default(),
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
    register_idle_socket_with_read_permits(
        poller,
        device,
        buffer_pool,
        state,
        Arc::new(tokio::sync::Semaphore::new(1)),
    )
}

fn register_idle_socket_with_read_permits(
    poller: &DevicePoller,
    device: &crate::rdma::RdmaDevice,
    buffer_pool: &Arc<crate::BufferPool>,
    state: &Arc<State>,
    read_permits: Arc<tokio::sync::Semaphore>,
) -> Arc<RdmaSocket> {
    register_socket_with_config(
        poller,
        device,
        buffer_pool,
        state,
        read_permits,
        SocketTestConfig::default(),
    )
}

struct SocketTestConfig {
    capacity: usize,
    window: u32,
    aggregate: bool,
    read_timeout: Option<Duration>,
}

impl Default for SocketTestConfig {
    fn default() -> Self {
        Self {
            capacity: 4,
            window: 1,
            aggregate: false,
            read_timeout: None,
        }
    }
}

fn register_socket_with_config(
    poller: &DevicePoller,
    device: &crate::rdma::RdmaDevice,
    buffer_pool: &Arc<crate::BufferPool>,
    state: &Arc<State>,
    read_permits: Arc<tokio::sync::Semaphore>,
    pending: SocketTestConfig,
) -> Arc<RdmaSocket> {
    let connection = crate::rdma::RdmaConnectionConfig {
        recv_queue_len: pending.window * 2,
        max_msg_size: 1024,
        max_large_msg_size: 0,
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
    let (pending_sender, pending_receiver) = tokio::sync::mpsc::channel(pending.capacity);
    let socket = Arc::new(RdmaSocket::new(
        reservation.bind(qp),
        buffer_pool.clone(),
        pending_sender,
        poller.waker(),
        RdmaSocketConfig {
            max_msg_size: 1024,
            max_large_msg_size: 0,
            max_inflight_large_bytes: 128 << 20,
            send_window: pending.window,
            path: RdmaPathInfo {
                local: nic.clone(),
                remote: nic.clone(),
                same_connectivity_domain: true,
            },
            read_timeout: pending.read_timeout,
            read_permits,
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
                send_window: pending.window,
                msg_aggregation: pending.aggregate,
                supervisor_guard: crate::TaskSupervisor::create().start_async_task(),
                ring_reservation: RingReservation::add(&Arc::new(AtomicUsize::new(0)), 0).0,
                conn_count_guard: ConnCountGuard::acquire(&Arc::new(vec![AtomicUsize::new(0)]), 0),
            },
            || Ok(()),
        )
        .unwrap();
    socket
}

fn connect_read_pair(device: &crate::rdma::RdmaDevice, first: &QueuePair, second: &QueuePair) {
    let info = device.info();
    let port = info.ports.iter().find(|p| p.is_usable()).unwrap();
    let gid = port
        .gids
        .iter()
        .find(|g| g.gid_type == ruapc_rdma::GidType::RoCEv2)
        .or_else(|| port.gids.first())
        .unwrap();
    let config = |remote: &QueuePair| ruapc_rdma::QpConnectionConfig {
        local_port_num: port.port_num,
        local_gid_index: gid.index,
        pkey_index: 0,
        link_layer: port.port_attr.link_layer,
        path_mtu: port.port_attr.active_mtu,
        remote_qp_num: remote.qp_num(),
        remote_gid: gid.gid,
        remote_lid: port.port_attr.lid,
        local_psn: 7,
        remote_psn: 7,
        max_rd_atomic: 1,
        max_dest_rd_atomic: 1,
        traffic_class: 0,
    };
    first.connect(&config(second)).unwrap();
    second.connect(&config(first)).unwrap();
}

/// Consume exactly one authentic READ completion, leaving later CQEs untouched.
async fn complete_one_read(poller: &DevicePoller, poll_loop: &mut PollLoop) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut storage = CompletionBatch::<1>::new();
        loop {
            if let Some(completion) = poller.cq().poll_batch(&mut storage).unwrap().next() {
                assert!(completion.info().succ());
                assert!(completion.info().is_read());
                poll_loop.dispatch(completion, &mut Vec::new());
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("READ must complete on the connected QP");
}

#[tokio::test]
async fn successful_receive_queued_before_close_is_not_reposted() {
    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (poller, mut poll_loop) = manual_poller(device.context());
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let socket = register_idle_socket(&poller, device, &pool, &state);
    let peer = register_idle_socket(&poller, device, &pool, &state);
    connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
    assert!(poll_loop.drain_incoming(false));
    socket.queue_pair.recv(pool.allocate(64).unwrap()).unwrap();
    poll_loop
        .conns
        .get_mut(&socket.queue_pair.qp_num())
        .unwrap()
        .flow
        .receive_posted();
    let mut message = pool.allocate(64).unwrap();
    message.set_len(64);
    message.fill(0x5a);
    peer.queue_pair
        .send_signaled(message, ruapc_rdma::ibv_send_flags::IBV_SEND_SIGNALED)
        .unwrap();

    let cq = poller.cq().clone();
    let mut storage = CompletionBatch::<1>::new();
    let mut batch = DispatchBatch::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(completion) = cq.poll_batch(&mut storage).unwrap().next() {
                assert!(completion.info().succ());
                if completion.info().is_recv() {
                    assert_eq!(completion.qp_num(), socket.queue_pair.qp_num());
                    // The provider has completed a receive, but shutdown wins
                    // before the poller consumes its CQ-issued proof. An ERR
                    // transition must not be followed by a new receive post.
                    socket.set_error();
                    poll_loop.dispatch(completion, &mut batch);
                    break;
                }
                poll_loop.dispatch(completion, &mut batch);
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    })
    .await
    .expect("the connected peer must produce a successful receive");

    let conn = poll_loop
        .conns
        .get_mut(&socket.queue_pair.qp_num())
        .unwrap();
    assert!(
        conn.flow.flushed(),
        "a successful CQE consumed after closure must not replenish the receive ring"
    );
    assert!(
        batch.is_empty(),
        "closed connections must not dispatch messages"
    );
    conn.update_flow_control().unwrap();
    assert!(conn.ready_to_remove());
    poll_loop.maintain_connections(Instant::now(), false, false);
    assert!(!poll_loop.conns.contains_key(&socket.queue_pair.qp_num()));
    poll_loop.shutdown_cleanup();
}

#[tokio::test]
async fn close_between_receive_health_check_and_post_is_flushed_by_poller() {
    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (poller, mut poll_loop) = manual_poller(device.context());
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let socket = register_idle_socket(&poller, device, &pool, &state);
    let peer = register_idle_socket(&poller, device, &pool, &state);
    connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
    assert!(poll_loop.drain_incoming(false));

    // Reproduce the interleaving without a timing-dependent race: the poller
    // has checked health when another task requests closure. Its in-progress
    // receive post must still precede the provider's ERR transition.
    assert!(socket.state.is_ok());
    socket.set_error();
    assert!(
        !poll_loop
            .conns
            .get_mut(&socket.queue_pair.qp_num())
            .unwrap()
            .ready_to_remove(),
        "software closure cannot remove the QP before requesting its provider flush"
    );
    socket.queue_pair.recv(pool.allocate(64).unwrap()).unwrap();
    let conn = poll_loop
        .conns
        .get_mut(&socket.queue_pair.qp_num())
        .unwrap();
    conn.flow.receive_posted();
    assert!(!conn.ready_to_remove());
    conn.update_flow_control().unwrap();
    assert!(
        !conn.ready_to_remove(),
        "the posted receive still needs its CQE"
    );

    let cq = poller.cq().clone();
    let mut storage = CompletionBatch::<1>::new();
    let mut batch = DispatchBatch::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(completion) = cq.poll_batch(&mut storage).unwrap().next() {
                assert_eq!(completion.qp_num(), socket.queue_pair.qp_num());
                assert!(completion.info().is_recv());
                assert_eq!(
                    completion.info().status,
                    ruapc_rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR
                );
                poll_loop.dispatch(completion, &mut batch);
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("poller closure must flush the receive admitted before its ERR transition");

    let conn = poll_loop
        .conns
        .get_mut(&socket.queue_pair.qp_num())
        .unwrap();
    assert!(conn.flow.flushed());
    assert!(conn.ready_to_remove());
    assert!(batch.is_empty());
    poll_loop.maintain_connections(Instant::now(), false, false);
    assert!(!poll_loop.conns.contains_key(&socket.queue_pair.qp_num()));
    poll_loop.shutdown_cleanup();
}

#[tokio::test]
async fn read_deadlines_bound_first_nic_admission_and_restore_unposted_targets() {
    use crate::remote_memory::{CopyOp, WriteTarget, scatter::SpaceLayout};

    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let short = Duration::from_millis(50);
    for (read_timeout, parent_remaining, expected) in [
        (Some(short), None, ErrorKind::RdmaReadTimeout),
        (
            Some(Duration::from_secs(5)),
            Some(short),
            ErrorKind::Timeout,
        ),
        (None, Some(short), ErrorKind::Timeout),
    ] {
        let (poller, mut poll_loop) = manual_poller(device.context());
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let socket = register_socket_with_config(
            &poller,
            device,
            &pool,
            &state,
            permits.clone(),
            SocketTestConfig {
                read_timeout,
                ..Default::default()
            },
        );
        let peer =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        assert!(poll_loop.drain_incoming(false));
        let mut source = pool.allocate(64).unwrap();
        source.set_len(64);
        source.fill(0x5a);
        let regions = [source.remote_buffer_info(&device.index()).unwrap()];
        let layout = SpaceLayout::from_lens([64]).unwrap();
        let ops = [CopyOp::new(0, 0, 64)];
        let mut other_destination = pool.allocate(64).unwrap();
        other_destination.set_len(64);
        let other_target = WriteTarget::new(vec![other_destination]).unwrap();
        let mut other_read =
            Box::pin(peer.read_into_target(&regions, &layout, &ops, other_target, None));
        assert!(futures_util::poll!(&mut other_read).is_pending());
        assert_eq!(peer.queue_pair.pending_read_count(), 1);

        let mut destination = pool.allocate(64).unwrap();
        destination.set_len(64);
        destination.fill(0xa5);
        let target = WriteTarget::new(vec![destination]).unwrap();
        let mut read = Box::pin(socket.read_into_target(
            &regions,
            &layout,
            &ops,
            target.clone(),
            parent_remaining.map(|remaining| Instant::now() + remaining),
        ));
        assert!(futures_util::poll!(&mut read).is_pending());
        assert_eq!(socket.queue_pair.pending_read_count(), 0);
        assert_eq!(permits.available_permits(), 0);

        // No completion or housekeeping pass runs: a batch waiting for its
        // first NIC permit has not entered the QP's READ map yet.
        let error = tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .expect("READ admission must honor its own and the parent's deadline")
            .unwrap_err();
        assert_eq!(error.kind, expected);
        assert!(
            socket.state.is_ok(),
            "unposted expiry must not close the QP"
        );
        assert_eq!(socket.queue_pair.pending_read_count(), 0);
        let returned = WriteTarget::try_into_buffers(target).unwrap();
        assert_eq!(returned[0].as_slice(), &[0xa5; 64]);
        assert_eq!(permits.available_permits(), 0);
        assert_eq!(peer.queue_pair.pending_read_count(), 1);

        drop(other_read);
        poll_loop.shutdown_cleanup();
        drop(socket);
        assert_eq!(permits.available_permits(), 0);
        drop(peer);
        assert_eq!(permits.available_permits(), 1);
        drop(source);
    }
}

#[tokio::test]
async fn read_deadlines_bound_partial_posting_without_releasing_dma() {
    use crate::remote_memory::{CopyOp, WriteTarget, scatter::SpaceLayout};

    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let short = Duration::from_millis(500);
    for (read_timeout, parent_remaining, expected) in [
        (Some(short), None, ErrorKind::RdmaReadTimeout),
        (
            Some(Duration::from_secs(5)),
            Some(short),
            ErrorKind::Timeout,
        ),
    ] {
        let (poller, mut poll_loop) = manual_poller(device.context());
        let permits = Arc::new(tokio::sync::Semaphore::new(2));
        let socket = register_socket_with_config(
            &poller,
            device,
            &pool,
            &state,
            permits.clone(),
            SocketTestConfig {
                read_timeout,
                ..Default::default()
            },
        );
        let peer =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        assert!(poll_loop.drain_incoming(false));
        let mut source = pool.allocate(192).unwrap();
        source.set_len(192);
        source.fill(0x5a);
        let regions = [source.remote_buffer_info(&device.index()).unwrap()];
        let layout = SpaceLayout::from_lens([192]).unwrap();
        let ops = [
            CopyOp::new(0, 0, 64),
            CopyOp::new(64, 64, 64),
            CopyOp::new(128, 128, 64),
        ];
        // A separate destination pool lets the test observe DMA ownership
        // independently of the socket's pool and the remote source.
        let destination_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let destination_lifetime = Arc::downgrade(&destination_pool);
        let mut destination = destination_pool.allocate(192).unwrap();
        destination.set_len(192);
        let target = WriteTarget::new(vec![destination]).unwrap();
        drop(destination_pool);
        let mut read = Box::pin(socket.read_into_target(
            &regions,
            &layout,
            &ops,
            target.clone(),
            parent_remaining.map(|remaining| Instant::now() + remaining),
        ));
        assert!(futures_util::poll!(&mut read).is_pending());
        assert_eq!(socket.queue_pair.pending_read_count(), 2);

        let mut other_destination = pool.allocate(64).unwrap();
        other_destination.set_len(64);
        let other_target = WriteTarget::new(vec![other_destination]).unwrap();
        let other_ops = [CopyOp::new(0, 0, 64)];
        let mut other_read =
            Box::pin(peer.read_into_target(&regions, &layout, &other_ops, other_target, None));
        assert!(futures_util::poll!(&mut other_read).is_pending());
        assert_eq!(peer.queue_pair.pending_read_count(), 0);

        // The first completion gives SQ capacity back to this batch, but
        // its NIC permit goes to the other QP already waiting in the FIFO.
        complete_one_read(&poller, &mut poll_loop).await;
        assert!(futures_util::poll!(&mut other_read).is_pending());
        assert_eq!(peer.queue_pair.pending_read_count(), 1);
        assert!(futures_util::poll!(&mut read).is_pending());
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        assert_eq!(permits.available_permits(), 0);

        let error = tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .expect("partial posting must stop waiting for NIC capacity at its deadline")
            .unwrap_err();
        assert_eq!(error.kind, expected);
        assert!(socket.state.is_ok());
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        assert_eq!(permits.available_permits(), 0);
        assert!(WriteTarget::try_into_buffers(target).is_none());
        assert!(destination_lifetime.upgrade().is_some());

        // Admission expiry alone does not prove this QP is faulty. Its
        // outstanding prefix is still subject to the poller's hard watchdog.
        poll_loop.maintain_connections(Instant::now() + Duration::from_secs(10), true, false);
        assert!(!socket.state.is_ok());
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        assert_eq!(permits.available_permits(), 0);

        drop(other_read);
        poll_loop.shutdown_cleanup();
        assert!(destination_lifetime.upgrade().is_some());
        assert_eq!(permits.available_permits(), 0);
        drop(socket);
        assert!(destination_lifetime.upgrade().is_none());
        assert_eq!(permits.available_permits(), 1);
        drop(peer);
        assert_eq!(permits.available_permits(), 2);
        drop(source);
    }
}

#[tokio::test]
async fn parent_read_completion_deadline_preserves_the_hard_watchdog() {
    use crate::remote_memory::{CopyOp, WriteTarget, scatter::SpaceLayout};

    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (poller, mut poll_loop) = manual_poller(device.context());
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let socket = register_socket_with_config(
        &poller,
        device,
        &pool,
        &state,
        permits.clone(),
        SocketTestConfig {
            read_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        },
    );
    let peer =
        register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
    connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
    assert!(poll_loop.drain_incoming(false));
    let mut source = pool.allocate(64).unwrap();
    source.set_len(64);
    let regions = [source.remote_buffer_info(&device.index()).unwrap()];
    let layout = SpaceLayout::from_lens([64]).unwrap();
    let mut destination = pool.allocate(64).unwrap();
    destination.set_len(64);
    let target = WriteTarget::new(vec![destination]).unwrap();
    let ops = [CopyOp::new(0, 0, 64)];
    // An expired absolute request deadline cannot post even when both
    // resources are immediately available, and leaves the target reusable.
    let error = socket
        .read_into_target(
            &regions,
            &layout,
            &ops,
            target.clone(),
            Some(Instant::now()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert_eq!(socket.queue_pair.pending_read_count(), 0);
    assert_eq!(permits.available_permits(), 1);
    let mut read = Box::pin(socket.read_into_target(
        &regions,
        &layout,
        &ops,
        target.clone(),
        Some(Instant::now() + Duration::from_millis(50)),
    ));
    assert!(futures_util::poll!(&mut read).is_pending());
    assert_eq!(socket.queue_pair.pending_read_count(), 1);
    let now = Instant::now();
    poll_loop.maintain_connections(now + Duration::from_millis(100), true, false);
    let error = tokio::time::timeout(Duration::from_secs(1), read)
        .await
        .expect("the parent deadline must settle an already-posted batch")
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(socket.state.is_ok());
    assert_eq!(socket.queue_pair.pending_read_count(), 1);
    assert_eq!(permits.available_permits(), 0);
    assert!(WriteTarget::try_into_buffers(target).is_none());

    // Taking the receiver at the parent deadline must not disable the
    // independent watchdog for READs still owned by this QP.
    poll_loop.maintain_connections(now + Duration::from_secs(2), true, false);
    assert!(!socket.state.is_ok());
    assert_eq!(socket.queue_pair.pending_read_count(), 1);
    assert_eq!(permits.available_permits(), 0);
    poll_loop.shutdown_cleanup();
    drop(socket);
    assert_eq!(permits.available_permits(), 1);
    drop(peer);
    drop(source);
}

#[tokio::test]
async fn closing_a_posted_read_without_a_watchdog_wakes_before_any_completion() {
    use crate::remote_memory::{CopyOp, WriteTarget, scatter::SpaceLayout};

    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (poller, mut poll_loop) = manual_poller(device.context());
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let socket =
        register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
    let peer =
        register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
    connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
    assert!(poll_loop.drain_incoming(false));
    let mut source = pool.allocate(64).unwrap();
    source.set_len(64);
    let regions = [source.remote_buffer_info(&device.index()).unwrap()];
    let layout = SpaceLayout::from_lens([64]).unwrap();
    let destination_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let destination_lifetime = Arc::downgrade(&destination_pool);
    let mut destination = destination_pool.allocate(64).unwrap();
    destination.set_len(64);
    let target = WriteTarget::new(vec![destination]).unwrap();
    drop(destination_pool);
    let ops = [CopyOp::new(0, 0, 64)];
    let mut read = Box::pin(socket.read_into_target(&regions, &layout, &ops, target.clone(), None));
    assert!(futures_util::poll!(&mut read).is_pending());
    assert_eq!(socket.queue_pair.pending_read_count(), 1);
    socket.set_error();
    let error = tokio::time::timeout(Duration::from_secs(1), read)
        .await
        .expect("closing a QP must wake its READ even with read_timeout_ms = 0")
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::ConnectionClosed);
    assert_eq!(socket.queue_pair.pending_read_count(), 1);
    assert_eq!(permits.available_permits(), 0);
    assert!(WriteTarget::try_into_buffers(target).is_none());
    assert!(destination_lifetime.upgrade().is_some());
    poll_loop.shutdown_cleanup();
    assert!(destination_lifetime.upgrade().is_some());
    drop(socket);
    assert!(destination_lifetime.upgrade().is_none());
    assert_eq!(permits.available_permits(), 1);
    drop(peer);
    drop(source);
}

#[tokio::test]
async fn shutdown_returns_unpolled_read_credits_only_after_qp_destruction() {
    use crate::remote_memory::{CopyOp, WriteTarget, scatter::SpaceLayout};

    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();

    // Exercise an unpublished connection, an ordinary registered connection,
    // a partially completed batch, and fully completed work (no double refund).
    for (completed, incoming) in [(0, true), (0, false), (1, false), (2, false)] {
        let (poller, mut poll_loop) = manual_poller(device.context());
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let socket =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        let peer =
            register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        if !incoming {
            assert!(poll_loop.drain_incoming(false));
        }
        let mut source = pool.allocate(128).unwrap();
        source.set_len(128);
        source.fill(0x5a);
        let regions = [source.remote_buffer_info(&device.index()).unwrap()];
        let layout = SpaceLayout::from_lens([128]).unwrap();
        let mut destination = pool.allocate(128).unwrap();
        destination.set_len(128);
        let target = WriteTarget::new(vec![destination]).unwrap();
        let ops = [CopyOp::new(0, 0, 64), CopyOp::new(64, 64, 64)];
        let mut read =
            Box::pin(socket.read_into_target(&regions, &layout, &ops, target.clone(), None));
        assert!(futures_util::poll!(&mut read).is_pending());
        assert_eq!(socket.queue_pair.pending_read_count(), 1);
        assert_eq!(permits.available_permits(), 0);

        for count in 0..completed {
            complete_one_read(&poller, &mut poll_loop).await;
            assert_eq!(socket.queue_pair.pending_read_count(), 0);
            if count == 0 {
                // The second WR was waiting for the NIC credit returned by
                // the first CQE. The semaphore has assigned that credit to
                // its waiter already, before the future is polled again.
                assert_eq!(permits.available_permits(), 0);
                assert!(futures_util::poll!(&mut read).is_pending());
                assert_eq!(socket.queue_pair.pending_read_count(), 1);
                assert_eq!(permits.available_permits(), 0);
            } else {
                assert_eq!(permits.available_permits(), 1);
            }
        }
        if completed == 2 {
            tokio::time::timeout(Duration::from_secs(1), read)
                .await
                .expect("processed READ completions must settle their waiter")
                .unwrap();
            let buffers = target.take_for_read().unwrap();
            assert_eq!(&buffers[0][..], &source[..]);
        } else {
            poll_loop.shutdown_cleanup();
            let error = tokio::time::timeout(Duration::from_secs(1), read)
                .await
                .expect("shutdown must settle the READ waiter")
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::ConnectionClosed);
            assert_eq!(socket.queue_pair.pending_read_count(), 1);
            assert_eq!(
                permits.available_permits(),
                0,
                "failing the waiter cannot end DMA"
            );
        }
        poll_loop.shutdown_cleanup();
        assert!(socket.read_credits.is_closed());
        assert!(peer.read_credits.is_closed());
        drop(socket);
        assert_eq!(
            permits.available_permits(),
            1,
            "QP destruction must refund only remaining READs"
        );
        drop(peer);
        assert_eq!(permits.available_permits(), 1);
        // The remote source outlives both provider QPs, including unpolled READs.
        drop(source);
    }
}

#[tokio::test]
async fn shutdown_closes_live_connections_before_refunding_incoming_reads() {
    use crate::remote_memory::{CopyOp, WriteTarget, scatter::SpaceLayout};
    use std::task::{Context, Wake, Waker};

    struct RefundProbe {
        live: Arc<RdmaSocket>,
        woke: AtomicBool,
    }
    impl Wake for RefundProbe {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert!(
                self.live.read_credits.is_closed(),
                "a stopped CQ must not admit new READs from a refund"
            );
            self.woke.store(true, Ordering::Release);
        }
    }

    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (poller, mut poll_loop) = manual_poller(device.context());
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let live =
        register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
    assert!(poll_loop.drain_incoming(false));
    let incoming =
        register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
    connect_read_pair(device, &incoming.queue_pair, &live.queue_pair);
    let mut source = pool.allocate(64).unwrap();
    source.set_len(64);
    let regions = [source.remote_buffer_info(&device.index()).unwrap()];
    let layout = SpaceLayout::from_lens([64]).unwrap();
    let mut destination = pool.allocate(64).unwrap();
    destination.set_len(64);
    let target = WriteTarget::new(vec![destination]).unwrap();
    let ops = [CopyOp::new(0, 0, 64)];
    let mut read = Box::pin(incoming.read_into_target(&regions, &layout, &ops, target, None));
    assert!(futures_util::poll!(&mut read).is_pending());
    drop(read);
    assert_eq!(incoming.queue_pair.pending_read_count(), 1);
    drop(incoming); // The inbox now owns the only socket reference.

    let probe = Arc::new(RefundProbe {
        live,
        woke: AtomicBool::new(false),
    });
    let waker = Waker::from(probe.clone());
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(permits.acquire());
    assert!(std::future::Future::poll(waiting.as_mut(), &mut context).is_pending());
    poll_loop.shutdown_cleanup();
    assert!(
        probe.woke.load(Ordering::Acquire),
        "incoming QP destruction must wake the NIC waiter"
    );
    drop(waiting.await.unwrap());
    assert_eq!(permits.available_permits(), 1);
    drop(waker);
    drop(probe);
    drop(source);
}

#[tokio::test]
async fn a_read_posted_after_the_shutdown_scan_does_not_wait_for_a_stopped_poller() {
    let devices = crate::rdma::test_utils::make_rdma_devices();
    let device = &devices.devices()[0];
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    let (poller, mut poll_loop) = manual_poller(device.context());
    let (state, _stop) = State::create(
        crate::Router::default(),
        &crate::SocketPoolConfig {
            rdma: None,
            ..Default::default()
        },
    )
    .unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let socket =
        register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
    let peer =
        register_idle_socket_with_read_permits(&poller, device, &pool, &state, permits.clone());
    connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
    let mut source = pool.allocate(64).unwrap();
    source.set_len(64);
    let remote = source.remote_buffer_info(&device.index()).unwrap();
    let mut destination = pool.allocate(64).unwrap();
    destination.set_len(64);
    let (mut posting, receiver) = socket
        .queue_pair
        .prepare_reads(
            vec![destination],
            vec![ruapc_rdma::ReadRequest {
                remote_addr: remote.addr,
                rkey: remote.key.rkey,
                segments: vec![ruapc_rdma::ReadSegment {
                    buffer: 0,
                    offset: 0,
                    len: 64,
                }],
            }],
            None,
        )
        .unwrap();
    let device_permit = permits.acquire().await.unwrap();
    assert!(socket.state.is_ok());
    // Pause after the final health check, before publishing to the QP. The
    // cleanup's failure scan sees no batch and will never consume another CQE.
    poll_loop.shutdown_cleanup();
    assert_eq!(socket.queue_pair.pending_read_count(), 0);
    // Providers such as RXE accept WRs on an ERR QP. Publish through the real
    // ownership map, without repeating admission or consuming its CQEs.
    match socket.queue_pair.post_read(&mut posting) {
        Ok(()) => {
            device_permit.forget();
            assert_eq!(socket.queue_pair.pending_read_count(), 1);
            let error =
                tokio::time::timeout(Duration::from_secs(1), socket.await_read_batch(receiver))
                    .await
                    .expect("late posting must not strand its waiter after shutdown")
                    .unwrap_err();
            assert_eq!(error.kind, ErrorKind::ConnectionClosed);
            drop(posting);
            assert_eq!(socket.queue_pair.pending_read_count(), 1);
            assert_eq!(permits.available_permits(), 0);
        }
        Err(_) => {
            // A provider may instead reject the late WR. Ownership must roll
            // back and the still-unposted permit must return through RAII.
            assert_eq!(socket.queue_pair.pending_read_count(), 0);
            assert_eq!(posting.remaining(), 1);
            assert_eq!(posting.cancel().unwrap()[0].len(), 64);
            drop(device_permit);
            assert_eq!(permits.available_permits(), 1);
        }
    }
    drop(socket);
    assert_eq!(permits.available_permits(), 1);
    drop(peer);
    drop(source);
}

fn mark_dirty(poll_loop: &mut PollLoop, qp_num: u32) {
    PollLoop::mark_dirty(
        poll_loop.conns.get_mut(&qp_num).unwrap(),
        &mut poll_loop.dirty_qps,
    );
}

/// Fill both pending stages without posting to the fixture's unconnected QP.
/// Byte markers make FIFO order observable without involving frame decoding.
fn fill_blocked_send_stages(
    poll_loop: &mut PollLoop,
    socket: &RdmaSocket,
    pool: &Arc<crate::BufferPool>,
) {
    assert!(matches!(
        socket.state.try_acquire(),
        crate::rdma::SendPermit::Granted { .. }
    ));
    let capacity = socket.pending_sender.max_capacity();
    for batch in 0..2 {
        for index in 0..capacity {
            let mut buffer = pool.allocate(1).unwrap();
            buffer.set_len(1);
            buffer[0] = (batch * capacity + index) as u8;
            socket.pending_sender.try_send(buffer).unwrap();
        }
        if batch == 0 {
            poll_loop
                .conns
                .get_mut(&socket.queue_pair.qp_num())
                .unwrap()
                .drain_pending();
        }
    }
    assert_eq!(socket.pending_sender.capacity(), 0);
    assert_eq!(
        poll_loop.conns[&socket.queue_pair.qp_num()]
            .pending_sends
            .len(),
        capacity
    );
}

#[tokio::test]
async fn pending_send_backlog_stays_bounded_and_cancellation_preserves_capacity() {
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
    let socket = register_idle_socket(&poller, device, &pool, &state);
    let qp_num = socket.queue_pair.qp_num();
    assert!(poll_loop.drain_incoming(false));
    fill_blocked_send_stages(&mut poll_loop, &socket, &pool);

    let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
    let mut meta = MsgMeta {
        msgid,
        flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
        ..Default::default()
    };
    let mut sending = Box::pin(socket.send(&mut meta, &27, &state));
    assert!(futures_util::poll!(sending.as_mut()).is_pending());

    // Repeated sender wakes and housekeeping must not move the ingress
    // backlog into an ever-growing FIFO while the SEND window is exhausted.
    for _ in 0..8 {
        poll_loop.maintain_connections(Instant::now(), false, false);
        assert_eq!(poll_loop.conns[&qp_num].pending_sends.len(), 4);
        assert_eq!(socket.pending_sender.capacity(), 0);
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
    }
    drop(sending);
    drop(response);
    assert_eq!(state.waiter.pending_count(), 0);
    assert_eq!(socket.pending_sender.capacity(), 0);

    // Releasing one staging slot permits exactly one ingress dequeue. The
    // cancelled producer must not retain the newly available channel slot.
    let conn = poll_loop.conns.get_mut(&qp_num).unwrap();
    assert_eq!(conn.pending_sends.pop_front().unwrap()[0], 0);
    conn.drain_pending();
    assert_eq!(
        conn.pending_sends
            .iter()
            .map(|frame| frame[0])
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(socket.pending_sender.capacity(), 1);
    let slot = socket.pending_sender.try_reserve().unwrap();
    assert_eq!(socket.pending_sender.capacity(), 0);
    drop(slot);
    assert_eq!(socket.pending_sender.capacity(), 1);
    poll_loop.shutdown_cleanup();
}

#[tokio::test]
async fn full_pending_send_backlog_wakes_on_close_and_poller_shutdown() {
    for shutdown in [false, true] {
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
        let socket = register_idle_socket(&poller, device, &pool, &state);
        let qp_num = socket.queue_pair.qp_num();
        assert!(poll_loop.drain_incoming(false));
        fill_blocked_send_stages(&mut poll_loop, &socket, &pool);
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            ..Default::default()
        };
        let mut sending = Box::pin(socket.send(&mut meta, &27, &state));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());

        if shutdown {
            poll_loop.shutdown_cleanup();
            assert!(socket.pending_sender.is_closed());
        } else {
            socket.set_error();
            // Keep the receiver and both full stages alive: logical closure
            // must wake admission without depending on queue destruction.
            assert!(!socket.pending_sender.is_closed());
            assert_eq!(socket.pending_sender.capacity(), 0);
            assert_eq!(poll_loop.conns[&qp_num].pending_sends.len(), 4);
        }
        let error = tokio::time::timeout(Duration::from_secs(1), sending.as_mut())
            .await
            .expect("closed pending admission must wake while a socket owner remains")
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        drop(sending);
        drop(response);
        poll_loop.shutdown_cleanup();
    }
}

#[tokio::test]
async fn pending_send_refill_spends_remaining_window_in_one_maintenance_pass() {
    for aggregate in [false, true] {
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
        let capacity = if aggregate { 2 } else { 1 };
        let socket = register_socket_with_config(
            &poller,
            device,
            &pool,
            &state,
            Arc::new(tokio::sync::Semaphore::new(1)),
            SocketTestConfig {
                capacity,
                window: 2,
                aggregate,
                ..Default::default()
            },
        );
        let peer = register_idle_socket(&poller, device, &pool, &state);
        connect_read_pair(device, &socket.queue_pair, &peer.queue_pair);
        assert!(poll_loop.drain_incoming(false));
        for _ in 0..2 {
            peer.queue_pair.recv(pool.allocate(64).unwrap()).unwrap();
        }

        let qp_num = socket.queue_pair.qp_num();
        for batch in 0..2 {
            for index in 0..capacity {
                let mut buffer = pool.allocate(1).unwrap();
                buffer.set_len(1);
                buffer[0] = (batch * capacity + index) as u8;
                socket.pending_sender.try_send(buffer).unwrap();
            }
            if batch == 0 {
                poll_loop.conns.get_mut(&qp_num).unwrap().drain_pending();
            }
        }
        assert_eq!(socket.pending_sender.capacity(), 0);
        assert_eq!(poll_loop.conns[&qp_num].pending_sends.len(), capacity);

        // No CQ polling occurs between batches. The second SEND credit must
        // drain the ingress stage after the first FIFO batch is posted.
        poll_loop.maintain_connections(Instant::now(), false, false);
        assert!(poll_loop.conns[&qp_num].pending_sends.is_empty());
        assert_eq!(socket.pending_sender.capacity(), capacity);
        assert_eq!(socket.state.try_acquire(), crate::rdma::SendPermit::Full);
        assert!(socket.state.is_ok());

        let mut received = Vec::new();
        let mut sends_completed = 0;
        let mut cursor = ruapc_rdma::CompletionCursor::default();
        let mut storage = CompletionBatch::<1>::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while received.len() < 2 || sends_completed < 2 {
                if let Some(completion) = poller.cq().poll_batch(&mut storage).unwrap().next() {
                    assert!(completion.info().succ());
                    if completion.qp_num() == qp_num {
                        assert!(!completion.info().is_recv());
                        poll_loop.dispatch(completion, &mut Vec::new());
                        sends_completed += 1;
                    } else {
                        assert_eq!(completion.qp_num(), peer.queue_pair.qp_num());
                        assert!(completion.info().is_recv());
                        // Consume genuine CQ-issued ownership evidence while
                        // retaining the received bytes for the FIFO assertion.
                        let completed = peer.queue_pair.complete(completion, &mut cursor).unwrap();
                        let buffer = completed.buffer.unwrap().into_single().unwrap();
                        received.push(buffer[..completed.wc.byte_len as usize].to_vec());
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        })
        .await
        .expect("both pending batches must arrive on the connected peer");
        let expected: Vec<Vec<u8>> = (0..2)
            .map(|batch| {
                (0..capacity)
                    .map(|index| (batch * capacity + index) as u8)
                    .collect()
            })
            .collect();
        assert_eq!(received, expected);
        poll_loop.shutdown_cleanup();
    }
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
    // The poller owns provider ERR transitions, after receive posting ends.
    poll_loop.maintain_connections(Instant::now(), false, false);
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
                max_large_msg_size: 0,
                traffic_class: 0,
            }))
            .unwrap()
            .bind(qp),
        buffer_pool.clone(),
        pending_sender,
        poller.waker(),
        RdmaSocketConfig {
            max_msg_size: 1024,
            max_large_msg_size: 0,
            max_inflight_large_bytes: 128 << 20,
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
                set_qp_error(socket);
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
    conn.start_error_flush();
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
