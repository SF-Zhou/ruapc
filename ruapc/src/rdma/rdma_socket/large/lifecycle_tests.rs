//! Real provider-QP ownership tests without traffic or timing dependencies.
//!
//! The fixture reserves a live CQ and creates a registered-memory pool, but
//! never registers its socket with the poller. Large sends still cross the
//! normal transport-acceptance boundary; their descriptors remain in the
//! owned pending queue instead of being posted on the unconnected QP.

use super::*;
use crate::{
    MsgFlags, Router, SocketPoolConfig, SocketTrait,
    rdma::{
        DevicePollers, PollerConfig, RdmaBandwidthLimiter, RdmaConnectionConfig, RdmaNicInfo,
        RdmaPathInfo, RdmaQueuePairConfig, RdmaSocketConfig,
        frame::{Frame, decode_frames},
    },
};
use ruapc_bufpool::Device as _;
use ruapc_rdma::{CompletionQueue, QueuePair};

const INLINE_BYTES: usize = 16 * 1024;
const LARGE_BYTES: usize = 1024 * 1024;
const BYTE_BUDGET: usize = 4 * 1024 * 1024;

struct Fixture {
    socket: Arc<RdmaSocket>,
    pending: tokio::sync::mpsc::Receiver<Buffer>,
    cq: Arc<CompletionQueue>,
    _pollers: DevicePollers,
    state: Arc<State>,
    _stop: tokio_util::sync::DropGuard,
}

impl Fixture {
    fn new() -> Self {
        let devices = crate::rdma::test_utils::make_rdma_devices();
        let device = &devices.devices()[0];
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let connection = RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: 4,
                max_recv_wr: 4,
                max_send_sge: 1,
                max_recv_sge: 1,
            },
            recv_queue_len: 2,
            max_msg_size: INLINE_BYTES as u32,
            max_large_msg_size: LARGE_BYTES as u32,
            traffic_class: 0,
        };
        let pollers = DevicePollers::default();
        let (poller, reservation) = pollers
            .reserve(
                device,
                PollerConfig {
                    cq_len: 64,
                    read_limit: 2,
                    spin_us: 0,
                    dispatch_workers: 1,
                },
                1,
                &connection,
            )
            .unwrap();
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
        let (pending_sender, pending) = tokio::sync::mpsc::channel(4);
        let socket = Arc::new(RdmaSocket::new(
            reservation.bind(qp),
            pool,
            pending_sender,
            poller.waker(),
            RdmaSocketConfig {
                max_msg_size: INLINE_BYTES,
                max_large_msg_size: LARGE_BYTES,
                max_inflight_large_bytes: BYTE_BUDGET,
                send_window: 1,
                path: RdmaPathInfo {
                    local: nic.clone(),
                    remote: nic.clone(),
                    same_connectivity_domain: true,
                },
                read_timeout: None,
                read_permits: Arc::new(Semaphore::new(2)),
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
        let (state, stop) = State::create(
            Router::default(),
            &SocketPoolConfig {
                rdma: None,
                ..Default::default()
            },
        )
        .unwrap();
        Self {
            socket,
            pending,
            cq: poller.cq().clone(),
            _pollers: pollers,
            state,
            _stop: stop,
        }
    }

    async fn publish_request(&mut self, msgid: u64) -> (LargePayloadOffer, usize) {
        let mut meta = MsgMeta {
            msgid,
            method: "Lifecycle/large".into(),
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 30_000,
            ..Default::default()
        };
        self.socket
            .send(&mut meta, &"x".repeat(64 * 1024), &self.state)
            .await
            .unwrap();
        let descriptor = self.pending.try_recv().expect("accepted offer is queued");
        let mut offer = None;
        decode_frames(&Bytes::from_owner(descriptor), |frame| {
            let Frame::Control(ControlFrame::Offer(value)) = frame else {
                panic!("large request must enqueue a rendezvous offer");
            };
            assert!(offer.replace(value).is_none());
            Ok(())
        })
        .unwrap();
        let offer = offer.unwrap();
        let capacity = self.socket.large.sources.lock().unwrap().active[&offer.transfer_id]
            ._buffer
            .capacity();
        (offer, capacity)
    }
}

#[tokio::test]
async fn accepted_sources_survive_waiter_cancellation_expiry_and_socket_error() {
    let mut fixture = Fixture::new();
    let state = fixture.state.clone();
    let budget = fixture.socket.large.send_bytes.clone();
    let cq = fixture.cq.clone();
    let socket = Arc::downgrade(&fixture.socket);
    let (cancelled_id, cancelled) = state.waiter.alloc(Duration::from_secs(30));
    let (_, first_capacity) = fixture.publish_request(cancelled_id).await;
    let (expired_id, expired) = state.waiter.alloc(Duration::from_secs(30));
    let (_, second_capacity) = fixture.publish_request(expired_id).await;
    let retained = BYTE_BUDGET - first_capacity - second_capacity;
    assert_eq!(budget.available_permits(), retained);

    drop(cancelled);
    assert!(!state.waiter.contains_message_id(cancelled_id));
    state
        .waiter
        .expire(Instant::now() + Duration::from_secs(60));
    assert!(!state.waiter.contains_message_id(expired_id));
    assert_eq!(budget.available_permits(), retained);
    assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 2);

    fixture.socket.set_error();
    assert!(!fixture.socket.state.is_ok());
    assert!(budget.is_closed());
    assert_eq!(budget.available_permits(), retained);
    assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 2);
    assert_eq!(cq.qp_registry_stats().active, 1);

    drop(expired);
    drop(fixture);
    assert!(socket.upgrade().is_none());
    assert_eq!(cq.qp_registry_stats().active, 0);
    assert_eq!(budget.available_permits(), BYTE_BUDGET);
    state.socket_pool.join().await;
}

#[tokio::test]
async fn abandoning_a_receive_closes_the_qp_without_releasing_accepted_sources() {
    let mut fixture = Fixture::new();
    let state = fixture.state.clone();
    let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
    let (_, capacity) = fixture.publish_request(msgid).await;
    let budget = fixture.socket.large.send_bytes.clone();
    let cq = fixture.cq.clone();
    let transfer_id = 71;
    fixture
        .socket
        .large
        .receivers
        .lock()
        .unwrap()
        .active
        .insert(transfer_id, Receiving::Reading);

    drop(ReceiveGuard {
        socket: fixture.socket.clone(),
        id: transfer_id,
        settled: false,
    });
    assert!(!fixture.socket.state.is_ok());
    assert!(
        fixture
            .socket
            .large
            .receivers
            .lock()
            .unwrap()
            .active
            .is_empty()
    );
    assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 1);
    assert_eq!(budget.available_permits(), BYTE_BUDGET - capacity);
    assert_eq!(cq.qp_registry_stats().active, 1);

    drop(response);
    drop(fixture);
    assert_eq!(cq.qp_registry_stats().active, 0);
    assert_eq!(budget.available_permits(), BYTE_BUDGET);
    state.socket_pool.join().await;
}

#[tokio::test]
async fn a_replayed_offer_cannot_start_a_second_receiver() {
    let mut fixture = Fixture::new();
    let state = fixture.state.clone();
    let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
    let (mut offer, capacity) = fixture.publish_request(msgid).await;
    let budget = fixture.socket.large.send_bytes.clone();
    // The spawned receiver will skip READ because no waiter wants this
    // response. This allows real control dispatch on an unconnected QP.
    offer.meta.flags = MsgFlags::IsRsp | MsgFlags::UseMessagePack;
    offer.meta.msgid = u64::MAX;
    fixture
        .socket
        .handle_control(ControlFrame::Offer(offer.clone()), &state)
        .unwrap();
    let owners = Arc::strong_count(&fixture.socket);
    let error = fixture
        .socket
        .handle_control(ControlFrame::Offer(offer), &state)
        .unwrap_err();
    assert!(error.msg.contains("replayed"), "{error}");
    assert_eq!(Arc::strong_count(&fixture.socket), owners);
    assert_eq!(
        fixture.socket.large.receivers.lock().unwrap().active.len(),
        1
    );
    assert_eq!(
        fixture.socket.large.recv_bytes.available_permits(),
        BYTE_BUDGET
    );

    // Cancel and join the one supervised receiver before dropping its socket.
    state.socket_pool.stop();
    state.socket_pool.join().await;
    assert_eq!(Arc::strong_count(&fixture.socket), 1);
    assert!(!fixture.socket.state.is_ok());
    assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 1);
    assert_eq!(budget.available_permits(), BYTE_BUDGET - capacity);
    let cq = fixture.cq.clone();
    drop(response);
    drop(fixture);
    assert_eq!(cq.qp_registry_stats().active, 0);
    assert_eq!(budget.available_permits(), BYTE_BUDGET);
}

#[tokio::test]
async fn abandoned_destination_budget_is_held_until_provider_qp_destruction() {
    let fixture = Fixture::new();
    let state = fixture.state.clone();
    let budget = fixture.socket.large.recv_bytes.clone();
    let cq = fixture.cq.clone();
    let permit = budget
        .clone()
        .try_acquire_many_owned(LARGE_BYTES as u32)
        .unwrap();
    drop(DestinationBudget {
        socket: fixture.socket.clone(),
        permit: Some(permit),
    });

    assert!(!fixture.socket.state.is_ok());
    assert!(budget.is_closed());
    assert_eq!(
        fixture.socket.large.failed_receives.lock().unwrap().len(),
        1
    );
    assert_eq!(budget.available_permits(), BYTE_BUDGET - LARGE_BYTES);
    assert_eq!(cq.qp_registry_stats().active, 1);
    // Repeated closure must not reclaim permits for an unobserved READ.
    fixture.socket.set_error();
    assert_eq!(budget.available_permits(), BYTE_BUDGET - LARGE_BYTES);

    drop(fixture);
    assert_eq!(cq.qp_registry_stats().active, 0);
    assert_eq!(budget.available_permits(), BYTE_BUDGET);
    state.socket_pool.join().await;
}

#[tokio::test]
async fn exhausted_transfer_ids_reject_before_acceptance_without_wrapping() {
    let fixture = Fixture::new();
    let state = fixture.state.clone();
    let budget = fixture.socket.large.send_bytes.clone();
    let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
    fixture.socket.large.sources.lock().unwrap().next = u64::MAX;
    let mut meta = MsgMeta {
        msgid,
        method: "Lifecycle/large".into(),
        flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
        timeout_ms: 30_000,
        ..Default::default()
    };

    let error = fixture
        .socket
        .send(&mut meta, &"x".repeat(64 * 1024), &state)
        .await
        .unwrap_err();
    assert!(error.msg.contains("transfer IDs exhausted"), "{error}");
    assert_eq!(fixture.socket.large.sources.lock().unwrap().next, u64::MAX);
    assert!(
        fixture
            .socket
            .large
            .sources
            .lock()
            .unwrap()
            .active
            .is_empty()
    );
    assert!(fixture.pending.is_empty());
    assert_eq!(budget.available_permits(), BYTE_BUDGET);
    assert!(fixture.socket.state.is_ok());

    drop(response);
    drop(fixture);
    state.socket_pool.join().await;
}

#[tokio::test]
async fn cancelling_a_queue_blocked_send_releases_unpublished_source_and_budget() {
    let fixture = Fixture::new();
    let state = fixture.state.clone();
    let budget = fixture.socket.large.send_bytes.clone();
    let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
    for _ in 0..4 {
        let mut queued = fixture.socket.rdmabuf_pool.allocate(1).unwrap();
        queued.set_len(0);
        fixture.socket.pending_sender.try_send(queued).unwrap();
    }
    let mut meta = MsgMeta {
        msgid,
        method: "Lifecycle/large".into(),
        flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
        timeout_ms: 30_000,
        ..Default::default()
    };
    let payload = "x".repeat(64 * 1024);
    let mut sending = Box::pin(fixture.socket.send(&mut meta, &payload, &state));
    assert!(futures_util::poll!(sending.as_mut()).is_pending());
    assert!(budget.available_permits() < BYTE_BUDGET);
    assert!(
        fixture
            .socket
            .large
            .sources
            .lock()
            .unwrap()
            .active
            .is_empty()
    );
    assert_eq!(fixture.socket.large.sources.lock().unwrap().next, 0);
    assert_eq!(fixture.pending.len(), 4);

    drop(sending);
    assert_eq!(budget.available_permits(), BYTE_BUDGET);
    assert!(
        fixture
            .socket
            .large
            .sources
            .lock()
            .unwrap()
            .active
            .is_empty()
    );
    assert_eq!(fixture.pending.len(), 4);
    assert!(fixture.socket.state.is_ok());

    drop(response);
    drop(fixture);
    state.socket_pool.join().await;
}

#[tokio::test]
async fn queue_blocked_send_obeys_business_deadline_without_publishing_source() {
    let fixture = Fixture::new();
    let state = fixture.state.clone();
    let budget = fixture.socket.large.send_bytes.clone();
    for _ in 0..4 {
        let mut queued = fixture.socket.rdmabuf_pool.allocate(1).unwrap();
        queued.set_len(0);
        fixture.socket.pending_sender.try_send(queued).unwrap();
    }
    let payload = "x".repeat(64 * 1024);
    // Start the business deadline after device setup, registration, and queue
    // preparation, so only the blocked send consumes this short budget.
    let (msgid, response) = state.waiter.alloc(Duration::from_millis(100));
    let mut meta = MsgMeta {
        msgid,
        method: "Lifecycle/large".into(),
        flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
        timeout_ms: 100,
        ..Default::default()
    };
    let mut sending = Box::pin(fixture.socket.send(&mut meta, &payload, &state));
    assert!(futures_util::poll!(sending.as_mut()).is_pending());
    assert!(budget.available_permits() < BYTE_BUDGET);
    assert!(
        fixture
            .socket
            .large
            .sources
            .lock()
            .unwrap()
            .active
            .is_empty()
    );
    assert_eq!(fixture.socket.large.sources.lock().unwrap().next, 0);
    assert_eq!(fixture.pending.len(), 4);

    let error = tokio::time::timeout(Duration::from_secs(2), sending.as_mut())
        .await
        .expect("queue admission must finish within the request's short deadline")
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    drop(sending);
    assert!(
        fixture
            .socket
            .large
            .sources
            .lock()
            .unwrap()
            .active
            .is_empty()
    );
    assert_eq!(fixture.socket.large.sources.lock().unwrap().next, 0);
    assert_eq!(budget.available_permits(), BYTE_BUDGET);
    assert_eq!(fixture.pending.len(), 4);
    assert!(fixture.socket.state.is_ok());

    drop(response);
    drop(fixture);
    state.socket_pool.join().await;
}
