//! Real-QP checks with faults confined to built-in RPC handlers. No provider,
//! port or application RPC is disabled, and reports never drive acquisition.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use crate::{
    Client, Context, Endpoint, Error, ErrorKind, MsgMeta, Result, Router, Server, Socket,
    SocketPoolConfig, State, Transport,
    core::ContextEndpoint,
    rdma::{
        RdmaConnDirection, RdmaSocket, StripePhase, probe_connection, test_utils::small_rdma_config,
    },
    remote_memory::ReadSource,
    service,
    services::RequestStatusRequest,
};

use super::{probe_delay, record_result};

#[test]
fn probe_jitter_stays_positive_and_bounded_for_all_configured_intervals() {
    for interval_ms in [1, 2, 3, 60_000, u64::MAX] {
        let interval = Duration::from_millis(interval_ms);
        let lower = interval / 2;
        let upper = interval + interval / 2;
        let samples = (0..1_000).chain([u64::MAX]);
        let delays = samples
            .map(|sample| probe_delay(interval, sample))
            .collect::<BTreeSet<_>>();
        assert!(
            delays
                .iter()
                .all(|delay| { !delay.is_zero() && *delay >= lower && *delay < upper })
        );
        assert_eq!(delays.len(), 1_000, "even 1ms intervals retain jitter");
    }
}

#[test]
fn timeout_threshold_counts_both_timeout_kinds_and_resets_on_other_results() {
    let mut count = 0;
    assert!(!record_result(
        &mut count,
        &Err(ErrorKind::Timeout.into()),
        3
    ));
    assert!(!record_result(
        &mut count,
        &Err(ErrorKind::RdmaReadTimeout.into()),
        3
    ));
    assert!(record_result(
        &mut count,
        &Err(ErrorKind::Timeout.into()),
        3
    ));
    assert_eq!(count, 3);
    for result in [
        Ok(()),
        Err(Error::kind(ErrorKind::Overloaded)),
        Err(Error::kind(ErrorKind::InvalidArgument)),
    ] {
        count = 2;
        assert!(!record_result(&mut count, &result, 3));
        assert_eq!(count, 0);
        assert!(!record_result(
            &mut count,
            &Err(ErrorKind::Timeout.into()),
            3
        ));
        assert_eq!(count, 1);
    }
}

#[test]
fn timeout_threshold_supports_one_and_saturates_without_wrapping() {
    let mut count = 0;
    assert!(record_result(
        &mut count,
        &Err(ErrorKind::Timeout.into()),
        1
    ));
    count = u32::MAX;
    assert!(record_result(
        &mut count,
        &Err(ErrorKind::Timeout.into()),
        u32::MAX
    ));
    assert_eq!(count, u32::MAX);
}

#[service]
trait ProbeTestRpc {
    async fn ping(&self, ctx: &Context, value: &u32) -> Result<u32>;
}

impl ProbeTestRpc for () {
    async fn ping(&self, _: &Context, value: &u32) -> Result<u32> {
        Ok(*value)
    }
}

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    DropProbe,
    TwoTimeoutsThenSuccess,
    DropReadValidation,
}

#[derive(Default)]
struct Observations {
    probes: BTreeMap<u32, usize>,
    reads: BTreeMap<u32, usize>,
    target: u32,
    fault: Fault,
    attempts: usize,
    dropped: usize,
    source: Option<Weak<ReadSource>>,
}

#[derive(Clone, Default)]
struct Observer(Arc<Mutex<Observations>>);

fn socket(ctx: &Context) -> &Arc<RdmaSocket> {
    let ContextEndpoint::Connected(Socket::RDMA(socket)) = &ctx.endpoint else {
        panic!("health checks must use an established RDMA socket");
    };
    socket
}

impl Observer {
    fn router(&self) -> Router {
        let mut router = Router::default();
        ProbeTestRpc::ruapc_export(Arc::new(()), &mut router);
        let observer = self.clone();
        router.wrap_method("RdmaService/probe", move |original| {
            Box::new(move |ctx, payload| {
                let qp = socket(&ctx).queue_pair.qp_num();
                assert_eq!(
                    ctx.msg_meta.read_regions.iter().map(|r| r.len).sum::<u64>(),
                    8,
                    "each probe must advertise its READ challenge"
                );
                let drop_response = {
                    let mut observations = observer.0.lock().unwrap();
                    *observations.probes.entry(qp).or_default() += 1;
                    if qp == observations.target
                        && matches!(
                            observations.fault,
                            Fault::DropProbe | Fault::TwoTimeoutsThenSuccess
                        )
                    {
                        observations.attempts += 1;
                        let drop_response = matches!(observations.fault, Fault::DropProbe)
                            || !observations.attempts.is_multiple_of(3);
                        observations.dropped += usize::from(drop_response);
                        drop_response
                    } else {
                        false
                    }
                };
                if drop_response {
                    Ok(())
                } else {
                    original(ctx, payload)
                }
            })
        });
        let observer = self.clone();
        router.wrap_method("MemoryService/request_is_pending", move |original| {
            Box::new(move |ctx, payload| {
                let qp = socket(&ctx).queue_pair.qp_num();
                let drop_response = {
                    let mut observations = observer.0.lock().unwrap();
                    *observations.reads.entry(qp).or_default() += 1;
                    if qp == observations.target
                        && matches!(observations.fault, Fault::DropReadValidation)
                    {
                        // This reverse RPC is sent only after the real READ
                        // completed. Capture ownership without extending it.
                        let request: RequestStatusRequest = payload.deserialize(&ctx.msg_meta)?;
                        observations.source = ctx
                            .state
                            .waiter
                            .read_source(request.request_id)
                            .map(|source| Arc::downgrade(&source));
                        observations.attempts += 1;
                        observations.dropped += 1;
                        true
                    } else {
                        false
                    }
                };
                if drop_response {
                    Ok(())
                } else {
                    original(ctx, payload)
                }
            })
        });
        router
    }

    fn arm(&self, target: u32, fault: Fault) {
        let mut observations = self.0.lock().unwrap();
        observations.target = target;
        observations.fault = fault;
        observations.attempts = 0;
        observations.dropped = 0;
    }

    fn dropped(&self) -> usize {
        self.0.lock().unwrap().dropped
    }

    fn attempts(&self) -> usize {
        self.0.lock().unwrap().attempts
    }

    fn covered(&self, qps: &BTreeSet<u32>) -> bool {
        let observations = self.0.lock().unwrap();
        qps.iter().all(|qp| {
            observations.probes.get(qp).copied().unwrap_or(0) >= 2
                && observations.reads.get(qp).copied().unwrap_or(0) >= 2
        })
    }
}

fn config(interval_ms: u64, timeout_ms: u64) -> SocketPoolConfig {
    let mut config = small_rdma_config();
    let rdma = config.rdma.as_mut().unwrap();
    rdma.peers.connections_per_peer = 2;
    // Replacement must work even when remote-NIC preconnect and periodic
    // maintenance are disabled; there are still two desired initial stripes.
    rdma.peers.preconnect_max_per_peer = 2;
    rdma.probe.interval_ms = interval_ms;
    rdma.probe.timeout_ms = timeout_ms;
    rdma.probe.max_consecutive_timeouts = 3;
    config
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("health-check observation did not arrive");
}

async fn healthy_qps(state: &State, direction: RdmaConnDirection) -> BTreeSet<u32> {
    state
        .rdma_path_report()
        .await
        .unwrap()
        .paths
        .into_iter()
        .filter(|path| {
            path.direction == direction && path.healthy && path.phase == StripePhase::Active
        })
        .map(|path| path.qp_num)
        .collect()
}

struct Fixture {
    server: Server,
    context: Context,
    client_observer: Observer,
    server_observer: Observer,
    outbound: BTreeSet<u32>,
    inbound: BTreeSet<u32>,
}

impl Fixture {
    async fn new(client_interval: u64, server_interval: u64, timeout_ms: u64) -> Self {
        let client_observer = Observer::default();
        let server_observer = Observer::default();
        let server = Server::create(
            server_observer.router(),
            &config(server_interval, timeout_ms),
        )
        .unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let context = Context::create_with_router(
            client_observer.router(),
            &config(client_interval, timeout_ms),
        )
        .unwrap()
        .with_endpoint(Endpoint::rdma(Transport::TCP, [address]).unwrap());
        let client = Client {
            max_retries: 0,
            ..Default::default()
        };
        assert_eq!(client.ping(&context, &19).await.unwrap(), 19);
        let outbound = healthy_qps(&context.state, RdmaConnDirection::Outbound).await;
        let inbound = healthy_qps(server.state(), RdmaConnDirection::Inbound).await;
        assert_eq!(outbound.len(), 2);
        assert_eq!(inbound.len(), 2);
        Self {
            server,
            context,
            client_observer,
            server_observer,
            outbound,
            inbound,
        }
    }

    fn connected_client(&self, qp: u32) -> Context {
        let pool = self.context.state.socket_pool.rdma_pool().unwrap();
        let socket = pool
            .peers
            .iter()
            .flat_map(|peer| peer.all_sockets())
            .find(|socket| socket.queue_pair.qp_num() == qp)
            .expect("initial client QP exists");
        Context {
            state: self.context.state.clone(),
            endpoint: ContextEndpoint::Connected(Socket::RDMA(socket)),
            drop_guard: None,
            msg_meta: MsgMeta::default(),
            deadline: None,
        }
    }

    async fn assert_unchanged(&self) {
        assert_eq!(
            healthy_qps(&self.context.state, RdmaConnDirection::Outbound).await,
            self.outbound
        );
        assert_eq!(
            healthy_qps(self.server.state(), RdmaConnDirection::Inbound).await,
            self.inbound
        );
    }

    async fn wait_for_one_replacement(&self) {
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let outbound = healthy_qps(&self.context.state, RdmaConnDirection::Outbound).await;
                let inbound = healthy_qps(self.server.state(), RdmaConnDirection::Inbound).await;
                if outbound.len() == 2
                    && inbound.len() == 2
                    && outbound.intersection(&self.outbound).count() == 1
                    && inbound.intersection(&self.inbound).count() == 1
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "one idle stripe was not replaced: client={:?}, server={:?}",
            self.context.state.rdma_path_report().await.unwrap(),
            self.server.state().rdma_path_report().await.unwrap()
        );
    }

    async fn shutdown(self) {
        self.context.state.socket_pool.stop();
        self.server.stop();
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(self.context.state.socket_pool.join(), self.server.join());
        })
        .await
        .expect("health checks must not hold shutdown open");
        assert_eq!(self.context.state.waiter.pending_count(), 0);
        assert_eq!(self.server.state().waiter.pending_count(), 0);
    }
}

#[tokio::test]
async fn both_ends_complete_rpc_and_read_probe_on_every_stripe() {
    // Keep this independent of the periodic observation test: direct calls
    // must not contribute to its evidence that the timer probes idle sockets.
    let fixture = Fixture::new(0, 0, 500).await;
    let outbound: Vec<_> = fixture
        .context
        .state
        .socket_pool
        .rdma_pool()
        .unwrap()
        .peers
        .iter()
        .flat_map(|peer| peer.all_sockets())
        .collect();
    let inbound: Vec<_> = fixture
        .server
        .state()
        .socket_pool
        .rdma_pool()
        .unwrap()
        .inbound
        .lock()
        .unwrap()
        .iter()
        .filter_map(Weak::upgrade)
        .collect();
    for (state, sockets, expected) in [
        (&fixture.context.state, outbound, &fixture.outbound),
        (fixture.server.state(), inbound, &fixture.inbound),
    ] {
        assert_eq!(
            sockets
                .iter()
                .map(|socket| socket.queue_pair.qp_num())
                .collect::<BTreeSet<_>>(),
            *expected
        );
        for socket in sockets {
            probe_connection(&socket, state, Duration::from_millis(500))
                .await
                .expect("every connection must complete RPC and return the real READ nonce");
        }
    }
    fixture.assert_unchanged().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn both_ends_probe_rpc_and_real_read_on_every_idle_stripe() {
    let fixture = Fixture::new(40, 40, 500).await;
    wait_until(|| {
        fixture.client_observer.covered(&fixture.outbound)
            && fixture.server_observer.covered(&fixture.inbound)
    })
    .await;
    fixture.assert_unchanged().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn third_timeout_replaces_only_the_failed_stripe_without_user_traffic() {
    let fixture = Fixture::new(30, 400, 180).await;
    let target = *fixture.inbound.first().unwrap();
    fixture.server_observer.arm(target, Fault::DropProbe);
    wait_until(|| fixture.server_observer.dropped() >= 2).await;
    fixture.assert_unchanged().await;
    wait_until(|| fixture.server_observer.dropped() >= 3).await;
    // Arrival of the third request is not its timeout. Both first and second
    // timeouts must have left this concrete connection usable.
    fixture.assert_unchanged().await;
    fixture.wait_for_one_replacement().await;
    assert_eq!(fixture.server_observer.dropped(), 3);
    fixture.shutdown().await;
}

#[tokio::test]
async fn successful_probe_resets_consecutive_timeouts() {
    let fixture = Fixture::new(30, 0, 150).await;
    fixture.server_observer.arm(
        *fixture.inbound.first().unwrap(),
        Fault::TwoTimeoutsThenSuccess,
    );
    wait_until(|| fixture.server_observer.attempts() >= 9).await;
    fixture.assert_unchanged().await;
    assert!(fixture.server_observer.dropped() >= 6);
    fixture.shutdown().await;
}

#[tokio::test]
async fn remote_read_validation_timeout_detects_failure_while_rpc_still_works() {
    let fixture = Fixture::new(30, 400, 180).await;
    let target = *fixture.outbound.first().unwrap();
    fixture
        .client_observer
        .arm(target, Fault::DropReadValidation);
    wait_until(|| fixture.client_observer.dropped() >= 1).await;
    // Pin an ordinary RPC to the exact QP with the broken READ validation.
    // After this check, only the background tasks can recover the connection.
    let context = fixture.connected_client(target);
    assert_eq!(Client::default().ping(&context, &73).await.unwrap(), 73);
    drop(context);
    wait_until(|| fixture.client_observer.dropped() >= 3).await;
    fixture.wait_for_one_replacement().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn inbound_probe_failure_is_recovered_by_the_idle_initiator() {
    let fixture = Fixture::new(400, 30, 180).await;
    fixture
        .client_observer
        .arm(*fixture.outbound.first().unwrap(), Fault::DropProbe);
    wait_until(|| fixture.client_observer.dropped() >= 3).await;
    fixture.wait_for_one_replacement().await;
    assert_eq!(fixture.client_observer.dropped(), 3);
    fixture.shutdown().await;
}

#[tokio::test]
async fn stopping_an_inflight_probe_releases_waiters_sources_and_state() {
    let fixture = Fixture::new(30, 0, 30_000).await;
    let target = *fixture.outbound.first().unwrap();
    fixture
        .client_observer
        .arm(target, Fault::DropReadValidation);
    wait_until(|| fixture.client_observer.dropped() >= 1).await;
    let source = fixture
        .client_observer
        .0
        .lock()
        .unwrap()
        .source
        .clone()
        .expect("post-READ validation must still own its original source");
    assert!(source.upgrade().is_some());
    let client_state = Arc::downgrade(&fixture.context.state);
    let server_state = Arc::downgrade(fixture.server.state());
    fixture.shutdown().await;
    wait_until(|| {
        source.upgrade().is_none()
            && client_state.upgrade().is_none()
            && server_state.upgrade().is_none()
    })
    .await;
}

#[tokio::test]
async fn zero_interval_disables_both_probe_tasks() {
    let fixture = Fixture::new(0, 0, 100).await;
    for state in [&fixture.context.state, fixture.server.state()] {
        assert!(
            !state
                .socket_pool
                .rdma_pool()
                .unwrap()
                .probe_started
                .load(std::sync::atomic::Ordering::Acquire)
        );
    }
    tokio::time::sleep(Duration::from_millis(120)).await;
    for observer in [&fixture.client_observer, &fixture.server_observer] {
        let observations = observer.0.lock().unwrap();
        assert!(observations.probes.is_empty());
        assert!(observations.reads.is_empty());
    }
    fixture.assert_unchanged().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn maintenance_retains_an_idle_empty_peer_until_probe_reconnects_it() {
    let fixture = Fixture::new(60_000, 0, 500).await;
    let pool = fixture.context.state.socket_pool.rdma_pool().unwrap();
    let peer = pool.peers.iter().next().unwrap().value().clone();
    // Prevent the first maintenance pass from reconnecting synchronously so
    // pruning can be checked independently of bootstrap speed.
    let connecting = peer.connect.lock().await;
    peer.meta.lock().unwrap().last_used = Some(Instant::now() - Duration::from_secs(120));
    for stripe in peer.active_snapshot() {
        stripe.socket.set_error();
    }
    pool.run_maintenance(&fixture.context.state).await;
    assert!(peer.active_snapshot().is_empty());
    assert!(peer.needs_reconnect());
    let endpoint = peer.endpoint.clone();
    let original = Arc::downgrade(&peer);
    {
        let mut meta = peer.meta.lock().unwrap();
        meta.last_used = Some(Instant::now() - Duration::from_secs(120));
        meta.backoff.insert(
            String::new(),
            super::super::reconcile::RetryBackoff {
                failures: 1,
                retry_at: Instant::now() + Duration::from_secs(60),
            },
        );
    }
    // No connect guard or external strong reference may prevent eviction in
    // this pass: only recovery intent protects the idle, empty peer.
    drop(connecting);
    drop(peer);
    pool.run_maintenance(&fixture.context.state).await;
    let retained = pool
        .existing_peer(&endpoint)
        .expect("an idle desired peer must survive maintenance eviction");
    assert!(Arc::ptr_eq(&retained, &original.upgrade().unwrap()));
    retained.meta.lock().unwrap().backoff.clear();
    drop(retained);
    pool.probe_round(&fixture.context.state, &mut HashMap::new())
        .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let qps = healthy_qps(&fixture.context.state, RdmaConnDirection::Outbound).await;
            let recovered = pool
                .existing_peer(&endpoint)
                .is_some_and(|peer| !peer.needs_reconnect());
            if qps.len() == 2 && qps.is_disjoint(&fixture.outbound) && recovered {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a fully pruned idle peer must recover without an acquisition");
    fixture.shutdown().await;
}

#[tokio::test]
async fn active_failure_keeps_recovery_pending_until_healthy_targets_are_restored() {
    let fixture = Fixture::new(0, 0, 100).await;
    let pool = fixture.context.state.socket_pool.rdma_pool().unwrap();
    let peer = pool.peers.iter().next().unwrap().value().clone();
    let initial = peer.active_snapshot();
    let failed = initial[0].socket.clone();
    let survivor = initial[1].socket.clone();
    assert!(peer.contains_active(&failed));
    assert!(!peer.needs_reconnect());

    assert!(peer.fail_active(&failed));
    assert!(!failed.state.is_ok());
    assert!(!peer.contains_active(&failed));
    assert!(peer.contains_active(&survivor));
    assert!(survivor.state.is_ok());
    assert_eq!(peer.active_snapshot().len(), 1);
    assert!(peer.needs_reconnect());
    // Per-NIC coverage is disabled in this fixture. One healthy socket
    // still does not satisfy its two-connection target.
    peer.finish_recovery(&[], &pool.config.peers);
    assert!(peer.needs_reconnect());

    pool.replenish_peer(&peer, &fixture.context.state).await;
    let restored = peer.active_snapshot();
    assert_eq!(restored.len(), 2);
    assert!(restored.iter().all(|stripe| stripe.socket.state.is_ok()));
    assert!(peer.contains_active(&survivor));
    assert!(!peer.needs_reconnect());
    // A stale failure for the old identity cannot request another recovery.
    assert!(!peer.fail_active(&failed));
    assert!(!peer.needs_reconnect());
    drop((initial, restored, failed, survivor, peer));
    fixture.shutdown().await;
}

#[tokio::test]
async fn late_probe_failure_does_not_close_a_draining_connection() {
    let fixture = Fixture::new(0, 0, 100).await;
    let pool = fixture.context.state.socket_pool.rdma_pool().unwrap();
    let peer = pool.peers.iter().next().unwrap().value().clone();
    let socket = {
        let mut stripes = peer.stripes.write().unwrap();
        let stripe = stripes.active.pop().unwrap();
        let socket = stripe.socket.clone();
        stripes.draining.push(stripe);
        socket
    };
    let (msgid, receiver) = fixture.context.state.waiter.alloc(Duration::from_secs(30));
    fixture
        .context
        .state
        .waiter
        .bind_connection(msgid, socket.conn_id);
    pool.probe_connection_closed(&fixture.context.state, &socket, Some(&peer));
    assert!(socket.state.is_ok());
    assert!(!peer.needs_reconnect());
    assert!(fixture.context.state.waiter.contains_message_id(msgid));
    assert_eq!(peer.stripes.read().unwrap().draining.len(), 1);
    drop(receiver);
    drop((peer, socket));
    fixture.shutdown().await;
}
