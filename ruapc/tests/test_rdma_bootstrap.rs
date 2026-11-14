#![forbid(unsafe_code)]
#![cfg(feature = "rdma")]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use ruapc::rdma::{RdmaConnDirection, RdmaPathReport};
use ruapc::{Client, Context, Endpoint, ListenMode, Listener, Server, SocketPoolConfig, Transport};

#[ruapc::service]
trait BootstrapProbe {
    async fn ping(&self, ctx: &Context, value: &u32) -> ruapc::Result<u32>;
}

struct BootstrapProbeImpl;

impl BootstrapProbe for BootstrapProbeImpl {
    async fn ping(&self, _: &Context, value: &u32) -> ruapc::Result<u32> {
        Ok(*value)
    }
}

fn config() -> SocketPoolConfig {
    // Constrain both ends to one NIC so CQ exhaustion cannot be avoided by
    // placing the second stripe on another device. Match the CI RXE policy.
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    let device = ruapc_rdma::ActiveDevice::available()
        .expect("RDMA devices must be available")
        .into_iter()
        .find(|device| {
            (!prefer_rxe || device.info().name.starts_with("rxe"))
                && device.info().ports.iter().any(|port| {
                    port.is_usable()
                        && (!port.port_attr.link_layer.is_ethernet() || !port.gids.is_empty())
                })
        })
        .expect("a connectable RDMA device must be available");
    let mut config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        ..Default::default()
    };
    let rdma = config.rdma.as_mut().unwrap();
    rdma.path.device_filter = vec![device.info().name.clone()];
    rdma.connection.qp.max_send_wr = 16;
    rdma.connection.qp.max_recv_wr = 16;
    rdma.connection.qp.max_send_sge = 1;
    rdma.connection.recv_queue_len = 4;
    rdma.connection.max_msg_size = 16 * 1024;
    rdma.peers.connections_per_peer = 2;
    rdma.polling.poll_threads_per_device = 1;
    rdma.polling.dispatch_workers = 1;
    // Disable periodic replenishment so it cannot hide a lost stripe. The
    // first acquisition still establishes one stripe synchronously; the
    // second one comes from the first-use background preconnection.
    rdma.maintenance.interval_ms = 0;
    config
}

async fn start_server(config: &SocketPoolConfig) -> (Server, std::net::SocketAddr) {
    let mut router = ruapc::Router::default();
    Arc::new(BootstrapProbeImpl).ruapc_export(&mut router);
    let server = Server::create(router, config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    (server, address)
}

async fn stop_server(server: &Server) {
    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .expect("server must release its connections promptly");
}

fn healthy_qps(report: &RdmaPathReport, direction: RdmaConnDirection) -> BTreeSet<u32> {
    report
        .paths
        .iter()
        .filter(|path| path.direction == direction && path.healthy)
        .map(|path| path.qp_num)
        .collect()
}

/// The first call publishes one stripe; background reconciliation adds the
/// rest. Waits until both ends report `count` healthy connections.
async fn wait_for_stripes(client: &Context, server: &Server, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let outbound = healthy_qps(
                &client.state.rdma_path_report().await.unwrap(),
                RdmaConnDirection::Outbound,
            );
            let inbound = healthy_qps(
                &server.state().rdma_path_report().await.unwrap(),
                RdmaConnDirection::Inbound,
            );
            if outbound.len() == count && inbound.len() == count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("configured stripes connect in the background");
}

/// Equivalent group spellings reuse one stripe set. Losing every bootstrap
/// listener does not close connections already serving RDMA requests.
#[tokio::test]
async fn bootstrap_group_reuses_stripes_after_listeners_stop() {
    let config = config();
    let mut router = ruapc::Router::default();
    Arc::new(BootstrapProbeImpl).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let first = Listener::new();
    let second = Listener::new();
    let first_addr = first
        .start_listen("127.0.0.1:0".parse().unwrap(), server.state())
        .await
        .unwrap();
    let second_addr = second
        .start_listen("127.0.0.1:0".parse().unwrap(), server.state())
        .await
        .unwrap();
    let unavailable = "127.0.0.1:0".parse().unwrap();
    let endpoint = Endpoint::rdma(Transport::TCP, [first_addr, unavailable, second_addr]).unwrap();
    let context = Context::create(&config)
        .unwrap()
        .with_endpoint(endpoint.clone());
    let reordered = context.with_endpoint(
        Endpoint::rdma(
            Transport::TCP,
            [second_addr, first_addr, second_addr, unavailable],
        )
        .unwrap(),
    );
    let client = Client {
        max_retries: 0,
        ..Default::default()
    };
    let (first_reply, second_reply) =
        tokio::join!(client.ping(&context, &1), client.ping(&reordered, &2),);
    assert_eq!(first_reply.unwrap(), 1);
    assert_eq!(second_reply.unwrap(), 2);
    wait_for_stripes(&context, &server, 2).await;
    let report = context.state.rdma_path_report().await.unwrap();
    let qps = healthy_qps(&report, RdmaConnDirection::Outbound);
    assert_eq!(qps.len(), 2);
    assert_eq!(report.paths.len(), 2);
    assert!(
        report
            .paths
            .iter()
            .all(|path| path.peer.as_ref() == Some(&endpoint))
    );

    first.stop();
    second.stop();
    first.join().await;
    second.join().await;
    for value in 3..7 {
        assert_eq!(client.ping(&reordered, &value).await.unwrap(), value);
    }
    assert_eq!(
        healthy_qps(
            &context.state.rdma_path_report().await.unwrap(),
            RdmaConnDirection::Outbound,
        ),
        qps,
    );
    drop((context, reordered));
    stop_server(&server).await;
}

/// The first call returns on one published stripe. The background attempt for
/// the second stripe fails while reserving completion capacity, before the peer
/// prepare RPC creates a lease. The published stripe must stay healthy and no
/// half-prepared connection may leak.
#[tokio::test]
async fn background_stripe_failure_keeps_the_published_stripe_healthy() {
    let server_config = config();
    let (server, address) = start_server(&server_config).await;
    let mut client_config = server_config.clone();
    let rdma = client_config.rdma.as_mut().unwrap();
    // One stripe needs R4 + W2 + A2 + min(H32, K8) = 16 CQEs.
    // Request 16: providers may round to 31, still below two stripes' 32.
    rdma.polling.device_cq_len = 16;
    let endpoint = Endpoint::rdma(Transport::TCP, [address]).unwrap();
    let context = Context::create(&client_config)
        .unwrap()
        .with_endpoint(endpoint.clone());
    let client = Client {
        max_retries: 0,
        ..Default::default()
    };
    // The first call succeeds on the one stripe that fits the CQ budget; the
    // scheduled background attempt fails without disturbing it.
    assert_eq!(client.ping(&context, &7).await.unwrap(), 7);

    // Wait until the background attempt has failed and left its fill pending.
    // With periodic maintenance disabled nothing retries it here; this test
    // checks that the failed fill neither leaks a connection nor loses the
    // published stripe, and that the pending fill is reported.
    let client_report = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = context.state.rdma_path_report().await.unwrap();
            if report.pending_fill_peers.contains(&endpoint) {
                break report;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the failed background fill must stay pending");
    assert_eq!(client_report.pending_fill_peers, vec![endpoint.clone()]);
    // A failed fill is not health-detected recovery.
    assert!(client_report.recovering_peers.is_empty());
    let server_report = server.state().rdma_path_report().await.unwrap();
    assert_eq!(
        healthy_qps(&client_report, RdmaConnDirection::Outbound).len(),
        1,
        "a failed background fill changed the outbound stripe set: {client_report:?}"
    );
    assert_eq!(
        healthy_qps(&server_report, RdmaConnDirection::Inbound).len(),
        1,
        "a failed background fill leaked an accepted connection: {server_report:?}"
    );
    for value in 8..10 {
        assert_eq!(client.ping(&context, &value).await.unwrap(), value);
    }
    drop(context);
    stop_server(&server).await;
}

/// Every stripe needs its own activation receive, including stripes that no
/// user RPC uses. Leases must not close activated QPs once they expire.
#[tokio::test]
async fn all_initial_stripes_survive_accept_lease_expiration() {
    let mut config = config();
    config.rdma.as_mut().unwrap().peers.connect_lease_ms = 15_000;
    let (server, address) = start_server(&config).await;
    let context = Context::create(&config)
        .unwrap()
        .with_endpoint(Endpoint::rdma(Transport::TCP, [address]).unwrap());
    let client = Client {
        max_retries: 0,
        ..Default::default()
    };
    assert_eq!(client.ping(&context, &7).await.unwrap(), 7);
    wait_for_stripes(&context, &server, 2).await;

    let outbound = healthy_qps(
        &context.state.rdma_path_report().await.unwrap(),
        RdmaConnDirection::Outbound,
    );
    let inbound = healthy_qps(
        &server.state().rdma_path_report().await.unwrap(),
        RdmaConnDirection::Inbound,
    );
    assert_eq!(outbound.len(), 2);
    assert_eq!(inbound.len(), 2);

    // The lease sweeper runs at most one second apart; leave time for it to
    // pass the lease deadline before checking that the QPs survived.
    tokio::time::sleep(Duration::from_secs(17)).await;
    for value in 8..12 {
        assert_eq!(client.ping(&context, &value).await.unwrap(), value);
    }
    let client_report = context.state.rdma_path_report().await.unwrap();
    let server_report = server.state().rdma_path_report().await.unwrap();
    assert_eq!(
        healthy_qps(&client_report, RdmaConnDirection::Outbound),
        outbound
    );
    assert_eq!(
        healthy_qps(&server_report, RdmaConnDirection::Inbound),
        inbound
    );
    for report in [&client_report, &server_report] {
        assert_eq!(
            report
                .devices
                .iter()
                .map(|device| device.connections)
                .sum::<usize>(),
            2
        );
        assert_eq!(report.paths.len(), 2);
    }
    drop(context);
    stop_server(&server).await;
}

/// Bootstrap RPCs use the stream transport named by the RDMA endpoint scheme;
/// the data plane remains RDMA.
#[tokio::test]
async fn stream_bootstrap_transports_establish_rdma_connections() {
    for (bootstrap, listen_mode) in [
        (Transport::WS, ListenMode::WS),
        (Transport::HTTP, ListenMode::HTTP),
    ] {
        let mut config = config();
        config.listen_mode = listen_mode;
        let (server, address) = start_server(&config).await;
        let endpoint = Endpoint::rdma(bootstrap, [address]).unwrap();
        let context = Context::create(&config)
            .unwrap()
            .with_endpoint(endpoint.clone());
        let client = Client {
            max_retries: 0,
            ..Default::default()
        };
        assert_eq!(client.ping(&context, &7).await.unwrap(), 7);
        let report = context.state.rdma_path_report().await.unwrap();
        assert!(!report.paths.is_empty(), "{bootstrap:?}");
        assert!(
            report
                .paths
                .iter()
                .all(|path| path.peer.as_ref() == Some(&endpoint)),
            "{bootstrap:?}: {report:?}"
        );
        drop(context);
        stop_server(&server).await;
    }
}
