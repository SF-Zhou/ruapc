#![forbid(unsafe_code)]
#![cfg(feature = "rdma")]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use ruapc::rdma::{RdmaConnDirection, RdmaPathReport};
use ruapc::{
    Client, Context, Endpoint, ErrorKind, ListenMode, Server, SocketPoolConfig, Transport,
};

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
    // Only handshake cleanup may change the tested connections. Maintenance
    // must not hide a lost stripe by replenishing it in the background.
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

/// The first stripe is already confirmed when the second stripe fails local
/// registration. Both the first committed lease and the second prepared lease
/// must be rolled back, without waiting for either accept lease to expire.
#[tokio::test]
async fn initial_stripe_failure_rolls_back_all_unpublished_connections() {
    let server_config = config();
    let (server, address) = start_server(&server_config).await;
    let mut client_config = server_config.clone();
    let rdma = client_config.rdma.as_mut().unwrap();
    // register_socket reserves twice the combined SQ/RQ depth per stripe.
    rdma.polling.device_cq_len =
        2 * (rdma.connection.qp.max_send_wr + rdma.connection.qp.max_recv_wr);
    let context = Context::create(&client_config)
        .unwrap()
        .with_endpoint(Endpoint::new(Transport::RDMA, address));
    let client = Client {
        max_retries: 0,
        ..Default::default()
    };
    let error = client.ping(&context, &7).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Overloaded, "{error}");
    for expected in [
        "register socket",
        "shared CQ capacity exhausted",
        "64 + 64 > 64",
        "attempt",
        &address.to_string(),
    ] {
        assert!(
            error.msg.contains(expected),
            "missing {expected:?}: {error}"
        );
    }
    let cleanup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let client_report = context.state.rdma_path_report().await.unwrap();
            let server_report = server.state().rdma_path_report().await.unwrap();
            if [&client_report, &server_report].iter().all(|report| {
                report.paths.is_empty()
                    && report.devices.iter().all(|device| device.connections == 0)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if cleanup.is_err() {
        let client_report = context.state.rdma_path_report().await.unwrap();
        let server_report = server.state().rdma_path_report().await.unwrap();
        panic!(
            "bootstrap rollback retained connections: client={client_report:?}, server={server_report:?}"
        );
    }
    drop(context);
    stop_server(&server).await;
}

/// Every stripe needs its own activation receive, including stripes that the
/// first user RPC does not use. Tombstone expiration must keep both QPs alive.
#[tokio::test]
async fn all_initial_stripes_survive_accept_lease_expiration() {
    let mut config = config();
    config.rdma.as_mut().unwrap().peers.connect_lease_ms = 15_000;
    let (server, address) = start_server(&config).await;
    let context = Context::create(&config)
        .unwrap()
        .with_endpoint(Endpoint::new(Transport::RDMA, address));
    let client = Client {
        max_retries: 0,
        ..Default::default()
    };
    assert_eq!(client.ping(&context, &7).await.unwrap(), 7);

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
    // process the expired tombstones before checking that the QPs survived.
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
