//! First-use RDMA coverage runs independently of periodic maintenance.

#![forbid(unsafe_code)]
#![cfg(feature = "rdma")]

use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    sync::Arc,
    time::Duration,
};

use ruapc::rdma::{RdmaConnDirection, RdmaPathReport, RdmaSubnetPolicy, StripePhase};
use ruapc::{Client, Context, Endpoint, ListenMode, Router, Server, SocketPoolConfig, Transport};
use ruapc_rdma::{ActiveDevice, GidType};
use tokio::{sync::Barrier, task::JoinSet, time::timeout};

#[ruapc::service]
trait PreconnectProbe {
    async fn ping(&self, ctx: &Context, value: &u32) -> ruapc::Result<u32>;
}

struct Probe;

impl PreconnectProbe for Probe {
    async fn ping(&self, _: &Context, value: &u32) -> ruapc::Result<u32> {
        Ok(*value)
    }
}

struct Nic {
    name: String,
    ips: BTreeSet<IpAddr>,
}

/// Same-host RoCE paths can match their own GID IPs without assuming that
/// separate physical NIC networks can route to each other. RXE CI has one NIC.
fn nics() -> Vec<Nic> {
    let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
    let mut nics: Vec<_> = ActiveDevice::available()
        .expect("RDMA device discovery must succeed")
        .iter()
        .filter_map(|device| {
            let info = device.info();
            if prefer_rxe && !info.name.starts_with("rxe") {
                return None;
            }
            let ips: BTreeSet<_> = info
                .ports
                .iter()
                .filter(|port| port.is_usable() && port.port_attr.link_layer.is_ethernet())
                .flat_map(|port| &port.gids)
                .filter(|gid| gid.gid_type == GidType::RoCEv2)
                .filter_map(|gid| {
                    let v6 = gid.gid.as_ipv6();
                    if v6.is_unicast_link_local() {
                        return None;
                    }
                    let ip = v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4);
                    (!ip.is_unspecified() && !ip.is_loopback()).then_some(ip)
                })
                .collect();
            (!ips.is_empty()).then(|| Nic {
                name: info.name.clone(),
                ips,
            })
        })
        .collect();
    nics.sort_by(|left, right| left.name.cmp(&right.name));
    assert!(
        !nics.is_empty(),
        "preconnect hardware tests require an active RoCEv2/RXE NIC with a usable IP GID; RUAPC_PREFER_RXE={prefer_rxe}"
    );
    nics
}

fn config(nics: &[Nic], minimum: u32, maximum: u32) -> SocketPoolConfig {
    let mut config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 128 << 20,
        ..Default::default()
    };
    let rdma = config.rdma.as_mut().unwrap();
    rdma.path.device_filter = nics.iter().map(|nic| nic.name.clone()).collect();
    // Every IP gets a separate connectivity domain: only identical local
    // and remote IPs match, even when their devices have multiple GIDs.
    rdma.path.subnets = nics
        .iter()
        .flat_map(|nic| &nic.ips)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|ip| format!("{ip}/{}", if ip.is_ipv4() { 32 } else { 128 }))
        .collect::<Vec<_>>()
        .join(";")
        .parse()
        .unwrap();
    rdma.path.subnet_policy = RdmaSubnetPolicy::Require;
    rdma.connection.qp.max_send_wr = 16;
    rdma.connection.qp.max_recv_wr = 16;
    rdma.connection.qp.max_send_sge = 1;
    rdma.connection.recv_queue_len = 4;
    rdma.connection.max_msg_size = 16 * 1024;
    rdma.peers.connections_per_peer = 1;
    rdma.peers.min_connections_per_remote_nic = minimum;
    rdma.peers.preconnect_max_per_peer = maximum;
    rdma.polling.poll_threads_per_device = 1;
    rdma.polling.dispatch_workers = 1;
    rdma.maintenance.interval_ms = 0;
    config
}

async fn start(
    server_config: &SocketPoolConfig,
    client_config: &SocketPoolConfig,
) -> (Server, Context) {
    let mut router = Router::default();
    Arc::new(Probe).ruapc_export(&mut router);
    let server = Server::create(router, server_config).unwrap();
    let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let context = Context::create(client_config)
        .unwrap()
        .with_endpoint(Endpoint::new(Transport::RDMA, [addr]).unwrap());
    (server, context)
}

fn client() -> Client {
    Client {
        connect_timeout: Duration::from_secs(10),
        timeout: Duration::from_secs(5),
        max_retries: 0,
        ..Default::default()
    }
}

fn counts(report: &RdmaPathReport) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for path in &report.paths {
        if path.direction == RdmaConnDirection::Outbound
            && path.phase == StripePhase::Active
            && path.healthy
        {
            *counts.entry(path.path.remote.device.clone()).or_default() += 1;
        }
    }
    counts
}

async fn wait_for_counts(context: &Context, expected: &BTreeMap<String, usize>) -> RdmaPathReport {
    let completed = timeout(Duration::from_secs(10), async {
        loop {
            let report = context.state.rdma_path_report().await.unwrap();
            let actual = counts(&report);
            assert!(
                actual
                    .iter()
                    .all(|(nic, count)| *count <= expected.get(nic).copied().unwrap_or(0)),
                "unexpected preconnect stripes: expected={expected:?}, report={report:?}"
            );
            if actual == *expected {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    match completed {
        Ok(report) => report,
        Err(_) => panic!(
            "background coverage did not finish with maintenance disabled: expected={expected:?}, report={:?}",
            context.state.rdma_path_report().await.unwrap()
        ),
    }
}

/// Observe background work without driving another acquire or user request.
async fn assert_counts_stay(context: &Context, expected: &BTreeMap<String, usize>) {
    let until = tokio::time::Instant::now() + Duration::from_millis(500);
    loop {
        let report = context.state.rdma_path_report().await.unwrap();
        assert_eq!(counts(&report), *expected, "{report:?}");
        if tokio::time::Instant::now() >= until {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn stop(server: Server, context: Context) {
    drop(context);
    server.stop();
    timeout(Duration::from_secs(5), server.join())
        .await
        .expect("server must stop promptly after background preconnect");
}

#[tokio::test]
async fn first_acquires_cover_every_matching_remote_nic_without_maintenance() {
    let nics = nics();
    let expected: BTreeMap<_, _> = nics.iter().map(|nic| (nic.name.clone(), 2)).collect();
    // Leave room above the desired count so the cap cannot hide duplicate
    // background tasks overfilling coverage after concurrent first acquires.
    let maximum = u32::try_from(nics.len() * 2 + 2).unwrap();
    let config = config(&nics, 2, maximum);
    let (server, context) = start(&config, &config).await;
    let barrier = Arc::new(Barrier::new(16));
    let mut calls = JoinSet::new();
    for value in 0..16 {
        let context = context.clone();
        let barrier = barrier.clone();
        calls.spawn(async move {
            barrier.wait().await;
            assert_eq!(client().ping(&context, &value).await.unwrap(), value);
        });
    }
    while let Some(call) = calls.join_next().await {
        call.unwrap();
    }

    // No further RPCs trigger connection acquisition. Even one-NIC hosts
    // need background work because initial=1 and minimum-per-remote=2.
    let report = wait_for_counts(&context, &expected).await;
    assert!(
        report
            .paths
            .iter()
            .all(|path| path.path.same_connectivity_domain)
    );
    assert_counts_stay(&context, &expected).await;
    stop(server, context).await;
}

#[tokio::test]
async fn coverage_respects_disabled_minimum_and_connection_cap() {
    let nics = nics();
    let one = &nics[..1];
    let expected = BTreeMap::from([(one[0].name.clone(), 1)]);
    for (minimum, maximum) in [(0, 4), (2, 1)] {
        let config = config(one, minimum, maximum);
        let (server, context) = start(&config, &config).await;
        assert_eq!(client().ping(&context, &7).await.unwrap(), 7);
        assert_counts_stay(&context, &expected).await;
        stop(server, context).await;
    }
}

#[tokio::test]
async fn coverage_skips_remote_nics_without_a_local_domain_match() {
    let nics = nics();
    let Some(local_index) = nics
        .iter()
        .position(|local| nics.iter().any(|remote| local.ips.is_disjoint(&remote.ips)))
    else {
        eprintln!(
            "skipping only remote-NIC exclusion case: need two active RoCEv2 NICs with disjoint GID IPs; full coverage and limits still run on one NIC"
        );
        return;
    };
    let local = &nics[local_index];
    let expected: BTreeMap<_, _> = nics
        .iter()
        .filter(|remote| !local.ips.is_disjoint(&remote.ips))
        .map(|remote| (remote.name.clone(), 2))
        .collect();
    let maximum = u32::try_from(nics.len() * 2 + 2).unwrap();
    let server_config = config(&nics, 2, maximum);
    let client_config = config(&nics[local_index..=local_index], 2, maximum);
    let (server, context) = start(&server_config, &client_config).await;
    assert_eq!(client().ping(&context, &9).await.unwrap(), 9);
    let report = wait_for_counts(&context, &expected).await;
    assert!(report.paths.iter().all(|path| {
        path.path.local.device == local.name && path.path.same_connectivity_domain
    }));
    assert_counts_stay(&context, &expected).await;

    let server_report = server.state().rdma_path_report().await.unwrap();
    for remote in &nics {
        if !expected.contains_key(&remote.name) {
            let load = server_report
                .devices
                .iter()
                .find(|device| device.device == remote.name)
                .expect("unmatched remote NIC must still be enabled on the server");
            assert_eq!(load.connections, 0, "{server_report:?}");
        }
    }
    stop(server, context).await;
}
