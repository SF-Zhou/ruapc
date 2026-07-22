#![forbid(unsafe_code)]
#![feature(return_type_notation)]
#![cfg(feature = "rdma")]

use std::{str::FromStr, sync::Arc};

use ruapc::{RdmaConnDirection, RdmaPathSelector, SocketPoolConfig, SocketType};

#[ruapc::service]
trait Foo {
    async fn hello(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String>;
}

struct FooImpl;

impl Foo for FooImpl {
    async fn hello(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        Ok(format!("hello {}!", req))
    }
}

/// End-to-end NIC awareness: after an RDMA call, both sides report the
/// connection with its full path (NIC pair), the per-device connection
/// counters are populated, and explicit path selectors are honored.
#[tokio::test]
async fn test_rdma_path_report_and_selector() {
    let _ = tracing_subscriber::fmt().try_init();

    let foo = Arc::new(FooImpl);
    let mut router = ruapc::Router::default();
    foo.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        ..Default::default()
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = ruapc::Client {
        socket_type: Some(SocketType::RDMA),
        ..Default::default()
    };
    let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);
    let rsp = client.hello(&ctx, &"ruapc".to_string()).await.unwrap();
    assert_eq!(rsp, "hello ruapc!");

    // Client side: one healthy outbound path towards the server.
    let report = ctx.state.rdma_path_report().await.unwrap();
    let outbound: Vec<_> = report
        .paths
        .iter()
        .filter(|p| p.direction == RdmaConnDirection::Outbound)
        .collect();
    assert!(!outbound.is_empty());
    let path = &outbound[0].path;
    assert_eq!(outbound[0].peer, Some(addr));
    assert!(outbound[0].healthy);
    assert!(!outbound[0].pinned);
    assert!(!path.local.device.is_empty());
    assert!(!path.remote.device.is_empty());
    // The local NIC's live connection counter accounts this connection.
    let load = report
        .devices
        .iter()
        .find(|d| d.device == path.local.device)
        .unwrap();
    assert!(load.connections >= 1);

    // Server side: the mirrored inbound path, including the client's
    // device name (carried in the connect request).
    let server_report = server.state().rdma_path_report().await.unwrap();
    let inbound = server_report
        .paths
        .iter()
        .find(|p| p.direction == RdmaConnDirection::Inbound)
        .unwrap();
    assert_eq!(inbound.peer, None);
    assert_eq!(inbound.path.local.device, path.remote.device);
    assert_eq!(inbound.path.remote.device, path.local.device);

    // Explicit selectors matching the established path are honored.
    for selector in [
        RdmaPathSelector::local_device(path.local.device.clone()),
        RdmaPathSelector::remote_device(path.remote.device.clone()),
    ] {
        let hinted = ctx.with_rdma_path(selector);
        let rsp = client.hello(&hinted, &"path".to_string()).await.unwrap();
        assert_eq!(rsp, "hello path!");
    }

    // A selector no NIC can satisfy fails the request.
    let bad = ctx.with_rdma_path(RdmaPathSelector::remote_device("nonexistent"));
    client.hello(&bad, &"nope".to_string()).await.unwrap_err();

    // Non-RDMA requests ignore the selector.
    let tcp_client = ruapc::Client {
        socket_type: Some(SocketType::TCP),
        ..Default::default()
    };
    let hinted = ctx.with_rdma_path(RdmaPathSelector::remote_device("nonexistent"));
    let rsp = tcp_client.hello(&hinted, &"tcp".to_string()).await.unwrap();
    assert_eq!(rsp, "hello tcp!");

    server.stop();
    tokio::time::timeout(std::time::Duration::from_secs(30), server.join())
        .await
        .unwrap();
}
