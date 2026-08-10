#![forbid(unsafe_code)]
// `#[service]` request types must be owned deserializable types behind a
// reference (`&String`), so `&str` is not an option here.
#![allow(clippy::ptr_arg)]
#![cfg(feature = "rdma")]

use std::{str::FromStr, sync::Arc};

use ruapc::{Endpoint, ListenMode, RdmaConnDirection, SocketPoolConfig, Transport};

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
/// connection with its full path and per-device connection counters.
#[tokio::test]
async fn test_rdma_path_report() {
    let _ = tracing_subscriber::fmt().try_init();

    let foo = Arc::new(FooImpl);
    let mut router = ruapc::Router::default();
    foo.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        rdma: Some(Default::default()),
        ..Default::default()
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = ruapc::Client::default();
    let ctx = ruapc::Context::create(&config)
        .unwrap()
        .with_endpoint(Endpoint::new(Transport::RDMA, addr));
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
        .find(|p| {
            p.direction == RdmaConnDirection::Inbound
                && p.path.local.device == path.remote.device
                && p.path.remote.device == path.local.device
        })
        .unwrap();
    assert_eq!(inbound.peer, None);
    assert_eq!(inbound.path.local.device, path.remote.device);
    assert_eq!(inbound.path.remote.device, path.local.device);

    server.stop();
    tokio::time::timeout(std::time::Duration::from_secs(30), server.join())
        .await
        .unwrap();
}
