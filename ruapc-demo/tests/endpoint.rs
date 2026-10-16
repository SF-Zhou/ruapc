//! Endpoint configuration stays portable across transport feature sets.

mod common;

use std::time::Duration;

use common::{TEST_TIMEOUT, TestServer, config};
use ruapc::{Client, Context, Endpoint, Error, ErrorKind, SocketPool, SocketPoolConfig, Transport};
use ruapc_demo::{EchoService, Request};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

/// An application-owned configuration, independent of RuaPC's internal types.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ApplicationConfig {
    endpoint: Endpoint,
}

fn restored_caches(config: &ApplicationConfig) -> [ApplicationConfig; 2] {
    let json = serde_json::to_vec(config).unwrap();
    let message_pack = rmp_serde::to_vec_named(config).unwrap();
    [
        serde_json::from_slice(&json).unwrap(),
        rmp_serde::from_slice(&message_pack).unwrap(),
    ]
}

/// Cargo may enable ruapc/rdma through another workspace package without
/// enabling this package's feature. Disable the backend when its actual
/// configuration field exists, without adding an unknown field otherwise.
fn stream_config() -> SocketPoolConfig {
    let mut value = serde_json::to_value(config("")).unwrap();
    if let Some(rdma) = value.get_mut("rdma") {
        *rdma = serde_json::Value::Null;
    }
    serde_json::from_value(value).unwrap()
}

#[test]
fn application_endpoint_caches_use_normalized_strings_for_every_transport() {
    for transport in [
        Transport::TCP,
        Transport::WS,
        Transport::HTTP,
        Transport::RDMA,
    ] {
        let input =
            format!("{transport}://[::1]:8000,127.0.0.1:8001,127.0.0.1:8000,127.0.0.1:8001");
        let canonical = format!("{transport}://127.0.0.1:8000,127.0.0.1:8001,[::1]:8000");
        let app: ApplicationConfig =
            serde_json::from_str(&format!(r#"{{"endpoint":"{input}"}}"#)).unwrap();

        assert_eq!(app.endpoint.transport(), transport);
        assert_eq!(app.endpoint.addrs().len(), 3);
        assert_eq!(app.endpoint, canonical.parse::<Endpoint>().unwrap());
        assert_eq!(
            serde_json::to_string(&app).unwrap(),
            format!(r#"{{"endpoint":"{canonical}"}}"#),
        );
        let packed = rmp_serde::to_vec_named(&app).unwrap();
        let packed_fields: serde_json::Value = rmp_serde::from_slice(&packed).unwrap();
        assert_eq!(packed_fields, serde_json::json!({ "endpoint": canonical }));
        for restored in restored_caches(&app) {
            assert_eq!(restored, app);
        }
    }
}

#[tokio::test]
async fn restored_stream_endpoints_can_be_passed_directly_to_rpc_contexts() {
    let server = TestServer::start("").await;
    let base = Context::create(&stream_config()).unwrap();
    let client = Client::default();
    let result = timeout(TEST_TIMEOUT, async {
        for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
            let app = ApplicationConfig {
                endpoint: Endpoint::new(transport, [server.addr, server.addr]).unwrap(),
            };
            for restored in restored_caches(&app) {
                let ctx = base.with_endpoint(restored.endpoint);
                let request = Request(format!("cached {transport} endpoint"));
                assert_eq!(client.echo(&ctx, &request).await.unwrap(), request.0);
            }
        }
    })
    .await;
    server.stop().await;
    result.expect("restored endpoints should complete RPCs");
}

fn assert_rdma_unavailable(error: Error) {
    assert_eq!(error.kind, ErrorKind::InvalidArgument);
    assert!(
        error.msg.contains("RDMA endpoint requires"),
        "unexpected RDMA availability error: {error}",
    );
}

#[tokio::test]
async fn cached_rdma_endpoint_reports_backend_unavailable_when_used() {
    let config = stream_config();
    let base = Context::create(&config).unwrap();
    let pool = SocketPool::create(&config, &base.state.devices, &base.state.buffer_pool).unwrap();
    let app = ApplicationConfig {
        endpoint: "rdma://127.0.0.1:0,[::1]:0".parse().unwrap(),
    };

    let result = timeout(TEST_TIMEOUT, async {
        for restored in restored_caches(&app) {
            assert_rdma_unavailable(
                pool.acquire(&restored.endpoint, &base.state)
                    .await
                    .unwrap_err(),
            );
            let ctx = base.with_endpoint(restored.endpoint);
            for client in [
                Client::default(),
                Client {
                    connect_timeout: Duration::ZERO,
                    max_retries: 3,
                    ..Default::default()
                },
            ] {
                assert_rdma_unavailable(
                    client
                        .echo(&ctx, &Request("unavailable".into()))
                        .await
                        .unwrap_err(),
                );
            }
        }
    })
    .await;
    pool.stop();
    timeout(TEST_TIMEOUT, pool.join())
        .await
        .expect("unused pool should stop");
    result.expect("unavailable RDMA must return an error without connecting");
}
