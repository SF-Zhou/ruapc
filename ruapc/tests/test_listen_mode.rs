//! Every listener combination admits exactly its configured protocols.

#![forbid(unsafe_code)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use ruapc::{
    Client, Context, Endpoint, ListenMode, Result, Router, Server, SocketPoolConfig, Transport,
};
use tokio::time::timeout;

#[ruapc::service]
trait ModeService {
    async fn echo(&self, ctx: &Context, request: &u64) -> Result<u64>;
}

#[derive(Default)]
struct ModeServiceImpl {
    calls: AtomicUsize,
}

impl ModeService for ModeServiceImpl {
    async fn echo(&self, _ctx: &Context, request: &u64) -> Result<u64> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(*request)
    }
}

#[tokio::test]
async fn all_listener_combinations_gate_protocols_and_http_routes() {
    let client = Client {
        connect_timeout: Duration::from_secs(5),
        timeout: Duration::from_secs(5),
        max_retries: 0,
        ..Default::default()
    };
    let disabled_client = Client {
        connect_timeout: Duration::from_millis(250),
        timeout: Duration::from_millis(250),
        ..client.clone()
    };
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    // A TCP-only client configuration must still allow outbound WS and HTTP.
    let client_config = SocketPoolConfig {
        buffer_pool_memory: 16 << 20,
        http_base_path: "/api".into(),
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    };
    let base = Context::create(&client_config).unwrap();

    for listen_mode in [
        ListenMode::TCP,
        ListenMode::HTTP,
        ListenMode::WS,
        ListenMode::TCP | ListenMode::HTTP,
        ListenMode::TCP | ListenMode::WS,
        ListenMode::HTTP | ListenMode::WS,
        ListenMode::UNIFIED,
    ] {
        let service = Arc::new(ModeServiceImpl::default());
        let mut router = Router::default();
        service.clone().ruapc_export(&mut router);
        let config = SocketPoolConfig {
            listen_mode,
            ..client_config.clone()
        };
        let server = Server::create(router, &config).unwrap();
        let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let mut expected_calls = 0;

        // The typed HTTP client uses /_rpc under its configured base path.
        for (transport, flag) in [
            (Transport::TCP, ListenMode::TCP),
            (Transport::HTTP, ListenMode::HTTP),
            (Transport::WS, ListenMode::WS),
        ] {
            let ctx = base.with_endpoint(Endpoint::new(transport, [addr]).unwrap());
            let client = if listen_mode.contains(flag) {
                &client
            } else {
                &disabled_client
            };
            let result = timeout(Duration::from_secs(10), client.echo(&ctx, &42))
                .await
                .unwrap_or_else(|_| panic!("{listen_mode} stalled for {transport}"));
            if listen_mode.contains(flag) {
                assert_eq!(
                    result.unwrap_or_else(|error| panic!("{listen_mode}, {transport}: {error}")),
                    42
                );
                expected_calls += 1;
            } else {
                // A protocol mismatch can close the connection or stall while
                // the selected decoder waits for its own framing or headers.
                result.expect_err("disabled protocol reached the handler");
            }
            assert_eq!(service.calls.load(Ordering::SeqCst), expected_calls);
        }

        let http_timeout = if listen_mode.contains(ListenMode::HTTP) {
            Duration::from_secs(5)
        } else {
            Duration::from_millis(250)
        };
        let response = http
            .post(format!("http://{addr}/api/ModeService/echo"))
            .timeout(http_timeout)
            .json(&43_u64)
            .send()
            .await;
        if listen_mode.contains(ListenMode::HTTP) {
            let response = response.unwrap().error_for_status().unwrap();
            assert_eq!(response.json::<Result<u64>>().await.unwrap().unwrap(), 43);
            expected_calls += 1;
        } else {
            assert_http_rejected(response, listen_mode, "unary RPC");
        }
        assert_eq!(service.calls.load(Ordering::SeqCst), expected_calls);

        for path in ["/openapi.json", "/rapidoc", "/rapidoc/rapidoc-min.js"] {
            let response = http
                .get(format!("http://{addr}/api{path}"))
                .timeout(http_timeout)
                .send()
                .await;
            if listen_mode.contains(ListenMode::HTTP) {
                assert!(
                    response.unwrap().status().is_success(),
                    "{listen_mode} did not expose {path}"
                );
            } else {
                assert_http_rejected(response, listen_mode, path);
            }
        }
        assert_eq!(service.calls.load(Ordering::SeqCst), expected_calls);

        if listen_mode.contains(ListenMode::WS) && !listen_mode.contains(ListenMode::HTTP) {
            // A POST with otherwise valid upgrade headers is not a WebSocket
            // handshake, and must not dispatch the HTTP method either.
            let response = http
                .post(format!("http://{addr}/api/ModeService/echo"))
                .header("Connection", "upgrade")
                .header("Upgrade", "websocket")
                .header("Sec-WebSocket-Version", "13")
                .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
                .json(&44_u64)
                .send()
                .await;
            match response {
                Ok(response) => {
                    assert_ne!(response.status().as_u16(), 101);
                    assert!(!response.status().is_success());
                }
                Err(error) => assert!(!error.is_timeout(), "{listen_mode}: {error}"),
            }
            assert_eq!(service.calls.load(Ordering::SeqCst), expected_calls);
        }

        server.stop();
        timeout(Duration::from_secs(5), server.join())
            .await
            .unwrap();
    }
}

fn assert_http_rejected(
    response: std::result::Result<reqwest::Response, reqwest::Error>,
    mode: ListenMode,
    route: &str,
) {
    if let Ok(response) = response {
        assert!(
            !response.status().is_success(),
            "{mode} unexpectedly exposed {route}"
        );
    }
}
