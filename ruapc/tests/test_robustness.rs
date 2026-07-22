//! Robustness tests: eager failure of in-flight requests on disconnect,
//! dead-connection eviction from socket pools (reconnect after restart),
//! handler panic containment, and HTTP body size limits.

#![forbid(unsafe_code)]
#![feature(return_type_notation)]

use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use ruapc::{ErrorKind, SocketPoolConfig, SocketType};

#[ruapc::service]
trait Robust {
    async fn echo(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String>;
    async fn sleepy(&self, _: &ruapc::Context, req: &u64) -> ruapc::Result<()>;
    async fn panicky(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String>;
}

struct RobustImpl;

impl Robust for RobustImpl {
    async fn echo(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        Ok(req.clone())
    }

    async fn sleepy(&self, _: &ruapc::Context, req: &u64) -> ruapc::Result<()> {
        tokio::time::sleep(Duration::from_secs(*req)).await;
        Ok(())
    }

    async fn panicky(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        panic!("intentional panic: {req}");
    }
}

fn make_server(config: &SocketPoolConfig) -> ruapc::Server {
    let service = Arc::new(RobustImpl);
    let mut router = ruapc::Router::default();
    service.ruapc_export(&mut router);
    ruapc::Server::create(router, config).unwrap()
}

fn unified_config() -> SocketPoolConfig {
    SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        ..Default::default()
    }
}

/// In-flight requests must fail eagerly with `ConnectionClosed` when the
/// connection dies, instead of hanging until the request timeout.
#[tokio::test]
async fn test_inflight_request_fails_fast_on_disconnect() {
    let _ = tracing_subscriber::fmt().try_init();

    for socket_type in [SocketType::TCP, SocketType::WS, SocketType::HTTP] {
        let config = unified_config();
        let server = make_server(&config);
        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
        let addr = server.listen(addr).await.unwrap();

        // Long client timeout: if the eager failure path doesn't work, the
        // elapsed-time assertion below fails long before this timeout.
        let client = ruapc::Client {
            socket_type: Some(socket_type),
            timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);

        let request = {
            let client = client.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move { client.sleepy(&ctx, &60).await })
        };

        // Let the request reach the server, then kill the server.
        tokio::time::sleep(Duration::from_millis(300)).await;
        server.stop();
        server.join().await;

        let started = std::time::Instant::now();
        let result = request.await.unwrap();
        let err = result.unwrap_err();
        assert_eq!(
            err.kind,
            ErrorKind::ConnectionClosed,
            "socket_type={socket_type:?}, err={err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "eager failure took too long for {socket_type:?}"
        );
    }
}

/// A dead connection must be evicted from the pool so that the next request
/// establishes a fresh connection (regression test: the HTTP pool used to
/// keep broken sockets forever).
#[tokio::test]
async fn test_pool_reconnects_after_server_restart() {
    let _ = tracing_subscriber::fmt().try_init();

    for socket_type in [SocketType::TCP, SocketType::WS, SocketType::HTTP] {
        let config = unified_config();
        let server = make_server(&config);
        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
        let addr = server.listen(addr).await.unwrap();

        let client = ruapc::Client {
            socket_type: Some(socket_type),
            ..Default::default()
        };
        let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);
        let rsp = client.echo(&ctx, &"one".to_string()).await.unwrap();
        assert_eq!(rsp, "one");

        server.stop();
        server.join().await;

        // Restart a server on the same address and verify the client
        // recovers. Retry: eviction is asynchronous and the listener may
        // need a moment to rebind.
        let server = make_server(&config);
        let addr = server.listen(addr).await.unwrap();
        let ctx = ctx.with_addr(addr);

        let mut recovered = false;
        for _ in 0..50 {
            if let Ok(rsp) = client.echo(&ctx, &"two".to_string()).await {
                assert_eq!(rsp, "two");
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(recovered, "client never recovered for {socket_type:?}");

        server.stop();
        server.join().await;
    }
}

/// A panicking handler must produce an error response instead of leaving the
/// client hanging, and must not affect subsequent requests.
#[tokio::test]
async fn test_handler_panic_returns_error() {
    let _ = tracing_subscriber::fmt().try_init();

    for socket_type in [SocketType::TCP, SocketType::HTTP] {
        let config = unified_config();
        let server = make_server(&config);
        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
        let addr = server.listen(addr).await.unwrap();

        // Long timeout: proves the error response arrives eagerly rather
        // than the client timing out.
        let client = ruapc::Client {
            socket_type: Some(socket_type),
            timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);

        let started = std::time::Instant::now();
        let err = client.panicky(&ctx, &"boom".to_string()).await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::HandlerPanic, "err={err:?}");
        assert!(err.msg.contains("intentional panic: boom"));
        assert!(started.elapsed() < Duration::from_secs(5));

        // The server must still be functional.
        let rsp = client.echo(&ctx, &"alive".to_string()).await.unwrap();
        assert_eq!(rsp, "alive");

        server.stop();
        server.join().await;
    }
}

/// Plain HTTP POST bodies beyond the wire-format limit must be rejected
/// with `413 Payload Too Large` instead of being buffered in memory.
#[tokio::test]
async fn test_http_unary_body_size_limit() {
    let _ = tracing_subscriber::fmt().try_init();

    let config = unified_config();
    let server = make_server(&config);
    let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = reqwest::Client::new();

    // 64 MiB + 1: just above MAX_MSG_SIZE.
    let body = vec![b'x'; (64 << 20) + 1];
    let rsp = client
        .post(format!("http://{addr}/Robust/echo"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(rsp.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);

    // A normal-sized request still works.
    let rsp = client
        .post(format!("http://{addr}/Robust/echo"))
        .body("\"hello\"")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<ruapc::Result<String>>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rsp, "hello");

    server.stop();
    server.join().await;
}
