#![feature(return_type_notation)]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ruapc::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ==========================================================================
// Service definition
// ==========================================================================

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct WriteReq {
    data: Vec<u8>,
    #[serde(default)]
    delay_ms: u64,
}

#[service]
trait WriteTestService {
    async fn push_data(&self, ctx: &Context, req: &WriteReq) -> Result<()>;
}

struct WriteTestImpl;

impl WriteTestService for WriteTestImpl {
    async fn push_data(&self, ctx: &Context, req: &WriteReq) -> Result<()> {
        if req.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(req.delay_ms)).await;
        }
        // Allocate a server-side buffer, fill with data, push to client.
        let mut local_buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local_buf[..req.data.len()].copy_from_slice(&req.data);
        local_buf.set_len(req.data.len());

        let local_buf = ctx.remote_write(local_buf).await?;
        drop(local_buf);
        Ok(())
    }
}

// ==========================================================================
// Test framework
// ==========================================================================

struct TestCase {
    /// Data the server should push to the client.
    data: &'static [u8],
    /// Server-side delay before performing remote_write (ms).
    delay_ms: u64,
    /// Client timeout duration.
    client_timeout: Duration,
    /// Client socket type.
    socket_type: SocketType,
    /// Expected outcome.
    expect: Expected,
}

enum Expected {
    /// Expect success and verify received buffer matches data.
    Ok,
    /// Expect failure with this error kind.
    Err(ErrorKind),
}

async fn run_test(tc: TestCase) {
    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(WriteTestImpl).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();
    let ctx = Context::create(&config).unwrap().with_addr(addr);

    let client = Client {
        socket_type: Some(tc.socket_type),
        timeout: tc.client_timeout,
        ..Default::default()
    };
    let req = WriteReq {
        data: tc.data.to_vec(),
        delay_ms: tc.delay_ms,
    };

    let c = client.ready_to_recv();
    let result: Result<()> = c.push_data(&ctx, &req).await;

    match tc.expect {
        Expected::Ok => {
            result.unwrap();
            let received = c.take_write_buffer().expect("expected write buffer");
            assert_eq!(&received[..], tc.data);
        }
        Expected::Err(expected_kind) => {
            let err = result.unwrap_err();
            assert_eq!(err.kind, expected_kind);
        }
    }

    // Allow server to finish processing before shutdown.
    if tc.delay_ms > tc.client_timeout.as_millis() as u64 {
        tokio::time::sleep(Duration::from_millis(tc.delay_ms + 100)).await;
    }

    server.stop();
    tokio::time::timeout(Duration::from_secs(30), server.join())
        .await
        .unwrap();
}

// ==========================================================================
// Tests
// ==========================================================================

#[tokio::test]
async fn test_tcp_remote_write() {
    run_test(TestCase {
        data: b"Hello, Remote Write!",
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        expect: Expected::Ok,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_remote_write_timeout() {
    run_test(TestCase {
        data: b"timeout-write",
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        socket_type: SocketType::TCP,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write() {
    run_test(TestCase {
        data: b"Hello, RDMA Remote Write!",
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write_timeout() {
    run_test(TestCase {
        data: b"rdma-timeout-write",
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        socket_type: SocketType::RDMA,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}
