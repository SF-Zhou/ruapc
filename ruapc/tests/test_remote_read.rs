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
struct ReadReq {
    expected_len: usize,
    #[serde(default)]
    delay_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
struct ReadRsp {
    data: Vec<u8>,
}

#[service]
trait RemoteReadService {
    async fn read_buf(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp>;
}

struct RemoteReadServiceImpl;

impl RemoteReadService for RemoteReadServiceImpl {
    async fn read_buf(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp> {
        if req.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(req.delay_ms)).await;
        }
        let buffer_info = ctx
            .msg_meta
            .buffer_info
            .as_ref()
            .expect("expected buffer_info in msg_meta");
        let local_buf = ctx.state.buffer_pool.allocate().unwrap();
        let local_buf = ctx.remote_read(buffer_info, local_buf).await?;
        Ok(ReadRsp {
            data: local_buf[..req.expected_len].to_vec(),
        })
    }
}

// ==========================================================================
// Test framework
// ==========================================================================

struct TestCase {
    /// Data to fill into the client buffer. If empty, buffer is left zeroed.
    data: &'static [u8],
    /// How many bytes the server should read from the buffer.
    expected_len: usize,
    /// Server-side delay before performing remote_read (ms).
    delay_ms: u64,
    /// Client timeout duration.
    client_timeout: Duration,
    /// Client socket type.
    socket_type: SocketType,
    /// Expected outcome: Ok(data) or Err(error_kind).
    expect: Expected,
}

enum Expected {
    /// Expect success and verify returned data matches `TestCase::data`.
    Ok,
    /// Expect failure with this error kind.
    Err(ErrorKind),
}

async fn run_test(tc: TestCase) {
    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let mut router = Router::default();
    Arc::new(RemoteReadServiceImpl).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();
    let ctx = Context::create(&config).unwrap().with_addr(addr);

    let mut buf = ctx.state.buffer_pool.allocate().unwrap();
    if !tc.data.is_empty() {
        buf[..tc.data.len()].copy_from_slice(tc.data);
    }

    let client = Client {
        socket_type: Some(tc.socket_type),
        timeout: tc.client_timeout,
        ..Default::default()
    };
    let req = ReadReq {
        expected_len: tc.expected_len,
        delay_ms: tc.delay_ms,
    };

    let result: Result<ReadRsp> = client.read_buf_with_buffer(&ctx, &req, &buf).await;

    match tc.expect {
        Expected::Ok => {
            let rsp = result.unwrap();
            assert_eq!(rsp.data, tc.data);
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
async fn test_tcp_uuid_validation_success() {
    run_test(TestCase {
        data: b"uuid-check-pass",
        expected_len: 15,
        delay_ms: 50,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        expect: Expected::Ok,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_uuid_validation_timeout() {
    run_test(TestCase {
        data: b"uuid-check-fail",
        expected_len: 15,
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        socket_type: SocketType::TCP,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

/// When expected_len exceeds the buffer capacity, the server handler panics
/// (slice out of bounds), no response is sent, and the client times out.
#[tokio::test]
async fn test_tcp_remote_read_expected_len_exceeds_buffer() {
    run_test(TestCase {
        data: b"",
        expected_len: 3 * 1024 * 1024, // Exceeds 4 KiB local_buf
        delay_ms: 0,
        client_timeout: Duration::from_millis(500),
        socket_type: SocketType::TCP,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_uuid_validation_success() {
    run_test(TestCase {
        data: b"rdma-uuid-pass",
        expected_len: 14,
        delay_ms: 50,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_uuid_validation_timeout() {
    run_test(TestCase {
        data: b"rdma-uuid-fail",
        expected_len: 14,
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        socket_type: SocketType::RDMA,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}
