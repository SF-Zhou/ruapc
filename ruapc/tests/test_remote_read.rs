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
    #[serde(default)]
    delay_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
struct ReadRsp {
    data: Vec<u8>,
}

#[service]
trait RemoteReadService {
    /// Reads the client's attached buffer via `remote_read_request` and
    /// echoes the data back. The transferred size is exactly the client
    /// buffer's logical length.
    async fn read_buf(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp>;

    /// Reads the client's attached buffer into a deliberately small local
    /// buffer to exercise the `BufferTooSmall` error path.
    async fn read_into_small_buf(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp>;
}

struct RemoteReadServiceImpl;

impl RemoteReadService for RemoteReadServiceImpl {
    async fn read_buf(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp> {
        if req.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(req.delay_ms)).await;
        }
        // Allocates a right-sized buffer and reads exactly the client's
        // logical data length; the returned buffer's len matches.
        let local_buf = ctx.remote_read_request().await?;
        Ok(ReadRsp {
            data: local_buf[..].to_vec(),
        })
    }

    async fn read_into_small_buf(&self, ctx: &Context, _req: &ReadReq) -> Result<ReadRsp> {
        // 1 KiB request rounds up to the smallest size class (64 KiB), which
        // is still smaller than the client's attached data.
        let local_buf = ctx.state.buffer_pool.allocate(1024).unwrap();
        match ctx.remote_read(ctx.request_buffer_info()?, local_buf).await {
            Ok(buf) => Ok(ReadRsp {
                data: buf[..].to_vec(),
            }),
            Err(mut e) => {
                // The consumed buffer must be recoverable on this failure
                // path. If it is not, report a different error kind so the
                // client-side assertion on BufferTooSmall catches it.
                if e.take_buffer().is_none() {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "buffer not recovered from RemoteIoError".into(),
                    ));
                }
                Err(e.into())
            }
        }
    }
}

// ==========================================================================
// Test framework
// ==========================================================================

struct TestCase {
    /// Data to fill into the client buffer.
    data: &'static [u8],
    /// Server-side delay before performing remote_read (ms).
    delay_ms: u64,
    /// Client timeout duration.
    client_timeout: Duration,
    /// Client socket type.
    socket_type: SocketType,
    /// Which service method to call.
    method: Method,
    /// Expected outcome: Ok(data) or Err(error_kind).
    expect: Expected,
}

enum Method {
    ReadBuf,
    ReadIntoSmallBuf,
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
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(RemoteReadServiceImpl).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();
    let ctx = Context::create(&config).unwrap().with_addr(addr);

    // Fill the client buffer and set its logical length; the server reads
    // exactly `data.len()` bytes.
    let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    buf[..tc.data.len()].copy_from_slice(tc.data);
    buf.set_len(tc.data.len());

    let client = Client {
        socket_type: Some(tc.socket_type),
        timeout: tc.client_timeout,
        ..Default::default()
    };
    let req = ReadReq {
        delay_ms: tc.delay_ms,
    };

    let c = client.with_read_buffer(&buf);
    let result: Result<ReadRsp> = match tc.method {
        Method::ReadBuf => c.read_buf(&ctx, &req).await,
        Method::ReadIntoSmallBuf => c.read_into_small_buf(&ctx, &req).await,
    };

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
        delay_ms: 50,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        method: Method::ReadBuf,
        expect: Expected::Ok,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_uuid_validation_timeout() {
    run_test(TestCase {
        data: b"uuid-check-fail",
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        socket_type: SocketType::TCP,
        method: Method::ReadBuf,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

/// The client attaches more data than the server's local buffer can hold:
/// the server fails fast with `BufferTooSmall` (no data is transferred) and
/// the error propagates back to the client through the RPC response.
#[tokio::test]
async fn test_tcp_remote_read_buffer_too_small() {
    run_test(TestCase {
        data: &[0x5a; 128 * 1024],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        method: Method::ReadIntoSmallBuf,
        expect: Expected::Err(ErrorKind::BufferTooSmall),
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_uuid_validation_success() {
    run_test(TestCase {
        data: b"rdma-uuid-pass",
        delay_ms: 50,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        method: Method::ReadBuf,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_uuid_validation_timeout() {
    run_test(TestCase {
        data: b"rdma-uuid-fail",
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        socket_type: SocketType::RDMA,
        method: Method::ReadBuf,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_read_buffer_too_small() {
    run_test(TestCase {
        data: &[0xa5; 128 * 1024],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        method: Method::ReadIntoSmallBuf,
        expect: Expected::Err(ErrorKind::BufferTooSmall),
    })
    .await;
}
