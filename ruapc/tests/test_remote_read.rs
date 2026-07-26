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

/// Length + checksum of the data the server read; large transfers must
/// not be echoed inline (RPC responses are bounded by the negotiated
/// message size — bulk data belongs to the remote read/write paths).
#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
struct ReadRsp {
    len: u64,
    checksum: u64,
}

/// FNV-1a over the logical content of buffers.
fn digest_bufs(bufs: &[Buffer]) -> ReadRsp {
    let mut len = 0u64;
    let mut checksum = 0xcbf2_9ce4_8422_2325u64;
    for buf in bufs {
        len += buf.len() as u64;
        for &b in &buf[..] {
            checksum = (checksum ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    ReadRsp { len, checksum }
}

/// FNV-1a over a byte slice (client-side expectation).
fn digest(data: &[u8]) -> ReadRsp {
    let mut checksum = 0xcbf2_9ce4_8422_2325u64;
    for &b in data {
        checksum = (checksum ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    ReadRsp {
        len: data.len() as u64,
        checksum,
    }
}

#[service]
trait RemoteReadService {
    /// Reads the client's entire read space via `remote_read_all` and
    /// echoes the data back. The transferred size is exactly the total of
    /// the client buffers' logical lengths.
    async fn read_all(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp>;

    /// Reads the client's read space with a *vectored* op batch: the
    /// second half first, then the first half, into two local buffers —
    /// exercising offsets, multi-buffer spaces and dst scatter.
    async fn read_swapped(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp>;

    /// Issues an out-of-bounds op batch to exercise `InvalidCopyOp`.
    async fn read_out_of_bounds(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp>;
}

struct RemoteReadServiceImpl;

impl RemoteReadService for RemoteReadServiceImpl {
    async fn read_all(&self, ctx: &Context, req: &ReadReq) -> Result<ReadRsp> {
        if req.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(req.delay_ms)).await;
        }
        // Allocates right-sized buffers (mirroring the client's regions)
        // and reads exactly the client's logical data.
        let local = ctx.remote_read_all().await?;
        Ok(digest_bufs(&local))
    }

    async fn read_swapped(&self, ctx: &Context, _req: &ReadReq) -> Result<ReadRsp> {
        let space = ctx.remote_read_space()?;
        let total = space.total_len();
        let half = total / 2;

        // Local space: two buffers whose logical lengths split the total
        // at an odd boundary, so ops cross local buffer edges too.
        let first = (total / 3).max(1) as usize;
        let mut a = ctx
            .state
            .buffer_pool
            .allocate(first)
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        a.set_len(first);
        let mut b = ctx
            .state
            .buffer_pool
            .allocate((total as usize - first).max(1))
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        b.set_len(total as usize - first);

        // Swap halves: remote [half, total) -> local [0, total-half),
        // remote [0, half) -> local [total-half, total).
        let ops = [
            CopyOp::new(half, 0, total - half),
            CopyOp::new(0, total - half, half),
        ];
        let local = ctx.remote_read(&ops, vec![a, b]).await?;
        Ok(digest_bufs(&local))
    }

    async fn read_out_of_bounds(&self, ctx: &Context, _req: &ReadReq) -> Result<ReadRsp> {
        let space = ctx.remote_read_space()?;
        let mut buf = ctx
            .state
            .buffer_pool
            .allocate(64 * 1024)
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        buf.set_len(1);
        // One byte past the end of the remote space.
        let ops = [CopyOp::new(space.total_len(), 0, 1)];
        match ctx.remote_read(&ops, vec![buf]).await {
            Ok(local) => Ok(digest_bufs(&local)),
            Err(mut e) => {
                // The consumed buffers must be recoverable on this
                // validation-failure path.
                if e.take_buffers().is_none() {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "buffers not recovered from RemoteIoError".into(),
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
    /// Data to fill into the client buffers (split across `splits`).
    data: &'static [u8],
    /// Byte counts per client buffer; must sum to `data.len()`.
    splits: &'static [usize],
    /// Server-side delay before performing the read (ms).
    delay_ms: u64,
    /// Client timeout duration.
    client_timeout: Duration,
    /// Client socket type.
    socket_type: SocketType,
    /// Which service method to call.
    method: Method,
    /// Expected outcome.
    expect: Expected,
}

enum Method {
    All,
    Swapped,
    OutOfBounds,
}

enum Expected {
    /// Expect success and verify returned data matches `TestCase::data`.
    Ok,
    /// Expect success with the two halves of `data` swapped.
    OkSwapped,
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

    // Fill the client buffers and set their logical lengths; together
    // they form the read space the server operates on.
    assert_eq!(tc.splits.iter().sum::<usize>(), tc.data.len());
    let mut bufs = Vec::new();
    let mut offset = 0usize;
    for &split in tc.splits {
        let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        buf[..split].copy_from_slice(&tc.data[offset..offset + split]);
        buf.set_len(split);
        bufs.push(buf);
        offset += split;
    }

    let client = Client {
        socket_type: Some(tc.socket_type),
        timeout: tc.client_timeout,
        ..Default::default()
    };
    let req = ReadReq {
        delay_ms: tc.delay_ms,
    };

    let c = client.with_read_buffers(&bufs);
    let result: Result<ReadRsp> = match tc.method {
        Method::All => c.read_all(&ctx, &req).await,
        Method::Swapped => c.read_swapped(&ctx, &req).await,
        Method::OutOfBounds => c.read_out_of_bounds(&ctx, &req).await,
    };

    match tc.expect {
        Expected::Ok => {
            let rsp = result.unwrap();
            assert_eq!(rsp, digest(tc.data));
        }
        Expected::OkSwapped => {
            let rsp = result.unwrap();
            let half = tc.data.len() / 2;
            let mut swapped = tc.data[half..].to_vec();
            swapped.extend_from_slice(&tc.data[..half]);
            assert_eq!(rsp, digest(&swapped));
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
async fn test_tcp_read_single_buffer() {
    run_test(TestCase {
        data: b"uuid-check-pass",
        splits: &[15],
        delay_ms: 50,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        method: Method::All,
        expect: Expected::Ok,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_read_multi_buffer() {
    run_test(TestCase {
        data: b"multi-buffer logical space over tcp!",
        splits: &[7, 0, 21, 8],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        method: Method::All,
        expect: Expected::Ok,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_read_vectored_ops() {
    run_test(TestCase {
        data: b"0123456789abcdefghij",
        splits: &[9, 11],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        method: Method::Swapped,
        expect: Expected::OkSwapped,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_read_liveness_timeout() {
    run_test(TestCase {
        data: b"uuid-check-fail",
        splits: &[15],
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        socket_type: SocketType::TCP,
        method: Method::All,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

/// The server issues an op past the end of the client's read space: the
/// batch fails fast with `InvalidCopyOp` (nothing is transferred) and the
/// error propagates back through the RPC response.
#[tokio::test]
async fn test_tcp_read_out_of_bounds() {
    run_test(TestCase {
        data: &[0x5a; 128 * 1024],
        splits: &[128 * 1024],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        method: Method::OutOfBounds,
        expect: Expected::Err(ErrorKind::InvalidCopyOp),
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_read_single_buffer() {
    run_test(TestCase {
        data: b"rdma-uuid-pass",
        splits: &[14],
        delay_ms: 50,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        method: Method::All,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_read_multi_buffer() {
    run_test(TestCase {
        data: &[0xa5; 300 * 1024],
        splits: &[100 * 1024, 64 * 1024, 136 * 1024],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        method: Method::All,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_read_vectored_ops() {
    run_test(TestCase {
        data: b"0123456789abcdefghijklmnopqrstuv",
        splits: &[13, 19],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        method: Method::Swapped,
        expect: Expected::OkSwapped,
    })
    .await;
}

/// More work requests than the per-NIC in-flight READ budget
/// (`rdma.max_inflight_read_wrs`, default 32): 48 regions produce 48 READ
/// WRs, so the batch must queue on the device semaphore and complete as
/// permits cycle back through the poll thread.
#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_read_exceeds_device_read_budget() {
    run_test(TestCase {
        data: &[0x7e; 48 * 1024],
        splits: &[1024; 48],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        method: Method::All,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_read_liveness_timeout() {
    run_test(TestCase {
        data: b"rdma-uuid-fail",
        splits: &[14],
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        socket_type: SocketType::RDMA,
        method: Method::All,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_read_out_of_bounds() {
    run_test(TestCase {
        data: &[0xa5; 128 * 1024],
        splits: &[128 * 1024],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        method: Method::OutOfBounds,
        expect: Expected::Err(ErrorKind::InvalidCopyOp),
    })
    .await;
}
