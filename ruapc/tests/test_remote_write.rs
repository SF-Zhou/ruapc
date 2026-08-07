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

/// Flattens the logical content of buffers into one byte vector.
fn flatten(bufs: &[Buffer]) -> Vec<u8> {
    let mut out = Vec::new();
    for buf in bufs {
        out.extend_from_slice(&buf[..]);
    }
    out
}

#[service]
trait WriteTestService {
    /// Typed contract: the handler transfers `req.data` into the client's
    /// pinned write buffers via `remote_write_all` and replies with the
    /// number of bytes written; the client receives all its buffers back
    /// through `WithBuffers`.
    async fn push_data(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffers<u64>;

    /// Writes `req.data` in two vectored ops with the halves swapped in
    /// the destination space, from two server-local source buffers.
    async fn push_swapped(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffers<u64>;

    /// Fulfills the buffer contract without transferring anything.
    async fn push_empty(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffers<u32>;

    /// Never touches buffers; used to simulate a mismatched peer.
    async fn push_nothing(&self, ctx: &Context, req: &WriteReq) -> Result<()>;
}

struct WriteTestImpl;

impl WriteTestService for WriteTestImpl {
    async fn push_data(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffers<u64> {
        if req.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(req.delay_ms)).await;
        }
        let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local[..req.data.len()].copy_from_slice(&req.data);
        local.set_len(req.data.len());
        // Transfer first; the response is decided afterwards.
        let sent = ctx.remote_write_all(vec![local]).await?;
        Ok(sent.reply(req.data.len() as u64))
    }

    async fn push_swapped(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffers<u64> {
        let total = req.data.len() as u64;
        let half = total / 2;
        // Source space: two server-local buffers split at an odd boundary.
        let first = (req.data.len() / 3).max(1);
        let mut a = ctx.state.buffer_pool.allocate(first).unwrap();
        a[..first].copy_from_slice(&req.data[..first]);
        a.set_len(first);
        let mut b = ctx
            .state
            .buffer_pool
            .allocate(req.data.len() - first)
            .unwrap();
        b[..req.data.len() - first].copy_from_slice(&req.data[first..]);
        b.set_len(req.data.len() - first);

        // src [0, half) -> dst [total-half, total),
        // src [half, total) -> dst [0, total-half).
        let ops = [
            CopyOp::new(0, total - half, half),
            CopyOp::new(half, 0, total - half),
        ];
        let sent = ctx.remote_write(&ops, vec![a, b]).await?;
        Ok(sent.reply(total))
    }

    async fn push_empty(&self, ctx: &Context, _req: &WriteReq) -> ResultWithBuffers<u32> {
        // Nothing to transfer: the explicit no-op witness fulfills the
        // contract without touching the network.
        Ok(ctx.sent_nothing().reply(42))
    }

    async fn push_nothing(&self, _ctx: &Context, _req: &WriteReq) -> Result<()> {
        Ok(())
    }
}

/// A client-side trait with the same wire identity ("WriteTestService") but
/// declaring `push_nothing` as `ResultWithBuffers`, simulating a mismatched
/// peer whose handler never writes.
mod mismatched {
    use super::*;

    #[service]
    pub trait WriteTestService {
        async fn push_nothing(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffers<()>;
    }
}

// --------------------------------------------------------------------------
// Custom alias + custom error type: the buffer contract is recognized by
// the type system, not by name, so user-defined aliases work.
// --------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
struct MyError {
    code: u32,
    msg: String,
}

impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyError({}): {}", self.code, self.msg)
    }
}

impl std::error::Error for MyError {}

impl From<Error> for MyError {
    fn from(e: Error) -> Self {
        Self {
            code: 500,
            msg: e.to_string(),
        }
    }
}

/// A fully user-defined alias: custom name, custom error type.
type MyFetchResult = std::result::Result<WithBuffers<u64>, MyError>;

#[service]
trait AliasService {
    async fn fetch(&self, ctx: &Context, req: &WriteReq) -> MyFetchResult;
}

struct AliasImpl;

impl AliasService for AliasImpl {
    async fn fetch(&self, ctx: &Context, req: &WriteReq) -> MyFetchResult {
        let mut buf = ctx
            .state
            .buffer_pool
            .allocate(req.data.len().max(1))
            .unwrap();
        buf[..req.data.len()].copy_from_slice(&req.data);
        buf.set_len(req.data.len());
        let pushed = buf.len() as u64;
        let sent = ctx
            .remote_write_all(vec![buf])
            .await
            .map_err(|e| MyError::from(Error::from(e)))?;
        Ok(sent.reply(pushed))
    }
}

// ==========================================================================
// Test framework
// ==========================================================================

struct TestCase {
    /// Data the server should write into the client's buffers.
    data: &'static [u8],
    /// Logical lengths of the client-provided write buffers; their sum is
    /// the write space size and must be >= `data.len()`.
    write_splits: &'static [usize],
    /// Server-side delay before performing remote_write (ms).
    delay_ms: u64,
    /// Client timeout duration.
    client_timeout: Duration,
    /// Client transport.
    transport: Transport,
    /// Which method to call.
    method: Method,
    /// Expected outcome.
    expect: Expected,
}

enum Method {
    PushData,
    PushSwapped,
}

enum Expected {
    /// Expect success; the write space must start with `data` (and the
    /// response must report `data.len()` bytes written).
    Ok,
    /// Expect success with the halves of `data` swapped in the space.
    OkSwapped,
    /// Expect failure with this error kind.
    Err(ErrorKind),
}

async fn run_test(tc: TestCase) {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        rdma: Some(Default::default()),
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(WriteTestImpl).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();
    let ctx = Context::create(&config)
        .unwrap()
        .with_endpoint(Endpoint::new(tc.transport, addr));

    let client = Client {
        timeout: tc.client_timeout,
        ..Default::default()
    };
    let req = WriteReq {
        data: tc.data.to_vec(),
        delay_ms: tc.delay_ms,
    };

    // Pre-provide the destination buffers; ownership moves into the call
    // and every buffer comes back through the `WithBuffers` result.
    assert!(tc.write_splits.iter().sum::<usize>() >= tc.data.len());
    let mut write_bufs = Vec::new();
    for &split in tc.write_splits {
        let mut buf = ctx.state.buffer_pool.allocate(split.max(1)).unwrap();
        buf.set_len(split);
        write_bufs.push(buf);
    }

    let c = client.with_write_buffers(write_bufs);
    let result: ResultWithBuffers<u64> = match tc.method {
        Method::PushData => c.push_data(&ctx, &req).await,
        Method::PushSwapped => c.push_swapped(&ctx, &req).await,
    };

    match tc.expect {
        Expected::Ok => {
            let (written, buffers) = result.unwrap().into_parts();
            assert_eq!(written, tc.data.len() as u64);
            assert_eq!(buffers.len(), tc.write_splits.len());
            let space = flatten(&buffers);
            assert_eq!(&space[..tc.data.len()], tc.data);
        }
        Expected::OkSwapped => {
            let (written, buffers) = result.unwrap().into_parts();
            assert_eq!(written, tc.data.len() as u64);
            let half = tc.data.len() / 2;
            let mut swapped = tc.data[half..].to_vec();
            swapped.extend_from_slice(&tc.data[..half]);
            let space = flatten(&buffers);
            assert_eq!(&space[..tc.data.len()], &swapped[..]);
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
        write_splits: &[1024],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        transport: Transport::TCP,
        method: Method::PushData,
        expect: Expected::Ok,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_remote_write_multi_buffer() {
    run_test(TestCase {
        data: b"a write space made of several client buffers",
        write_splits: &[10, 0, 25, 100],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        transport: Transport::TCP,
        method: Method::PushData,
        expect: Expected::Ok,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_remote_write_vectored() {
    run_test(TestCase {
        data: b"0123456789abcdefghij",
        write_splits: &[7, 13],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        transport: Transport::TCP,
        method: Method::PushSwapped,
        expect: Expected::OkSwapped,
    })
    .await;
}

#[tokio::test]
async fn test_tcp_remote_write_timeout() {
    run_test(TestCase {
        data: b"timeout-write",
        write_splits: &[64],
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        transport: Transport::TCP,
        method: Method::PushData,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write() {
    run_test(TestCase {
        data: b"Hello, RDMA Remote Write!",
        write_splits: &[1024],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        transport: Transport::RDMA,
        method: Method::PushData,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write_multi_buffer() {
    run_test(TestCase {
        data: &[0x3c; 200 * 1024],
        write_splits: &[64 * 1024, 96 * 1024, 64 * 1024],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        transport: Transport::RDMA,
        method: Method::PushData,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write_vectored() {
    run_test(TestCase {
        data: b"0123456789abcdefghijklmnopqrstuv",
        write_splits: &[11, 21],
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        transport: Transport::RDMA,
        method: Method::PushSwapped,
        expect: Expected::OkSwapped,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write_timeout() {
    run_test(TestCase {
        data: b"rdma-timeout-write",
        write_splits: &[64],
        delay_ms: 200,
        client_timeout: Duration::from_millis(100),
        transport: Transport::RDMA,
        method: Method::PushData,
        expect: Expected::Err(ErrorKind::Timeout),
    })
    .await;
}

/// Spawns a server and returns (server, client ctx).
async fn setup(transport: Transport) -> (Arc<Server>, Context) {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        rdma: Some(Default::default()),
        ..Default::default()
    };
    let mut router = Router::default();
    WriteTestService::ruapc_export(Arc::new(WriteTestImpl), &mut router);
    AliasService::ruapc_export(Arc::new(AliasImpl), &mut router);
    let server = Arc::new(Server::create(router, &config).unwrap());
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();
    let ctx = Context::create(&config)
        .unwrap()
        .with_endpoint(Endpoint::new(transport, addr));
    (server, ctx)
}

/// A handler branch with no payload uses `sent_nothing()`: no transfer
/// takes place, and the client's buffers come back untouched.
#[tokio::test]
async fn test_tcp_reply_without_transfer() {
    run_empty_test(Transport::TCP).await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_reply_without_transfer() {
    run_empty_test(Transport::RDMA).await;
}

async fn run_empty_test(transport: Transport) {
    let (server, ctx) = setup(transport).await;
    let client = Client::default();
    let req = WriteReq {
        data: vec![],
        delay_ms: 0,
    };

    // With write buffers attached: they come back untouched.
    let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
    buf[..4].copy_from_slice(b"keep");
    buf.set_len(4);
    let (rsp, buffers) = client
        .with_write_buffers(vec![buf])
        .push_empty(&ctx, &req)
        .await
        .unwrap()
        .into_parts();
    assert_eq!(rsp, 42);
    assert_eq!(buffers.len(), 1);
    assert_eq!(&buffers[0][..], b"keep");

    // Without write buffers: an empty list.
    let (rsp, buffers) = client.push_empty(&ctx, &req).await.unwrap().into_parts();
    assert_eq!(rsp, 42);
    assert!(buffers.is_empty());

    server.stop();
    server.join().await;
}

/// Writing without client-attached write buffers fails with
/// `MissingBufferInfo` (surfaced through the RPC response).
#[tokio::test]
async fn test_tcp_write_without_attached_buffers_fails() {
    let (server, ctx) = setup(Transport::TCP).await;
    let client = Client::default();
    let req = WriteReq {
        data: b"data".to_vec(),
        delay_ms: 0,
    };
    let err = client.push_data(&ctx, &req).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::MissingBufferInfo);
    server.stop();
    server.join().await;
}

/// Writing more than the client's write space holds fails with
/// `InvalidCopyOp`.
#[tokio::test]
async fn test_tcp_write_space_too_small_fails() {
    let (server, ctx) = setup(Transport::TCP).await;
    let client = Client::default();
    let req = WriteReq {
        data: vec![0x77; 256],
        delay_ms: 0,
    };
    let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
    buf.set_len(16); // space smaller than the payload
    let wrapper = client.with_write_buffers(vec![buf]);
    let err = wrapper.push_data(&ctx, &req).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::InvalidCopyOp);
    // The attached buffers are recoverable after the failed call.
    let recovered = wrapper.take_write_buffers().expect("buffers recoverable");
    assert_eq!(recovered.len(), 1);
    server.stop();
    server.join().await;
}

/// A peer whose handler never writes is indistinguishable from one that
/// wrote nothing: the client simply gets its buffers back untouched.
#[tokio::test]
async fn test_tcp_no_write_returns_buffers_untouched() {
    let (server, ctx) = setup(Transport::TCP).await;
    let client = Client::default();
    let req = WriteReq {
        data: vec![],
        delay_ms: 0,
    };

    let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
    buf.set_len(8);
    let wrapper = client.with_write_buffers(vec![buf]);
    let ((), buffers) = mismatched::WriteTestService::push_nothing(&wrapper, &ctx, &req)
        .await
        .unwrap()
        .into_parts();
    assert_eq!(buffers.len(), 1);

    server.stop();
    server.join().await;
}

/// The buffer contract works through user-defined aliases and custom error
/// types: recognition is by type identity, not by name.
#[tokio::test]
async fn test_custom_alias_and_error_type() {
    let (server, ctx) = setup(Transport::TCP).await;
    let client = Client::default();
    let req = WriteReq {
        data: b"alias + custom error".to_vec(),
        delay_ms: 0,
    };

    let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
    buf.set_len(req.data.len());
    let (len, buffers) = client
        .with_write_buffers(vec![buf])
        .fetch(&ctx, &req)
        .await
        .unwrap()
        .into_parts();
    assert_eq!(len, req.data.len() as u64);
    assert_eq!(&flatten(&buffers)[..], &req.data[..]);

    server.stop();
    server.join().await;
}

/// Upload and download in one call: read buffers and write buffers
/// attached to the same request.
#[tokio::test]
async fn test_tcp_read_and_write_in_one_call() {
    run_roundtrip_test(Transport::TCP).await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_read_and_write_in_one_call() {
    run_roundtrip_test(Transport::RDMA).await;
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct EchoReq {}

#[service]
trait EchoBufService {
    /// Reads the client's read space and writes it back (reversed) into
    /// the client's write space.
    async fn echo_reversed(&self, ctx: &Context, req: &EchoReq) -> ResultWithBuffers<u64>;
}

struct EchoBufImpl;

impl EchoBufService for EchoBufImpl {
    async fn echo_reversed(&self, ctx: &Context, _req: &EchoReq) -> ResultWithBuffers<u64> {
        let local = ctx.remote_read_all().await?;
        let mut data = flatten(&local);
        drop(local);
        data.reverse();
        let mut out = ctx
            .state
            .buffer_pool
            .allocate(data.len().max(1))
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        out[..data.len()].copy_from_slice(&data);
        out.set_len(data.len());
        let sent = ctx.remote_write_all(vec![out]).await?;
        Ok(sent.reply(data.len() as u64))
    }
}

async fn run_roundtrip_test(transport: Transport) {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        rdma: Some(Default::default()),
        ..Default::default()
    };
    let mut router = Router::default();
    EchoBufService::ruapc_export(Arc::new(EchoBufImpl), &mut router);
    let server = Arc::new(Server::create(router, &config).unwrap());
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();
    let ctx = Context::create(&config)
        .unwrap()
        .with_endpoint(Endpoint::new(transport, addr));
    let client = Client::default();

    let payload = b"palindrome-me";
    let mut src = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
    src[..payload.len()].copy_from_slice(payload);
    src.set_len(payload.len());
    let mut dst = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
    dst.set_len(payload.len());

    let read_bufs = [src];
    let (written, buffers) = client
        .with_read_buffers(&read_bufs)
        .with_write_buffers(vec![dst])
        .echo_reversed(&ctx, &EchoReq {})
        .await
        .unwrap()
        .into_parts();
    assert_eq!(written, payload.len() as u64);
    let mut expected = payload.to_vec();
    expected.reverse();
    assert_eq!(&flatten(&buffers)[..], &expected[..]);

    server.stop();
    server.join().await;
}
