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
    /// Typed contract: the handler returns the buffer in `ResultWithBuffer`
    /// and the framework performs the `remote_write`; the client receives
    /// `WithBuffer<()>` from the same method.
    async fn push_data(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffer<()>;

    /// Fulfills the buffer contract with no payload: pushes an empty
    /// buffer via `reply_with_empty_buffer`.
    async fn push_empty(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffer<u32>;

    /// Never pushes a buffer; used to simulate a mismatched peer.
    async fn push_nothing(&self, ctx: &Context, req: &WriteReq) -> Result<()>;
}

struct WriteTestImpl;

impl WriteTestService for WriteTestImpl {
    async fn push_data(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffer<()> {
        if req.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(req.delay_ms)).await;
        }
        let mut local_buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local_buf[..req.data.len()].copy_from_slice(&req.data);
        local_buf.set_len(req.data.len());
        // Transfer first; the response is decided afterwards.
        let sent = ctx.remote_write(local_buf).await?;
        Ok(sent.reply(()))
    }

    async fn push_empty(&self, ctx: &Context, _req: &WriteReq) -> ResultWithBuffer<u32> {
        // No data to transfer: Buffer::empty() costs no memory and
        // remote_write short-circuits without touching the network.
        let sent = ctx
            .remote_write(Buffer::empty(&ctx.state.buffer_pool))
            .await?;
        Ok(sent.reply(42))
    }

    async fn push_nothing(&self, _ctx: &Context, _req: &WriteReq) -> Result<()> {
        Ok(())
    }
}

/// A client-side trait with the same wire identity ("WriteTestService") but
/// declaring `push_nothing` as `ResultWithBuffer`, simulating a mismatched
/// peer whose handler never pushes a buffer.
mod mismatched {
    use super::*;

    #[service]
    pub trait WriteTestService {
        async fn push_nothing(&self, ctx: &Context, req: &WriteReq) -> ResultWithBuffer<()>;
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
type MyFetchResult = std::result::Result<WithBuffer<u64>, MyError>;

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
            .remote_write(buf)
            .await
            .map_err(|e| MyError::from(Error::from(e)))?;
        Ok(sent.reply(pushed))
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

    // Typed contract: the pushed buffer is part of the return value, no
    // manual ready_to_recv / take_write_buffer dance.
    let result: ResultWithBuffer<()> = client.push_data(&ctx, &req).await;

    match tc.expect {
        Expected::Ok => {
            let ((), buffer) = result.unwrap().into_parts();
            assert_eq!(&buffer[..], tc.data);
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

/// Spawns a server and returns (server, client ctx).
async fn setup() -> (Arc<Server>, Context) {
    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        ..Default::default()
    };
    let mut router = Router::default();
    WriteTestService::ruapc_export(Arc::new(WriteTestImpl), &mut router);
    AliasService::ruapc_export(Arc::new(AliasImpl), &mut router);
    let server = Arc::new(Server::create(router, &config).unwrap());
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();
    let ctx = Context::create(&config).unwrap().with_addr(addr);
    (server, ctx)
}

/// A handler branch with no payload replies with an empty buffer: no
/// transfer takes place, and the client receives a buffer with len == 0.
#[tokio::test]
async fn test_tcp_reply_with_empty_buffer() {
    run_empty_test(SocketType::TCP).await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_reply_with_empty_buffer() {
    run_empty_test(SocketType::RDMA).await;
}

async fn run_empty_test(socket_type: SocketType) {
    let (server, ctx) = setup().await;
    let client = Client {
        socket_type: Some(socket_type),
        ..Default::default()
    };
    let req = WriteReq {
        data: vec![],
        delay_ms: 0,
    };

    let (rsp, buffer) = client.push_empty(&ctx, &req).await.unwrap().into_parts();
    assert_eq!(rsp, 42);
    assert!(buffer.is_empty());

    server.stop();
    server.join().await;
}

/// Zero-length data through the regular reply_with_buffer path also works
/// end-to-end on both transports.
#[tokio::test]
async fn test_tcp_remote_write_zero_len() {
    run_test(TestCase {
        data: b"",
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::TCP,
        expect: Expected::Ok,
    })
    .await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write_zero_len() {
    run_test(TestCase {
        data: b"",
        delay_ms: 0,
        client_timeout: Duration::from_secs(5),
        socket_type: SocketType::RDMA,
        expect: Expected::Ok,
    })
    .await;
}

/// A response that arrives without a pushed buffer decodes as an empty
/// reply: "no push on the wire" is the encoding of
/// `reply_with_empty_buffer`, so a peer whose handler returns plain
/// `Result<()>` is indistinguishable from one replying with an empty
/// buffer — by design.
#[tokio::test]
async fn test_tcp_no_push_decodes_as_empty_buffer() {
    let (server, ctx) = setup().await;
    let client = Client::default();
    let req = WriteReq {
        data: vec![],
        delay_ms: 0,
    };

    let ((), buffer) = mismatched::WriteTestService::push_nothing(&client, &ctx, &req)
        .await
        .unwrap()
        .into_parts();
    assert!(buffer.is_empty());

    server.stop();
    server.join().await;
}

/// The buffer contract works through user-defined aliases and custom error
/// types: recognition is by type identity, not by name.
#[tokio::test]
async fn test_custom_alias_and_error_type() {
    let (server, ctx) = setup().await;
    let client = Client::default();
    let req = WriteReq {
        data: b"alias + custom error".to_vec(),
        delay_ms: 0,
    };

    let (len, buffer) = client.fetch(&ctx, &req).await.unwrap().into_parts();
    assert_eq!(len, req.data.len() as u64);
    assert_eq!(&buffer[..], &req.data[..]);

    server.stop();
    server.join().await;
}
