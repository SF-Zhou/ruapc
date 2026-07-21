#![feature(return_type_notation)]

use std::str::FromStr;
use std::sync::Arc;

use ruapc::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --------------------------------------------------------------------------
// Service: server reads client's buffer via remote_read
// --------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct UploadReq {}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
struct UploadRsp {
    data: Vec<u8>,
}

#[service]
trait UploadService {
    async fn upload(&self, ctx: &Context, req: &UploadReq) -> Result<UploadRsp>;
}

struct UploadServiceImpl;

impl UploadService for UploadServiceImpl {
    async fn upload(&self, ctx: &Context, _req: &UploadReq) -> Result<UploadRsp> {
        // Allocates a right-sized local buffer and reads exactly the
        // client's logical data length. Returns MissingBufferInfo if the
        // client did not attach a buffer.
        let local_buf = ctx.remote_read_request().await?;

        // Return the data read from the client's buffer.
        Ok(UploadRsp {
            data: local_buf[..].to_vec(),
        })
    }
}

// --------------------------------------------------------------------------
// Service: normal request (no buffer) — verify backwards compatibility
// --------------------------------------------------------------------------

#[service]
trait PingService {
    async fn ping(&self, ctx: &Context, req: &String) -> Result<String>;
}

struct PingServiceImpl;

impl PingService for PingServiceImpl {
    async fn ping(&self, ctx: &Context, _req: &String) -> Result<String> {
        // Normal requests should NOT have buffer_info.
        assert!(
            ctx.msg_meta.buffer_info.is_none(),
            "buffer_info should be None for normal requests"
        );
        Ok("pong".to_string())
    }
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

#[tokio::test]
async fn test_upload_with_read_buffer_tcp() {
    let upload_svc = Arc::new(UploadServiceImpl);
    let ping_svc = Arc::new(PingServiceImpl);
    let mut router = Router::default();
    upload_svc.ruapc_export(&mut router);
    ping_svc.ruapc_export(&mut router);

    let config = SocketPoolConfig::default();
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_addr(addr);

    // 1. Test normal request (no buffer) — backwards compatibility.
    let client = Client::default();
    let rsp = client.ping(&ctx, &"hello".to_string()).await.unwrap();
    assert_eq!(rsp, "pong");

    // 2. Test with_read_buffer — client sends buffer for server to read.
    //    set_len marks the logical data length; the server reads exactly
    //    this many bytes.
    let test_data = b"Hello, WithReadBuffer!";
    let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    buf[..test_data.len()].copy_from_slice(test_data);
    buf.set_len(test_data.len());

    let req = UploadReq {};

    // Use client.with_read_buffer(&buf) to attach the buffer.
    let rsp: UploadRsp = client
        .with_read_buffer(&buf)
        .upload(&ctx, &req)
        .await
        .unwrap();
    assert_eq!(rsp.data, test_data);

    // 3. Verify the same buffer can be reused (retry scenario).
    let rsp2: UploadRsp = client
        .with_read_buffer(&buf)
        .upload(&ctx, &req)
        .await
        .unwrap();
    assert_eq!(rsp2.data, test_data);

    // 4. Verify upload without an attached buffer fails with a proper
    //    MissingBufferInfo error (not a server panic / timeout).
    let rsp3 = client.upload(&ctx, &req).await;
    let err = rsp3.expect_err("upload without buffer should fail");
    assert_eq!(err.kind, ErrorKind::MissingBufferInfo);

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_upload_with_read_buffer_websocket() {
    let upload_svc = Arc::new(UploadServiceImpl);
    let mut router = Router::default();
    upload_svc.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        ..Default::default()
    };
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_addr(addr);

    let test_data = b"WebSocket buffer test!";
    let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    buf[..test_data.len()].copy_from_slice(test_data);
    buf.set_len(test_data.len());

    let client = Client {
        socket_type: Some(SocketType::WS),
        ..Default::default()
    };
    let req = UploadReq {};

    let rsp: UploadRsp = client
        .with_read_buffer(&buf)
        .upload(&ctx, &req)
        .await
        .unwrap();
    assert_eq!(rsp.data, test_data);

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_upload_with_read_buffer_http() {
    let upload_svc = Arc::new(UploadServiceImpl);
    let mut router = Router::default();
    upload_svc.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        ..Default::default()
    };
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_addr(addr);

    let test_data = b"HTTP buffer test!";
    let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    buf[..test_data.len()].copy_from_slice(test_data);
    buf.set_len(test_data.len());

    let client = Client {
        socket_type: Some(SocketType::HTTP),
        ..Default::default()
    };
    let req = UploadReq {};

    let rsp: UploadRsp = client
        .with_read_buffer(&buf)
        .upload(&ctx, &req)
        .await
        .unwrap();
    assert_eq!(rsp.data, test_data);

    server.stop();
    server.join().await;
}
