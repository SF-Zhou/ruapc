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
struct UploadReq {
    expected_len: usize,
}

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
    async fn upload(&self, ctx: &Context, req: &UploadReq) -> Result<UploadRsp> {
        // The server should see buffer_info in the message metadata.
        let buffer_info = ctx.msg_meta.buffer_info.as_ref().expect(
            "expected buffer_info in msg_meta — client should have used with_read_buffer()",
        );

        // Allocate a local buffer and remote_read the client's buffer.
        let mut local_buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local_buf = ctx.remote_read(buffer_info, local_buf).await?;

        // Return the data read from the client's buffer.
        Ok(UploadRsp {
            data: local_buf[..req.expected_len].to_vec(),
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
    let test_data = b"Hello, WithReadBuffer!";
    let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    buf[..test_data.len()].copy_from_slice(test_data);

    let req = UploadReq {
        expected_len: test_data.len(),
    };

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

    // 4. Verify normal upload (without buffer) — should panic on server
    //    because buffer_info is None. We expect an error.
    let rsp3 = client.upload(&ctx, &req).await;
    assert!(rsp3.is_err(), "upload without buffer should fail");

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

    let client = Client {
        socket_type: Some(SocketType::WS),
        ..Default::default()
    };
    let req = UploadReq {
        expected_len: test_data.len(),
    };

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

    let client = Client {
        socket_type: Some(SocketType::HTTP),
        ..Default::default()
    };
    let req = UploadReq {
        expected_len: test_data.len(),
    };

    let rsp: UploadRsp = client
        .with_read_buffer(&buf)
        .upload(&ctx, &req)
        .await
        .unwrap();
    assert_eq!(rsp.data, test_data);

    server.stop();
    server.join().await;
}
