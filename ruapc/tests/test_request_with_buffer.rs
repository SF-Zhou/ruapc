// `#[service]` request types must be owned deserializable types behind a
// reference (`&String`), so `&str` is not an option here.
#![allow(clippy::ptr_arg)]

use std::slice;
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
        // Allocates right-sized local buffers and reads exactly the
        // client's logical data. Returns MissingBufferInfo if the client
        // did not attach buffers.
        let local = ctx.remote_read_allocated().await?;

        // Return the data read from the client's buffers.
        let mut data = Vec::new();
        for buf in &local {
            data.extend_from_slice(&buf[..]);
        }
        Ok(UploadRsp { data })
    }
}

// --------------------------------------------------------------------------
// Service: normal request carries no memory attachments
// --------------------------------------------------------------------------

#[service]
trait PingService {
    async fn ping(&self, ctx: &Context, req: &String) -> Result<String>;
}

struct PingServiceImpl;

impl PingService for PingServiceImpl {
    async fn ping(&self, ctx: &Context, _req: &String) -> Result<String> {
        // Normal requests should NOT carry memory regions.
        assert!(
            ctx.msg_meta.read_regions.is_empty() && ctx.msg_meta.write_regions.is_empty(),
            "regions should be empty for normal requests"
        );
        Ok("pong".to_string())
    }
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

#[tokio::test]
async fn test_upload_with_source_reference_tcp() {
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
    let ctx = ctx.with_endpoint(ruapc::Endpoint::tcp([addr]).unwrap());

    // 1. Test a normal request without memory attachments.
    let client = Client::default();
    let rsp = client.ping(&ctx, &"hello".to_string()).await.unwrap();
    assert_eq!(rsp, "pong");

    // 2. Borrow a source buffer for the server to read.
    //    set_len marks the logical data length; the server reads exactly
    //    this many bytes.
    let test_data = b"Hello, WithReadBuffer!";
    let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    buf[..test_data.len()].copy_from_slice(test_data);
    buf.set_len(test_data.len());

    let req = UploadReq {};

    // One scope reuses the same source reference across requests.
    let Completed { result, buffers } = client
        .with_buffers(slice::from_ref(&buf), vec![])
        .run(async |source| {
            let rsp: UploadRsp = source.upload(&ctx, &req).await?;
            assert_eq!(rsp.data, test_data);
            source.upload(&ctx, &req).await
        })
        .await;
    assert_eq!(result.unwrap().data, test_data);
    assert!(buffers.unwrap().is_empty());
    assert_eq!(&buf[..], test_data);

    // 4. Verify upload without an attached buffer fails with a proper
    //    MissingBufferInfo error (not a server panic / timeout).
    let rsp3 = client.upload(&ctx, &req).await;
    let err = rsp3.expect_err("upload without buffer should fail");
    assert_eq!(err.kind, ErrorKind::MissingBufferInfo);

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_upload_with_source_reference_websocket() {
    let upload_svc = Arc::new(UploadServiceImpl);
    let mut router = Router::default();
    upload_svc.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        listen_mode: ruapc::ListenMode::UNIFIED,
        ..Default::default()
    };
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_endpoint(ruapc::Endpoint::new(ruapc::Transport::WS, [addr]).unwrap());

    let test_data = b"WebSocket buffer test!";
    let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    buf[..test_data.len()].copy_from_slice(test_data);
    buf.set_len(test_data.len());

    let client = Client::default();
    let req = UploadReq {};

    let rsp: UploadRsp = client
        .with_buffers(slice::from_ref(&buf), vec![])
        .run(async |source| source.upload(&ctx, &req).await)
        .await
        .result
        .unwrap();
    assert_eq!(rsp.data, test_data);

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_upload_with_source_reference_http() {
    let upload_svc = Arc::new(UploadServiceImpl);
    let mut router = Router::default();
    upload_svc.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        listen_mode: ruapc::ListenMode::UNIFIED,
        ..Default::default()
    };
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_endpoint(ruapc::Endpoint::new(ruapc::Transport::HTTP, [addr]).unwrap());

    let test_data = b"HTTP buffer test!";
    let mut buf = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    buf[..test_data.len()].copy_from_slice(test_data);
    buf.set_len(test_data.len());

    let client = Client::default();
    let req = UploadReq {};

    let rsp: UploadRsp = client
        .with_buffers(slice::from_ref(&buf), vec![])
        .run(async |source| source.upload(&ctx, &req).await)
        .await
        .result
        .unwrap();
    assert_eq!(rsp.data, test_data);

    server.stop();
    server.join().await;
}
