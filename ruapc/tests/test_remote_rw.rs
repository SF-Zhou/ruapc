#![feature(return_type_notation)]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ruapc::*;
use ruapc_bufpool::RemoteBufferInfo;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ==========================================================================
// Service definition
// ==========================================================================

/// Client sends RemoteBufferInfo + data for the server to write.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct RemoteWriteReq {
    info: RemoteBufferInfo,
    data: Vec<u8>,
}

#[service]
trait WriteTestService {
    async fn write_remote(&self, ctx: &Context, req: &RemoteWriteReq) -> Result<()>;
}

struct WriteTestImpl;

impl WriteTestService for WriteTestImpl {
    async fn write_remote(&self, ctx: &Context, req: &RemoteWriteReq) -> Result<()> {
        let mut local_buf = ctx.state.buffer_pool.allocate().unwrap();
        local_buf[..req.data.len()].copy_from_slice(&req.data);
        let local_buf = ctx.remote_write(&req.info, local_buf).await?;
        drop(local_buf);
        Ok(())
    }
}

// ==========================================================================
// Helpers
// ==========================================================================

struct TestEnv {
    ctx: Context,
    server: Arc<Server>,
}

impl TestEnv {
    async fn start(config: &SocketPoolConfig) -> Self {
        let mut router = Router::default();
        Arc::new(WriteTestImpl).ruapc_export(&mut router);
        let server = Server::create(router, config).unwrap();
        let server = Arc::new(server);
        let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
        let addr = server.clone().listen(addr).await.unwrap();
        let ctx = Context::create(config).unwrap().with_addr(addr);
        Self { ctx, server }
    }

    async fn stop(self) {
        self.server.stop();
        tokio::time::timeout(Duration::from_secs(30), self.server.join())
            .await
            .unwrap();
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[tokio::test]
async fn test_tcp_remote_write() {
    let env = TestEnv::start(&SocketPoolConfig::default()).await;

    let client_buf = env.ctx.state.buffer_pool.allocate().unwrap();
    assert!(client_buf[..10].iter().all(|&b| b == 0));

    let client_device = env.ctx.state.devices.tcp_device();
    let rbi = client_buf.remote_buffer_info(client_device).unwrap();
    let write_data = b"Hello, Remote Write!";
    let rbi_for_req = RemoteBufferInfo {
        key: rbi.key,
        addr: rbi.addr,
        len: write_data.len() as u64,
    };

    let client = Client::default();
    client
        .write_remote(
            &env.ctx,
            &RemoteWriteReq {
                info: rbi_for_req,
                data: write_data.to_vec(),
            },
        )
        .await
        .unwrap();
    assert_eq!(&client_buf[..write_data.len()], write_data);

    env.stop().await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write() {
    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let env = TestEnv::start(&config).await;

    let rdma_device = env
        .ctx
        .state
        .devices
        .rdma_devices()
        .iter()
        .next()
        .expect("no RDMA device discovered");

    let client_buf = env.ctx.state.buffer_pool.allocate().unwrap();
    assert!(client_buf[..10].iter().all(|&b| b == 0));

    let rbi = client_buf.remote_buffer_info(rdma_device).unwrap();
    let write_data = b"Hello, RDMA Remote Write!";
    let rbi_for_req = RemoteBufferInfo {
        key: rbi.key,
        addr: rbi.addr,
        len: write_data.len() as u64,
    };

    let client = Client {
        socket_type: Some(SocketType::RDMA),
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    client
        .write_remote(
            &env.ctx,
            &RemoteWriteReq {
                info: rbi_for_req,
                data: write_data.to_vec(),
            },
        )
        .await
        .unwrap();
    assert_eq!(&client_buf[..write_data.len()], write_data);

    env.stop().await;
}
