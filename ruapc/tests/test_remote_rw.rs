#![feature(return_type_notation)]

use std::str::FromStr;
use std::sync::Arc;

use ruapc::*;
use ruapc_bufpool::RemoteBufferInfo;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Client sends its RemoteBufferInfo to the server.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct RemoteReadReq {
    info: RemoteBufferInfo,
}

/// Server returns the data it read from the client's buffer.
#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
struct RemoteReadRsp {
    data: Vec<u8>,
}

#[service]
trait ReadTestService {
    async fn read_remote(&self, ctx: &Context, req: &RemoteReadReq) -> Result<RemoteReadRsp>;
}

struct ReadTestImpl;

impl ReadTestService for ReadTestImpl {
    async fn read_remote(&self, ctx: &Context, req: &RemoteReadReq) -> Result<RemoteReadRsp> {
        // Allocate a local buffer from the shared buffer pool.
        let mut local_buf = ctx.state.buffer_pool.allocate().unwrap();

        // Remote read: pull data from the client's registered memory.
        local_buf = ctx.remote_read(&req.info, local_buf).await?;

        // Return the data we read.
        let len = req.info.len as usize;
        Ok(RemoteReadRsp {
            data: local_buf[..len].to_vec(),
        })
    }
}

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
        // Allocate a local buffer from the shared buffer pool, fill with data.
        let mut local_buf = ctx.state.buffer_pool.allocate().unwrap();
        local_buf[..req.data.len()].copy_from_slice(&req.data);

        // Remote write: push data to the client's registered memory.
        let local_buf = ctx.remote_write(&req.info, local_buf).await?;
        drop(local_buf);

        Ok(())
    }
}

#[tokio::test]
async fn test_tcp_remote_read() {
    let config = SocketPoolConfig::default();

    // Set up server with ReadTestService.
    let read_svc = Arc::new(ReadTestImpl);
    let mut router = Router::default();
    read_svc.ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    // Set up client context (devices are auto-discovered from config).
    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_addr(addr);

    // Allocate client buffer and fill with test data.
    let mut client_buf = ctx.state.buffer_pool.allocate().unwrap();
    let test_data = b"Hello, Remote Read!";
    client_buf[..test_data.len()].copy_from_slice(test_data);

    // Get RemoteBufferInfo for the client's buffer (use first device = TCP).
    let client_device = ctx.state.devices.tcp_device();
    let rbi = client_buf.remote_buffer_info(client_device).unwrap();
    let rbi_for_req = RemoteBufferInfo {
        key: rbi.key,
        addr: rbi.addr,
        len: test_data.len() as u64,
    };

    // Call the server's ReadTestService — it will reverse-RPC to read our buffer.
    let client = Client::default();
    let rsp: RemoteReadRsp = client
        .read_remote(&ctx, &RemoteReadReq { info: rbi_for_req })
        .await
        .unwrap();

    assert_eq!(rsp.data, test_data);

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_tcp_remote_write() {
    let config = SocketPoolConfig::default();

    // Set up server with WriteTestService.
    let write_svc = Arc::new(WriteTestImpl);
    let mut router = Router::default();
    write_svc.ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    // Set up client context.
    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_addr(addr);

    // Allocate client buffer (initially zeroed by the allocator).
    let client_buf = ctx.state.buffer_pool.allocate().unwrap();
    assert!(client_buf[..10].iter().all(|&b| b == 0));

    // Get RemoteBufferInfo for the client's buffer.
    let client_device = ctx.state.devices.tcp_device();
    let rbi = client_buf.remote_buffer_info(client_device).unwrap();
    let write_data = b"Hello, Remote Write!";
    let rbi_for_req = RemoteBufferInfo {
        key: rbi.key,
        addr: rbi.addr,
        len: write_data.len() as u64,
    };

    // Call the server's WriteTestService — it will reverse-RPC to write to our buffer.
    let client = Client::default();
    client
        .write_remote(
            &ctx,
            &RemoteWriteReq {
                info: rbi_for_req,
                data: write_data.to_vec(),
            },
        )
        .await
        .unwrap();

    // Verify the data was written to our buffer.
    assert_eq!(&client_buf[..write_data.len()], write_data);

    server.stop();
    server.join().await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_read() {
    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };

    // Set up server with ReadTestService.
    let read_svc = Arc::new(ReadTestImpl);
    let mut router = Router::default();
    read_svc.ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Set up client context (RDMA devices auto-discovered).
    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_addr(addr);

    // Find the RDMA device for buffer registration.
    let rdma_device = ctx
        .state
        .devices
        .rdma_devices()
        .iter()
        .next()
        .expect("no RDMA device discovered");

    // Allocate client buffer and fill with test data.
    let mut client_buf = ctx.state.buffer_pool.allocate().unwrap();
    let test_data = b"Hello, RDMA Remote Read!";
    client_buf[..test_data.len()].copy_from_slice(test_data);

    // Get RemoteBufferInfo with RDMA key.
    let rbi = client_buf.remote_buffer_info(rdma_device).unwrap();
    let rbi_for_req = RemoteBufferInfo {
        key: rbi.key,
        addr: rbi.addr,
        len: test_data.len() as u64,
    };

    // Use RDMA socket type for the RPC call.
    let client = Client {
        socket_type: Some(SocketType::RDMA),
        ..Default::default()
    };
    let rsp: RemoteReadRsp = client
        .read_remote(&ctx, &RemoteReadReq { info: rbi_for_req })
        .await
        .unwrap();

    assert_eq!(rsp.data, test_data);

    server.stop();
    tokio::time::timeout(std::time::Duration::from_secs(30), server.join())
        .await
        .unwrap();
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma_remote_write() {
    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };

    // Set up server with WriteTestService.
    let write_svc = Arc::new(WriteTestImpl);
    let mut router = Router::default();
    write_svc.ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Set up client context (RDMA devices auto-discovered).
    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_addr(addr);

    // Find the RDMA device.
    let rdma_device = ctx
        .state
        .devices
        .rdma_devices()
        .iter()
        .next()
        .expect("no RDMA device discovered");

    // Allocate client buffer (initially zeroed).
    let client_buf = ctx.state.buffer_pool.allocate().unwrap();
    assert!(client_buf[..10].iter().all(|&b| b == 0));

    // Get RemoteBufferInfo with RDMA key.
    let rbi = client_buf.remote_buffer_info(rdma_device).unwrap();
    let write_data = b"Hello, RDMA Remote Write!";
    let rbi_for_req = RemoteBufferInfo {
        key: rbi.key,
        addr: rbi.addr,
        len: write_data.len() as u64,
    };

    // Use RDMA socket type for the RPC call.
    let client = Client {
        socket_type: Some(SocketType::RDMA),
        ..Default::default()
    };
    client
        .write_remote(
            &ctx,
            &RemoteWriteReq {
                info: rbi_for_req,
                data: write_data.to_vec(),
            },
        )
        .await
        .unwrap();

    // Verify the data was written to our buffer via RDMA Write.
    assert_eq!(&client_buf[..write_data.len()], write_data);

    server.stop();
    tokio::time::timeout(std::time::Duration::from_secs(30), server.join())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_tcp_remote_read_bounds_check() {
    let config = SocketPoolConfig::default();

    // Set up server with ReadTestService.
    let read_svc = Arc::new(ReadTestImpl);
    let mut router = Router::default();
    read_svc.ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    // Set up client context.
    let ctx = Context::create(&config).unwrap();
    let ctx = ctx.with_addr(addr);

    let client_device = ctx.state.devices.tcp_device();
    let client_buf = ctx.state.buffer_pool.allocate().unwrap();
    let rbi = client_buf.remote_buffer_info(client_device).unwrap();

    // Try to read beyond the buffer bounds.
    let bad_rbi = RemoteBufferInfo {
        key: rbi.key,
        addr: rbi.addr,
        len: (3 * 1024 * 1024) as u64, // Exceeds 2 MiB buffer
    };

    let client = Client::default();
    let result: Result<RemoteReadRsp> = client
        .read_remote(&ctx, &RemoteReadReq { info: bad_rbi })
        .await;

    // Should fail with a bounds error.
    assert!(result.is_err());

    server.stop();
    server.join().await;
}
