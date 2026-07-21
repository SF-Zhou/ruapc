use ruapc_bufpool::MemoryKey;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, RemoteReadOptions, Result};

/// Request to read from a remote memory region.
///
/// After reading, the service verifies that the original request (identified
/// by `msgid`) is still being awaited. If the request has already timed out,
/// the data is discarded and a Timeout error is returned, since the buffer
/// may have been reclaimed.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryReadReq {
    pub key: MemoryKey,
    pub addr: u64,
    pub len: u64,
    /// Message ID of the original request. Used to verify the request
    /// is still alive after reading, ensuring the buffer data is valid.
    pub msgid: u64,
}

/// Request to push data from the server to the client (TCP path).
///
/// The server sends the data inline. The client allocates a buffer from its
/// pool, copies the data in, and stores it in the waiter's buffer_map keyed
/// by `msgid`. The buffer will be returned to the caller when the original
/// RPC response arrives.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryPushReq {
    /// Message ID of the original client request.
    pub msgid: u64,
    /// Data to push to the client.
    pub data: Vec<u8>,
}

/// Request to push data from the server to the client (RDMA path).
///
/// The server attaches its local buffer via `buffer_info` in MsgMeta.
/// The client allocates a buffer from its pool, performs a `remote_read`
/// to pull data from the server's buffer, then stores it in the waiter's
/// buffer_map keyed by `msgid`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryPullReq {
    /// Message ID of the original client request.
    pub msgid: u64,
    /// Number of bytes to read from the server's buffer.
    pub len: u64,
}

/// Built-in service for remote memory operations.
///
/// Provides methods for:
/// - `tcp_read`: Server reads client's registered memory (reverse RPC)
/// - `tcp_push`: Server pushes data to client via TCP (data inline in request)
/// - `rdma_pull`: Server pushes data to client via RDMA (client pulls from server's buffer)
#[ruapc_macro::service]
pub trait MemoryService {
    /// Reads data from a registered memory region over TCP.
    ///
    /// After reading, verifies the original request is still alive (not timed out).
    async fn tcp_read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<Vec<u8>>;

    /// Receives data pushed by the server (TCP path).
    ///
    /// Allocates a buffer from the client's pool, copies the data in, and stores
    /// it in the waiter's buffer_map. Returns Ok if the original request is still
    /// alive and the buffer was stored successfully.
    async fn tcp_push(&self, ctx: &Context, req: &MemoryPushReq) -> Result<()>;

    /// Receives data pushed by the server (RDMA path).
    ///
    /// The server's buffer info is in `ctx.msg_meta.buffer_info`. The client
    /// allocates a local buffer, performs a `remote_read` from the server's buffer,
    /// and stores the result in the waiter's buffer_map.
    async fn rdma_pull(&self, ctx: &Context, req: &MemoryPullReq) -> Result<()>;
}

impl MemoryService for () {
    async fn tcp_read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<Vec<u8>> {
        let data = ctx
            .state
            .devices
            .tcp_device()
            .read_memory(req.key.lkey, req.addr, req.len)
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;

        // After reading, verify the original request is still alive.
        if !ctx.state.waiter.contains_message_id(req.msgid) {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "tcp_read: original request has already timed out, data discarded".into(),
            ));
        }

        Ok(data)
    }

    async fn tcp_push(&self, ctx: &Context, req: &MemoryPushReq) -> Result<()> {
        // Allocate a right-sized buffer from the client's pool and copy data in.
        let mut buf = ctx
            .state
            .buffer_pool
            .allocate(req.data.len().max(1))
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;
        buf[..req.data.len()].copy_from_slice(&req.data);
        buf.set_len(req.data.len());

        // Store the buffer in the waiter. If the request has already timed out,
        // the buffer is simply dropped back to the pool.
        if !ctx.state.waiter.store_write_buffer(req.msgid, buf) {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "tcp_push: original request has already timed out".into(),
            ));
        }
        Ok(())
    }

    async fn rdma_pull(&self, ctx: &Context, req: &MemoryPullReq) -> Result<()> {
        // Get the server's buffer info from msg_meta.
        let buffer_info = ctx.msg_meta.buffer_info.as_ref().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::MissingBufferInfo,
                "rdma_pull: missing buffer_info in msg_meta".into(),
            )
        })?;

        // Allocate a local buffer and pull data from the server's buffer.
        // Skip verification because the server buffer's lifetime is guaranteed
        // by the server holding &buf across the rdma_pull .await.
        let mut local_buf = ctx
            .state
            .buffer_pool
            .allocate((req.len as usize).max(1))
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;
        let options = RemoteReadOptions { skip_verify: true };
        local_buf = ctx
            .remote_read_with_options(buffer_info, local_buf, &options)
            .await?;
        // `buffer_info.len` is the server's logical data length, so the
        // returned buffer already has len == req.len; truncate defensively
        // in case a peer advertises more than the requested length.
        local_buf.set_len((req.len as usize).min(local_buf.len()));

        // Store the buffer in the waiter.
        if !ctx.state.waiter.store_write_buffer(req.msgid, local_buf) {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "rdma_pull: original request has already timed out".into(),
            ));
        }
        Ok(())
    }
}
