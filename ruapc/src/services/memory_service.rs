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
    ///
    /// `serde_bytes` routes the field through serde's byte-string channel:
    /// MessagePack encodes it as a `bin` chunk (header + memcpy) instead of
    /// a per-element integer array — this is the difference between an RPC
    /// framework moving bulk data and one serializing a million tiny ints.
    /// (Internal reverse RPCs always use MessagePack; the JSON fallback
    /// still works, as an integer array.)
    #[serde(with = "serde_bytes")]
    #[schemars(with = "Vec<u8>")]
    pub data: Vec<u8>,
}

/// Response of [`MemoryService::tcp_read`], carrying the bytes read from
/// the client's registered memory.
///
/// A struct (rather than a bare `Vec<u8>`) so the field can opt into
/// `serde_bytes` — see [`MemoryPushReq::data`] for why that matters.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryReadRsp {
    /// The bytes read from the requested memory region.
    #[serde(with = "serde_bytes")]
    #[schemars(with = "Vec<u8>")]
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
    async fn tcp_read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<MemoryReadRsp>;

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
    async fn tcp_read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<MemoryReadRsp> {
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

        Ok(MemoryReadRsp { data })
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The bulk fields must go through serde's byte-string channel:
    /// MessagePack `bin` (length header + memcpy), not a per-element
    /// integer array. 0xFF bytes would cost 2 bytes each as integers, so a
    /// compact encoding proves the `bin` path is taken.
    #[test]
    fn test_bulk_fields_use_msgpack_bin() {
        const LEN: usize = 1024;
        let data = vec![0xFFu8; LEN];

        let req = MemoryPushReq {
            msgid: 7,
            data: data.clone(),
        };
        let encoded = rmp_serde::to_vec_named(&req).unwrap();
        assert!(
            encoded.len() < LEN + 64,
            "MemoryPushReq must encode data as msgpack bin, got {} bytes for {LEN} data bytes",
            encoded.len()
        );
        let decoded: MemoryPushReq = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.msgid, 7);
        assert_eq!(decoded.data, data);

        let rsp = MemoryReadRsp { data: data.clone() };
        let encoded = rmp_serde::to_vec_named(&rsp).unwrap();
        assert!(
            encoded.len() < LEN + 32,
            "MemoryReadRsp must encode data as msgpack bin, got {} bytes for {LEN} data bytes",
            encoded.len()
        );
        let decoded: MemoryReadRsp = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.data, data);
    }

    /// The JSON fallback (e.g. curl without MessagePack) must still
    /// roundtrip the byte fields.
    #[test]
    fn test_bulk_fields_json_roundtrip() {
        let req = MemoryPushReq {
            msgid: 1,
            data: vec![0, 1, 127, 255],
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: MemoryPushReq = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.msgid, 1);
        assert_eq!(decoded.data, vec![0, 1, 127, 255]);

        let rsp = MemoryReadRsp {
            data: vec![42, 255],
        };
        let json = serde_json::to_string(&rsp).unwrap();
        let decoded: MemoryReadRsp = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.data, vec![42, 255]);
    }
}
