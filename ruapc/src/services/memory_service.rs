use ruapc_bufpool::RemoteBufferInfo;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    Context, CopyOp, Result,
    core::ContextEndpoint,
    core::scatter::{self, MAX_REGIONS, SpaceLayout},
};

/// Request to read byte ranges of the client's read space (TCP/WS/HTTP
/// fallback of `Context::remote_read`).
///
/// `regions` echo the read regions the client attached to the original
/// request (they describe the *client's own* memory, so the client can
/// re-validate every access against its registration table). Each op's
/// `src_offset` addresses the logical concatenation of `regions`;
/// `dst_offset` addresses the server's local space and is opaque to the
/// client.
///
/// After reading, the service verifies that the original request
/// (identified by `msgid`) is still being awaited. If it has already timed
/// out, the data is discarded and a Timeout error is returned, since the
/// buffers may have been reclaimed.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryReadReq {
    /// The client's read regions, in space order.
    pub regions: Vec<RemoteBufferInfo>,
    /// The validated op batch; response bytes are the op payloads
    /// concatenated in op order.
    pub ops: Vec<CopyOp>,
    /// Message ID of the original request. Used to verify the request
    /// is still alive after reading, ensuring the buffer data is valid.
    pub msgid: u64,
}

/// Response of [`MemoryService::read`]: the requested op payloads,
/// concatenated in op order.
///
/// A struct (rather than a bare `Vec<u8>`) so the field can opt into
/// `serde_bytes` — see [`MemoryPushReq::data`] for why that matters.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryReadRsp {
    /// The bytes read from the requested ranges.
    #[serde(with = "serde_bytes")]
    #[schemars(with = "Vec<u8>")]
    pub data: Vec<u8>,
}

/// Request to write into the client's pinned write space with inline data
/// (TCP/WS/HTTP fallback of `Context::remote_write`).
///
/// Each op's `dst_offset` addresses the client's write space; `data` is
/// the op payloads concatenated in op order (`src_offset` describes the
/// server-local source and is opaque to the client).
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryPushReq {
    /// Message ID of the original client request (identifies the pinned
    /// write target).
    pub msgid: u64,
    /// The op batch; validated against the write space on arrival.
    pub ops: Vec<CopyOp>,
    /// Op payloads, concatenated in op order.
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

/// Request to write into the client's pinned write space by letting the
/// client RDMA-READ from the server (RDMA path of `Context::remote_write`).
///
/// The server's source buffers arrive as the `read_regions` of this
/// request's metadata (the same mechanism a client uses to attach read
/// buffers — the roles are symmetric). Each op's `src_offset` addresses
/// that source space, `dst_offset` the client's write space; the client
/// fragments the batch into RDMA READ work requests into its pinned
/// buffers.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryPullReq {
    /// Message ID of the original client request (identifies the pinned
    /// write target).
    pub msgid: u64,
    /// The op batch; validated against both spaces on arrival.
    pub ops: Vec<CopyOp>,
}

/// Built-in service for remote memory operations.
///
/// Provides methods for:
/// - `read`: peer reads ranges of this side's registered memory (reverse
///   RPC, data inline in the response)
/// - `push`: peer writes ranges of this side's pinned write buffers (data
///   inline in the request)
/// - `pull`: peer asks this side to RDMA-READ from its memory into this
///   side's pinned write buffers
#[ruapc_macro::service]
pub trait MemoryService {
    /// Reads byte ranges from registered memory regions (TCP fallback).
    ///
    /// After reading, verifies the original request is still alive (not
    /// timed out).
    async fn read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<MemoryReadRsp>;

    /// Receives data pushed by the server into the pinned write target
    /// (TCP fallback).
    async fn push(&self, ctx: &Context, req: &MemoryPushReq) -> Result<()>;

    /// Executes RDMA READs from the server's advertised regions
    /// (`read_regions` of this request's metadata) into the pinned write
    /// target (RDMA path).
    async fn pull(&self, ctx: &Context, req: &MemoryPullReq) -> Result<()>;
}

impl MemoryService for () {
    async fn read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<MemoryReadRsp> {
        if req.regions.len() > MAX_REGIONS {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidCopyOp,
                format!("too many regions: {}", req.regions.len()),
            ));
        }
        let layout = SpaceLayout::from_lens(req.regions.iter().map(|r| r.len))?;
        // The destination space is server-local and opaque here; only the
        // source side is checked (each region access is additionally
        // validated against the registration table below).
        let total = scatter::validate_ops(&req.ops, layout.total(), u64::MAX)?;
        let mut data = Vec::with_capacity(usize::try_from(total).unwrap_or(0));
        for op in &req.ops {
            layout.for_each_slice::<crate::Error>(op.src_offset, op.len, |seg, off, len| {
                let region = &req.regions[seg];
                let bytes = ctx
                    .state
                    .devices
                    .tcp_device()
                    .read_memory(region.key.lkey, region.addr.wrapping_add(off), len)
                    .map_err(|e| {
                        crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string())
                    })?;
                data.extend_from_slice(&bytes);
                Ok(())
            })?;
        }

        // After reading, verify the original request is still alive.
        if !ctx.state.waiter.contains_message_id(req.msgid) {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "read: original request has already timed out, data discarded".into(),
            ));
        }

        Ok(MemoryReadRsp { data })
    }

    async fn push(&self, ctx: &Context, req: &MemoryPushReq) -> Result<()> {
        // The write target pins the destination buffers; if the original
        // request already resolved or expired, there is nothing to write
        // into.
        let Some(target) = ctx.state.waiter.write_target(req.msgid) else {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "push: original request is gone (timed out, completed, or \
                 attached no write buffers)"
                    .into(),
            ));
        };
        // The source space is server-local and opaque here; validate the
        // destination side against the pinned write space.
        let total = scatter::validate_ops(&req.ops, u64::MAX, target.total_len())?;
        if total != req.data.len() as u64 {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidCopyOp,
                format!(
                    "push carries {} bytes but the ops describe {total}",
                    req.data.len()
                ),
            ));
        }
        let mut cursor = 0usize;
        for op in &req.ops {
            let len = op.len as usize;
            target.copy_in(op.dst_offset, &req.data[cursor..cursor + len])?;
            cursor += len;
        }
        Ok(())
    }

    async fn pull(&self, ctx: &Context, req: &MemoryPullReq) -> Result<()> {
        // The server's source buffers are advertised as this request's
        // read regions.
        let regions = &ctx.msg_meta.read_regions;
        if regions.is_empty() {
            return Err(crate::Error::new(
                crate::ErrorKind::MissingBufferInfo,
                "pull: request metadata carries no read regions".into(),
            ));
        }
        let Some(target) = ctx.state.waiter.write_target(req.msgid) else {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "pull: original request is gone (timed out, completed, or \
                 attached no write buffers)"
                    .into(),
            ));
        };
        for region in regions {
            if region.addr.checked_add(region.len).is_none() {
                return Err(crate::Error::new(
                    crate::ErrorKind::InvalidCopyOp,
                    "pull: region addr + len overflows u64".into(),
                ));
            }
        }
        let src_layout = SpaceLayout::from_lens(regions.iter().map(|r| r.len))?;
        scatter::validate_ops(&req.ops, src_layout.total(), target.total_len())?;

        // The `Arc<WriteTarget>` clone keeps the destination memory alive
        // for the whole transfer, even if the original request expires
        // mid-flight — no post-transfer liveness check is needed on this
        // side (and the server holds its source buffers across the await).
        match &ctx.endpoint {
            ContextEndpoint::Connected(socket) => {
                socket
                    .pull_into_target(regions, &src_layout, &req.ops, target)
                    .await
            }
            _ => Err(crate::Error::new(
                crate::ErrorKind::NotConnected,
                "pull requires a connected socket".into(),
            )),
        }
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
            ops: vec![CopyOp::new(0, 0, LEN as u64)],
            data: data.clone(),
        };
        let encoded = rmp_serde::to_vec_named(&req).unwrap();
        assert!(
            encoded.len() < LEN + 128,
            "MemoryPushReq must encode data as msgpack bin, got {} bytes for {LEN} data bytes",
            encoded.len()
        );
        let decoded: MemoryPushReq = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.msgid, 7);
        assert_eq!(decoded.ops.len(), 1);
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
            ops: vec![CopyOp::new(4, 2, 4)],
            data: vec![0, 1, 127, 255],
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: MemoryPushReq = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.msgid, 1);
        assert_eq!(decoded.ops, vec![CopyOp::new(4, 2, 4)]);
        assert_eq!(decoded.data, vec![0, 1, 127, 255]);

        let rsp = MemoryReadRsp {
            data: vec![42, 255],
        };
        let json = serde_json::to_string(&rsp).unwrap();
        let decoded: MemoryReadRsp = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.data, vec![42, 255]);
    }

    /// `push` must reject batches whose inline data length disagrees with
    /// the ops, and requests without a pinned write target.
    #[tokio::test]
    async fn test_push_validation() {
        let ctx = Context::create(&crate::SocketPoolConfig::default()).unwrap();

        // No pending request with a write target.
        let req = MemoryPushReq {
            msgid: 42,
            ops: vec![CopyOp::new(0, 0, 4)],
            data: vec![0; 4],
        };
        let err = ().push(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::Timeout);

        // Pin a 8-byte write target on a pending request.
        let (msgid, _rx) = ctx.state.waiter.alloc(std::time::Duration::from_secs(5));
        let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
        buf.set_len(8);
        let target = crate::core::WriteTarget::new(vec![buf]).unwrap();
        ctx.state.waiter.bind_write_target(msgid, target);

        // Length mismatch between ops and data.
        let req = MemoryPushReq {
            msgid,
            ops: vec![CopyOp::new(0, 0, 4)],
            data: vec![0; 3],
        };
        let err = ().push(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::InvalidCopyOp);

        // Out-of-bounds destination.
        let req = MemoryPushReq {
            msgid,
            ops: vec![CopyOp::new(0, 5, 4)],
            data: vec![0; 4],
        };
        let err = ().push(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::InvalidCopyOp);

        // Valid push writes through.
        let req = MemoryPushReq {
            msgid,
            ops: vec![CopyOp::new(0, 2, 4)],
            data: b"data".to_vec(),
        };
        ().push(&ctx, &req).await.unwrap();
        let target = ctx.state.waiter.write_target(msgid).unwrap();
        // The waiter entry still holds a clone, so unwrapping fails here —
        // which is exactly the pinning behavior we want.
        assert!(crate::core::WriteTarget::try_into_buffers(target).is_none());
    }
}
