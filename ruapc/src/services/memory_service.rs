use ruapc_bufpool::RemoteBufferInfo;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    Context, CopyOp, Result,
    core::ContextEndpoint,
    remote_memory::scatter::{self, SpaceLayout},
};

/// Request to read byte ranges of the client's read space (TCP/WS/HTTP
/// fallback of `Context::remote_read`).
///
/// The request ID selects this side's owned source buffers. The peer supplies
/// logical offsets only; a local reader keeps the source alive through copying,
/// even if the original request is cancelled at the same time.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ReadInlineRequest {
    /// The validated op batch; response bytes are the op payloads
    /// concatenated in op order.
    pub(crate) ops: Vec<CopyOp>,
    /// Message ID of the original request. Used to verify the request
    /// still has an attached read source.
    pub(crate) request_id: u64,
}

/// Response of [`MemoryService::read_inline`]: the requested op payloads,
/// concatenated in op order.
///
/// A struct (rather than a bare `Vec<u8>`) so the field can opt into
/// `serde_bytes` — see [`WriteInlineRequest::bytes`] for why that matters.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ReadInlineResponse {
    /// The bytes read from the requested ranges.
    #[serde(with = "serde_bytes")]
    #[schemars(with = "Vec<u8>")]
    pub(crate) bytes: Vec<u8>,
}

/// Request to write into the client's pinned write space with inline data
/// (TCP/WS/HTTP fallback of `Context::remote_write`).
///
/// Each op's `dst_offset` addresses the client's write space; `bytes` is
/// the op payloads concatenated in op order (`src_offset` describes the
/// server-local source and is opaque to the client).
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct WriteInlineRequest {
    /// Message ID of the original client request (identifies the pinned
    /// write target).
    pub(crate) request_id: u64,
    /// The op batch; validated against the write space on arrival.
    pub(crate) ops: Vec<CopyOp>,
    /// Op payloads, concatenated in op order.
    ///
    /// `serde_bytes` uses a MessagePack `bin` value instead of per-byte
    /// integers. JSON represents the same field as an integer array.
    #[serde(with = "serde_bytes")]
    #[schemars(with = "Vec<u8>")]
    pub(crate) bytes: Vec<u8>,
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
pub(crate) struct ReadIntoTargetRequest {
    /// Message ID of the original client request (identifies the pinned
    /// write target).
    pub(crate) request_id: u64,
    /// The op batch; validated against both spaces on arrival.
    pub(crate) ops: Vec<CopyOp>,
}

/// Identifies the original request for a post-READ liveness check.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct RequestStatusRequest {
    /// Message ID of the original request.
    pub(crate) request_id: u64,
}

/// Internal reverse RPCs for inline copies, client-side RDMA READs, and
/// source-request liveness checks.
#[ruapc_macro::service(name = "_ruapc.memory", internal)]
pub(crate) trait MemoryService {
    /// Reads the original request's owned source (TCP/WS/HTTP fallback).
    /// A local `Arc` retains the source throughout the CPU copy.
    async fn read_inline(
        &self,
        ctx: &Context,
        req: &ReadInlineRequest,
    ) -> Result<ReadInlineResponse>;

    /// Receives data pushed by the server into the pinned write target
    /// (TCP/WS/HTTP fallback).
    async fn write_inline(&self, ctx: &Context, req: &WriteInlineRequest) -> Result<()>;

    /// Executes RDMA READs from the server's advertised regions
    /// (`read_regions` of this request's metadata) into the pinned write
    /// target (RDMA path).
    async fn read_into_target(&self, ctx: &Context, req: &ReadIntoTargetRequest) -> Result<()>;

    /// Reports whether the original request is still pending on this peer.
    ///
    /// RDMA uses this to reject stale READ results. It neither acknowledges
    /// remote DMA completion nor delays source-buffer recovery.
    async fn request_is_pending(&self, ctx: &Context, req: &RequestStatusRequest) -> Result<bool>;
}

impl MemoryService for () {
    async fn read_inline(
        &self,
        ctx: &Context,
        req: &ReadInlineRequest,
    ) -> Result<ReadInlineResponse> {
        let source = ctx
            .state
            .waiter
            .read_source(req.request_id)
            .ok_or_else(|| {
                crate::Error::new(
                    crate::ErrorKind::Timeout,
                    "read_inline: original request is gone or has no read buffers".into(),
                )
            })?;
        let bytes = source.read_inline(&req.ops)?;
        Ok(ReadInlineResponse { bytes })
    }

    async fn write_inline(&self, ctx: &Context, req: &WriteInlineRequest) -> Result<()> {
        // The write target pins the destination buffers; if the original
        // request already resolved or expired, there is nothing to write
        // into.
        let Some(target) = ctx.state.waiter.write_target(req.request_id) else {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "write_inline: original request is gone (timed out, completed, or \
                 attached no write buffers)"
                    .into(),
            ));
        };
        // The source space is server-local and opaque here; validate the
        // destination side against the pinned write space.
        let total = scatter::validate_ops(&req.ops, u64::MAX, target.total_len())?;
        if total != req.bytes.len() as u64 {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidCopyOp,
                format!(
                    "write_inline carries {} bytes but the ops describe {total}",
                    req.bytes.len()
                ),
            ));
        }
        let mut cursor = 0usize;
        for op in &req.ops {
            let len = op.len as usize;
            target.copy_in(op.dst_offset, &req.bytes[cursor..cursor + len])?;
            cursor += len;
        }
        Ok(())
    }

    async fn read_into_target(&self, ctx: &Context, req: &ReadIntoTargetRequest) -> Result<()> {
        // The server's source buffers are advertised as this request's
        // read regions.
        let regions = &ctx.msg_meta.read_regions;
        if regions.is_empty() {
            return Err(crate::Error::new(
                crate::ErrorKind::MissingBufferInfo,
                "read_into_target: request metadata carries no read regions".into(),
            ));
        }
        let Some(target) = ctx.state.waiter.write_target(req.request_id) else {
            return Err(crate::Error::new(
                crate::ErrorKind::Timeout,
                "read_into_target: original request is gone (timed out, completed, or \
                 attached no write buffers)"
                    .into(),
            ));
        };
        validate_region_addresses(regions, "read_into_target")?;
        let src_layout = SpaceLayout::from_lens(regions.iter().map(|r| r.len))?;
        scatter::validate_ops(&req.ops, src_layout.total(), target.total_len())?;

        // The RDMA path moves destinations from this target into the QP.
        // Request expiry or handler cancellation cannot release posted DMA
        // memory; only completion or successful QP destruction can do that.
        match &ctx.endpoint {
            ContextEndpoint::Connected(socket) => {
                socket
                    .read_into_target(regions, &src_layout, &req.ops, target, ctx.remaining_time())
                    .await
            }
            _ => Err(crate::Error::new(
                crate::ErrorKind::NotConnected,
                "read_into_target requires a connected socket".into(),
            )),
        }
    }

    async fn request_is_pending(&self, ctx: &Context, req: &RequestStatusRequest) -> Result<bool> {
        Ok(ctx.state.waiter.contains_message_id(req.request_id))
    }
}

fn validate_region_addresses(regions: &[RemoteBufferInfo], operation: &str) -> Result<()> {
    for region in regions {
        if region.addr.checked_add(region.len).is_none() {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidCopyOp,
                format!("{operation}: region address + length overflows u64"),
            ));
        }
    }
    Ok(())
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
        let bytes = vec![0xFFu8; LEN];

        let req = WriteInlineRequest {
            request_id: 7,
            ops: vec![CopyOp::new(0, 0, LEN as u64)],
            bytes: bytes.clone(),
        };
        let encoded = rmp_serde::to_vec_named(&req).unwrap();
        assert!(
            encoded.len() < LEN + 128,
            "WriteInlineRequest must encode bytes as msgpack bin, got {} bytes for {LEN} data bytes",
            encoded.len()
        );
        let decoded: WriteInlineRequest = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.request_id, 7);
        assert_eq!(decoded.ops.len(), 1);
        assert_eq!(decoded.bytes, bytes);

        let rsp = ReadInlineResponse {
            bytes: bytes.clone(),
        };
        let encoded = rmp_serde::to_vec_named(&rsp).unwrap();
        assert!(
            encoded.len() < LEN + 32,
            "ReadInlineResponse must encode bytes as msgpack bin, got {} bytes for {LEN} data bytes",
            encoded.len()
        );
        let decoded: ReadInlineResponse = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.bytes, bytes);
    }

    /// The JSON fallback (e.g. curl without MessagePack) must still
    /// roundtrip the byte fields.
    #[test]
    fn test_bulk_fields_json_roundtrip() {
        let req = WriteInlineRequest {
            request_id: 1,
            ops: vec![CopyOp::new(4, 2, 4)],
            bytes: vec![0, 1, 127, 255],
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: WriteInlineRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.request_id, 1);
        assert_eq!(decoded.ops, vec![CopyOp::new(4, 2, 4)]);
        assert_eq!(decoded.bytes, vec![0, 1, 127, 255]);

        let rsp = ReadInlineResponse {
            bytes: vec![42, 255],
        };
        let json = serde_json::to_string(&rsp).unwrap();
        let decoded: ReadInlineResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.bytes, vec![42, 255]);
    }

    /// `write_inline` must reject batches whose inline data length disagrees with
    /// the ops, and requests without a pinned write target.
    #[tokio::test]
    async fn test_write_inline_validation() {
        let ctx = Context::create(&crate::SocketPoolConfig::default()).unwrap();

        // No pending request with a write target.
        let req = WriteInlineRequest {
            request_id: 42,
            ops: vec![CopyOp::new(0, 0, 4)],
            bytes: vec![0; 4],
        };
        let err = ().write_inline(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::Timeout);

        // Pin a 8-byte write target on a pending request.
        let (msgid, _rx) = ctx.state.waiter.alloc(std::time::Duration::from_secs(5));
        let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
        buf.set_len(8);
        let target = crate::remote_memory::WriteTarget::new(vec![buf]).unwrap();
        ctx.state.waiter.bind_write_target(msgid, target);

        // Length mismatch between ops and data.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 0, 4)],
            bytes: vec![0; 3],
        };
        let err = ().write_inline(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::InvalidCopyOp);

        // Out-of-bounds destination.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 5, 4)],
            bytes: vec![0; 4],
        };
        let err = ().write_inline(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::InvalidCopyOp);

        // A valid inline write reaches the pinned target.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 2, 4)],
            bytes: b"data".to_vec(),
        };
        ().write_inline(&ctx, &req).await.unwrap();
        let target = ctx.state.waiter.write_target(msgid).unwrap();
        // The waiter entry still holds a clone, so unwrapping fails here —
        // which is exactly the pinning behavior we want.
        assert!(crate::remote_memory::WriteTarget::try_into_buffers(target).is_none());
    }

    #[tokio::test]
    async fn test_request_is_pending() {
        let ctx = Context::create(&crate::SocketPoolConfig::default()).unwrap();
        let missing = RequestStatusRequest { request_id: 9999 };
        assert!(!().request_is_pending(&ctx, &missing).await.unwrap());

        let (request_id, _rx) = ctx.state.waiter.alloc(std::time::Duration::from_secs(1));
        let pending = RequestStatusRequest { request_id };
        assert!(().request_is_pending(&ctx, &pending).await.unwrap());
    }
}
