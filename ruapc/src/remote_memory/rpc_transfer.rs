//! Reverse-RPC requests, handlers and transport execution for remote memory.

use ruapc_bufpool::WriteBuffer;
use ruapc_memory::{CopyBatch, RemoteSpace};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::transfer::{PreparedRead, TransferCompletion};
#[cfg(feature = "rdma")]
use crate::{Buffer, Socket, client::ReadAttachment};
use crate::{Client, Completed, Context, CopyOp, Error, ErrorKind, Result, core::ContextEndpoint};

/// Request to read byte ranges of the client's read space (TCP/WS/HTTP
/// fallback of `Context::remote_read`).
///
/// The request ID selects the original borrowing future's mailbox. The peer
/// supplies logical offsets only; that future copies the bytes synchronously
/// while its Rust source borrow is valid.
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

/// Identifies the original request for post-READ waiter membership validation.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct RequestStatusRequest {
    /// Message ID of the original request.
    pub(crate) request_id: u64,
}

/// Reverse RPCs for inline copies, client-side RDMA READs, and
/// source-request waiter probes.
#[ruapc_macro::service]
pub(crate) trait MemoryService {
    /// Asks the original borrowing future to copy source bytes (stream fallback).
    /// The handler receives owned bytes and never obtains a source reference.
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
    /// target (RDMA path). A successful response follows completed READs;
    /// this operation does not perform a `request_is_pending` postcheck.
    async fn read_into_target(&self, ctx: &Context, req: &ReadIntoTargetRequest) -> Result<()>;

    /// Confirms that the original source's waiter is still present.
    ///
    /// Safe sources ask their borrowing future; caller-validated sources check
    /// membership directly. A missing source returns false. Neither path checks
    /// the original deadline clock; the probe's deadline bounds reply waiting.
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
                Error::new(
                    ErrorKind::Timeout,
                    "read_inline: original request is gone or has no read buffers".into(),
                )
            })?;
        let bytes = source.gather(&req.ops, ctx.deadline).await?;
        Ok(ReadInlineResponse { bytes })
    }

    async fn write_inline(&self, ctx: &Context, req: &WriteInlineRequest) -> Result<()> {
        // The write target pins the destination buffers; if the original
        // request already resolved or expired, there is nothing to write
        // into.
        let Some(target) = ctx.state.waiter.write_target(req.request_id) else {
            return Err(Error::new(
                ErrorKind::Timeout,
                "write_inline: original request is gone (timed out, completed, or \
                 attached no write buffers)"
                    .into(),
            ));
        };
        // The source space is server-local and opaque here; validate the
        // destination side against the pinned write space.
        let batch = CopyBatch::new(&req.ops)?;
        target.copy_inline(&batch, &req.bytes)
    }

    async fn read_into_target(&self, ctx: &Context, req: &ReadIntoTargetRequest) -> Result<()> {
        // The server's source buffers are advertised as this request's
        // read regions.
        let regions = &ctx.msg_meta.read_regions;
        if regions.is_empty() {
            return Err(Error::new(
                ErrorKind::MissingBufferInfo,
                "read_into_target: request metadata carries no read regions".into(),
            ));
        }
        let Some(target) = ctx.state.waiter.write_target(req.request_id) else {
            return Err(Error::new(
                ErrorKind::Timeout,
                "read_into_target: original request is gone (timed out, completed, or \
                 attached no write buffers)"
                    .into(),
            ));
        };
        let source = RemoteSpace::new(regions)?;
        let batch = CopyBatch::new(&req.ops)?;
        batch.check_bounds(source.total_len(), target.total_len())?;

        // The RDMA path moves destinations from this target into the QP.
        // Request expiry or handler cancellation cannot release posted DMA
        // memory; only completion or successful QP destruction can do that.
        match &ctx.endpoint {
            ContextEndpoint::Connected(socket) => match socket {
                #[cfg(feature = "rdma")]
                Socket::RDMA(socket) => {
                    socket
                        .read_into_target(&source, &batch, target, ctx.deadline)
                        .await
                }
                _ => Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "read_into_target requires RDMA (other transports use write_inline)".into(),
                )),
            },
            _ => Err(Error::new(
                ErrorKind::NotConnected,
                "read_into_target requires a connected socket".into(),
            )),
        }
    }

    async fn request_is_pending(&self, ctx: &Context, req: &RequestStatusRequest) -> Result<bool> {
        let Some(source) = ctx.state.waiter.read_source(req.request_id) else {
            return Ok(false);
        };
        source
            .validate(&ctx.state.waiter, req.request_id, ctx.deadline)
            .await
    }
}

/// Copies through the same owned-or-borrowed destination storage.
pub(crate) async fn read(transfer: PreparedRead<'_, '_>) -> TransferCompletion<WriteBuffer> {
    let mut local = transfer.local;
    let request = ReadInlineRequest {
        ops: transfer.ops.ops().to_vec(),
        request_id: transfer.ctx.msg_meta.msgid,
    };
    let result = async {
        let response = Client::default()
            .read_inline(transfer.ctx, &request)
            .await?;
        Ok(local.scatter(&transfer.ops, &response.bytes)?)
    }
    .await;
    Completed {
        result,
        buffers: Some(local.into_buffers()),
    }
}

pub(crate) async fn write(ctx: &Context, batch: CopyBatch<'_>, bytes: Vec<u8>) -> Result<()> {
    let request = WriteInlineRequest {
        ops: batch.ops().to_vec(),
        request_id: ctx.msg_meta.msgid,
        bytes,
    };
    Client::default().write_inline(ctx, &request).await
}

#[cfg(feature = "rdma")]
pub(crate) async fn write_via_read(
    ctx: &Context,
    batch: CopyBatch<'_>,
    source: &[Buffer],
) -> Result<()> {
    let request = ReadIntoTargetRequest {
        request_id: ctx.msg_meta.msgid,
        ops: batch.ops().to_vec(),
    };
    // The request exports metadata while this future holds the real reference.
    // Its waiter never receives a source that another local task could read.
    // A normal successful response acknowledges the peer's completed READ.
    Client::default()
        .ruapc_request::<_, (), Error>(
            ctx,
            &request,
            ReadAttachment::remote_source(source, batch.byte_count()),
            None,
            "MemoryService/read_into_target",
        )
        .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ruapc_bufpool::RemoteBufferInfo;
    use ruapc_memory::BufferSet;

    use super::*;

    use crate::{Socket, SocketPoolConfig, remote_memory::WriteTarget, tcp::TcpSocket};

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
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();

        // No pending request with a write target.
        let req = WriteInlineRequest {
            request_id: 42,
            ops: vec![CopyOp::new(0, 0, 4)],
            bytes: vec![0; 4],
        };
        let err = ().write_inline(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::Timeout);

        // Pin a 8-byte write target on a pending request.
        let (msgid, rx) = ctx.state.waiter.alloc(Duration::from_secs(5));
        let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
        buf.set_len(8);
        let pointer = buf.as_ptr();
        let target = WriteTarget::from_owned(BufferSet::try_new(vec![buf]).unwrap());
        ctx.state.waiter.bind_write_target(msgid, target);

        // Length mismatch between ops and data.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 0, 4)],
            bytes: vec![0; 3],
        };
        let err = ().write_inline(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidCopyOp);

        // Out-of-bounds destination.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 5, 4)],
            bytes: vec![0; 4],
        };
        let err = ().write_inline(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidCopyOp);

        // A valid inline write reaches the pinned target.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 2, 4)],
            bytes: b"data".to_vec(),
        };
        ().write_inline(&ctx, &req).await.unwrap();
        let target = ctx.state.waiter.write_target(msgid).unwrap();
        drop(rx);
        // Recovery revokes old handles instead of requiring Arc uniqueness.
        let buffers = WriteTarget::try_into_buffers(target.clone()).unwrap();
        assert_eq!(buffers[0].as_ptr(), pointer);
        assert_eq!(&buffers[0][2..6], b"data");
        assert_eq!(
            target.copy_in(2, b"late").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        assert_eq!(&buffers[0][2..6], b"data");
        let originals = buffers
            .into_iter()
            .map(|buffer| buffer.into_owned().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(originals[0].as_ptr(), pointer);
        assert_eq!(originals[0].len(), 8);
        assert_eq!(&originals[0][2..6], b"data");
    }

    #[tokio::test]
    async fn zero_byte_read_into_target_validates_attachments_and_connection() {
        for ops in [Vec::new(), vec![CopyOp::new(u64::MAX, u64::MAX, 0)]] {
            let config = SocketPoolConfig {
                #[cfg(feature = "rdma")]
                rdma: None,
                ..Default::default()
            };
            let mut ctx = Context::create(&config).unwrap();
            let (request_id, _rx) = ctx.state.waiter.alloc(Duration::from_secs(5));
            let request = ReadIntoTargetRequest { request_id, ops };

            assert_eq!(
                ().read_into_target(&ctx, &request).await.unwrap_err().kind,
                ErrorKind::MissingBufferInfo,
            );
            ctx.msg_meta.read_regions = [RemoteBufferInfo {
                addr: 0x1000,
                len: 4,
                ..Default::default()
            }]
            .into();
            assert_eq!(
                ().read_into_target(&ctx, &request).await.unwrap_err().kind,
                ErrorKind::Timeout,
            );

            let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
            buffer.set_len(4);
            let target = WriteTarget::from_owned(BufferSet::try_new(vec![buffer]).unwrap());
            ctx.state.waiter.bind_write_target(request_id, target);
            assert_eq!(
                ().read_into_target(&ctx, &request).await.unwrap_err().kind,
                ErrorKind::NotConnected,
            );

            ctx.endpoint = ContextEndpoint::Endpoint("tcp://127.0.0.1:8000".parse().unwrap());
            assert_eq!(
                ().read_into_target(&ctx, &request).await.unwrap_err().kind,
                ErrorKind::NotConnected,
            );

            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            ctx.endpoint = ContextEndpoint::Connected(Socket::TCP(TcpSocket::new(sender)));
            assert_eq!(
                ().read_into_target(&ctx, &request).await.unwrap_err().kind,
                ErrorKind::InvalidArgument,
            );
            assert!(receiver.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn test_request_is_pending() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let missing = RequestStatusRequest { request_id: 9999 };
        assert!(!().request_is_pending(&ctx, &missing).await.unwrap());

        let (request_id, rx) = ctx.state.waiter.alloc(Duration::from_secs(1));
        let pending = RequestStatusRequest { request_id };
        assert!(!().request_is_pending(&ctx, &pending).await.unwrap());

        let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        let buffers = [buffer];
        let source = ruapc_memory::BorrowedSource::new(&buffers).unwrap();
        let (sender, mut requests) = crate::remote_memory::SourceSender::channel();
        ctx.state.waiter.bind_read_source(request_id, sender);
        let mut validation = Box::pin(().request_is_pending(&ctx, &pending));
        assert!(futures_util::poll!(validation.as_mut()).is_pending());
        requests
            .serve_next(&source, &ctx.state.waiter, request_id)
            .await;
        assert!(validation.await.unwrap());
        drop(rx);
        assert!(!().request_is_pending(&ctx, &pending).await.unwrap());
    }
}
