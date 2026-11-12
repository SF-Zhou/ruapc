//! Reverse-RPC requests, handlers and transport execution for remote memory.

use std::{sync::Arc, time::Instant};

use ruapc_memory::{CopyBatch, RemoteSpace};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    BufferAvailability, ReadSourceSender, WriteTarget,
    transfer::{PreparedRead, PreparedWrite},
};
#[cfg(feature = "rdma")]
use crate::Socket;
use crate::{
    Buffer, Client, Context, CopyOp, Error, ErrorKind, RemoteIoError, Result, core::ContextEndpoint,
};

/// Request to read byte ranges of the client's read space (TCP/WS/HTTP
/// fallback of `Context::remote_read`).
///
/// The request ID routes logical operations to the original borrowing future,
/// which copies the bytes synchronously while its source reference is valid.
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

/// Identifies the original borrowing future for post-READ source validation.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ValidateSourceRequest {
    /// Message ID of the original request.
    pub(crate) request_id: u64,
}

/// Reverse RPCs for inline copies, client-side RDMA READs, and post-READ
/// source validation.
///
/// Every method names an original request by its ID, and reaches that
/// request's memory only when called on the connection carrying it. Request
/// IDs are sequential: an ID alone must not let another peer of this process
/// read a source or write a destination.
#[ruapc_macro::service]
pub(crate) trait MemoryService {
    /// Asks the original request future to copy its borrowed source.
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
    /// this operation does not perform a `validate_read_source` postcheck.
    async fn read_into_target(&self, ctx: &Context, req: &ReadIntoTargetRequest) -> Result<()>;

    /// Asks the original request future to confirm its source after a READ.
    /// Success is that future's fresh answer. Waiter membership alone cannot
    /// validate a borrowed source: a forgotten future leaves its mailbox
    /// behind but cannot answer another probe.
    async fn validate_read_source(&self, ctx: &Context, req: &ValidateSourceRequest) -> Result<()>;
}

/// The original request named by a reverse RPC is not pending on the
/// connection that delivered it, or attached no such memory. The cases are
/// deliberately indistinguishable to the caller.
fn request_gone(method: &str, attachment: &str) -> Error {
    Error::new(
        ErrorKind::Timeout,
        format!(
            "{method}: original request is gone (timed out, completed, on another \
             connection, or attached no {attachment} buffers)"
        ),
    )
}

/// Routes to the read source of the original request on this connection, with
/// the earlier of that request's deadline and this reverse RPC's.
fn read_source(
    ctx: &Context,
    request_id: u64,
    method: &str,
) -> Result<(ReadSourceSender, Instant)> {
    let (source, deadline) = ctx
        .conn_id()
        .and_then(|conn_id| ctx.state.waiter.read_source(request_id, conn_id))
        .ok_or_else(|| request_gone(method, "read"))?;
    Ok((
        source,
        ctx.deadline.map_or(deadline, |parent| parent.min(deadline)),
    ))
}

/// The pinned write target of the original request on this connection.
fn write_target(ctx: &Context, request_id: u64, method: &str) -> Result<Arc<WriteTarget>> {
    ctx.conn_id()
        .and_then(|conn_id| ctx.state.waiter.write_target(request_id, conn_id))
        .ok_or_else(|| request_gone(method, "write"))
}

impl MemoryService for () {
    async fn read_inline(
        &self,
        ctx: &Context,
        req: &ReadInlineRequest,
    ) -> Result<ReadInlineResponse> {
        let (source, deadline) = read_source(ctx, req.request_id, "read_inline")?;
        let bytes = source.gather(&req.ops, deadline).await?;
        Ok(ReadInlineResponse { bytes })
    }

    async fn write_inline(&self, ctx: &Context, req: &WriteInlineRequest) -> Result<()> {
        // The write target pins the destination buffers; if the original
        // request already resolved or expired, there is nothing to write
        // into.
        let target = write_target(ctx, req.request_id, "write_inline")?;
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
        let target = write_target(ctx, req.request_id, "read_into_target")?;
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

    async fn validate_read_source(&self, ctx: &Context, req: &ValidateSourceRequest) -> Result<()> {
        let (source, deadline) = read_source(ctx, req.request_id, "validate_read_source")?;
        source.validate(deadline).await
    }
}

/// Consumes a prepared stream read and recovers its local destinations.
pub(crate) async fn read(
    prepared: PreparedRead<'_, '_>,
) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
    let transfer = prepared.into_parts();
    let mut local = transfer.local;
    let request = ReadInlineRequest {
        ops: transfer.ops.ops().to_vec(),
        request_id: transfer.ctx.msg_meta.msgid,
    };
    let client = Client::default();
    let response = match client.read_inline(transfer.ctx, &request).await {
        Ok(response) => response,
        Err(error) => return Err(RemoteIoError::new(error, Some(local.into_buffers()))),
    };
    if let Err(error) = local.scatter(&transfer.ops, &response.bytes) {
        return Err(RemoteIoError::new(error.into(), Some(local.into_buffers())));
    }
    Ok(local.into_buffers())
}

/// Consumes a prepared stream write; a successful reply follows its CPU copy.
pub(crate) async fn write(
    prepared: PreparedWrite<'_, '_>,
) -> std::result::Result<BufferAvailability, RemoteIoError> {
    let transfer = prepared.into_parts();
    let local = transfer.local;
    let bytes = match local.gather(&transfer.ops) {
        Ok(bytes) => bytes,
        Err(error) => return Err(RemoteIoError::new(error.into(), Some(local.into_buffers()))),
    };
    let request = WriteInlineRequest {
        ops: transfer.ops.ops().to_vec(),
        request_id: transfer.ctx.msg_meta.msgid,
        bytes,
    };
    let client = Client::default();
    match client.write_inline(transfer.ctx, &request).await {
        Ok(()) => Ok(BufferAvailability::Available(local.into_buffers())),
        Err(error) => Err(RemoteIoError::new(error, Some(local.into_buffers()))),
    }
}

/// Asks the peer to READ the prepared source into its attached write target.
#[cfg(feature = "rdma")]
pub(crate) async fn write_via_read(
    prepared: PreparedWrite<'_, '_>,
) -> std::result::Result<BufferAvailability, RemoteIoError> {
    let transfer = prepared.into_parts();
    let request = ReadIntoTargetRequest {
        request_id: transfer.ctx.msg_meta.msgid,
        ops: transfer.ops.ops().to_vec(),
    };
    // Own the source locally and lend it to the reverse RPC until its reply.
    // Error recovery does not prove remote completion; see
    // Context::remote_write for the source reuse and result-validity contract.
    let client = Client::default();
    let result = client
        .with_read_buffers(transfer.local.buffers())
        .with_read_charge_bytes(transfer.ops.byte_count())
        .read_into_target(transfer.ctx, &request)
        .await;
    let buffers = transfer.local.into_buffers();
    match result {
        Ok(()) => Ok(BufferAvailability::Available(buffers)),
        Err(error) => Err(RemoteIoError::new(error, Some(buffers))),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ruapc_bufpool::RemoteBufferInfo;
    use ruapc_memory::{BufferSet, ReadSource};

    use super::*;

    use crate::{
        MAX_COPY_OPS, MsgMeta, Socket, SocketPoolConfig, remote_memory::serve_read_source,
        task::MemoryRoutes, tcp::TcpSocket,
    };

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

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(30)
    }

    /// A reverse-RPC context as delivered by one stream connection of `state`.
    fn arrival(state: &Arc<crate::State>) -> (Context, u64) {
        let (sender, _queue) = tokio::sync::mpsc::channel(1);
        let socket = TcpSocket::new(sender);
        let conn_id = socket.conn_id();
        let ctx = Context::server_ctx(
            state,
            Socket::TCP(socket),
            MsgMeta {
                timeout_ms: 30_000,
                ..Default::default()
            },
        );
        (ctx, conn_id)
    }

    fn connected() -> (Context, u64) {
        let config = SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        };
        arrival(&Context::create(&config).unwrap().state)
    }

    fn filled(ctx: &Context, bytes: &[u8]) -> Buffer {
        let mut buffer = ctx.state.buffer_pool.allocate(bytes.len()).unwrap();
        buffer.set_len(bytes.len());
        buffer.copy_from_slice(bytes);
        buffer
    }

    /// `write_inline` must reject batches whose inline data length disagrees with
    /// the ops, and requests without a pinned write target.
    #[tokio::test]
    async fn test_write_inline_validation() {
        let (ctx, conn_id) = connected();

        // No pending request with a write target.
        let req = WriteInlineRequest {
            request_id: 42,
            ops: vec![CopyOp::new(0, 0, 4)],
            bytes: vec![0; 4],
        };
        let err = ().write_inline(&ctx, &req).await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::Timeout);

        // Pin a 8-byte write target on a request pending on this connection.
        let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
        buf.set_len(8);
        let target = WriteTarget::new(BufferSet::new(vec![buf]));
        let (msgid, _rx) = ctx.state.waiter.alloc_until(
            later(),
            MemoryRoutes {
                write_target: Some(target.clone()),
                ..Default::default()
            },
        );
        ctx.state.waiter.bind_connection(msgid, conn_id);

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
        assert_eq!(&target.seal(None).await.unwrap()[0][2..6], b"data");
    }

    #[tokio::test]
    async fn zero_byte_read_into_target_validates_attachments_and_connection() {
        for ops in [Vec::new(), vec![CopyOp::new(u64::MAX, u64::MAX, 0)]] {
            let (mut ctx, conn_id) = connected();
            let connection = ctx.endpoint.clone();
            let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
            buffer.set_len(4);
            let (request_id, _rx) = ctx.state.waiter.alloc_until(
                later(),
                MemoryRoutes {
                    write_target: Some(WriteTarget::new(BufferSet::new(vec![buffer]))),
                    ..Default::default()
                },
            );
            ctx.state.waiter.bind_connection(request_id, conn_id);
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

            // Without the request's connection, its target cannot be named.
            for endpoint in [
                ContextEndpoint::Invalid,
                ContextEndpoint::Endpoint("tcp://127.0.0.1:8000".parse().unwrap()),
                arrival(&ctx.state).0.endpoint,
            ] {
                ctx.endpoint = endpoint;
                assert_eq!(
                    ().read_into_target(&ctx, &request).await.unwrap_err().kind,
                    ErrorKind::Timeout,
                );
            }

            ctx.endpoint = connection;
            let (unattached_id, _unattached) = ctx.state.waiter.alloc(Duration::from_secs(5));
            ctx.state.waiter.bind_connection(unattached_id, conn_id);
            let unattached = ReadIntoTargetRequest {
                request_id: unattached_id,
                ops: request.ops.clone(),
            };
            assert_eq!(
                ().read_into_target(&ctx, &unattached)
                    .await
                    .unwrap_err()
                    .kind,
                ErrorKind::Timeout,
            );
            assert_eq!(
                ().read_into_target(&ctx, &request).await.unwrap_err().kind,
                ErrorKind::InvalidArgument,
            );
        }
    }

    #[tokio::test]
    async fn oversized_inline_reads_are_rejected_without_polling_the_source() {
        let (ctx, conn_id) = connected();
        let buffer = filled(&ctx, b"data");
        let source = ReadSource::new(std::slice::from_ref(&buffer));
        let (route, _service) = serve_read_source(&source);
        let (request_id, _response) = ctx.state.waiter.alloc_until(
            later(),
            MemoryRoutes {
                read_source: Some(route),
                ..Default::default()
            },
        );
        ctx.state.waiter.bind_connection(request_id, conn_id);
        for ops in [
            vec![CopyOp::new(0, 0, 0); MAX_COPY_OPS + 1],
            // Overlapping source ranges would amplify four bytes into a reply
            // that no frame can carry.
            (0..MAX_COPY_OPS as u64)
                .map(|index| CopyOp::new(0, index << 16, 1 << 16))
                .collect(),
        ] {
            let request = ReadInlineRequest { request_id, ops };
            let result =
                tokio::time::timeout(Duration::from_secs(1), ().read_inline(&ctx, &request))
                    .await
                    .expect("oversized work must fail before entering the source mailbox");
            assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidCopyOp);
        }
    }

    #[tokio::test]
    async fn source_validation_requires_the_original_future_not_just_a_waiter() {
        let (ctx, conn_id) = connected();
        let missing = ValidateSourceRequest { request_id: 9999 };
        assert_eq!(
            ().validate_read_source(&ctx, &missing)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );

        // A pending request without a read attachment has no source to validate.
        let (request_id, _rx) = ctx.state.waiter.alloc(Duration::from_secs(1));
        ctx.state.waiter.bind_connection(request_id, conn_id);
        let unattached = ValidateSourceRequest { request_id };
        assert_eq!(
            ().validate_read_source(&ctx, &unattached)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );

        let buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        let source = ReadSource::new(std::slice::from_ref(&buffer));
        let (route, mut service) = serve_read_source(&source);
        let (request_id, _rx) = ctx.state.waiter.alloc_until(
            later(),
            MemoryRoutes {
                read_source: Some(route),
                ..Default::default()
            },
        );
        ctx.state.waiter.bind_connection(request_id, conn_id);
        let pending = ValidateSourceRequest { request_id };
        service
            .drive(().validate_read_source(&ctx, &pending))
            .await
            .unwrap();
        drop(service);
        assert_eq!(
            ().validate_read_source(&ctx, &pending)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
    }

    /// Request IDs are sequential. Guessing one must not give another
    /// connection of this process, or a caller without one, access to memory
    /// attached to a request sent elsewhere.
    #[tokio::test]
    async fn memory_routes_reject_reverse_rpcs_from_other_connections() {
        let (ctx, conn_id) = connected();
        let (other, _) = arrival(&ctx.state);
        let mut unconnected = ctx.clone();
        unconnected.endpoint = ContextEndpoint::Invalid;

        let buffer = filled(&ctx, b"read");
        let source = ReadSource::new(std::slice::from_ref(&buffer));
        let (route, mut service) = serve_read_source(&source);
        let target = WriteTarget::new(BufferSet::new(vec![filled(&ctx, b"keep")]));
        let (request_id, _response) = ctx.state.waiter.alloc_until(
            later(),
            MemoryRoutes {
                read_source: Some(route),
                write_target: Some(target.clone()),
            },
        );
        ctx.state.waiter.bind_connection(request_id, conn_id);

        let read = ReadInlineRequest {
            request_id,
            ops: vec![CopyOp::new(0, 0, 4)],
        };
        let write = WriteInlineRequest {
            request_id,
            ops: vec![CopyOp::new(0, 0, 4)],
            bytes: b"evil".to_vec(),
        };
        let validate = ValidateSourceRequest { request_id };
        let mut into_target = other.clone();
        into_target.msg_meta.read_regions = [RemoteBufferInfo {
            addr: 0x1000,
            len: 4,
            ..Default::default()
        }]
        .into();
        let read_into = ReadIntoTargetRequest {
            request_id,
            ops: vec![CopyOp::new(0, 0, 4)],
        };

        // The owner is polled throughout, so only the connection check can
        // refuse these callers.
        service
            .drive(async {
                for foreign in [&other, &unconnected] {
                    assert_eq!(
                        ().read_inline(foreign, &read).await.unwrap_err().kind,
                        ErrorKind::Timeout
                    );
                    assert_eq!(
                        ().write_inline(foreign, &write).await.unwrap_err().kind,
                        ErrorKind::Timeout
                    );
                    assert_eq!(
                        ().validate_read_source(foreign, &validate)
                            .await
                            .unwrap_err()
                            .kind,
                        ErrorKind::Timeout
                    );
                }
                assert_eq!(
                    ().read_into_target(&into_target, &read_into)
                        .await
                        .unwrap_err()
                        .kind,
                    ErrorKind::Timeout
                );

                // The request's own connection reaches both attachments.
                assert_eq!(().read_inline(&ctx, &read).await.unwrap().bytes, b"read");
                ().validate_read_source(&ctx, &validate).await.unwrap();
                let own = WriteInlineRequest {
                    request_id,
                    ops: vec![CopyOp::new(0, 2, 2)],
                    bytes: b"ok".to_vec(),
                };
                ().write_inline(&ctx, &own).await.unwrap();
            })
            .await;
        assert_eq!(&target.seal(None).await.unwrap()[0][..], b"keok");
    }
}
