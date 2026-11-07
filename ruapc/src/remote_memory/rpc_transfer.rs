//! Reverse-RPC requests, handlers and transport execution for remote memory.

use std::sync::Arc;

use bytes::Bytes;
use ruapc_memory::{CopyBatch, ReadSource, RemoteSpace};
use ruapc_wire::{frame, raw};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    BufferAvailability,
    transfer::{PreparedRead, PreparedWrite},
};
#[cfg(feature = "rdma")]
use crate::Socket;
use crate::{
    Buffer, Client, Context, CopyOp, Error, ErrorKind, Message, MethodContext, MethodProtocol,
    MethodTransport, MsgFlags, Payload, PayloadFormat, RemoteIoError, Result, Router,
    client::RequestPayload, core::ContextEndpoint,
};

/// Request to read byte ranges of the client's read space (TCP/WS/HTTP
/// fallback of `Context::remote_read`).
///
/// The request ID selects this side's owned source buffers. The peer supplies
/// logical offsets only; immutable slices keep the source alive until the
/// transport releases them, even if the original request is cancelled meanwhile.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ReadInlineRequest {
    /// The validated op batch; response bytes are the op payloads
    /// concatenated in op order.
    pub(crate) ops: Vec<CopyOp>,
    /// Message ID of the original request. Used to verify the request
    /// still has an attached read source.
    pub(crate) request_id: u64,
}

/// Control header for a raw write. The trailing unencoded bytes contain each
/// operation's source range in operation order; source offsets remain opaque
/// to the receiving peer. Destination offsets address its pinned write target.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct WriteInlineRequest {
    pub(crate) request_id: u64,
    pub(crate) ops: Vec<CopyOp>,
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
    /// Executes RDMA READs from the server's advertised regions
    /// (`read_regions` of this request's metadata) into the pinned write
    /// target (RDMA path). A successful response follows completed READs;
    /// this operation does not perform a `request_is_pending` postcheck.
    async fn read_into_target(&self, ctx: &Context, req: &ReadIntoTargetRequest) -> Result<()>;

    /// Reports whether the original request remains in this peer's waiter table.
    ///
    /// This checks membership, not the deadline clock. [`Context::remote_read`]
    /// defines how a positive post-READ reply validates the copied bytes.
    async fn request_is_pending(&self, ctx: &Context, req: &RequestStatusRequest) -> Result<bool>;
}

impl MemoryService for () {
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
        Ok(ctx.state.waiter.contains_message_id(req.request_id))
    }
}

const READ_INLINE: &str = "MemoryService/read_inline";
const WRITE_INLINE: &str = "MemoryService/write_inline";

/// Raw handlers use the same method directory, admission and panic boundary
/// as generated services. Their schemas describe the control values only.
pub(crate) fn register_inline(router: &mut Router) {
    router.add_handler_with_protocol::<ReadInlineRequest, Result<()>, _, _>(
        READ_INLINE,
        MethodProtocol {
            response_format: PayloadFormat::Raw,
            transport: MethodTransport::Stream,
            context: MethodContext::OriginalPeer,
            ..Default::default()
        },
        |mut ctx, payload| async move {
            let req = payload.deserialize::<ReadInlineRequest>(&ctx.msg_meta)?;
            let parts = read_inline(&ctx, &req)?;
            ctx.send_raw_rsp(&Ok::<(), Error>(()), &parts).await
        },
    );
    router.add_handler_with_protocol::<WriteInlineRequest, Result<()>, _, _>(
        WRITE_INLINE,
        MethodProtocol {
            request_format: PayloadFormat::Raw,
            transport: MethodTransport::Stream,
            context: MethodContext::OriginalPeer,
            ..Default::default()
        },
        |mut ctx, payload| async move {
            let (req, bytes) = raw::decode::<WriteInlineRequest>(&payload, ctx.msg_meta.flags)?;
            write_inline(&ctx, &req, bytes)?;
            ctx.send_rsp(Ok::<(), Error>(())).await
        },
    );
}

fn read_inline(ctx: &Context, req: &ReadInlineRequest) -> Result<Vec<Bytes>> {
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
    let batch = CopyBatch::new(&req.ops)?;
    source_parts(&source, &batch)
}

pub(super) fn write_inline(ctx: &Context, req: &WriteInlineRequest, bytes: &[u8]) -> Result<()> {
    let target = ctx
        .state
        .waiter
        .write_target(req.request_id)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Timeout,
                "write_inline: original request is gone or has no write buffers".into(),
            )
        })?;
    let batch = CopyBatch::new(&req.ops)?;
    target.copy_inline(&batch, bytes)
}

fn source_parts(source: &Arc<ReadSource>, batch: &CopyBatch<'_>) -> Result<Vec<Bytes>> {
    // Reject an impossible frame before splitting repeated ranges into source
    // views. The encoder still checks the complete metadata/control overhead.
    if batch.byte_count() >= (frame::MAX_MSG_SIZE - frame::HEADER_LEN) as u64 {
        return Err(Error::new(
            ErrorKind::ParseMsgFailed,
            format!("inline transfer is too long: {}", batch.byte_count()),
        ));
    }
    Ok(source
        .slices(batch)?
        .into_iter()
        .map(Bytes::from_owner)
        .collect())
}

/// A successful read retains the receive allocation and advances past only
/// the small control header. Current peers send failures through the framework
/// error boundary, which the client handles before calling this decoder. Keep
/// accepting ordinary and raw Result errors from earlier peers as well.
fn read_response(mut message: Message) -> Result<Payload> {
    if !message.meta.flags.contains(MsgFlags::HasRawBytes) {
        message.payload.deserialize::<Result<()>>(&message.meta)??;
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            "missing raw read response".into(),
        ));
    }
    let (result, bytes) = raw::decode::<Result<()>>(&message.payload, message.meta.flags)?;
    result?;
    let offset = message.payload.len() - bytes.len();
    message.payload.advance(offset);
    Ok(message.payload)
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
    let response = match client
        .request_message(
            transfer.ctx,
            RequestPayload::Serialized(&request),
            READ_INLINE,
            read_response,
        )
        .await
    {
        Ok(response) => response,
        Err(error) => return Err(RemoteIoError::new(error, Some(local.into_buffers()))),
    };
    if let Err(error) = local.scatter(&transfer.ops, &response) {
        return Err(RemoteIoError::new(error.into(), Some(local.into_buffers())));
    }
    Ok(local.into_buffers())
}

/// Consumes a prepared stream write; a successful reply follows its CPU copy.
pub(crate) async fn write(
    prepared: PreparedWrite<'_, '_>,
) -> std::result::Result<BufferAvailability, RemoteIoError> {
    let transfer = prepared.into_parts();
    let source = Arc::new(ReadSource::from(transfer.local));
    let result = async {
        let request = WriteInlineRequest {
            ops: transfer.ops.ops().to_vec(),
            request_id: transfer.ctx.msg_meta.msgid,
        };
        let parts = source_parts(&source, &transfer.ops)?;
        Client::default()
            .request_message(
                transfer.ctx,
                RequestPayload::Raw {
                    control: &request,
                    data: &parts,
                },
                WRITE_INLINE,
                |message| message.payload.deserialize::<Result<()>>(&message.meta)?,
            )
            .await
    }
    .await;
    // Queued sends own immutable source slices independently of this future.
    // Failure/cancellation must never recover a source still held by a writer.
    let buffers = Arc::try_unwrap(source).ok().map(ReadSource::into_buffers);
    match result {
        Ok(()) => Ok(buffers.map_or(
            BufferAvailability::Unavailable,
            BufferAvailability::Available,
        )),
        Err(error) => Err(RemoteIoError::new(error, buffers)),
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
    // Retain the source until the reverse RPC replies after the peer's READs.
    // Error recovery proves local uniqueness, not remote completion; see
    // Context::remote_write for the source reuse and result-validity contract.
    let client = Client::default();
    let mut source = client
        .with_read_source(Arc::new(ReadSource::from(transfer.local)))
        .with_read_charge_bytes(transfer.ops.byte_count());
    let result = source.read_into_target(transfer.ctx, &request).await;
    let buffers = source.take_read_buffers();
    match result {
        Ok(()) => Ok(buffers),
        Err(error) => Err(RemoteIoError::new(error, buffers.into_available())),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use ruapc_bufpool::{BufferPool, EmptyDevices, RemoteBufferInfo};
    use ruapc_memory::{BufferSet, MAX_COPY_OPS, MAX_REGIONS};
    use tokio::sync::mpsc;

    use super::*;

    use crate::{
        Message, MsgMeta, Socket, SocketPoolConfig, remote_memory::WriteTarget, tcp::TcpSocket,
    };

    fn stream_context(sender: mpsc::Sender<frame::SendFrame>) -> Context {
        let base = Context::create(&SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        })
        .unwrap();
        Context::server_ctx(
            &base.state,
            Socket::TCP(TcpSocket::new(sender)),
            MsgMeta {
                msgid: 42,
                timeout_ms: 30_000,
                write_regions: vec![RemoteBufferInfo {
                    addr: 0x1000,
                    len: 8,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
    }

    fn source_buffer(ctx: &Context) -> Buffer {
        let mut buffer = ctx.state.buffer_pool.allocate(8).unwrap();
        buffer.set_len(8);
        buffer.copy_from_slice(b"retained");
        buffer
    }

    fn queued_meta(frame: &frame::SendFrame) -> MsgMeta {
        let prefix = frame.chunks().next().unwrap();
        // The first chunk contains the complete metadata even though the raw
        // payload remains in separate, owned chunks.
        ruapc_wire::parse_body(&prefix[frame::HEADER_LEN..])
            .unwrap()
            .0
    }

    #[tokio::test]
    async fn inline_execution_failures_use_framework_errors_for_both_codecs() {
        for codec in [MsgFlags::default(), MsgFlags::UseMessagePack] {
            for write in [false, true] {
                for missing in [false, true] {
                    let (sender, mut queue) = mpsc::channel(1);
                    let mut ctx = stream_context(sender);
                    let state = ctx.state.clone();
                    let (request_id, _original_request) =
                        state.waiter.alloc(Duration::from_secs(30));
                    if !missing {
                        ctx.state.waiter.bind_read_source(
                            request_id,
                            Arc::new(ReadSource::try_new(vec![source_buffer(&ctx)]).unwrap()),
                        );
                        ctx.state.waiter.bind_write_target(
                            request_id,
                            WriteTarget::new(
                                BufferSet::try_new(vec![source_buffer(&ctx)]).unwrap(),
                            ),
                        );
                    }
                    ctx.msg_meta.flags = MsgFlags::IsReq | codec;
                    ctx.msg_meta.method = if write { WRITE_INLINE } else { READ_INLINE }.into();
                    let ops = vec![CopyOp::new(0, 0, 9)];
                    let encoded = if write {
                        ctx.msg_meta.flags.insert(MsgFlags::HasRawBytes);
                        frame::encode_raw(
                            &ctx.msg_meta,
                            &WriteInlineRequest { request_id, ops },
                            &[Bytes::from_static(b"too large")],
                        )
                        .unwrap()
                        .into_bytes()
                    } else {
                        frame::encode(&ctx.msg_meta, &ReadInlineRequest { request_id, ops })
                            .unwrap()
                    };
                    let request = Message::parse(encoded.slice(frame::HEADER_LEN..)).unwrap();
                    state.router.dispatch(ctx, request.payload);
                    let response = tokio::time::timeout(Duration::from_secs(5), queue.recv())
                        .await
                        .unwrap()
                        .unwrap()
                        .into_bytes();
                    let response = Message::parse(response.slice(frame::HEADER_LEN..)).unwrap();
                    assert_eq!(
                        response.meta.flags,
                        MsgFlags::IsRsp | MsgFlags::IsFrameworkError | codec,
                    );
                    let error = response
                        .payload
                        .deserialize::<Result<()>>(&response.meta)
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(
                        error.kind,
                        if missing {
                            ErrorKind::Timeout
                        } else {
                            ErrorKind::InvalidCopyOp
                        },
                    );
                }
            }
        }
    }

    #[test]
    fn oversized_inline_batch_is_rejected_before_source_views_are_created() {
        let pool = BufferPool::new(Arc::new(EmptyDevices));
        let buffers = (0..MAX_REGIONS)
            .map(|_| {
                let mut buffer = pool.allocate(1024).unwrap();
                buffer.set_len(1024);
                buffer
            })
            .collect();
        let source = Arc::new(ReadSource::try_new(buffers).unwrap());
        let source_len = source.layout().total();
        let ops = (0..MAX_COPY_OPS)
            .map(|index| CopyOp::new(0, index as u64 * source_len, source_len))
            .collect::<Vec<_>>();
        let batch = CopyBatch::new(&ops).unwrap();
        assert_eq!(batch.byte_count(), frame::MAX_MSG_SIZE as u64);
        assert_eq!(
            source_parts(&source, &batch).unwrap_err().kind,
            ErrorKind::ParseMsgFailed,
        );
        assert!(Arc::try_unwrap(source).is_ok());
    }

    #[test]
    fn read_response_accepts_legacy_result_errors_for_both_codecs() {
        for codec in [MsgFlags::default(), MsgFlags::UseMessagePack] {
            for raw_reply in [false, true] {
                let flags = MsgFlags::IsRsp | codec;
                let result = Err::<(), Error>(Error::kind(ErrorKind::Timeout));
                let (flags, payload) = if raw_reply {
                    let flags = flags | MsgFlags::HasRawBytes;
                    (flags, raw::encode_header(&result, flags).unwrap())
                } else {
                    let meta = MsgMeta {
                        flags,
                        ..Default::default()
                    };
                    let encoded = frame::encode(&meta, &result).unwrap();
                    let message = Message::parse(encoded.slice(frame::HEADER_LEN..)).unwrap();
                    (flags, Bytes::from(message.payload))
                };
                let message = Message::new(
                    MsgMeta {
                        flags,
                        ..Default::default()
                    },
                    payload.into(),
                );
                assert_eq!(read_response(message).unwrap_err().kind, ErrorKind::Timeout);
            }
        }
    }

    #[tokio::test]
    async fn queued_read_response_retains_source_after_original_waiter_is_cancelled() {
        let (sender, mut queue) = mpsc::channel(1);
        let ctx = stream_context(sender);
        let source = Arc::new(ReadSource::try_new(vec![source_buffer(&ctx)]).unwrap());
        let pointer = source.buffers()[0].as_ptr();
        let (request_id, original_request) = ctx.state.waiter.alloc(Duration::from_secs(30));
        ctx.state
            .waiter
            .bind_read_source(request_id, source.clone());
        let request = ReadInlineRequest {
            request_id,
            ops: vec![CopyOp::new(0, 0, 8)],
        };
        let parts = read_inline(&ctx, &request).unwrap();
        let mut meta = MsgMeta {
            msgid: 77,
            flags: MsgFlags::IsRsp | MsgFlags::HasRawBytes | MsgFlags::UseMessagePack,
            ..Default::default()
        };
        let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
            unreachable!();
        };
        socket
            .send_raw(&mut meta, &Ok::<(), Error>(()), &parts, &ctx.state)
            .await
            .unwrap();
        drop(parts);
        drop(original_request);
        assert_eq!(ctx.state.waiter.pending_count(), 0);

        let source = Arc::try_unwrap(source)
            .expect_err("the accepted response must retain the original immutable source");
        let frame = queue.try_recv().unwrap();
        let raw_bytes = frame.chunks().last().unwrap();
        assert_eq!(raw_bytes.as_ptr(), pointer);
        assert_eq!(raw_bytes.as_ref(), b"retained");
        drop(frame);
        let buffers = Arc::try_unwrap(source).unwrap().into_buffers();
        assert_eq!(buffers[0].as_ptr(), pointer);
        assert_eq!(buffers[0].as_slice(), b"retained");
    }

    #[tokio::test]
    async fn queued_write_failure_does_not_recover_aliased_source_buffers() {
        for expire in [false, true] {
            let (sender, mut queue) = mpsc::channel(1);
            let ctx = stream_context(sender);
            let buffer = source_buffer(&ctx);
            let pointer = buffer.as_ptr();
            let mut writing = Box::pin(ctx.remote_write_all(vec![buffer]));
            assert!(futures_util::poll!(writing.as_mut()).is_pending());
            let frame = queue
                .try_recv()
                .expect("the write reached transport acceptance");
            assert_eq!(queued_meta(&frame).method, WRITE_INLINE);
            let expected = if expire {
                ctx.state
                    .waiter
                    .expire(Instant::now() + Duration::from_secs(60));
                ErrorKind::Timeout
            } else {
                let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
                    unreachable!();
                };
                ctx.state.waiter.fail_connection(
                    socket.conn_id().unwrap(),
                    &Error::kind(ErrorKind::ConnectionClosed),
                );
                ErrorKind::ConnectionClosed
            };
            let error = writing.await.unwrap_err();
            assert_eq!(error.error.kind, expected);
            assert!(error.recovered_buffers().is_none());
            assert_eq!(ctx.state.waiter.pending_count(), 0);
            assert!(queue.try_recv().is_err(), "accepted writes must not retry");
            let raw_bytes = frame.chunks().last().unwrap();
            assert_eq!(raw_bytes.as_ptr(), pointer);
            assert_eq!(raw_bytes.as_ref(), b"retained");
        }
    }

    #[tokio::test]
    async fn queued_write_success_keeps_result_and_source_availability_separate() {
        let (sender, mut queue) = mpsc::channel(1);
        let ctx = stream_context(sender);
        let buffer = source_buffer(&ctx);
        let pointer = buffer.as_ptr();
        let mut writing = Box::pin(ctx.remote_write_all(vec![buffer]));
        assert!(futures_util::poll!(writing.as_mut()).is_pending());
        let frame = queue.try_recv().unwrap();
        let meta = queued_meta(&frame);
        // Model a response arriving before the transport drops its final
        // source owner. A successful transfer cannot make aliased memory
        // mutable, even when its response reports completion.
        ctx.state.waiter.post(
            meta.msgid,
            Message::new(
                MsgMeta {
                    msgid: meta.msgid,
                    flags: MsgFlags::IsRsp | MsgFlags::UseMessagePack,
                    ..Default::default()
                },
                Bytes::from(rmp_serde::to_vec_named(&Ok::<(), Error>(())).unwrap()).into(),
            ),
        );
        let mut receipt = writing.await.unwrap();
        assert!(matches!(
            receipt.take_sources(),
            BufferAvailability::Unavailable
        ));
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        let raw_bytes = frame.chunks().last().unwrap();
        assert_eq!(raw_bytes.as_ptr(), pointer);
        assert_eq!(raw_bytes.as_ref(), b"retained");
    }

    #[tokio::test]
    async fn rejected_queued_write_recovers_source_without_late_submission() {
        let (sender, mut queue) = mpsc::channel(1);
        sender
            .try_send(Bytes::from_static(b"occupied").into())
            .unwrap();
        let ctx = stream_context(sender);
        let buffer = source_buffer(&ctx);
        let pointer = buffer.as_ptr();
        let mut writing = Box::pin(ctx.remote_write_all(vec![buffer]));
        assert!(futures_util::poll!(writing.as_mut()).is_pending());
        let ContextEndpoint::Connected(Socket::TCP(socket)) = &ctx.endpoint else {
            unreachable!();
        };
        socket.mark_closed();
        let error = writing.await.unwrap_err();
        assert_eq!(error.error.kind, ErrorKind::ConnectionClosed);
        let (_, recovered) = error.into_parts();
        let buffers = recovered.expect("a rejected write has no transport source owner");
        assert_eq!(buffers[0].as_ptr(), pointer);
        assert_eq!(buffers[0].as_slice(), b"retained");
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        assert_eq!(queue.try_recv().unwrap().into_bytes(), b"occupied"[..]);
        assert!(queue.try_recv().is_err());
    }

    #[tokio::test]
    async fn cancelled_queue_admission_does_not_retain_response_sources() {
        let (sender, mut queue) = mpsc::channel(1);
        sender
            .try_send(Bytes::from_static(b"occupied").into())
            .unwrap();
        let ctx = stream_context(sender);
        let source = Arc::new(ReadSource::try_new(vec![source_buffer(&ctx)]).unwrap());
        let operations = [CopyOp::new(0, 0, 8)];
        let parts = source_parts(&source, &CopyBatch::new(&operations).unwrap()).unwrap();
        let mut meta = MsgMeta {
            flags: MsgFlags::IsRsp | MsgFlags::HasRawBytes | MsgFlags::UseMessagePack,
            ..Default::default()
        };
        let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
            unreachable!();
        };
        let mut sending = Box::pin(socket.send_raw(&mut meta, &(), &parts, &ctx.state));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        drop(sending);
        drop(parts);
        let buffers = Arc::try_unwrap(source).unwrap().into_buffers();
        assert_eq!(buffers[0].as_slice(), b"retained");
        assert_eq!(queue.try_recv().unwrap().into_bytes(), b"occupied"[..]);
        assert!(queue.try_recv().is_err());
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
        };
        let err = write_inline(&ctx, &req, &[0; 4]).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Timeout);

        // Pin a 8-byte write target on a pending request.
        let (msgid, _rx) = ctx.state.waiter.alloc(Duration::from_secs(5));
        let mut buf = ctx.state.buffer_pool.allocate(64 * 1024).unwrap();
        buf.set_len(8);
        let target = WriteTarget::new(BufferSet::try_new(vec![buf]).unwrap());
        ctx.state.waiter.bind_write_target(msgid, target);

        // Length mismatch between ops and data.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 0, 4)],
        };
        let err = write_inline(&ctx, &req, &[0; 3]).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidCopyOp);

        // Out-of-bounds destination.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 5, 4)],
        };
        let err = write_inline(&ctx, &req, &[0; 4]).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidCopyOp);

        // A valid inline write reaches the pinned target.
        let req = WriteInlineRequest {
            request_id: msgid,
            ops: vec![CopyOp::new(0, 2, 4)],
        };
        write_inline(&ctx, &req, b"data").unwrap();
        let target = ctx.state.waiter.write_target(msgid).unwrap();
        // The waiter entry still holds a clone, so unwrapping fails here —
        // which is exactly the pinning behavior we want.
        assert!(WriteTarget::try_into_buffers(target).is_none());
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
            ctx.msg_meta.read_regions = vec![RemoteBufferInfo {
                addr: 0x1000,
                len: 4,
                ..Default::default()
            }];
            assert_eq!(
                ().read_into_target(&ctx, &request).await.unwrap_err().kind,
                ErrorKind::Timeout,
            );

            let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
            buffer.set_len(4);
            let target = WriteTarget::new(BufferSet::try_new(vec![buffer]).unwrap());
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

        let (request_id, _rx) = ctx.state.waiter.alloc(Duration::from_secs(1));
        let pending = RequestStatusRequest { request_id };
        assert!(().request_is_pending(&ctx, &pending).await.unwrap());
    }
}
