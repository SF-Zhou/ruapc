//! Reverse-RPC requests, handlers and transport execution for remote memory.

use std::{sync::Arc, time::Instant};

use ruapc_memory::{CopyBatch, MAX_COPY_OPS, RangeCursor, RemoteSpace};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    BufferAvailability, ReadSourceSender, WriteTarget, WriteTargetOwner,
    data::{self, LentSources, request_gone},
    transfer::{PreparedRead, PreparedWrite},
};
#[cfg(feature = "rdma")]
use crate::Socket;
use crate::msg::frame::{MAX_DATA_LEN, deserialize_ranges};
use crate::{
    Buffer, Client, Context, CopyOp, Error, ErrorKind, RemoteIoError, Result,
    client::ReadAttachment, core::ContextEndpoint,
};

/// Wire name of [`MemoryService::read_inline`].
const READ_INLINE: &str = "MemoryService/read_inline";

/// Request to read byte ranges of the client's read space (TCP/WS/HTTP
/// fallback of `Context::remote_read`).
///
/// The original borrowing future supplies these ranges in order. The reader
/// alone holds the destination plan and scatters the packed response frames.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ReadInlineRequest {
    /// Ordered (offset, length) pairs in the original request's read space.
    #[serde(deserialize_with = "deserialize_ranges::<MAX_COPY_OPS, _>")]
    #[schemars(with = "Vec<[u64; 2]>", length(max = MAX_COPY_OPS))]
    pub(crate) ranges: Vec<(u64, u64)>,
    /// Message ID of the original request. Used to verify the request
    /// still has an attached read source.
    pub(crate) request_id: u64,
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

/// Reverse RPCs for inline reads, client-side RDMA READs and post-READ source
/// validation. Stream writes need none: they are plain data frames.
///
/// Every method names an original request by its ID, and reaches that
/// request's memory only when called on the connection carrying it. Request
/// IDs are sequential: an ID alone must not let another peer of this process
/// read a source or write a destination.
#[ruapc_macro::service]
pub(crate) trait MemoryService {
    /// Asks the original request future to copy its borrowed source, and
    /// returns those bytes in packed frames (TCP/WS/HTTP).
    /// A successful response follows the last data frame on the connection.
    async fn read_inline(&self, ctx: &Context, req: &ReadInlineRequest) -> Result<()>;

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
    async fn read_inline(&self, ctx: &Context, req: &ReadInlineRequest) -> Result<()> {
        let (source, deadline) = read_source(ctx, req.request_id, "read_inline")?;
        // Only the request's own connection gets this far.
        let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
            return Err(Error::new(
                ErrorKind::NotConnected,
                "read_inline requires a connected socket".into(),
            ));
        };
        let channel = data::data_channel(socket)?;
        let mut ranges = RangeCursor::from_slice(&req.ranges, source.source_len())?;
        while ranges.remaining() > 0 {
            let piece = ranges.split_to(MAX_DATA_LEN as u64);
            let len = piece.remaining() as usize;
            let head = data::packed_head(ctx.msg_meta.msgid, len)?;
            let reservation = channel.reserve_data(head.len() + len, deadline).await?;
            reservation
                .send_gathered(|permit| source.gather_reserved(piece, head, deadline, permit))
                .await?;
            ctx.state.metrics.data_frame_sent(len);
        }
        // This handler's response is queued after its frames, so the server
        // has applied every one of them when it sees the response.
        Ok(())
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
                    "read_into_target requires RDMA (other transports push data frames)".into(),
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
///
/// Only the reverse call owns the destination plan. It seals the target after
/// the ordered packed frames and their response, or on cancellation.
pub(crate) async fn read(
    prepared: PreparedRead<'_, '_>,
) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
    let transfer = prepared.into_parts();
    let request = ReadInlineRequest {
        ranges: transfer
            .ops
            .ops()
            .iter()
            .filter(|op| op.len != 0)
            .map(|op| (op.src_offset, op.len))
            .collect(),
        request_id: transfer.ctx.msg_meta.msgid,
    };
    let target = WriteTargetOwner::new(WriteTarget::expecting(transfer.local, &transfer.ops));
    let result = Client::default()
        .ruapc_request::<_, (), Error>(
            transfer.ctx,
            &request,
            ReadAttachment::new(None, None),
            Some(&target),
            READ_INLINE,
        )
        .await;
    // A stream connection never lends the set to a READ: sealing returns it.
    let buffers = target.seal_now();
    // A successful reply requires every byte of the local scatter plan.
    let result = result.and_then(|()| target.delivery());
    match (result, buffers) {
        (Ok(()), Some(buffers)) => Ok(buffers),
        (Ok(()), None) => Err(RemoteIoError::new(
            Error::new(
                ErrorKind::BuffersInUse,
                "read destinations are unavailable".into(),
            ),
            None,
        )),
        (Err(error), buffers) => Err(RemoteIoError::new(error, buffers)),
    }
}

/// Consumes a prepared stream write: pushes the sources into the request's
/// write space as data frames that share the buffers, and returns once the
/// peer has confirmed the last frame.
///
/// The buffers come back when the connection has dropped every frame that
/// shares them. On success that is awaited within the transfer's deadline,
/// and a set that is still shared then is reported unavailable. A failed
/// transfer does not wait: frames it queued can outlive it.
pub(crate) async fn write(
    prepared: PreparedWrite<'_, '_>,
) -> std::result::Result<BufferAvailability, RemoteIoError> {
    let transfer = prepared.into_parts();
    // The budget of a reverse call made with a default client.
    let deadline = Client::default().response_deadline(transfer.ctx);
    let layout = transfer.local.layout().clone();
    let (lent, sources) = LentSources::lend(transfer.local.into_buffers());
    let pushed = data::push(
        transfer.ctx,
        transfer.socket,
        &transfer.ops,
        &layout,
        &sources,
        deadline,
    )
    .await;
    drop(sources);
    match pushed {
        Ok(()) => Ok(match lent.recover(Some(deadline)).await {
            Some(buffers) => BufferAvailability::Available(buffers),
            None => BufferAvailability::Unavailable,
        }),
        Err(error) => Err(RemoteIoError::new(error, lent.recover(None).await)),
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
        MAX_COPY_OPS, MsgMeta, Socket, SocketPoolConfig,
        msg::frame::{DataPlacement, MAX_DATA_LEN},
        remote_memory::serve_read_source,
        sockets::{Outbound, test_util::Sent},
        task::MemoryRoutes,
        tcp::TcpSocket,
    };

    /// Stream writes use data frames: the directory offers
    /// the inline read under the name its caller sends, and no inline write.
    #[test]
    fn inline_read_is_registered_under_its_wire_name() {
        let router = crate::Router::default();
        assert!(router.has_method(READ_INLINE));
        assert!(!router.has_method("MemoryService/write_inline"));
        // The request names operations only; no bytes travel in a payload.
        let request = ReadInlineRequest {
            request_id: 1,
            ranges: vec![(4, 4)],
        };
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({
                "request_id": 1,
                "ranges": [[4, 4]],
            })
        );
    }

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(30)
    }

    type Queue = tokio::sync::mpsc::Receiver<Outbound>;

    /// A reverse-RPC context as delivered by one stream connection of
    /// `state`, with the frames queued for that connection.
    fn arrival(state: &Arc<crate::State>) -> (Context, u64, Queue) {
        let (sender, queue) = tokio::sync::mpsc::channel(64);
        let socket = TcpSocket::new(sender);
        let conn_id = socket.conn_id();
        let ctx = Context::server_ctx(
            state,
            Socket::TCP(socket),
            MsgMeta {
                msgid: 77,
                timeout_ms: 30_000,
                ..Default::default()
            },
        );
        (ctx, conn_id, queue)
    }

    fn connected() -> (Context, u64, Queue) {
        let config = SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        };
        arrival(&Context::create(&config).unwrap().state)
    }

    #[tokio::test]
    async fn inline_read_bounds_range_deserialization_in_both_codecs() {
        use crate::{MsgFlags, msg::frame, sockets::test_util::deliver};

        let schema = schemars::schema_for!(ReadInlineRequest);
        assert_eq!(
            schema.as_value()["properties"]["ranges"]["maxItems"],
            MAX_COPY_OPS
        );

        let (ctx, conn_id, mut queue) = connected();
        let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
            unreachable!()
        };
        let source = ReadSource::new(&[]);
        let (route, _service) = serve_read_source(&source);
        let (request_id, _pending) = ctx.state.waiter.alloc_until(
            later(),
            MemoryRoutes {
                read_source: Some(route),
                ..Default::default()
            },
        );
        ctx.state.waiter.bind_connection(request_id, conn_id);

        for (codec, decode_error) in [
            (MsgFlags::UseMessagePack, ErrorKind::DeserializeFailed),
            (MsgFlags::empty(), ErrorKind::SerdeJsonError),
        ] {
            for count in [MAX_COPY_OPS, MAX_COPY_OPS + 1] {
                let request = ReadInlineRequest {
                    ranges: vec![(0, 0); count],
                    request_id,
                };
                let meta = MsgMeta {
                    method: READ_INLINE.into(),
                    flags: MsgFlags::IsReq | codec,
                    msgid: 77,
                    timeout_ms: 5_000,
                    ..Default::default()
                };
                let frame = frame::encode(&meta, &request).unwrap();
                deliver(&ctx.state, socket, frame.into(), 1);
                let response = Sent::decode(
                    tokio::time::timeout(Duration::from_secs(5), queue.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                )
                .message();
                let result = response
                    .payload
                    .deserialize::<Result<()>>(&response.meta)
                    .unwrap();
                if count == MAX_COPY_OPS {
                    result.unwrap();
                } else {
                    assert!(response.meta.flags.contains(MsgFlags::IsFrameworkError));
                    let error = result.unwrap_err();
                    assert_eq!(error.kind, decode_error);
                    assert!(error.msg.contains(&MAX_COPY_OPS.to_string()));
                }
            }
        }
    }

    fn queued_data(queue: &mut Queue, id: u64) -> Vec<Vec<u8>> {
        std::iter::from_fn(|| queue.try_recv().ok())
            .map(|frame| {
                let (head, bytes) = Sent::decode(frame).data();
                assert_eq!(head.id, id);
                assert_eq!(head.placement, DataPlacement::Packed);
                bytes.to_vec()
            })
            .collect()
    }

    fn filled(ctx: &Context, bytes: &[u8]) -> Buffer {
        let mut buffer = ctx.state.buffer_pool.allocate(bytes.len()).unwrap();
        buffer.set_len(bytes.len());
        buffer.copy_from_slice(bytes);
        buffer
    }

    #[tokio::test]
    async fn inline_read_returns_packed_bytes_without_destination_metadata() {
        let (ctx, conn_id, mut queue) = connected();
        let buffers = [filled(&ctx, b"abcd"), filled(&ctx, b"efgh")];
        let source = ReadSource::new(&buffers);
        let (route, mut service) = serve_read_source(&source);
        let (request_id, _response) = ctx.state.waiter.alloc_until(
            later(),
            MemoryRoutes {
                read_source: Some(route),
                ..Default::default()
            },
        );
        ctx.state.waiter.bind_connection(request_id, conn_id);

        // Source ranges cross the buffer boundary and may overlap.
        let request = ReadInlineRequest {
            request_id,
            ranges: vec![(2, 4), (u64::MAX, 0), (0, 8)],
        };
        service.drive(().read_inline(&ctx, &request)).await.unwrap();
        assert_eq!(
            queued_data(&mut queue, ctx.msg_meta.msgid),
            [b"cdefabcdefgh"]
        );
        for ranges in [vec![(7, 2)], vec![(0, 4), (u64::MAX, 1)]] {
            let request = ReadInlineRequest { request_id, ranges };
            let result = service.drive(().read_inline(&ctx, &request)).await;
            assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidCopyOp);
        }
        assert!(queue.try_recv().is_err());
    }

    #[tokio::test]
    async fn inline_reads_beyond_one_frame_are_split_in_operation_order() {
        let (ctx, conn_id, mut queue) = connected();
        const LEN: usize = 2 * MAX_DATA_LEN + 3;
        let pattern: Vec<u8> = (0..LEN).map(|index| (index % 251) as u8).collect();
        let buffer = filled(&ctx, &pattern);
        let source = ReadSource::new(std::slice::from_ref(&buffer));
        let (route, mut service) = serve_read_source(&source);
        let (request_id, _response) = ctx.state.waiter.alloc_until(
            later(),
            MemoryRoutes {
                read_source: Some(route),
                ..Default::default()
            },
        );
        ctx.state.waiter.bind_connection(request_id, conn_id);
        let request = ReadInlineRequest {
            request_id,
            ranges: vec![(1, 5), (0, LEN as u64)],
        };
        service.drive(().read_inline(&ctx, &request)).await.unwrap();

        let frames = queued_data(&mut queue, ctx.msg_meta.msgid);
        assert_eq!(
            frames.iter().map(Vec::len).collect::<Vec<_>>(),
            [MAX_DATA_LEN, MAX_DATA_LEN, 8]
        );
        let bytes = frames.concat();
        assert_eq!(&bytes[..5], &pattern[1..6]);
        assert!(bytes[5..] == pattern[..]);
    }

    #[tokio::test]
    async fn zero_byte_read_into_target_validates_attachments_and_connection() {
        for ops in [Vec::new(), vec![CopyOp::new(u64::MAX, u64::MAX, 0)]] {
            let (mut ctx, conn_id, _queue) = connected();
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
    async fn inline_read_waits_for_byte_capacity_before_contacting_the_source() {
        let (ctx, conn_id, mut queue) = connected();
        let buffers = [filled(&ctx, b"data")];
        let source = ReadSource::new(&buffers);
        let (route, service) = serve_read_source(&source);
        let (request_id, _pending) = ctx.state.waiter.alloc_until(
            later(),
            MemoryRoutes {
                read_source: Some(route),
                ..Default::default()
            },
        );
        ctx.state.waiter.bind_connection(request_id, conn_id);
        let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
            unreachable!()
        };
        let reserved = data::data_channel(socket)
            .unwrap()
            .reserve_data(8 << 20, later())
            .await
            .unwrap();
        let request = ReadInlineRequest {
            request_id,
            ranges: vec![(0, 4)],
        };
        let read = ().read_inline(&ctx, &request);
        tokio::pin!(read);
        assert!(futures_util::poll!(read.as_mut()).is_pending());
        // Dropping the source wakes any gather already sent to its mailbox.
        // While the connection has no capacity this read still waits for bytes.
        drop(service);
        assert!(futures_util::poll!(read.as_mut()).is_pending());
        drop(reserved);
        assert_eq!(read.await.unwrap_err().kind, ErrorKind::Timeout);
        assert!(queue.try_recv().is_err());
    }

    #[tokio::test]
    async fn invalid_inline_reads_are_rejected_without_polling_the_source() {
        let (ctx, conn_id, mut queue) = connected();
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
        for ranges in [vec![(0, 0); MAX_COPY_OPS + 1], vec![(0, 4), (3, 2)]] {
            let request = ReadInlineRequest { request_id, ranges };
            let result =
                tokio::time::timeout(Duration::from_secs(1), ().read_inline(&ctx, &request))
                    .await
                    .expect("invalid work must fail before entering the source mailbox");
            assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidCopyOp);
        }
        assert!(queue.try_recv().is_err());
    }

    #[tokio::test]
    async fn source_validation_requires_the_original_future_not_just_a_waiter() {
        let (ctx, conn_id, _queue) = connected();
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
        let (ctx, conn_id, mut queue) = connected();
        let (other, _, mut other_queue) = arrival(&ctx.state);
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
            ranges: vec![(0, 4)],
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
                assert!(other_queue.try_recv().is_err());

                // The request's own connection reaches its source.
                ().read_inline(&ctx, &read).await.unwrap();
                ().validate_read_source(&ctx, &validate).await.unwrap();
            })
            .await;
        assert_eq!(queued_data(&mut queue, ctx.msg_meta.msgid), [b"read"]);
        // Data frames check the connection the same way; see `data`.
        assert_eq!(&target.seal(None).await.unwrap()[0][..], b"keep");
    }
}
