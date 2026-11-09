//! Safe source references, destination adapters and shared transfer boundaries.
use super::{Recovery, transfer};
use crate::{Buffer, Completed, Context, CopyOp, Error, ErrorKind, RemoteSpace, Result};
use ruapc_bufpool::WriteBuffer;

impl Context {
    /// The concatenated immutable source space attached to this request.
    pub fn remote_read_space(&self) -> Result<RemoteSpace<'_>> {
        if self.msg_meta.read_regions.is_empty() {
            return Err(Error::new(
                ErrorKind::MissingBufferInfo,
                "request carries no read regions".into(),
            ));
        }
        Ok(RemoteSpace::new(&self.msg_meta.read_regions)?)
    }

    /// The concatenated write destination space attached to this request.
    pub fn remote_write_space(&self) -> Result<RemoteSpace<'_>> {
        if self.msg_meta.write_regions.is_empty() {
            return Err(Error::new(
                ErrorKind::MissingBufferInfo,
                "request carries no write regions".into(),
            ));
        }
        Ok(RemoteSpace::new(&self.msg_meta.write_regions)?)
    }

    /// Allocates destinations and reads the complete remote source space.
    /// Failed reads may leave destinations held by the runtime for background
    /// settlement after the configured recovery grace.
    pub async fn remote_read_allocated(&self) -> Result<Vec<Buffer>> {
        transfer::read_allocated(self).await
    }

    /// Moves local destinations into the common READ executor.
    /// Both result branches return original buffers when locally settled. If
    /// recovery exceeds its grace, `buffers` is `None` and the runtime retains
    /// outstanding destinations until DMA settles.
    /// Cancellation retains posted read destinations until local settlement.
    pub fn own_buffers(&self, buffers: Vec<Buffer>) -> RemoteBuffers<'_> {
        RemoteBuffers { ctx: self, buffers }
    }

    /// Established storage boundary for the separate unsafe borrowing facade.
    /// Owned capabilities retain their destination allocations through local
    /// DMA settlement. Borrowed capabilities still require their unsafe
    /// constructor's lifetime and exclusive-access guarantees; this bridge adds
    /// no allocation keepalive and does not make their cancellation safe.
    /// `None` operations select the complete remote source space.
    #[doc(hidden)]
    pub async fn read_buffer_storage(
        &self,
        ops: Option<&[CopyOp]>,
        buffers: Vec<WriteBuffer>,
    ) -> Completed<Result<()>, Vec<WriteBuffer>> {
        transfer::read(self, ops, buffers, Recovery::Wait).await
    }

    /// Writes from borrowed sources into the client's advertised destinations.
    ///
    /// The original handles stay in place. Stream transports copy the requested
    /// bytes before awaiting I/O; RDMA exports metadata without publishing local
    /// CPU access. Cancelling or forgetting the future is memory-safe. A normal
    /// successful return acknowledges the destination transfer; failure or
    /// cancellation does not prove that remote READs have finished.
    pub async fn remote_write(&self, ops: &[CopyOp], local: &[Buffer]) -> Result<()> {
        transfer::write(self, Some(ops), local).await
    }

    /// Writes the entire borrowed source space to the remote destination prefix.
    pub async fn remote_write_all(&self, local: &[Buffer]) -> Result<()> {
        transfer::write(self, None, local).await
    }
}

/// Owned remote READ destinations using the common executor.
///
/// Cancellation retains local READ destinations through settlement.
#[must_use]
pub struct RemoteBuffers<'a> {
    ctx: &'a Context,
    buffers: Vec<Buffer>,
}
impl RemoteBuffers<'_> {
    /// Reads into owned destinations. Success returns the originals; failure
    /// returns them if locally settled within the recovery grace, otherwise
    /// returns `buffers: None` while the runtime continues holding them.
    /// Dropping the future does not release destinations still used by local DMA.
    pub async fn remote_read(self, ops: &[CopyOp]) -> Completed<Result<()>, Vec<Buffer>> {
        self.read(Some(ops)).await
    }

    /// Reads the whole remote source space into owned destinations.
    pub async fn remote_read_all(self) -> Completed<Result<()>, Vec<Buffer>> {
        self.read(None).await
    }

    async fn read(self, ops: Option<&[CopyOp]>) -> Completed<Result<()>, Vec<Buffer>> {
        let completed = transfer::read(
            self.ctx,
            ops,
            self.buffers.into_iter().map(WriteBuffer::from).collect(),
            Recovery::DetachOwned,
        )
        .await;
        Completed {
            result: completed.result,
            buffers: completed.buffers.map(|buffers| {
                buffers
                    .into_iter()
                    .map(|b| b.into_owned().expect("owned destinations"))
                    .collect()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "rdma")]
    use ruapc_memory::{BufferSet, CopyBatch};
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use ruapc_bufpool::RemoteBufferInfo;
    use serde::Serialize;

    #[cfg(feature = "rdma")]
    use crate::remote_memory::{MemoryService, WriteTarget, rpc_transfer::WriteInlineRequest};
    use crate::{
        Error, ErrorKind, MAX_COPY_OPS, Message, MsgFlags, MsgMeta, Payload, Socket,
        SocketPoolConfig, msg::frame, tcp::TcpSocket,
    };

    #[derive(Serialize)]
    struct PackedReadReply {
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    }

    #[tokio::test]
    async fn test_send_rsp_invalid_endpoint_logs_and_does_not_panic() {
        // Context starts without a destination by default.
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        // This should log an error and silently return (no panic).
        ctx.send_err_rsp(Error::kind(ErrorKind::Timeout)).await;
    }

    fn ctx_with_read_regions(regions: Vec<RemoteBufferInfo>) -> Context {
        let (ctx, _) = connected_context(MsgMeta {
            read_regions: regions.into(),
            ..Default::default()
        });
        ctx
    }

    fn connected_context(meta: MsgMeta) -> (Context, tokio::sync::mpsc::Receiver<Bytes>) {
        let config = SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        };
        let base = Context::create(&config).unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        (
            Context::server_ctx(&base.state, Socket::TCP(TcpSocket::new(sender)), meta),
            receiver,
        )
    }

    fn region(len: u64) -> RemoteBufferInfo {
        RemoteBufferInfo {
            key: ruapc_bufpool::MemoryKey { lkey: 0, rkey: 0 },
            addr: 0x1000,
            len,
        }
    }

    #[tokio::test]
    async fn test_remote_read_missing_regions_recovers_buffers() {
        let ctx = ctx_with_read_regions(vec![]);
        let local = vec![ctx.state.buffer_pool.allocate(1024 * 1024).unwrap()];
        let pointer = local[0].as_ptr();
        let len = local[0].len();
        let Completed {
            result,
            buffers: local,
        } = ctx
            .own_buffers(local)
            .remote_read(&[CopyOp::new(0, 0, 1)])
            .await;
        let local = local.unwrap();
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingBufferInfo);
        assert_eq!(local[0].as_ptr(), pointer);
        assert_eq!(local[0].len(), len);
    }

    #[tokio::test]
    async fn test_remote_read_invalid_endpoint_returns_err() {
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        ctx.msg_meta.read_regions = [region(8)].into();
        let mut local = vec![ctx.state.buffer_pool.allocate(1024 * 1024).unwrap()];
        local[0].set_len(8);
        local[0].copy_from_slice(b"original");
        let pointer = local[0].as_ptr();
        let Completed {
            result,
            buffers: local,
        } = ctx
            .own_buffers(local)
            .remote_read(&[CopyOp::new(0, 0, 8)])
            .await;
        let local = local.unwrap();
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::NotConnected);
        assert_eq!(local[0].as_ptr(), pointer);
        assert_eq!(local[0].capacity(), 1024 * 1024);
        assert_eq!(local[0].as_slice(), b"original");
    }

    #[tokio::test]
    async fn test_remote_read_rejects_out_of_bounds_ops() {
        let ctx = ctx_with_read_regions(vec![region(8), region(8)]);
        let space = ctx.remote_read_space().unwrap();
        assert_eq!(space.total_len(), 16);
        assert_eq!(space.region_count(), 2);
        let mut local = vec![ctx.state.buffer_pool.allocate(1024 * 1024).unwrap()];
        local[0].set_len(16);
        local[0].fill(0x5a);
        let pointer = local[0].as_ptr();
        for ops in [
            vec![CopyOp::new(9, 0, 8)],
            vec![CopyOp::new(0, 0, 8), CopyOp::new(8, 4, 8)],
        ] {
            let completed = ctx.own_buffers(local).remote_read(&ops).await;
            local = completed.buffers.unwrap();
            let error = completed.result.unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
            assert_eq!(local[0].as_ptr(), pointer);
            assert_eq!(local[0].as_slice(), &[0x5a; 16]);
        }
    }

    #[tokio::test]
    async fn zero_byte_transfers_validate_context_and_batch_without_sending() {
        let disconnected = Context::create(&SocketPoolConfig::default()).unwrap();
        let (missing, mut missing_queue) = connected_context(MsgMeta::default());
        let (connected, mut queue) = connected_context(MsgMeta {
            read_regions: [region(8)].into(),
            write_regions: [region(8)].into(),
            ..Default::default()
        });
        for ops in [vec![], vec![CopyOp::new(u64::MAX, u64::MAX, 0)]] {
            for (ctx, expected) in [
                (&disconnected, ErrorKind::NotConnected),
                (&missing, ErrorKind::MissingBufferInfo),
            ] {
                assert_eq!(
                    ctx.own_buffers(vec![])
                        .remote_read(&ops)
                        .await
                        .result
                        .unwrap_err()
                        .kind,
                    expected
                );
                assert_eq!(
                    ctx.remote_write(&ops, &[]).await.unwrap_err().kind,
                    expected
                );
            }
            connected
                .own_buffers(vec![])
                .remote_read(&ops)
                .await
                .result
                .unwrap();
            connected.remote_write(&ops, &[]).await.unwrap();
        }
        let ops = vec![CopyOp::new(0, 0, 0); MAX_COPY_OPS + 1];
        for write in [false, true] {
            let mut local = vec![connected.state.buffer_pool.allocate(4).unwrap()];
            local[0].set_len(4);
            local[0].copy_from_slice(b"keep");
            let pointer = local[0].as_ptr();
            let (result, local) = if write {
                (connected.remote_write(&ops, &local).await, local)
            } else {
                let completed = connected.own_buffers(local).remote_read(&ops).await;
                (completed.result, completed.buffers.unwrap())
            };
            let error = result.unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
            assert_eq!(local[0].as_ptr(), pointer);
            assert_eq!(local[0].as_slice(), b"keep");
        }
        assert!(queue.try_recv().is_err());
        assert!(missing_queue.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_remote_write_invalid_endpoint_returns_err() {
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        ctx.msg_meta.write_regions = [region(1024)].into();
        let mut local = vec![ctx.state.buffer_pool.allocate(1024 * 1024).unwrap()];
        local[0].set_len(1024);
        local[0].fill(0x5a);
        let pointer = local[0].as_ptr();
        let result = ctx.remote_write(&[CopyOp::new(0, 0, 1024)], &local).await;
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::NotConnected);
        assert_eq!(local[0].as_ptr(), pointer);
        assert_eq!(local[0].len(), 1024);
        assert!(local[0].iter().all(|&byte| byte == 0x5a));
    }

    #[tokio::test]
    async fn test_remote_write_zero_total_short_circuits() {
        let (ctx, mut queue) = connected_context(MsgMeta {
            write_regions: [region(0)].into(),
            ..Default::default()
        });
        ctx.remote_write_all(&[]).await.unwrap();
        assert!(queue.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_remote_spaces_missing_return_err() {
        let ctx = ctx_with_read_regions(vec![]);
        assert_eq!(
            ctx.remote_read_space().unwrap_err().kind,
            ErrorKind::MissingBufferInfo
        );
        assert_eq!(
            ctx.remote_write_space().unwrap_err().kind,
            ErrorKind::MissingBufferInfo
        );
        // remote_read_all surfaces the same error.
        let err = ctx.remote_read_allocated().await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::MissingBufferInfo);
    }

    #[tokio::test]
    async fn inline_read_bad_packed_replies_and_rpc_errors_preserve_destinations() {
        for (reply, expected) in [
            (Ok(vec![0x5a; 3]), ErrorKind::InvalidCopyOp),
            (Ok(vec![0x5a; 5]), ErrorKind::InvalidCopyOp),
            (
                Err(Error::kind(ErrorKind::Overloaded)),
                ErrorKind::Overloaded,
            ),
        ] {
            let base = Context::create(&SocketPoolConfig::default()).unwrap();
            let (sender, mut queue) = tokio::sync::mpsc::channel(1);
            let ctx = Context::server_ctx(
                &base.state,
                Socket::TCP(TcpSocket::new(sender)),
                MsgMeta {
                    msgid: 123,
                    read_regions: [region(4)].into(),
                    timeout_ms: 5_000,
                    ..Default::default()
                },
            );
            let mut local = ctx.state.buffer_pool.allocate(8).unwrap();
            local.set_len(8);
            local.copy_from_slice(b"........");
            let original_pointer = local.as_ptr();
            let ops = [CopyOp::new(2, 4, 2), CopyOp::new(0, 0, 2)];
            let peer = async {
                let encoded = queue.recv().await.unwrap();
                let mut encoded = BytesMut::from(encoded.as_ref());
                let request =
                    Message::parse(frame::parse_message(&mut encoded).unwrap().unwrap()).unwrap();
                assert_eq!(request.meta.method, "MemoryService/read_inline");
                let response = reply.map(|bytes| PackedReadReply { bytes });
                ctx.state.waiter.post(
                    request.meta.msgid,
                    Message::new(
                        MsgMeta {
                            msgid: request.meta.msgid,
                            flags: MsgFlags::UseMessagePack | MsgFlags::IsRsp,
                            ..Default::default()
                        },
                        Payload::from(Bytes::from(rmp_serde::to_vec_named(&response).unwrap())),
                    ),
                );
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(ctx.own_buffers(vec![local]).remote_read(&ops), peer)
            })
            .await
            .unwrap();
            let Completed { result, buffers } = result;
            let buffers = buffers.unwrap();
            let local = &buffers[0];
            let error = result.unwrap_err();
            assert_eq!(error.kind, expected);
            assert_eq!(local.as_ptr(), original_pointer);
            assert_eq!(local.as_slice(), b"........");
        }
    }

    fn local_buffers(ctx: &Context, bytes: &[u8]) -> Vec<Buffer> {
        let mut buffer = ctx.state.buffer_pool.allocate(bytes.len().max(1)).unwrap();
        buffer.set_len(bytes.len());
        buffer.copy_from_slice(bytes);
        vec![buffer]
    }

    fn identity(buffers: &[Buffer]) -> Vec<(usize, usize, usize)> {
        buffers
            .iter()
            .map(|buffer| (buffer.as_ptr() as usize, buffer.len(), buffer.capacity()))
            .collect()
    }

    fn post_inline_reply(ctx: &Context, msgid: u64, encoded: Vec<u8>) {
        ctx.state.waiter.post(
            msgid,
            Message::new(
                MsgMeta {
                    msgid,
                    flags: MsgFlags::UseMessagePack | MsgFlags::IsRsp,
                    ..Default::default()
                },
                Payload::from(Bytes::from(encoded)),
            ),
        );
    }

    #[tokio::test]
    async fn transfers_preserve_original_buffers_on_validation_errors() {
        let (ctx, mut queue) = connected_context(MsgMeta {
            read_regions: [region(4)].into(),
            write_regions: [region(4)].into(),
            ..Default::default()
        });
        for operation in 0..4 {
            let len = match operation {
                2 => 3,
                3 => 5,
                _ => 4,
            };
            let local = local_buffers(&ctx, &vec![0x5a; len]);
            let original = identity(&local);
            let ops = [CopyOp::new(4, 0, 1)];
            let (result, buffers) = match operation {
                0 | 2 => {
                    let owned = ctx.own_buffers(local);
                    let completed = if operation == 0 {
                        owned.remote_read(&ops).await
                    } else {
                        owned.remote_read_all().await
                    };
                    (completed.result, completed.buffers.unwrap())
                }
                1 => (ctx.remote_write(&ops, &local).await, local),
                _ => (ctx.remote_write_all(&local).await, local),
            };
            assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidCopyOp);
            assert_eq!(identity(&buffers), original);
            assert_eq!(buffers[0].as_slice(), vec![0x5a; len]);
        }
        assert!(
            queue.try_recv().is_err(),
            "invalid plans cannot send reverse RPCs"
        );
    }

    #[tokio::test]
    async fn borrowed_sources_and_owned_destinations_use_the_inline_copy_engine() {
        use crate::remote_memory::rpc_transfer::{ReadInlineRequest, WriteInlineRequest};

        for operation in 0..4 {
            let (ctx, mut queue) = connected_context(MsgMeta {
                msgid: 123,
                read_regions: [region(4)].into(),
                write_regions: [region(8)].into(),
                timeout_ms: 5_000,
                ..Default::default()
            });
            let write = operation >= 2;
            let all = operation % 2 != 0;
            let initial: &[u8] = if write {
                b"Rua!"
            } else if all {
                b"......"
            } else {
                b"...."
            };
            let local = local_buffers(&ctx, initial);
            let original = identity(&local);
            let ops = [CopyOp::new(1, 2, 2)];
            let expected_ops = if all {
                vec![CopyOp::new(0, 0, 4)]
            } else {
                ops.to_vec()
            };
            let expected_bytes: &[u8] = if all { b"Rua!" } else { b"ua" };
            let peer = async {
                let encoded = queue.recv().await.unwrap();
                let mut encoded = BytesMut::from(encoded.as_ref());
                let request =
                    Message::parse(frame::parse_message(&mut encoded).unwrap().unwrap()).unwrap();
                let reply = if write {
                    assert_eq!(request.meta.method, "MemoryService/write_inline");
                    let body: WriteInlineRequest =
                        request.payload.deserialize(&request.meta).unwrap();
                    assert_eq!(body.request_id, 123);
                    assert_eq!(body.ops, expected_ops);
                    assert_eq!(body.bytes, expected_bytes);
                    rmp_serde::to_vec_named(&Ok::<(), Error>(())).unwrap()
                } else {
                    assert_eq!(request.meta.method, "MemoryService/read_inline");
                    let body: ReadInlineRequest =
                        request.payload.deserialize(&request.meta).unwrap();
                    assert_eq!(body.request_id, 123);
                    assert_eq!(body.ops, expected_ops);
                    rmp_serde::to_vec_named(&Ok::<_, Error>(PackedReadReply {
                        bytes: expected_bytes.to_vec(),
                    }))
                    .unwrap()
                };
                post_inline_reply(&ctx, request.meta.msgid, reply);
            };
            let transfer = async {
                match operation {
                    0 | 1 => {
                        let owned = ctx.own_buffers(local);
                        let completed = if operation == 0 {
                            owned.remote_read(&ops).await
                        } else {
                            owned.remote_read_all().await
                        };
                        (completed.result, completed.buffers.unwrap())
                    }
                    2 => (ctx.remote_write(&ops, &local).await, local),
                    _ => (ctx.remote_write_all(&local).await, local),
                }
            };
            let ((result, buffers), ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(transfer, peer)
            })
            .await
            .unwrap();
            result.unwrap();
            assert_eq!(identity(&buffers), original);
            let expected: &[u8] = if write {
                b"Rua!"
            } else if all {
                b"Rua!.."
            } else {
                b"..ua"
            };
            assert_eq!(buffers[0].as_slice(), expected);

            assert_eq!(ctx.state.waiter.pending_count(), 0);
        }
    }

    #[tokio::test]
    async fn dropped_or_forgotten_writes_leave_only_owned_payloads() {
        use crate::remote_memory::{
            MemoryService,
            rpc_transfer::{ReadInlineRequest, WriteInlineRequest},
        };

        for forget in [false, true] {
            let (ctx, mut queue) = connected_context(MsgMeta {
                msgid: 123,
                write_regions: [region(4)].into(),
                timeout_ms: 5_000,
                ..Default::default()
            });
            let mut source = local_buffers(&ctx, b"old!");
            let mut writing = Box::pin(ctx.remote_write_all(&source));
            assert!(futures_util::poll!(writing.as_mut()).is_pending());
            let encoded = queue.recv().await.unwrap();
            let mut encoded = BytesMut::from(encoded.as_ref());
            let request =
                Message::parse(frame::parse_message(&mut encoded).unwrap().unwrap()).unwrap();
            assert!(ctx.state.waiter.contains_message_id(request.meta.msgid));
            assert!(ctx.state.waiter.read_source(request.meta.msgid).is_none());

            if forget {
                std::mem::forget(writing);
            } else {
                drop(writing);
                assert!(!ctx.state.waiter.contains_message_id(request.meta.msgid));
            }
            // Both actions end the safe Rust borrow. There is no borrowed
            // snapshot in the waiter for another task to dereference later.
            source[0].copy_from_slice(b"new!");
            drop(source);
            let body: WriteInlineRequest = request.payload.deserialize(&request.meta).unwrap();
            assert_eq!(body.bytes, b"old!");
            let read = ReadInlineRequest {
                request_id: request.meta.msgid,
                ops: vec![CopyOp::new(0, 0, 4)],
            };
            assert_eq!(
                ().read_inline(&ctx, &read).await.unwrap_err().kind,
                ErrorKind::Timeout
            );
            ctx.state
                .waiter
                .expire(std::time::Instant::now() + Duration::from_secs(60));
            assert_eq!(ctx.state.waiter.pending_count(), 0);
        }
    }

    #[tokio::test]
    async fn allocated_read_preserves_nonempty_region_lengths_and_skips_empty_regions() {
        use crate::remote_memory::rpc_transfer::ReadInlineRequest;

        let (ctx, mut queue) = connected_context(MsgMeta {
            msgid: 321,
            read_regions: [region(2), region(0), region(3)].into(),
            timeout_ms: 5_000,
            ..Default::default()
        });
        let peer = async {
            let encoded = queue.recv().await.unwrap();
            let mut encoded = BytesMut::from(encoded.as_ref());
            let request =
                Message::parse(frame::parse_message(&mut encoded).unwrap().unwrap()).unwrap();
            assert_eq!(request.meta.method, "MemoryService/read_inline");
            let body: ReadInlineRequest = request.payload.deserialize(&request.meta).unwrap();
            assert_eq!(body.request_id, 321);
            assert_eq!(body.ops, [CopyOp::new(0, 0, 5)]);
            post_inline_reply(
                &ctx,
                request.meta.msgid,
                rmp_serde::to_vec_named(&Ok::<_, Error>(PackedReadReply {
                    bytes: b"abcde".to_vec(),
                }))
                .unwrap(),
            );
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(ctx.remote_read_allocated(), peer)
        })
        .await
        .unwrap();
        let buffers = result.unwrap();
        assert_eq!(buffers.iter().map(Buffer::len).collect::<Vec<_>>(), [2, 3]);
        assert_eq!(buffers[0].as_slice(), b"ab");
        assert_eq!(buffers[1].as_slice(), b"cde");
        assert!(queue.try_recv().is_err());

        let (empty, mut empty_queue) = connected_context(MsgMeta {
            read_regions: [region(0), region(0)].into(),
            ..Default::default()
        });
        assert!(empty.remote_read_allocated().await.unwrap().is_empty());
        assert!(empty_queue.try_recv().is_err());
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn inline_write_checks_request_before_busy_and_keeps_empty_batch_semantics() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"keep");
        let target = WriteTarget::from_owned(BufferSet::try_new(vec![buffer]).unwrap());
        let (request_id, _receiver) = ctx.state.waiter.alloc(Duration::from_secs(5));
        ctx.state
            .waiter
            .bind_write_target(request_id, target.clone());
        let ((), checked_out) = target.checkout(|_| Ok(())).unwrap();

        let empty = CopyBatch::new(&[]).unwrap();
        target.copy_inline(&empty, &[]).unwrap();
        let zero_ops = [CopyOp::new(u64::MAX, u64::MAX, 0)];
        let zeros = CopyBatch::new(&zero_ops).unwrap();
        target.copy_inline(&zeros, &[]).unwrap();

        for (ops, bytes, expected) in [
            (
                vec![CopyOp::new(0, 0, 1)],
                vec![b'x'],
                ErrorKind::BuffersInUse,
            ),
            (vec![CopyOp::new(0, 0, 1)], vec![], ErrorKind::InvalidCopyOp),
            (
                vec![CopyOp::new(0, 4, 1)],
                vec![b'x'],
                ErrorKind::InvalidCopyOp,
            ),
        ] {
            let request = WriteInlineRequest {
                request_id,
                ops,
                bytes,
            };
            assert_eq!(
                ().write_inline(&ctx, &request).await.unwrap_err().kind,
                expected
            );
        }
        assert_eq!(checked_out.buffers()[0].as_slice(), b"keep");
        target.restore(checked_out);
        target.copy_inline(&zeros, &[]).unwrap();
    }
}

#[cfg(all(test, feature = "rdma"))]
mod timeout_tests {
    //! Exercise public remote-memory calls with a live connection whose peer
    //! consumes reverse RPCs without replying. Observe the server's API return
    //! independently of the original caller's timeout.

    use std::{sync::Arc, time::Duration};

    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc;

    use crate::rdma::test_utils::{small_rdma_config, transport_endpoint as endpoint};
    use crate::{
        Buffer, Client, Context, ErrorKind, ListenMode, Result, Router, Server, Socket,
        SocketPoolConfig, State, Transport, core::ContextEndpoint, http::HttpSocket, service,
    };

    const BYTES: usize = 64;
    const GUARD: Duration = Duration::from_secs(5);
    const METHODS: [&str; 4] = [
        "MemoryService/read_inline",
        "MemoryService/write_inline",
        "MemoryService/request_is_pending",
        "MemoryService/read_into_target",
    ];

    #[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
    enum Operation {
        Read,
        Write,
    }

    #[service]
    trait TimeoutRpc {
        async fn ping(&self, ctx: &Context, req: &()) -> Result<u64>;
        async fn transfer(&self, ctx: &Context, operation: &Operation) -> Result<()>;
    }

    struct Report {
        error: Option<ErrorKind>,
        parent_expired: bool,
        conn_id: u64,
    }

    struct Service {
        reports: mpsc::UnboundedSender<Report>,
        transport: Transport,
    }

    impl TimeoutRpc for Service {
        async fn ping(&self, ctx: &Context, _: &()) -> Result<u64> {
            Ok(connection_id(ctx))
        }

        async fn transfer(&self, ctx: &Context, operation: &Operation) -> Result<()> {
            if self.transport == Transport::HTTP {
                assert!(matches!(
                    ctx.endpoint,
                    ContextEndpoint::Connected(Socket::HTTP(HttpSocket::Stream(_)))
                ));
            }
            let result = match operation {
                Operation::Read => ctx.remote_read_allocated().await.map(|_| ()),
                Operation::Write => ctx.remote_write_all(&[buffer(&ctx.state)]).await,
            };
            self.reports
                .send(Report {
                    error: result.as_ref().err().map(|error| error.kind.clone()),
                    parent_expired: ctx.is_expired(),
                    conn_id: connection_id(ctx),
                })
                .unwrap();
            result
        }
    }

    fn connection_id(ctx: &Context) -> u64 {
        let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
            panic!("test requires a connected handler context");
        };
        socket
            .conn_id()
            .expect("test requires a bidirectional stream")
    }

    fn buffer(state: &State) -> Buffer {
        let mut buffer = state.buffer_pool.allocate(BYTES).unwrap();
        buffer.set_len(BYTES);
        buffer.fill(0x5a);
        buffer
    }

    struct Fixture {
        server: Server,
        client: Context,
        transport: Transport,
        reports: mpsc::UnboundedReceiver<Report>,
        dropped: mpsc::UnboundedReceiver<(&'static str, u32)>,
    }

    impl Fixture {
        async fn new(transport: Transport) -> Self {
            let config = if transport == Transport::RDMA {
                let mut config = small_rdma_config();
                let rdma = config.rdma.as_mut().unwrap();
                rdma.remote_memory.read_timeout_ms = 0;
                rdma.remote_memory.bandwidth_limit_ratio = 0.0;
                // The fixture also disables preconnect, maintenance and probes:
                // only request/response deadlines may resolve the injected fault.
                config
            } else {
                SocketPoolConfig {
                    listen_mode: ListenMode::UNIFIED,
                    buffer_pool_memory: 128 << 20,
                    rdma: None,
                    ..Default::default()
                }
            };
            let (reports_tx, reports) = mpsc::unbounded_channel();
            let mut router = Router::default();
            TimeoutRpc::ruapc_export(
                Arc::new(Service {
                    reports: reports_tx,
                    transport,
                }),
                &mut router,
            );
            let server = Server::create(router, &config).unwrap();
            let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
            let (dropped_tx, dropped) = mpsc::unbounded_channel();
            let mut router = Router::default();
            for method in METHODS {
                let dropped = dropped_tx.clone();
                router.wrap_method(method, move |_| {
                    Box::new(move |ctx, _| {
                        dropped.send((method, ctx.msg_meta.timeout_ms)).unwrap();
                        Ok(())
                    })
                });
            }
            let client = Context::create_with_router(router, &config)
                .unwrap()
                .with_endpoint(endpoint(transport, address));
            Client {
                timeout: GUARD,
                ..Default::default()
            }
            .ping(&client, &())
            .await
            .unwrap();
            Self {
                server,
                client,
                transport,
                reports,
                dropped,
            }
        }

        async fn check(&mut self, operation: Operation, parent_timeout: Duration) {
            let client = Client {
                timeout: parent_timeout,
                max_retries: 0,
                ..Default::default()
            };
            let (read, write) = match operation {
                Operation::Read => (vec![buffer(&self.client.state)], Vec::new()),
                Operation::Write => (Vec::new(), vec![buffer(&self.client.state)]),
            };
            let identity = read
                .iter()
                .chain(&write)
                .map(|b| (b.as_ptr(), b.len(), b.capacity()))
                .collect::<Vec<_>>();
            let attached = client.with_buffers(&read, write);
            // Both observations must settle independently; the owning adapter
            // returns its original attachments even when the RPC times out.
            let (result, report) = tokio::time::timeout(GUARD, async {
                tokio::join!(
                    attached.run(async |rpc| rpc.transfer(&self.client, &operation).await),
                    self.reports.recv(),
                )
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{:?} {operation:?}: server API stayed pending",
                    self.transport
                )
            });
            assert_eq!(result.result.unwrap_err().kind, ErrorKind::Timeout);
            let report = report.expect("server must independently report its remote I/O result");
            assert_eq!(report.error, Some(ErrorKind::Timeout));
            let default_timeout = Client::default().timeout;
            assert_eq!(report.parent_expired, parent_timeout < default_timeout);
            let (method, wire_budget) = self.dropped.try_recv().unwrap();
            let expected_method = match (self.transport, operation) {
                (Transport::RDMA, Operation::Read) => METHODS[2],
                (Transport::RDMA, Operation::Write) => METHODS[3],
                (_, Operation::Read) => METHODS[0],
                (_, Operation::Write) => METHODS[1],
            };
            assert_eq!(method, expected_method);
            assert!(wire_budget > 0);
            assert!(u128::from(wire_budget) <= parent_timeout.min(default_timeout).as_millis());
            assert!(
                self.dropped.try_recv().is_err(),
                "accepted RPCs must not retry"
            );
            assert_eq!(self.server.state().waiter.pending_count(), 0);
            assert_eq!(self.client.state.waiter.pending_count(), 0);
            let original = match operation {
                Operation::Read => &read,
                Operation::Write => result.buffers.as_ref().unwrap(),
            };
            assert_eq!(original.len(), 1);
            assert_eq!(
                (
                    original[0].as_ptr(),
                    original[0].len(),
                    original[0].capacity()
                ),
                identity[0]
            );
            assert!(original[0].iter().all(|&byte| byte == 0x5a));
            // A request deadline must not tear down the healthy connection.
            assert_eq!(
                Client::default().ping(&self.client, &()).await.unwrap(),
                report.conn_id,
                "request expiry must preserve the same connection"
            );
        }

        async fn shutdown(self) {
            self.server.stop();
            self.client.state.socket_pool.stop();
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(self.server.join(), self.client.state.socket_pool.join());
            })
            .await
            .expect("test connections must shut down");
        }
    }

    async fn check_transports(parent_timeout: Duration) {
        for transport in [
            Transport::TCP,
            Transport::WS,
            Transport::HTTP,
            Transport::RDMA,
        ] {
            let mut fixture = Fixture::new(transport).await;
            for operation in [Operation::Read, Operation::Write] {
                fixture.check(operation, parent_timeout).await;
            }
            fixture.shutdown().await;
        }
    }

    #[tokio::test]
    async fn parent_deadline_returns_from_remote_io_when_reverse_replies_are_lost() {
        check_transports(Duration::from_millis(250)).await;
    }

    #[tokio::test]
    async fn reverse_rpc_timeout_returns_from_remote_io_before_parent_deadline() {
        check_transports(Duration::from_secs(10)).await;
    }
}
