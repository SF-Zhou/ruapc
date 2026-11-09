use super::{Client, ReadAttachment};
use crate::{
    Buffer, Context, Error, ErrorKind, Result,
    remote_memory::{Recovery, WriteTarget},
};
use ruapc_bufpool::{ReadSources, WriteBuffer};
use ruapc_memory::{BorrowedSource, BufferSet};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Buffered execution handle available only inside a builder's `run` callback.
/// Sources remain ordinary immutable references; their request future services
/// local copies without publishing source pointers to waiters. Safe sources
/// also confirm validity there; an unsafe source capability permits a direct
/// waiter-membership check under the caller's immutability contract.
/// Destinations share one recovery executor for owned and unsafe borrowed inputs.
pub struct ClientWithBuffers<'a> {
    client: &'a Client,
    read: ReadSources<'a>,
    recovery: Recovery,
    attachments: tokio::sync::Mutex<Attachments>,
}

enum Attachments {
    Ready(Vec<WriteBuffer>),
    Pending(Arc<WriteTarget>),
    Detached,
}

impl Attachments {
    async fn recover(&mut self, recovery: Recovery) -> bool {
        let Self::Pending(target) = self else {
            return matches!(self, Self::Ready(_));
        };
        let write = match target.close_and_take().await {
            Ok(buffers) => {
                buffers.expect("pending target returns its original buffers exactly once")
            }
            Err(error) => {
                assert_eq!(recovery, Recovery::DetachOwned);
                assert_eq!(error.kind, ErrorKind::BufferRecoveryTimeout);
                *self = Self::Detached;
                return false;
            }
        };
        *self = Self::Ready(write);
        true
    }

    fn start(&mut self, recovery: Recovery) -> Result<Arc<WriteTarget>> {
        let Self::Ready(write) = std::mem::replace(self, Self::Detached) else {
            unreachable!("only recovered attachments can start a request");
        };
        let write = match BufferSet::try_new(write) {
            Ok(write) => write,
            Err(error) => {
                let (error, write) = error.into_parts();
                *self = Self::Ready(write);
                return Err(error.into());
            }
        };
        let target = WriteTarget::new_with_recovery(write, recovery);
        *self = Self::Pending(target.clone());
        Ok(target)
    }
}

impl<'a> ClientWithBuffers<'a> {
    pub(super) fn new(client: &'a Client, read: ReadSources<'a>, write: Vec<WriteBuffer>) -> Self {
        Self::with_recovery(client, read, write, Recovery::Wait)
    }

    pub(super) fn new_owned(client: &'a Client, read: ReadSources<'a>, write: Vec<Buffer>) -> Self {
        Self::with_recovery(
            client,
            read,
            write.into_iter().map(WriteBuffer::from).collect(),
            Recovery::DetachOwned,
        )
    }

    fn with_recovery(
        client: &'a Client,
        read: ReadSources<'a>,
        write: Vec<WriteBuffer>,
        recovery: Recovery,
    ) -> Self {
        Self {
            client,
            read,
            recovery,
            attachments: tokio::sync::Mutex::new(Attachments::Ready(write)),
        }
    }

    /// A forgotten inner call retains this gate; recovery cannot bypass it.
    pub(super) async fn recover(&mut self) {
        self.attachments.lock().await.recover(self.recovery).await;
    }

    pub(super) async fn into_owned(self) -> Option<Vec<Buffer>> {
        if !self.attachments.lock().await.recover(self.recovery).await {
            return None;
        }
        let Attachments::Ready(write) = self.attachments.into_inner() else {
            unreachable!("successful recovery leaves ready destinations");
        };
        Some(
            write
                .into_iter()
                .map(|buffer| {
                    buffer
                        .into_owned()
                        .expect("owning adapter contains owned destinations")
                })
                .collect(),
        )
    }

    pub(crate) async fn ruapc_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        let mut attachments = self.attachments.lock().await;
        if !attachments.recover(self.recovery).await {
            return Err(Error::new(
                ErrorKind::BuffersInUse,
                "owned destinations were abandoned after their recovery grace".into(),
            )
            .into());
        }
        let source = BorrowedSource::new(self.read.buffers())
            .map_err(Error::from)
            .map_err(E::from)?;
        let target = attachments.start(self.recovery).map_err(E::from)?;
        let result = self
            .client
            .ruapc_request(
                ctx,
                req,
                ReadAttachment::new(Some(&source), None)
                    .with_confirmation(self.read.requires_confirmation()),
                Some(&target),
                method_name,
            )
            .await;
        attachments.recover(self.recovery).await;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CopyOp, ErrorKind, MAX_REGIONS, Message, MsgFlags, MsgMeta, Payload, Socket,
        SocketPoolConfig, msg::frame, tcp::TcpSocket,
    };
    #[cfg(feature = "rdma")]
    use crate::{
        Router, Server, Transport,
        core::ContextEndpoint,
        rdma::{RdmaSocket, test_utils},
    };
    use bytes::{Bytes, BytesMut};
    use ruapc_memory::CopyBatch;
    use std::time::{Duration, Instant};

    #[crate::service]
    trait BufferedTest {
        async fn value(&self, ctx: &Context, req: &()) -> crate::Result<u32>;
    }

    fn fixture() -> (Context, tokio::sync::mpsc::Receiver<Bytes>) {
        let base = Context::create(&SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        })
        .unwrap();
        let (sender, queue) = tokio::sync::mpsc::channel(2);
        (
            Context::server_ctx(
                &base.state,
                Socket::TCP(TcpSocket::new(sender)),
                MsgMeta {
                    timeout_ms: 30_000,
                    ..Default::default()
                },
            ),
            queue,
        )
    }

    fn buffer(ctx: &Context, bytes: &[u8]) -> Buffer {
        let mut buffer = ctx.state.buffer_pool.allocate(bytes.len().max(1)).unwrap();
        buffer.set_len(bytes.len());
        buffer.copy_from_slice(bytes);
        buffer
    }

    fn identity(buffers: &[Buffer]) -> Vec<(usize, usize, usize)> {
        buffers
            .iter()
            .map(|b| (b.as_ptr() as usize, b.len(), b.capacity()))
            .collect()
    }

    async fn receive(queue: &mut tokio::sync::mpsc::Receiver<Bytes>) -> Message {
        let encoded = queue.recv().await.unwrap();
        let mut encoded = BytesMut::from(encoded.as_ref());
        Message::parse(frame::parse_message(&mut encoded).unwrap().unwrap()).unwrap()
    }

    fn respond(ctx: &Context, msgid: u64, result: crate::Result<u32>) {
        ctx.state.waiter.post(
            msgid,
            Message::new(
                MsgMeta {
                    msgid,
                    flags: MsgFlags::UseMessagePack | MsgFlags::IsRsp,
                    ..Default::default()
                },
                Payload::from(Bytes::from(rmp_serde::to_vec_named(&result).unwrap())),
            ),
        );
    }

    async fn gather(source: &crate::remote_memory::SourceSender) -> crate::Result<Vec<u8>> {
        let ops = [CopyOp::new(0, 0, 4)];
        source
            .gather(&ops, Some(Instant::now() + Duration::from_millis(100)))
            .await
    }

    fn attach<'a>(
        client: &'a Client,
        read: &'a [Buffer],
        write: Vec<Buffer>,
    ) -> ClientWithBuffers<'a> {
        ClientWithBuffers::new(
            client,
            ReadSources::new(read),
            write.into_iter().map(WriteBuffer::from).collect(),
        )
    }

    #[cfg(feature = "rdma")]
    async fn recovery_socket() -> (Server, Context, Arc<RdmaSocket>) {
        struct Capture(tokio::sync::mpsc::UnboundedSender<Arc<RdmaSocket>>);
        impl BufferedTest for Capture {
            async fn value(&self, ctx: &Context, _: &()) -> crate::Result<u32> {
                let ContextEndpoint::Connected(Socket::RDMA(socket)) = &ctx.endpoint else {
                    panic!("fixture requires RDMA");
                };
                self.0.send(socket.clone()).unwrap();
                Ok(7)
            }
        }

        let mut config = test_utils::small_rdma_config();
        config.rdma.as_mut().unwrap().remote_memory.settle_grace_ms = 5;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut router = Router::default();
        BufferedTest::ruapc_export(Arc::new(Capture(sender)), &mut router);
        let server = Server::create(router, &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let ctx = Context::create(&config)
            .unwrap()
            .with_endpoint(test_utils::transport_endpoint(Transport::RDMA, address));
        Client::default().value(&ctx, &()).await.unwrap();
        let socket = receiver.recv().await.unwrap();
        (server, ctx, socket)
    }

    type AttachmentIdentities = (Vec<(usize, usize, usize)>, Vec<(usize, usize, usize)>);

    fn assert_originals(
        read: &[Buffer],
        write: &[Buffer],
        expected: &[u8],
        identities: &AttachmentIdentities,
    ) {
        assert_eq!(identity(read), identities.0);
        assert_eq!(identity(write), identities.1);
        assert_eq!(read[0].as_slice(), b"read");
        assert_eq!(write[0].as_slice(), expected);
    }

    #[tokio::test]
    async fn unpolled_method_preserves_owned_attachments() {
        let (ctx, _queue) = fixture();
        let client = Client::default();
        let read = vec![buffer(&ctx, b"read")];
        let write = vec![buffer(&ctx, b"keep")];
        let identities = (identity(&read), identity(&write));
        let attached = attach(&client, &read, write);
        assert!(matches!(
            *attached.attachments.try_lock().unwrap(),
            Attachments::Ready(_)
        ));
        drop(attached.value(&ctx, &()));
        assert_originals(
            &read,
            &attached.into_owned().await.unwrap(),
            b"keep",
            &identities,
        );
    }

    #[tokio::test]
    async fn normal_success_and_error_return_originals_and_revoke_retained_handles() {
        for reject in [false, true] {
            let (ctx, mut queue) = fixture();
            let client = Client::default();
            let mut read = vec![buffer(&ctx, b"read")];
            let write = vec![buffer(&ctx, b"keep")];
            let identities = (identity(&read), identity(&write));
            let attached = attach(&client, &read, write);
            let mut call = Box::pin(attached.value(&ctx, &()));
            assert!(futures_util::poll!(call.as_mut()).is_pending());
            let request = receive(&mut queue).await;
            let source = ctx.state.waiter.read_source(request.meta.msgid).unwrap();
            let target = ctx.state.waiter.write_target(request.meta.msgid).unwrap();
            let mut gathering = Box::pin(gather(&source));
            assert!(futures_util::poll!(gathering.as_mut()).is_pending());
            assert!(futures_util::poll!(call.as_mut()).is_pending());
            assert_eq!(gathering.await.unwrap(), b"read");
            target.copy_in(0, b"done").unwrap();
            respond(
                &ctx,
                request.meta.msgid,
                if reject {
                    Err(Error::kind(ErrorKind::Overloaded))
                } else {
                    Ok(7)
                },
            );
            let result = call.await;
            if reject {
                assert_eq!(result.unwrap_err().kind, ErrorKind::Overloaded);
            } else {
                assert_eq!(result.unwrap(), 7);
            }
            assert_eq!(ctx.state.waiter.pending_count(), 0);
            assert_eq!(gather(&source).await.unwrap_err().kind, ErrorKind::Timeout);
            assert_eq!(
                target.copy_in(0, b"late").unwrap_err().kind,
                ErrorKind::BuffersInUse
            );
            let buffers = attached.into_owned().await.unwrap();
            assert_originals(&read, &buffers, b"done", &identities);
            read[0].copy_from_slice(b"next");
            assert_eq!(gather(&source).await.unwrap_err().kind, ErrorKind::Timeout);
        }
    }

    #[tokio::test]
    async fn owning_run_returns_both_sets_after_preflight_error() {
        let (mut ctx, _queue) = fixture();
        ctx.endpoint = crate::core::ContextEndpoint::Invalid;
        let client = Client::default();
        let read = vec![buffer(&ctx, b"read")];
        let write = vec![buffer(&ctx, b"keep")];
        let identities = (identity(&read), identity(&write));
        let completed = client
            .with_buffers(&read, write)
            .run(async |rpc| rpc.value(&ctx, &()).await)
            .await;
        assert_eq!(
            completed.result.unwrap_err().kind,
            ErrorKind::InvalidArgument
        );
        assert_originals(
            &read,
            completed.buffers.as_ref().unwrap(),
            b"keep",
            &identities,
        );
    }

    #[tokio::test]
    async fn invalid_attachment_layout_preserves_all_owned_buffers() {
        for invalid_read in [false, true] {
            let (ctx, mut queue) = fixture();
            let client = Client::default();
            let mut read = vec![buffer(&ctx, b"read")];
            let mut write = vec![buffer(&ctx, b"keep")];
            let invalid = if invalid_read { &mut read } else { &mut write };
            invalid.extend((0..MAX_REGIONS).map(|_| Buffer::empty(&ctx.state.buffer_pool)));
            let identities = (identity(&read), identity(&write));
            let completed = client
                .with_buffers(&read, write)
                .run(async |rpc| rpc.value(&ctx, &()).await)
                .await;
            assert_eq!(completed.result.unwrap_err().kind, ErrorKind::InvalidCopyOp);
            assert_originals(
                &read,
                completed.buffers.as_ref().unwrap(),
                b"keep",
                &identities,
            );
            assert!(queue.try_recv().is_err());
        }
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn responses_wait_for_owned_destinations_and_surface_transfer_failure() {
        for outcome in 0..3 {
            let (ctx, mut queue) = fixture();
            let client = Client::default();
            let read = vec![buffer(&ctx, b"read")];
            let write = vec![buffer(&ctx, b"keep")];
            let identities = (identity(&read), identity(&write));
            let attached = attach(&client, &read, write);
            let mut call = Box::pin(attached.value(&ctx, &()));
            assert!(futures_util::poll!(call.as_mut()).is_pending());
            let request = receive(&mut queue).await;
            let target = ctx.state.waiter.write_target(request.meta.msgid).unwrap();
            let (_, mut checked_out) = target.checkout(|_| Ok(())).unwrap();
            let ops = [CopyOp::new(0, 0, 4)];
            checked_out
                .scatter(&CopyBatch::new(&ops).unwrap(), b"done")
                .unwrap();
            respond(
                &ctx,
                request.meta.msgid,
                if outcome == 1 {
                    Err(Error::kind(ErrorKind::Overloaded))
                } else {
                    Ok(7)
                },
            );
            assert!(futures_util::poll!(call.as_mut()).is_pending());
            assert_eq!(ctx.state.waiter.pending_count(), 0);
            assert_eq!(
                checked_out.buffers()[0].as_ptr() as usize,
                identities.1[0].0
            );
            target.restore_with_error(
                checked_out,
                (outcome == 2).then(|| Error::kind(ErrorKind::RdmaSendFailed)),
            );
            let result = call.await;
            match outcome {
                0 => assert_eq!(result.unwrap(), 7),
                1 => assert_eq!(result.unwrap_err().kind, ErrorKind::Overloaded),
                _ => assert_eq!(result.unwrap_err().kind, ErrorKind::RdmaSendFailed),
            }
            assert_originals(
                &read,
                &attached.into_owned().await.unwrap(),
                b"done",
                &identities,
            );
            assert_eq!(
                target.copy_in(0, b"late").unwrap_err().kind,
                ErrorKind::BuffersInUse
            );
        }
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn cancelled_method_and_cancelled_recovery_keep_storage_until_settlement() {
        let (ctx, mut queue) = fixture();
        let client = Client::default();
        let read = vec![buffer(&ctx, b"read")];
        let write = vec![buffer(&ctx, b"keep")];
        let identities = (identity(&read), identity(&write));
        let mut attached = attach(&client, &read, write);
        let mut call = Box::pin(attached.value(&ctx, &()));
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        let request = receive(&mut queue).await;
        let source = ctx.state.waiter.read_source(request.meta.msgid).unwrap();
        let target = ctx.state.waiter.write_target(request.meta.msgid).unwrap();
        let (_, checked_out) = target.checkout(|_| Ok(())).unwrap();
        drop(call);
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        let mut recovering = Box::pin(attached.recover());
        assert!(futures_util::poll!(recovering.as_mut()).is_pending());
        assert_eq!(gather(&source).await.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(
            target.copy_in(0, b"late").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        drop(recovering);
        assert!(matches!(
            *attached.attachments.try_lock().unwrap(),
            Attachments::Pending(_)
        ));
        // Cancelling recovery does not take or replace the checked-out target.
        let mut recovering = Box::pin(attached.recover());
        assert!(futures_util::poll!(recovering.as_mut()).is_pending());
        drop(recovering);
        target.restore(checked_out);
        attached.recover().await;
        assert_originals(
            &read,
            &attached.into_owned().await.unwrap(),
            b"keep",
            &identities,
        );
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn storage_scope_recovers_cancelled_method_before_returning_callback_result() {
        let (ctx, mut queue) = fixture();
        let client = Client::default();
        let read = vec![buffer(&ctx, b"read")];
        let scope = client.with_buffer_storage(
            ReadSources::new(&read),
            vec![WriteBuffer::from(buffer(&ctx, b"keep"))],
        );
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let mut running = Box::pin(scope.run(async |rpc| {
            let mut call = Box::pin(rpc.value(&ctx, &()));
            assert!(futures_util::poll!(call.as_mut()).is_pending());
            cancelled.await.unwrap();
            drop(call);
            42
        }));
        assert!(futures_util::poll!(running.as_mut()).is_pending());
        let request = receive(&mut queue).await;
        let source = ctx.state.waiter.read_source(request.meta.msgid).unwrap();
        let target = ctx.state.waiter.write_target(request.meta.msgid).unwrap();
        let (_, checked_out) = target.checkout(|_| Ok(())).unwrap();
        cancel.send(()).unwrap();
        assert!(
            futures_util::poll!(running.as_mut()).is_pending(),
            "scope recovery must outlive the cancelled method"
        );
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        assert_eq!(gather(&source).await.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(
            target.copy_in(0, b"late").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        target.restore(checked_out);
        assert_eq!(running.await, 42);
        assert!(target.close_and_take().await.unwrap().is_none());
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn owning_recovery_timeout_returns_no_buffers_and_reclaims_after_late_restore() {
        for rejected in [false, true] {
            let (server, rdma_context, socket) = recovery_socket().await;
            let (ctx, mut queue) = fixture();
            let pool = ruapc_bufpool::BufferPoolBuilder::new(Vec::new())
                .max_memory(64 << 20)
                .thread_cache(false)
                .build();
            let mut read = pool.allocate(16 << 20).unwrap();
            read.set_len(4);
            read.copy_from_slice(b"read");
            let mut write = pool.allocate(16 << 20).unwrap();
            write.set_len(4);
            write.copy_from_slice(b"keep");
            let original = write.as_ptr();
            let client = Client::default();
            let read = vec![read];
            let mut running = Box::pin(client.with_buffers(&read, vec![write]).run(async |rpc| {
                let first = rpc.value(&ctx, &()).await;
                let second = rpc.value(&ctx, &()).await;
                (first, second)
            }));
            assert!(futures_util::poll!(running.as_mut()).is_pending());
            let request = receive(&mut queue).await;
            let source = ctx.state.waiter.read_source(request.meta.msgid).unwrap();
            let target = ctx.state.waiter.write_target(request.meta.msgid).unwrap();
            // Hold the exact checked-out vector as a delayed recovery task
            // would. No test DMA is posted; the real socket supplies the grace
            // and follows its ordinary ERR/destruction path on expiry.
            let (_, checked_out) = target.checkout_rdma(socket.clone(), |_| Ok(())).unwrap();
            respond(
                &ctx,
                request.meta.msgid,
                if rejected {
                    Err(Error::kind(ErrorKind::Overloaded))
                } else {
                    Ok(7)
                },
            );
            let completed = tokio::time::timeout(Duration::from_secs(2), running)
                .await
                .expect("owning calls must not wait for delayed destination recovery");
            let (first, second) = completed.result;
            assert_eq!(
                first.unwrap_err().kind,
                if rejected {
                    ErrorKind::Overloaded
                } else {
                    ErrorKind::BufferRecoveryTimeout
                }
            );
            assert_eq!(second.unwrap_err().kind, ErrorKind::BuffersInUse);
            assert!(completed.buffers.is_none());
            assert!(
                queue.try_recv().is_err(),
                "unavailable attachments cannot send again"
            );
            assert_eq!(ctx.state.waiter.pending_count(), 0);
            assert_eq!(gather(&source).await.unwrap_err().kind, ErrorKind::Timeout);
            assert_eq!(
                target.copy_in(0, b"late").unwrap_err().kind,
                ErrorKind::BuffersInUse
            );
            assert_eq!(checked_out.buffers()[0].as_ptr(), original);
            assert!(
                pool.allocate(64 << 20).is_err(),
                "outstanding destinations stay allocated"
            );

            drop(read);
            target.restore(checked_out);
            // Keep stale source and target handles alive while checking that
            // settled, abandoned buffers have actually returned to the pool.
            let reclaimed = pool
                .allocate(64 << 20)
                .expect("late settlement reclaims abandoned buffers");
            assert_eq!(
                target.close_and_take().await.unwrap_err().kind,
                ErrorKind::BufferRecoveryTimeout
            );
            drop(reclaimed);
            server.stop();
            rdma_context.state.socket_pool.stop();
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(server.join(), rdma_context.state.socket_pool.join());
            })
            .await
            .expect("recovery fixture must shut down");
        }
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn strict_recovery_keeps_waiting_after_grace_until_buffers_return() {
        let (server, rdma_context, socket) = recovery_socket().await;
        let (ctx, mut queue) = fixture();
        let client = Client::default();
        let read = vec![buffer(&ctx, b"read")];
        let write = vec![buffer(&ctx, b"keep")];
        let identities = (identity(&read), identity(&write));
        let attached = attach(&client, &read, write);
        let mut call = Box::pin(attached.value(&ctx, &()));
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        let request = receive(&mut queue).await;
        let target = ctx.state.waiter.write_target(request.meta.msgid).unwrap();
        let (_, checked_out) = target.checkout_rdma(socket, |_| Ok(())).unwrap();
        respond(&ctx, request.meta.msgid, Ok(7));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), call.as_mut())
                .await
                .is_err(),
            "strict storage cannot detach after its five-millisecond grace"
        );
        target.restore(checked_out);
        assert_eq!(call.await.unwrap(), 7);
        assert_originals(
            &read,
            &attached.into_owned().await.unwrap(),
            b"keep",
            &identities,
        );
        server.stop();
        rdma_context.state.socket_pool.stop();
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(server.join(), rdma_context.state.socket_pool.join());
        })
        .await
        .expect("strict recovery fixture must shut down");
    }

    #[tokio::test]
    async fn forgotten_owned_method_keeps_recovery_pending_and_allocations_reserved() {
        let (ctx, mut queue) = fixture();
        let pool = ruapc_bufpool::BufferPoolBuilder::new(Vec::new())
            .max_memory(64 << 20)
            .thread_cache(false)
            .build();
        let mut read = pool.allocate(16 << 20).unwrap();
        read.set_len(4);
        read.copy_from_slice(b"read");
        let mut write = pool.allocate(16 << 20).unwrap();
        write.set_len(4);
        write.copy_from_slice(b"keep");
        let client = Client::default();
        let read = vec![read];
        let mut attached =
            ClientWithBuffers::new_owned(&client, ReadSources::new(&read), vec![write]);
        let mut call = Box::pin(attached.value(&ctx, &()));
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        let request = receive(&mut queue).await;
        assert!(ctx.state.waiter.contains_message_id(request.meta.msgid));
        std::mem::forget(call);
        ctx.state
            .waiter
            .expire(Instant::now() + Duration::from_secs(60));
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        let mut recovering = Box::pin(attached.recover());
        assert!(
            futures_util::poll!(recovering.as_mut()).is_pending(),
            "forgotten method retains its asynchronous call gate"
        );
        drop(recovering);
        assert!(
            pool.allocate(64 << 20).is_err(),
            "forgotten future retains the allocations"
        );
        // The leaked future still holds the guard. Keep its mutex alive too;
        // the intentional leak is the safe outcome of forgetting owned work.
        std::mem::forget(attached);
    }
    #[tokio::test]
    async fn response_wins_over_queued_source_validation_and_closes_old_mailbox() {
        let (ctx, mut queue) = fixture();
        let client = Client::default();
        let read = vec![buffer(&ctx, b"read")];
        let attached = attach(&client, &read, Vec::new());
        let mut call = Box::pin(attached.value(&ctx, &()));
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        let request = receive(&mut queue).await;
        let source = ctx.state.waiter.read_source(request.meta.msgid).unwrap();
        let mut validate = Box::pin(source.validate(&ctx.state.waiter, request.meta.msgid, None));
        assert!(futures_util::poll!(validate.as_mut()).is_pending());
        respond(&ctx, request.meta.msgid, Ok(7));
        assert_eq!(call.await.unwrap(), 7);
        assert_eq!(validate.await.unwrap_err().kind, ErrorKind::Timeout);
        assert!(source.is_closed());

        let mut next = Box::pin(attached.value(&ctx, &()));
        assert!(futures_util::poll!(next.as_mut()).is_pending());
        let next_request = receive(&mut queue).await;
        assert_ne!(next_request.meta.msgid, request.meta.msgid);
        assert_eq!(
            source
                .validate(&ctx.state.waiter, request.meta.msgid, None)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
        respond(&ctx, next_request.meta.msgid, Ok(8));
        assert_eq!(next.await.unwrap(), 8);
    }

    #[tokio::test]
    async fn forgotten_scope_cannot_read_or_validate_reused_sources() {
        let (ctx, mut queue) = fixture();
        let client = Client::default();
        let mut read = vec![buffer(&ctx, b"read")];
        let mut running = Box::pin(
            client
                .with_buffers(&read, Vec::new())
                .run(async |rpc| rpc.value(&ctx, &()).await),
        );
        assert!(futures_util::poll!(running.as_mut()).is_pending());
        let request = receive(&mut queue).await;
        let source = ctx.state.waiter.read_source(request.meta.msgid).unwrap();
        std::mem::forget(running);
        // Safe Rust releases this borrow when the future is forgotten. The
        // retained waiter must therefore be unable to access or validate it.
        read[0].copy_from_slice(b"next");
        drop(read);
        assert!(ctx.state.waiter.contains_message_id(request.meta.msgid));
        let deadline = Some(Instant::now() + Duration::from_millis(10));
        assert_eq!(
            source
                .validate(&ctx.state.waiter, request.meta.msgid, deadline)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
        assert_eq!(gather(&source).await.unwrap_err().kind, ErrorKind::Timeout);
        ctx.state
            .waiter
            .expire(Instant::now() + Duration::from_secs(60));
    }
}
