//! Request-attached memory spaces and transfer validation.

use super::{BufferAvailability, WriteReceipt, transfer};
use crate::{Buffer, Context, CopyOp, Error, ErrorKind, RemoteIoError, RemoteSpace, Result};

impl Context {
    /// Returns the *read space* the client attached to the current request
    /// via [`Client::with_read_buffers`](crate::Client::with_read_buffers):
    /// the logical concatenation of the advertised regions, readable with
    /// [`remote_read`](Self::remote_read).
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::MissingBufferInfo`](crate::ErrorKind::MissingBufferInfo)
    /// if the request carries no read regions.
    pub fn remote_read_space(&self) -> Result<RemoteSpace<'_>> {
        if self.msg_meta.read_regions.is_empty() {
            return Err(Error::new(
                ErrorKind::MissingBufferInfo,
                "request carries no read regions; client must attach buffers \
                 via with_read_buffers()"
                    .into(),
            ));
        }
        Ok(RemoteSpace::new(&self.msg_meta.read_regions)?)
    }

    /// Returns the *write space* the client attached to the current request
    /// via [`Client::with_write_buffers`](crate::Client::with_write_buffers):
    /// the logical concatenation of the pinned destination regions,
    /// writable with [`remote_write`](Self::remote_write).
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::MissingBufferInfo`](crate::ErrorKind::MissingBufferInfo)
    /// if the request carries no write regions.
    pub fn remote_write_space(&self) -> Result<RemoteSpace<'_>> {
        if self.msg_meta.write_regions.is_empty() {
            return Err(Error::new(
                ErrorKind::MissingBufferInfo,
                "request carries no write regions; client must attach buffers \
                 via with_write_buffers()"
                    .into(),
            ));
        }
        Ok(RemoteSpace::new(&self.msg_meta.write_regions)?)
    }

    /// Executes a batch of reads from the client's read space into `local`.
    ///
    /// Both sides are logical contiguous spaces: the client's attached
    /// read buffers (source, see [`remote_read_space`](Self::remote_read_space))
    /// and the concatenation of the `local` buffers' logical lengths
    /// (destination). Each [`CopyOp`] copies `len` bytes from
    /// `src_offset` (client space) to `dst_offset` (local space).
    ///
    /// All transfers validate the connection, attached space, bounds, overflow,
    /// operation count and destination overlap. An empty or zero-byte batch
    /// returns `local` after validation without I/O.
    /// RDMA uses one-sided READs; TCP/WS/HTTP use a reverse inline-copy RPC.
    ///
    /// Returns the same buffers with the requested ranges filled. Failures
    /// return recoverable buffers in [`RemoteIoError`]; in-flight RDMA can
    /// retain them beyond the call. Converting the error with `?` to [`Error`]
    /// drops any returned buffers.
    ///
    /// For transfers that copy bytes, RDMA validates the completed READs with
    /// an original-request waiter membership probe. With the original immutable
    /// source still held and no request-ID reuse, a positive probe validates
    /// the copy. It is not a precise deadline check.
    ///
    /// Source recovery after cancellation does not wait for remote DMA.
    /// Failed validation can return readable destinations in [`RemoteIoError`];
    /// those bytes are not a valid transfer result. See the
    /// [source contract](https://github.com/SF-Zhou/ruapc/blob/main/docs/safe-boundaries.md#application-read-source-validation-and-recovery)
    /// for the success argument and failure behavior.
    pub async fn remote_read(
        &self,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        transfer::read(self, ops, local).await
    }

    /// Reads the client's entire read space into freshly allocated
    /// buffers, one per region (mirroring the client's segmentation).
    ///
    /// Convenience wrapper around
    /// [`remote_read_space`](Self::remote_read_space) and
    /// [`remote_read`](Self::remote_read). The returned buffers' logical
    /// lengths equal the transferred sizes; zero-length regions are
    /// skipped.
    pub async fn remote_read_all(&self) -> Result<Vec<Buffer>> {
        transfer::read_all(self).await
    }

    /// Executes a batch of writes from `local` into the client's write
    /// space and returns a [`WriteReceipt`](crate::WriteReceipt) witness of
    /// the completed transfer.
    ///
    /// Both sides are logical contiguous spaces: the concatenation of the
    /// `local` buffers' logical lengths (source) and the client's pinned
    /// write buffers (destination, see
    /// [`remote_write_space`](Self::remote_write_space)). Each [`CopyOp`]
    /// copies `len` bytes from `src_offset` (local space) to `dst_offset`
    /// (client space).
    ///
    /// All transfers validate the connection, attached space, bounds, overflow, operation count, and
    /// destination overlap within the batch. Callers coordinate writes across
    /// separate calls. RDMA uses a reverse RPC: the client READs `local` into
    /// owned destinations and replies after completion. The source is retained
    /// while awaiting that response; this path does not use the pending probe
    /// described in [`remote_read`](Self::remote_read). Failure or cancellation
    /// can still recover the source before remote DMA finishes. TCP/WS/HTTP
    /// instead send the bytes in an inline-copy RPC.
    ///
    /// The transfer happens *here*, inside the handler, so its latency and
    /// errors are directly observable. Pair the witness with a response
    /// value afterwards:
    ///
    /// ```rust,ignore
    /// let t0 = std::time::Instant::now();
    /// let sent = ctx.remote_write_all(bufs).await?;
    /// Ok(sent.into_reply(Stats { push_micros: t0.elapsed().as_micros() as u64 }))
    /// ```
    ///
    /// An empty or zero-byte batch returns a receipt after validation without I/O.
    /// A successful receipt records completion even if its source buffers cannot
    /// currently be recovered. Failures return recoverable local buffers in
    /// [`RemoteIoError`].
    pub async fn remote_write(
        &self,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<WriteReceipt, RemoteIoError> {
        transfer::write(self, ops, local).await
    }

    /// Writes the entire logical content of `local` to the beginning of
    /// the client's write space (a single 1:1 [`CopyOp`]).
    ///
    /// Zero total length (including an empty `local`) skips I/O after validating
    /// the connection and both memory spaces.
    pub async fn remote_write_all(
        &self,
        local: Vec<Buffer>,
    ) -> std::result::Result<WriteReceipt, RemoteIoError> {
        transfer::write_all(self, local).await
    }

    /// Produces an empty [`WriteReceipt`](crate::WriteReceipt) witness for
    /// handler paths that have nothing to transfer, fulfilling a
    /// `Result<BufferReply<T>, E>` contract without touching the network.
    #[must_use]
    pub fn sent_nothing(&self) -> WriteReceipt {
        WriteReceipt::new(BufferAvailability::Available(Vec::new()))
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
        BufferReply, Error, ErrorKind, MAX_COPY_OPS, Message, MsgFlags, MsgMeta, Payload, Socket,
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
            read_regions: regions,
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
        let result = ctx.remote_read(&[CopyOp::new(0, 0, 1)], local).await;
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::MissingBufferInfo);
        let recovered = err
            .take_recovered_buffers()
            .expect("buffers should be recovered");
        assert_eq!(recovered.len(), 1);
    }

    #[tokio::test]
    async fn test_remote_read_invalid_endpoint_returns_err() {
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        ctx.msg_meta.read_regions = vec![region(8)];
        let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local.set_len(8);
        let result = ctx.remote_read(&[CopyOp::new(0, 0, 8)], vec![local]).await;
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::NotConnected);
        // The consumed buffers are recoverable from the error.
        let recovered = err
            .take_recovered_buffers()
            .expect("buffers should be recovered");
        assert_eq!(recovered[0].capacity(), 1024 * 1024);
    }

    #[tokio::test]
    async fn test_remote_read_rejects_out_of_bounds_ops() {
        let ctx = ctx_with_read_regions(vec![region(8), region(8)]);
        let space = ctx.remote_read_space().unwrap();
        assert_eq!(space.total_len(), 16);
        assert_eq!(space.region_count(), 2);

        let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local.set_len(16);
        // Source range exceeds the 16-byte remote space.
        let result = ctx.remote_read(&[CopyOp::new(9, 0, 8)], vec![local]).await;
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::InvalidCopyOp);
        let local = err.take_recovered_buffers().unwrap();

        // Overlapping destination ranges are rejected.
        let ops = [CopyOp::new(0, 0, 8), CopyOp::new(8, 4, 8)];
        let err = ctx.remote_read(&ops, local).await.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::InvalidCopyOp);
    }

    #[tokio::test]
    async fn zero_byte_transfers_validate_context_and_batch_without_sending() {
        let disconnected = Context::create(&SocketPoolConfig::default()).unwrap();
        let (missing, mut missing_queue) = connected_context(MsgMeta::default());
        let (connected, mut queue) = connected_context(MsgMeta {
            read_regions: vec![region(8)],
            write_regions: vec![region(8)],
            ..Default::default()
        });
        for ops in [vec![], vec![CopyOp::new(u64::MAX, u64::MAX, 0)]] {
            for (ctx, expected) in [
                (&disconnected, ErrorKind::NotConnected),
                (&missing, ErrorKind::MissingBufferInfo),
            ] {
                assert_eq!(
                    ctx.remote_read(&ops, Vec::new())
                        .await
                        .unwrap_err()
                        .error
                        .kind,
                    expected
                );
                assert_eq!(
                    ctx.remote_write(&ops, Vec::new())
                        .await
                        .unwrap_err()
                        .error
                        .kind,
                    expected
                );
            }
            assert!(
                connected
                    .remote_read(&ops, Vec::new())
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                connected
                    .remote_write(&ops, Vec::new())
                    .await
                    .unwrap()
                    .source_buffers()
                    .available()
                    .unwrap()
                    .is_empty()
            );
        }
        let ops = vec![CopyOp::new(0, 0, 0); MAX_COPY_OPS + 1];
        for write in [false, true] {
            let mut buffer = connected.state.buffer_pool.allocate(4).unwrap();
            buffer.set_len(4);
            buffer.copy_from_slice(b"keep");
            let pointer = buffer.as_ptr();
            let mut error = if write {
                connected
                    .remote_write(&ops, vec![buffer])
                    .await
                    .unwrap_err()
            } else {
                connected.remote_read(&ops, vec![buffer]).await.unwrap_err()
            };
            assert_eq!(error.error.kind, ErrorKind::InvalidCopyOp);
            let recovered = error.take_recovered_buffers().unwrap();
            assert_eq!(recovered[0].as_ptr(), pointer);
            assert_eq!(recovered[0].as_slice(), b"keep");
        }
        assert!(queue.try_recv().is_err());
        assert!(missing_queue.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_remote_write_invalid_endpoint_returns_err() {
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        ctx.msg_meta.write_regions = vec![region(1024)];
        let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local.set_len(1024);
        let result = ctx
            .remote_write(&[CopyOp::new(0, 0, 1024)], vec![local])
            .await;
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::NotConnected);
        assert!(err.take_recovered_buffers().is_some());
        // Once taken, the buffers are gone.
        assert!(err.take_recovered_buffers().is_none());
        // Converting to Error drops any remaining buffers and keeps the kind.
        let plain: Error = err.into();
        assert_eq!(plain.kind, ErrorKind::NotConnected);
    }

    #[tokio::test]
    async fn test_remote_write_zero_total_short_circuits() {
        let (ctx, mut queue) = connected_context(MsgMeta {
            write_regions: vec![region(0)],
            ..Default::default()
        });
        let sent = ctx.remote_write_all(vec![]).await.unwrap();
        assert!(queue.try_recv().is_err());
        assert!(sent.source_buffers().available().unwrap().is_empty());
        let _rsp: BufferReply<u32> = sent.into_reply(7);
        // The explicit no-op witness works the same way.
        let _rsp: BufferReply<u32> = ctx.sent_nothing().into_reply(7);
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
        let err = ctx.remote_read_all().await.unwrap_err();
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
                    read_regions: vec![region(4)],
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
                tokio::join!(ctx.remote_read(&ops, vec![local]), peer)
            })
            .await
            .unwrap();
            let mut error = result.unwrap_err();
            assert_eq!(error.error.kind, expected);
            let recovered = error.take_recovered_buffers().unwrap();
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].as_ptr(), original_pointer);
            assert_eq!(recovered[0].as_slice(), b"........");
        }
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn inline_write_checks_request_before_busy_and_keeps_empty_batch_semantics() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"keep");
        let target = WriteTarget::new(BufferSet::try_new(vec![buffer]).unwrap());
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
        Buffer, Client, Context, Error, ErrorKind, ListenMode, Result, Router, Server, Socket,
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
                Operation::Read => ctx.remote_read_all().await.map(|_| ()),
                Operation::Write => ctx
                    .remote_write_all(vec![buffer(&ctx.state)])
                    .await
                    .map(|_| ())
                    .map_err(Error::from),
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
            let mut attached = match operation {
                Operation::Read => client.with_read_buffer(buffer(&self.client.state)).unwrap(),
                Operation::Write => client.with_write_buffers(vec![buffer(&self.client.state)]),
            };
            // Keep the read source wrapper alive until both observations settle.
            // A client-side timeout alone does not prove that the server stopped.
            let (result, report) = tokio::time::timeout(GUARD, async {
                tokio::join!(
                    attached.transfer(&self.client, &operation),
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
            assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);
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
            let recovered = match operation {
                Operation::Read => attached.take_read_buffers().into_available(),
                Operation::Write => attached.take_write_buffers().into_available(),
            }
            .expect("no DMA or inline copy remains after the injected reverse-RPC fault");
            assert_eq!(recovered.len(), 1);
            assert!(recovered[0].iter().all(|&byte| byte == 0x5a));
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
