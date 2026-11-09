//! READ planning in logical buffer coordinates. The verbs crate owns posted
//! destinations and validates their local ranges before exposing them to DMA.

use std::{sync::Arc, time::Instant};

use ruapc_bufpool::{RemoteBufferInfo, WriteBuffer};
use ruapc_memory::{BufferSet, CopyBatch, SpaceLayout};
use ruapc_rdma::{ReadCompletion, ReadFailure, ReadPosting, ReadRequest, ReadSegment};

mod credits;
mod pending;
pub(crate) use credits::ReadCredits;
pub(super) use pending::PendingReads;

use super::RdmaSocket;
use crate::{
    Buffer, Client, Completed, CopyOp, Error, ErrorKind, RemoteSpace, Result,
    remote_memory::{
        MemoryService, Recovery, RequestStatusRequest, WriteTarget, transfer::PreparedRead,
    },
};

fn build_planned_reads(
    source: &RemoteSpace<'_>,
    dst_layout: &SpaceLayout,
    ops: &CopyBatch<'_>,
    max_sge: usize,
) -> Result<Vec<ReadRequest>> {
    ops.plan_chunks(source.layout(), dst_layout, max_sge.max(1))?
        .into_iter()
        .map(|chunk| {
            let region = &source.regions()[chunk.seg];
            let segments = chunk
                .dst
                .into_iter()
                .map(|slice| {
                    Ok(ReadSegment {
                        buffer: slice.seg,
                        offset: usize::try_from(slice.off)
                            .map_err(|_| Error::kind(ErrorKind::InvalidCopyOp))?,
                        len: usize::try_from(slice.len)
                            .map_err(|_| Error::kind(ErrorKind::InvalidCopyOp))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ReadRequest {
                remote_addr: region
                    .addr
                    .checked_add(chunk.off)
                    .ok_or_else(|| Error::kind(ErrorKind::InvalidCopyOp))?,
                rkey: region.key.rkey,
                segments,
            })
        })
        .collect()
}

fn plan_error(error: ruapc_rdma::Error) -> Error {
    if error.kind == ruapc_rdma::ErrorKind::InvalidReadPlan {
        Error::new(ErrorKind::InvalidCopyOp, error.to_string())
    } else {
        error.into()
    }
}

fn read_failure_error(reason: ReadFailure) -> Error {
    match reason {
        ReadFailure::Timeout => Error::new(
            ErrorKind::RdmaReadTimeout,
            "RDMA READ exceeded rdma.remote_memory.read_timeout_ms".into(),
        ),
        ReadFailure::RequestTimeout => Error::new(
            ErrorKind::Timeout,
            "request deadline expired during RDMA READ".into(),
        ),
        ReadFailure::ConnectionClosed => Error::new(
            ErrorKind::ConnectionClosed,
            "RDMA connection closed with reads in flight".into(),
        ),
        ReadFailure::Completion | ReadFailure::Cancelled => Error::new(
            ErrorKind::RdmaSendFailed,
            "RDMA READ batch failed or was abandoned".into(),
        ),
    }
}

fn read_closed_error() -> Error {
    Error::new(
        ErrorKind::ConnectionClosed,
        "RDMA connection closed before READ posting".into(),
    )
}

#[derive(Clone, Copy, Debug)]
struct ReadDeadline {
    at: Instant,
    reason: ReadFailure,
}

impl ReadDeadline {
    fn earliest(read: Option<Instant>, request: Option<Instant>) -> Option<Self> {
        let (at, reason) = match (read, request) {
            (Some(read), Some(request)) if read < request => (read, ReadFailure::Timeout),
            (_, Some(request)) => (request, ReadFailure::RequestTimeout),
            (Some(read), None) => (read, ReadFailure::Timeout),
            (None, None) => return None,
        };
        Some(Self { at, reason })
    }

    fn check(deadline: Option<Self>) -> Result<()> {
        if let Some(deadline) = Self::expired(deadline, Instant::now()) {
            return Err(read_failure_error(deadline.reason));
        }
        Ok(())
    }

    fn expired(deadline: Option<Self>, now: Instant) -> Option<Self> {
        deadline.filter(|deadline| deadline.at <= now)
    }

    /// Use an already expired deadline; otherwise start cleanup at the first
    /// observation. A late observer must not restart an expired budget's grace.
    fn failure_start(deadline: Option<Self>, now: Instant) -> Instant {
        Self::expired(deadline, now).map_or(now, |deadline| deadline.at)
    }

    async fn wait(deadline: Option<Self>) -> ReadFailure {
        let Some(deadline) = deadline else {
            return std::future::pending().await;
        };
        tokio::time::sleep_until(deadline.at.into()).await;
        deadline.reason
    }
}

/// Internal settlement error; public borrowed calls return ordinary Error.
#[derive(Debug)]
struct ReadError {
    error: Error,
    buffers: Option<Vec<WriteBuffer>>,
}
impl ReadError {
    fn new(error: Error, buffers: Vec<WriteBuffer>) -> Self {
        Self {
            error,
            buffers: Some(buffers),
        }
    }
}

impl RdmaSocket {
    /// Transport payload READs use the same owned plans and completion evidence
    /// as remote memory, with a separate payload finish RPC for the source.
    pub(super) async fn read_payload(
        &self,
        regions: &[RemoteBufferInfo],
        buffer: Buffer,
        request_deadline: Option<Instant>,
    ) -> Result<Buffer> {
        let src = RemoteSpace::new(regions)?;
        let dst = SpaceLayout::from_lens([buffer.len() as u64])?;
        let ops = [CopyOp::new(0, 0, buffer.len() as u64)];
        let batch = CopyBatch::new(&ops)?;
        let reads = build_planned_reads(&src, &dst, &batch, self.queue_pair.gather_limit())?;
        let mut buffers = self
            .execute_reads(
                reads,
                vec![WriteBuffer::from(buffer)],
                request_deadline,
                Recovery::Wait,
            )
            .await
            .map_err(|e| e.error)?;
        Ok(buffers
            .pop()
            .expect("one payload destination")
            .into_owned()
            .expect("owned payload"))
    }

    pub(crate) fn fail_read_batches(&self) {
        self.pending_reads.fail();
    }

    pub(crate) fn expire_read_batches(&self, now: Instant) -> bool {
        self.pending_reads.expire(now, self.settle_grace())
    }

    /// Preparation transfers storage capabilities; local addresses and keys never
    /// cross this layer's interface. Failed/cancelled batches stay in the QP
    /// until all posted requests complete or it is successfully destroyed.
    async fn execute_reads(
        &self,
        reads: Vec<ReadRequest>,
        local: Vec<WriteBuffer>,
        request_deadline: Option<Instant>,
        recovery: Recovery,
    ) -> std::result::Result<Vec<WriteBuffer>, ReadError> {
        if !self.state.is_ok() {
            return Err(ReadError::new(read_closed_error(), local));
        }
        // Admission, completion and the poller's independent watchdog share
        // the earliest absolute READ/request deadline and its failure reason.
        let now = Instant::now();
        let read_deadline = self.read_timeout.map(|timeout| now + timeout);
        let deadline = ReadDeadline::earliest(read_deadline, request_deadline);
        let bytes = reads
            .iter()
            .flat_map(|read| &read.segments)
            .map(|segment| segment.len as u64)
            .sum();
        let admission = tokio::select! {
            biased;
            result = self.bandwidth_limiter.reserve_recv(bytes, deadline.map(|deadline| deadline.at)) => result,
            () = self.read_credits.closed() => Err(read_closed_error()),
        };
        if let Err(error) = admission {
            // The shaper rejects a wait beyond the supplied budget as Timeout,
            // including when the tighter budget came from the READ watchdog.
            let error = if error.kind == ErrorKind::Timeout {
                deadline.map_or(error, |deadline| read_failure_error(deadline.reason))
            } else {
                error
            };
            return Err(ReadError::new(error, local));
        }
        if let Err(error) = ReadDeadline::check(deadline) {
            return Err(ReadError::new(error, local));
        }
        let (mut posting, receiver) = self
            .queue_pair
            .prepare_reads(local, reads)
            .map_err(|(error, buffers)| ReadError::new(plan_error(error), buffers))?;
        let mut registered = false;
        while posting.remaining() != 0 {
            if let Err(error) = self.post_read(&mut posting, deadline).await {
                let reason = match error.kind {
                    ErrorKind::Timeout => ReadFailure::RequestTimeout,
                    ErrorKind::RdmaReadTimeout => ReadFailure::Timeout,
                    ErrorKind::ConnectionClosed => ReadFailure::ConnectionClosed,
                    _ => ReadFailure::Completion,
                };
                posting.handle().fail(reason);
                if registered
                    && !matches!(reason, ReadFailure::Timeout | ReadFailure::RequestTimeout)
                {
                    self.set_error();
                }
                // Account every unposted WR before waiting. Published READs
                // retain their original destinations through their CQEs.
                drop(posting);
                return Err(self
                    .settle_failed_read(receiver, error, deadline, recovery)
                    .await);
            }
            if !registered {
                // Publish after the first provider-accepted WR and before the
                // next await, including admission for a later WR. Cancellation
                // cannot skip registration while leaving posted DMA behind.
                self.pending_reads.register(posting.handle(), deadline);
                registered = true;
            }
        }
        let result = self
            .await_read_batch(receiver, &posting, deadline, recovery)
            .await;
        // Retain the posting owner's Arc until notification so the runtime
        // task, rather than the CQ poller, normally destroys the batch.
        drop(posting);
        result
    }

    /// Waits after posting finishes or its unposted suffix has been cancelled.
    async fn await_read_batch(
        &self,
        mut receiver: ReadCompletion,
        posting: &ReadPosting,
        deadline: Option<ReadDeadline>,
        recovery: Recovery,
    ) -> std::result::Result<Vec<WriteBuffer>, ReadError> {
        // Publication can race software closure after the provider accepted
        // the first READ. Record closure for this late batch as well; the
        // poller still drains it before destroying the QP.
        if !self.state.is_ok() {
            self.fail_read_batches();
        }
        let outcome = if recovery == Recovery::Wait {
            receiver.await
        } else {
            tokio::select! {
                biased;
                outcome = &mut receiver => outcome,
                reason = async {
                    tokio::select! {
                        biased;
                        () = self.read_credits.closed() => ReadFailure::ConnectionClosed,
                        reason = ReadDeadline::wait(deadline) => reason,
                    }
                } => {
                    let handle = posting.handle();
                    handle.fail(reason);
                    let reason = handle.failure().unwrap_or(reason);
                    return Err(self.settle_failed_read(
                        receiver, read_failure_error(reason), deadline, recovery,
                    ).await);
                }
            }
        };
        if matches!(outcome.result, Err(ReadFailure::Completion)) {
            self.set_error();
        }
        if recovery == Recovery::Wait && !self.state.is_ok() {
            self.queue_pair.wait_destroyed().await;
        }
        match outcome.result {
            Ok(()) => Ok(outcome.buffers),
            Err(reason) => Err(ReadError::new(read_failure_error(reason), outcome.buffers)),
        }
    }

    /// A failed owned operation may abandon recovery after its grace. The QP
    /// ledger keeps every posted allocation and permit until authentic CQEs
    /// settle it, independently of this receiver and its caller's lifetime.
    async fn settle_failed_read(
        &self,
        receiver: ReadCompletion,
        error: Error,
        deadline: Option<ReadDeadline>,
        recovery: Recovery,
    ) -> ReadError {
        let outcome = if recovery == Recovery::Wait {
            receiver.await
        } else {
            // A late poll must not restart the same deadline's recovery grace.
            let failed_at = ReadDeadline::failure_start(deadline, Instant::now());
            match tokio::time::timeout_at((failed_at + self.settle_grace()).into(), receiver).await
            {
                Ok(outcome) => outcome,
                Err(_) => {
                    self.set_error();
                    return ReadError {
                        error,
                        buffers: None,
                    };
                }
            }
        };
        // A completed batch's own destinations are locally safe. Owned calls
        // need not wait for unrelated QP work before returning those buffers.
        if recovery == Recovery::Wait && !self.state.is_ok() {
            self.queue_pair.wait_destroyed().await;
        }
        ReadError::new(error, outcome.buffers)
    }

    async fn post_read(
        &self,
        posting: &mut ReadPosting,
        deadline: Option<ReadDeadline>,
    ) -> Result<()> {
        let (sq_permit, device_permit) = self.read_credits.acquire(deadline).await?;
        if !self.state.is_ok() {
            return Err(read_closed_error());
        }
        // Covers uncontended admission and credits becoming ready together
        // with expiry, without creating a timer for the fast path.
        ReadDeadline::check(deadline)?;
        // The synchronous QP gate orders this post before provider ERR, or
        // rejects it after closure. A permit alone is not permission to post.
        self.queue_pair.post_read(posting).map_err(|error| {
            if self.state.is_ok() {
                plan_error(error)
            } else {
                read_closed_error()
            }
        })?;
        sq_permit.forget();
        device_permit.forget();
        Ok(())
    }

    pub(crate) async fn read_into_target(
        self: &Arc<Self>,
        source: &RemoteSpace<'_>,
        ops: &CopyBatch<'_>,
        target: Arc<WriteTarget>,
        request_deadline: Option<Instant>,
    ) -> Result<()> {
        ops.check_bounds(source.total_len(), target.total_len())?;
        if ops.byte_count() == 0 {
            return Ok(());
        }
        let task = self
            .supervisor
            .try_start_async_task()
            .ok_or_else(read_closed_error)?;
        let (planned, buffers) = target.checkout_rdma(self.clone(), |layout| {
            build_planned_reads(source, layout, ops, self.queue_pair.gather_limit())
        })?;
        let socket = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // Ownership recovery must outlive the reverse-RPC handler. Dropping its
        // future abandons only the notification, never the task restoring the
        // complete destination vector after CQE settlement.
        tokio::spawn(async move {
            let transfer = socket.execute_reads(
                planned,
                buffers.into_buffers(),
                request_deadline,
                Recovery::Wait,
            );
            tokio::pin!(transfer);
            // The supervisor tracks this task through restoration. Shutdown
            // closes admission but does not drop the buffer-owning future.
            let outcome = tokio::select! {
                biased;
                outcome = transfer.as_mut() => outcome,
                () = task.stopped() => {
                    socket.set_error();
                    transfer.await
                }
            };
            let (buffers, result): (_, Result<()>) = match outcome {
                Ok(buffers) => (buffers, Ok(())),
                Err(error) => (
                    error
                        .buffers
                        .expect("target restoration waits for its buffers"),
                    Err(error.error),
                ),
            };
            target.restore_with_error(
                BufferSet::try_new(buffers).expect("READ preserves buffer layout"),
                result.as_ref().err().cloned(),
            );
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            Error::new(
                ErrorKind::ConnectionClosed,
                "RDMA destination recovery task stopped".into(),
            )
        })?
    }

    pub(crate) async fn read_remote(
        &self,
        prepared: PreparedRead<'_, '_>,
    ) -> crate::remote_memory::transfer::TransferCompletion<WriteBuffer> {
        let transfer = prepared;
        let planned = match build_planned_reads(
            &transfer.remote,
            transfer.local.layout(),
            &transfer.ops,
            self.queue_pair.gather_limit(),
        ) {
            Ok(planned) => planned,
            Err(error) => {
                return Completed {
                    result: Err(error),
                    buffers: Some(transfer.local.into_buffers()),
                };
            }
        };
        let ctx = transfer.ctx;
        let buffers = match self
            .execute_reads(
                planned,
                transfer.local.into_buffers(),
                ctx.deadline,
                transfer.recovery,
            )
            .await
        {
            Ok(buffers) => buffers,
            Err(error) => {
                return Completed {
                    result: Err(error.error),
                    buffers: error.buffers,
                };
            }
        };
        let request = RequestStatusRequest {
            request_id: ctx.msg_meta.msgid,
        };
        let result = match Client::default().request_is_pending(ctx, &request).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::new(
                ErrorKind::Timeout,
                "RDMA read completed but client request has already timed out".into(),
            )),
            Err(error) => Err(error),
        };
        Completed {
            result,
            buffers: Some(buffers),
        }
    }
}

#[cfg(test)]
mod timeout_tests {
    //! Public server-side remote I/O must return when admission outlives its
    //! request, even with the independent READ watchdog and peer probes disabled.

    use std::{
        sync::{Arc, Mutex, Weak},
        time::Duration,
    };

    use tokio::sync::{OwnedSemaphorePermit, mpsc};

    use super::super::RdmaSocket;
    use crate::{
        Buffer, Client, Context, CopyOp, Endpoint, ErrorKind, Result, Router, Server, Socket,
        SocketPoolConfig, Transport,
        core::ContextEndpoint,
        rdma::test_utils::small_rdma_config,
        remote_memory::{ReadIntoTargetRequest, WriteTarget},
        service,
    };

    const READ_PERMITS: u32 = 1;
    const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    struct BlockedAdmission {
        socket: Arc<RdmaSocket>,
        _permit: OwnedSemaphorePermit,
    }

    type AdmissionBlock = Arc<Mutex<Option<BlockedAdmission>>>;

    fn connected_socket(ctx: &Context) -> &Arc<RdmaSocket> {
        let ContextEndpoint::Connected(Socket::RDMA(socket)) = &ctx.endpoint else {
            panic!("the fault must apply to the RDMA connection executing this handler");
        };
        socket
    }

    fn block_admission(ctx: &Context, blocked: &AdmissionBlock) {
        let socket = connected_socket(ctx);
        assert!(socket.read_timeout.is_none());
        assert!(ctx.deadline().is_some());
        // Client and server can use the same physical NIC with different State
        // instances. Take the semaphore of this exact receiving connection.
        let permit = socket
            .read_credits
            .device
            .clone()
            .try_acquire_many_owned(READ_PERMITS)
            .expect("the fixture starts with idle READ admission");
        assert!(
            blocked
                .lock()
                .unwrap()
                .replace(BlockedAdmission {
                    socket: socket.clone(),
                    _permit: permit,
                })
                .is_none()
        );
    }

    struct Completion {
        kind: ErrorKind,
        recovered: Option<Vec<Buffer>>,
    }

    struct TimeoutService {
        blocked: AdmissionBlock,
        completed: mpsc::UnboundedSender<Completion>,
    }

    #[service]
    trait AdmissionTimeoutRpc {
        async fn ping(&self, ctx: &Context, req: &()) -> Result<u64>;
        async fn read(&self, ctx: &Context, req: &()) -> Result<()>;
        async fn write(&self, ctx: &Context, req: &()) -> Result<()>;
    }

    impl AdmissionTimeoutRpc for TimeoutService {
        async fn ping(&self, ctx: &Context, _: &()) -> Result<u64> {
            Ok(connected_socket(ctx).conn_id)
        }

        async fn read(&self, ctx: &Context, _: &()) -> Result<()> {
            block_admission(ctx, &self.blocked);
            let mut destination = ctx.state.buffer_pool.allocate(64).unwrap();
            destination.set_len(64);
            destination.fill(0xa5);
            let ops = [CopyOp::new(0, 0, 64)];
            let error = {
                let reading = ctx.own_buffers(vec![destination]).remote_read(&ops);
                tokio::pin!(reading);
                assert!(
                    futures_util::poll!(&mut reading).is_pending(),
                    "the handler must enter READ admission before its deadline"
                );
                reading.await
            };
            let recovered = error.buffers;
            let error = error.result.expect_err("READ admission must expire");
            self.completed
                .send(Completion {
                    kind: error.kind.clone(),
                    recovered,
                })
                .unwrap();
            Err(error)
        }

        async fn write(&self, ctx: &Context, _: &()) -> Result<()> {
            let mut source = ctx.state.buffer_pool.allocate(64).unwrap();
            source.set_len(64);
            source.fill(0x5a);
            let error = ctx
                .remote_write(&[CopyOp::new(0, 0, 64)], std::slice::from_ref(&source))
                .await
                .expect_err("the client cannot admit the reverse READ before its deadline");
            let recovered = Some(vec![source]);
            self.completed
                .send(Completion {
                    kind: error.kind.clone(),
                    recovered,
                })
                .unwrap();
            Err(error)
        }
    }

    struct Fixture {
        server: Server,
        context: Context,
        server_conn_id: u64,
        completed: mpsc::UnboundedReceiver<Completion>,
    }

    impl Fixture {
        async fn new(client_router: Router, blocked: AdmissionBlock) -> Self {
            let mut config: SocketPoolConfig = small_rdma_config();
            let rdma = config.rdma.as_mut().unwrap();
            rdma.remote_memory.read_timeout_ms = 0;
            rdma.remote_memory.max_inflight_read_wrs = READ_PERMITS;
            rdma.remote_memory.bandwidth_limit_ratio = 0.0;
            rdma.peers.connections_per_peer = 1;
            rdma.peers.preconnect_max_per_peer = 1;
            assert_eq!(rdma.peers.min_connections_per_remote_nic, 0);
            assert_eq!(rdma.maintenance.interval_ms, 0);
            assert_eq!(rdma.probe.interval_ms, 0);
            let (completed, receiver) = mpsc::unbounded_channel();
            let mut router = Router::default();
            AdmissionTimeoutRpc::ruapc_export(
                Arc::new(TimeoutService { blocked, completed }),
                &mut router,
            );
            let server = Server::create(router, &config).unwrap();
            let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
            let context = Context::create_with_router(client_router, &config)
                .unwrap()
                .with_endpoint(Endpoint::rdma(Transport::TCP, [address]).unwrap());
            // Bootstrap and activation do not consume the short transfer budget.
            let server_conn_id = Client::default().ping(&context, &()).await.unwrap();
            Self {
                server,
                context,
                server_conn_id,
                completed: receiver,
            }
        }

        async fn handler_completed(&mut self) -> Completion {
            tokio::time::timeout(TEST_TIMEOUT, self.completed.recv())
                .await
                .expect("the server's remote I/O call itself must return")
                .expect("the server handler must report its result")
        }

        async fn shutdown(self) {
            self.context.state.socket_pool.stop();
            self.server.stop();
            tokio::time::timeout(TEST_TIMEOUT, async {
                tokio::join!(self.context.state.socket_pool.join(), self.server.join());
            })
            .await
            .expect("the fixture must shut down after fault injection is released");
        }
    }

    fn assert_admission_still_blocked(blocked: &AdmissionBlock) {
        let held = blocked.lock().unwrap();
        let held = held
            .as_ref()
            .expect("the real RDMA handler entered admission");
        assert_eq!(held.socket.read_credits.device.available_permits(), 0);
        assert_eq!(held.socket.queue_pair.pending_read_count(), 0);
        assert!(held.socket.state.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_remote_read_returns_when_nic_admission_exceeds_parent_deadline() {
        let blocked = AdmissionBlock::default();
        let mut fixture = Fixture::new(Router::default(), blocked.clone()).await;
        let context = fixture.context.clone();
        let caller = tokio::spawn(async move {
            let client = Client {
                timeout: REQUEST_TIMEOUT,
                max_retries: 0,
                ..Default::default()
            };
            let mut source = context.state.buffer_pool.allocate(64).unwrap();
            source.set_len(64);
            source.fill(0x5a);
            client
                .with_buffers(std::slice::from_ref(&source), Vec::new())
                .run(async |rpc| rpc.read(&context, &()).await)
                .await
                .result
        });

        // The outer client's own Timeout is not evidence that its handler exited.
        // Keep the NIC fault installed until the handler reports remote_read's result.
        let completed = fixture.handler_completed().await;
        assert_eq!(completed.kind, ErrorKind::Timeout);
        let recovered = completed.recovered.expect("no destination reached DMA");
        assert_eq!(recovered[0].as_slice(), &[0xa5; 64]);
        assert_admission_still_blocked(&blocked);
        let result = tokio::time::timeout(TEST_TIMEOUT, caller)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(
            Client::default().ping(&fixture.context, &()).await.unwrap(),
            fixture.server_conn_id,
            "request expiry must leave the established connection usable"
        );
        drop(recovered);
        drop(blocked.lock().unwrap().take());
        fixture.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_remote_write_and_client_read_handler_return_at_parent_deadline() {
        let blocked = AdmissionBlock::default();
        let observed_target: Arc<Mutex<Option<Weak<WriteTarget>>>> = Arc::default();
        let mut router = Router::default();
        let gate = blocked.clone();
        let target_observer = observed_target.clone();
        router.wrap_method("MemoryService/read_into_target", move |original| {
            Box::new(move |ctx, payload| {
                let request: ReadIntoTargetRequest = payload.deserialize(&ctx.msg_meta)?;
                let target = ctx
                    .state
                    .waiter
                    .write_target(request.request_id)
                    .expect("the reverse READ arrives while its original request is live");
                *target_observer.lock().unwrap() = Some(Arc::downgrade(&target));
                drop(target);
                block_admission(&ctx, &gate);
                original(ctx, payload)
            })
        });
        let mut fixture = Fixture::new(router, blocked.clone()).await;
        let context = fixture.context.clone();
        let caller = tokio::spawn(async move {
            let client = Client {
                timeout: REQUEST_TIMEOUT,
                max_retries: 0,
                ..Default::default()
            };
            let mut destination = context.state.buffer_pool.allocate(64).unwrap();
            destination.set_len(64);
            client
                .with_buffers(&[], vec![destination])
                .run(async |rpc| rpc.write(&context, &()).await)
                .await
                .result
        });

        // Prove that the actual reverse handler checked out its destination,
        // rather than merely timing out before it was dispatched.
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let target = observed_target
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(Weak::upgrade);
                if let Some(target) = target
                    && let Some(held) = blocked.lock().unwrap().as_ref()
                    && target
                        .export_regions(&Socket::RDMA(held.socket.clone()))
                        .is_err_and(|e| e.kind == ErrorKind::BuffersInUse)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the client reverse handler must actually reach READ admission");
        let completed = fixture.handler_completed().await;
        assert_eq!(completed.kind, ErrorKind::Timeout);
        assert_admission_still_blocked(&blocked);
        let result = tokio::time::timeout(TEST_TIMEOUT, caller)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);

        // A server-side reverse-RPC timeout alone could hide a stranded client
        // handler. Its target must also be released while NIC admission stays full.
        let target = observed_target.lock().unwrap().clone().unwrap();
        tokio::time::timeout(TEST_TIMEOUT, async {
            while target.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the client READ handler must exit and release its target independently");
        assert_admission_still_blocked(&blocked);
        assert_eq!(
            Client::default().ping(&fixture.context, &()).await.unwrap(),
            fixture.server_conn_id
        );
        drop(completed);
        drop(blocked.lock().unwrap().take());
        fixture.shutdown().await;
    }
}
