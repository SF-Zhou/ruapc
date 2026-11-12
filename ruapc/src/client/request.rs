//! Request execution: validate, acquire/send with retries, receive exactly once.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    Buffer, Context, Message, Receiver,
    core::ContextEndpoint,
    error::{Error, ErrorKind},
    msg::{MsgFlags, MsgMeta},
    remote_memory::{ReadSource, ReadSourceService, WriteTarget, serve_read_source},
    task::MemoryRoutes,
};

use super::attempt::{
    AttemptCycle, AttemptEndpoint, AttemptFailure, AttemptOptions, acquire_for_attempt,
    capped_deadline, export_attached_regions, wire_timeout_ms,
};

use super::Client;

#[derive(Clone, Copy)]
pub(crate) struct ReadAttachment<'a> {
    source: Option<&'a ReadSource<'a>>,
    charge_bytes: u64,
}

impl<'a> ReadAttachment<'a> {
    pub(crate) fn new(source: Option<&'a ReadSource<'a>>, charge_bytes: Option<u64>) -> Self {
        Self {
            source,
            charge_bytes: charge_bytes
                .unwrap_or_else(|| source.map_or(0, |source| source.layout().total())),
        }
    }

    fn buffers(self) -> &'a [Buffer] {
        self.source.map_or(&[], |source| source.buffers())
    }
}

impl Client {
    /// Executes one logical call, including pre-wire retries and metrics.
    /// The caller keeps `write_target` and recovers its buffers afterwards.
    pub(crate) async fn ruapc_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        read_attachment: ReadAttachment<'_>,
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        let metrics = ctx.state.metrics.client_method(method_name).start();
        let result = self
            .request_inner(ctx, req, read_attachment, write_target, method_name)
            .await;
        if result.is_err() {
            metrics.failed();
        }
        result
    }

    async fn request_inner<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        read_attachment: ReadAttachment<'_>,
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        if ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "request deadline already expired".to_string(),
            )
            .into());
        }

        // Builders only attach buffers. Validate both spaces before connection
        // acquisition, bandwidth admission, or waiter/source publication.
        if let Some(source) = read_attachment.source {
            source.layout().check_region_limit().map_err(Error::from)?;
        }
        if let Some(target) = write_target {
            target.check_region_limit()?;
        }

        let sent = self
            .send_with_retries(ctx, req, read_attachment, write_target, method_name)
            .await?;
        // Never retry after send: a missing response cannot tell us whether
        // the peer executed the request.
        let response = sent.recv().await?;
        if response.meta.flags.contains(MsgFlags::IsFrameworkError) {
            // Framework failures use Error regardless of the service's E.
            // Keep the Result envelope shared with application responses.
            response
                .payload
                .deserialize::<Result<Rsp, Error>>(&response.meta)?
                .map_err(Into::into)
        } else {
            response.payload.deserialize(&response.meta)?
        }
    }

    /// Acquires a socket and sends the request, retrying only failures that
    /// provably occur before the request reaches the wire.
    async fn send_with_retries<'a, 's, Req>(
        &self,
        ctx: &'a Context,
        req: &Req,
        read_attachment: ReadAttachment<'s>,
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
    ) -> Result<SentRequest<'a, 's>, Error>
    where
        Req: Serialize + JsonSchema,
    {
        let connect_deadline = capped_deadline(
            std::time::Instant::now(),
            self.connect_timeout,
            ctx.deadline,
        );
        let endpoint = match &ctx.endpoint {
            ContextEndpoint::Connected(socket) => AttemptEndpoint::Connected(socket.clone()),
            ContextEndpoint::Endpoint(endpoint) => AttemptEndpoint::Endpoint(endpoint.clone()),
            ContextEndpoint::Invalid => {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "client context without endpoint".into(),
                ));
            }
        };
        let mut cycle = AttemptCycle::new(u64::from(self.max_retries) + 1);
        loop {
            let result = self
                .try_send(
                    ctx,
                    req,
                    read_attachment,
                    write_target,
                    method_name,
                    AttemptOptions {
                        endpoint: endpoint.clone(),
                        connect_deadline,
                        remaining_acquire_attempts: cycle.remaining_attempts(),
                        avoided_remote_nics: cycle.avoided_remote_nics(),
                    },
                )
                .await;
            match result {
                Ok(sent) => return Ok(sent),
                Err(failure) => {
                    let attempt = cycle.attempt_index();
                    if !cycle.note_failure(&failure) {
                        return Err(failure.error);
                    }
                    tracing::warn!(
                        "attempt {attempt} for {method_name} failed, retrying: {}",
                        failure.error
                    );
                }
            }
        }
    }

    /// One connect + send attempt. Failures here are always safe to retry:
    /// an error from `acquire` or `send` means the request was never handed
    /// to the transport.
    ///
    /// The waiter entry is allocated between the two phases so that
    /// connection setup does not eat into the response budget; it is
    /// returned as a [`Receiver`](crate::Receiver) whose drop (on send
    /// failure) removes the entry again.
    async fn try_send<'a, 's, Req>(
        &self,
        ctx: &'a Context,
        req: &Req,
        read_attachment: ReadAttachment<'s>,
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
        attempt: AttemptOptions<'_>,
    ) -> std::result::Result<SentRequest<'a, 's>, AttemptFailure>
    where
        Req: Serialize + JsonSchema,
    {
        let AttemptOptions {
            endpoint,
            connect_deadline,
            remaining_acquire_attempts,
            avoided_remote_nics,
        } = attempt;
        let socket = acquire_for_attempt(
            &ctx.state,
            endpoint,
            connect_deadline,
            remaining_acquire_attempts,
            avoided_remote_nics,
        )
        .await?;
        // The response budget starts once a connection is available, still
        // capped by the parent context's remaining deadline (nested RPCs).
        let now = std::time::Instant::now();
        let response_deadline = capped_deadline(now, self.timeout, ctx.deadline);
        if response_deadline <= now {
            return Err(AttemptFailure::deadline());
        }

        let (read_regions, write_regions) =
            export_attached_regions(&socket, read_attachment.buffers(), write_target)?;
        let read_bytes = read_attachment.charge_bytes;
        if let Err(err) = socket
            .reserve_rdma_send_bandwidth(read_bytes, Some(response_deadline))
            .await
        {
            return Err(AttemptFailure::send(
                err,
                socket.rdma_remote_device().map(str::to_owned),
            ));
        }
        let timeout = response_deadline.saturating_duration_since(std::time::Instant::now());
        if timeout.is_zero() {
            return Err(AttemptFailure::deadline());
        }

        // Each attempt serves its source through its own route, so a stale
        // attempt can never be answered. The waiter entry is created holding
        // both memory routes: reverse handlers can reach the source mailbox
        // and the pinned destinations as soon as the request ID exists.
        let (read_source, mut source) = read_attachment.source.map(serve_read_source).unzip();
        // Response waiting uses the periodic sweep. A full transport queue
        // separately bounds admission by this same absolute deadline.
        let (msgid, receiver) = ctx.state.waiter.alloc_until(
            response_deadline,
            MemoryRoutes {
                read_source,
                write_target: write_target.cloned(),
            },
        );
        let mut flags = MsgFlags::IsReq;
        if self.use_msgpack {
            flags |= MsgFlags::UseMessagePack;
        }
        let mut meta = MsgMeta {
            method: method_name.into(),
            flags,
            msgid,
            read_regions,
            write_regions,
            // Ship the *effective* budget so the whole downstream call
            // tree inherits the shrunk deadline.
            timeout_ms: wire_timeout_ms(timeout),
        };
        // Publication can precede send completion. Serve reverse requests while
        // sending as well as while waiting for the business response.
        let sending = socket.send(&mut meta, req, &ctx.state);
        if let Err(err) = ReadSourceService::drive_optional(source.as_mut(), sending).await {
            // Closure and expiry can race while waiting for capacity. An
            // exhausted response budget must not start another send attempt.
            if std::time::Instant::now() >= response_deadline {
                return Err(AttemptFailure::deadline());
            }
            return Err(AttemptFailure::send(
                err,
                socket.rdma_remote_device().map(str::to_owned),
            ));
        }
        Ok(SentRequest { receiver, source })
    }
}

/// A successfully sent request attempt, waiting for its response.
struct SentRequest<'a, 's> {
    receiver: Receiver<'a>,
    /// The attempt's borrowed read source, if it attached one.
    source: Option<ReadSourceService<'s>>,
}

impl SentRequest<'_, '_> {
    /// Waits for the response while still answering source requests.
    async fn recv(self) -> Result<Message, Error> {
        let Self {
            receiver,
            mut source,
        } = self;
        ReadSourceService::drive_optional(source.as_mut(), receiver.recv()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Socket, SocketPoolConfig, tcp::TcpSocket};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use ruapc_bufpool::BufferPool;
    use std::{
        future::Future,
        task::{Context as TaskContext, Poll, Waker},
        time::Duration,
    };

    #[test]
    fn read_bandwidth_charge_uses_logical_lengths_unless_overridden() {
        let pool = BufferPool::new(Vec::new());
        let buffers = [4, 0, 7]
            .into_iter()
            .map(|len| {
                let mut buffer = pool.allocate(64).unwrap();
                buffer.set_len(len);
                buffer
            })
            .collect::<Vec<_>>();
        let source = ReadSource::new(&buffers);
        let empty = ReadSource::new(&[]);

        for (source, charge_bytes, expected) in [
            (None, None, 0),
            (Some(&empty), None, 0),
            (Some(&source), None, 11),
            (Some(&source), Some(3), 3),
            (Some(&source), Some(0), 0),
            (None, Some(3), 3),
        ] {
            assert_eq!(
                ReadAttachment::new(source, charge_bytes).charge_bytes,
                expected
            );
        }
    }

    fn connected_context(sender: tokio::sync::mpsc::Sender<bytes::Bytes>) -> Context {
        let config = SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        };
        let base = Context::create(&config).unwrap();
        Context::server_ctx(
            &base.state,
            Socket::TCP(TcpSocket::new(sender)),
            MsgMeta {
                timeout_ms: 30_000,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn forgotten_request_cannot_confirm_or_copy_a_reused_source() {
        let (sender, _queue) = tokio::sync::mpsc::channel(1);
        let ctx = connected_context(sender);
        let client = Client::default();
        let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"read");
        let route = {
            let source = ReadSource::new(std::slice::from_ref(&buffer));
            let mut request = Box::pin(client.ruapc_request::<(), (), Error>(
                &ctx,
                &(),
                ReadAttachment::new(Some(&source), None),
                None,
                "Test/forgotten",
            ));
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            let (route, _) = ctx
                .state
                .waiter
                .read_source(0, ctx.conn_id().unwrap())
                .unwrap();
            std::mem::forget(request);
            route
        };
        // The forgotten future leaves its waiter and mailbox alive, but no
        // borrow prevents reuse and no background task may access these bytes.
        buffer.copy_from_slice(b"next");
        assert!(ctx.state.waiter.contains_message_id(0));
        assert!(!route.is_closed());
        assert_eq!(
            route
                .validate(std::time::Instant::now() + Duration::from_millis(10))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
        assert_eq!(
            route
                .gather(
                    &[crate::CopyOp::new(0, 0, 4)],
                    std::time::Instant::now() + Duration::from_millis(10)
                )
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Timeout
        );
        assert_eq!(buffer.as_slice(), b"next");
        ctx.state
            .waiter
            .expire(std::time::Instant::now() + Duration::from_secs(60));
        assert_eq!(ctx.state.waiter.pending_count(), 0);
    }

    #[tokio::test]
    async fn source_requests_progress_during_send_and_response_then_stop_on_cancel() {
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        sender
            .send(bytes::Bytes::from_static(b"occupied"))
            .await
            .unwrap();
        let ctx = connected_context(sender);
        let client = Client::default();
        let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"read");
        {
            let source = ReadSource::new(std::slice::from_ref(&buffer));
            let mut request = Box::pin(client.ruapc_request::<(), (), Error>(
                &ctx,
                &(),
                ReadAttachment::new(Some(&source), None),
                None,
                "Test/blocked_send",
            ));
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            let (route, deadline) = ctx
                .state
                .waiter
                .read_source(0, ctx.conn_id().unwrap())
                .unwrap();
            let ops = [crate::CopyOp::new(1, 0, 2)];
            let mut copying = Box::pin(route.gather(&ops, deadline));
            assert!(futures_util::poll!(copying.as_mut()).is_pending());
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            assert_eq!(copying.await.unwrap(), b"ea");

            assert_eq!(queue.recv().await.unwrap(), b"occupied"[..]);
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            assert!(queue.try_recv().is_ok());
            let mut validating = Box::pin(route.validate(deadline));
            assert!(futures_util::poll!(validating.as_mut()).is_pending());
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            validating.await.unwrap();

            drop(request);
            assert!(route.is_closed());
            assert_eq!(ctx.state.waiter.pending_count(), 0);
            assert_eq!(
                route.validate(deadline).await.unwrap_err().kind,
                ErrorKind::Timeout
            );
        }
        buffer.fill(0);
        assert_eq!(buffer.as_slice(), [0; 4]);
    }

    #[tokio::test]
    async fn rejected_send_retries_are_bounded_and_release_read_sources() {
        let (sender, queue) = tokio::sync::mpsc::channel(1);
        drop(queue);
        let ctx = connected_context(sender);
        let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"read");
        let buffers = [buffer];
        let source = ReadSource::new(&buffers);
        let client = Client {
            max_retries: 2,
            ..Default::default()
        };
        let error = client
            .ruapc_request::<(), (), Error>(
                &ctx,
                &(),
                ReadAttachment::new(Some(&source), None),
                None,
                "Test/rejected",
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        let (next_id, _receiver) = ctx.state.waiter.alloc(Duration::from_secs(1));
        assert_eq!(next_id, 3, "two retries permit exactly three send attempts");
        assert_eq!(&source.buffers()[0][..], b"read");
    }

    #[tokio::test]
    async fn full_send_queue_observes_parent_deadline_without_retrying() {
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        sender
            .send(bytes::Bytes::from_static(b"occupied"))
            .await
            .unwrap();
        let mut ctx = connected_context(sender);
        ctx.deadline = Some(std::time::Instant::now() + Duration::from_millis(30));
        let client = Client {
            timeout: Duration::from_secs(30),
            max_retries: 10,
            ..Default::default()
        };
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.ruapc_request::<(), (), Error>(
                &ctx,
                &(),
                ReadAttachment::new(None, None),
                None,
                "Test/queue_timeout",
            ),
        )
        .await
        .expect("full queue must not hide the request deadline");
        assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        assert_eq!(queue.recv().await.unwrap(), b"occupied"[..]);
        assert!(
            queue.try_recv().is_err(),
            "expired request was never accepted"
        );
        let (next_id, _receiver) = ctx.state.waiter.alloc(Duration::from_secs(1));
        assert_eq!(
            next_id, 1,
            "queue timeout must be terminal, despite max_retries"
        );
    }

    #[tokio::test]
    async fn closure_after_response_deadline_does_not_retry_send() {
        #[derive(JsonSchema)]
        struct CloseAtDeadline<'a> {
            #[schemars(skip)]
            on_serialize: &'a dyn Fn(),
        }

        impl Serialize for CloseAtDeadline<'_> {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                (self.on_serialize)();
                serializer.serialize_unit()
            }
        }

        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        let ctx = connected_context(sender);
        let ContextEndpoint::Connected(Socket::TCP(socket)) = &ctx.endpoint else {
            unreachable!();
        };
        let serializations = std::cell::Cell::new(0);
        let close_at_deadline = || {
            serializations.set(serializations.get() + 1);
            let deadline = ctx
                .state
                .waiter
                .deadline_on_connection(0, socket.conn_id())
                .expect("serialization runs with a bound response waiter");
            socket.mark_closed();
            std::thread::sleep(deadline.saturating_duration_since(std::time::Instant::now()));
        };
        let payload = CloseAtDeadline {
            on_serialize: &close_at_deadline,
        };
        let client = Client {
            timeout: Duration::from_millis(20),
            max_retries: 10,
            ..Default::default()
        };
        let error = client
            .ruapc_request::<_, (), Error>(
                &ctx,
                &payload,
                ReadAttachment::new(None, None),
                None,
                "Test/close_at_deadline",
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(serializations.get(), 1);
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        assert!(
            queue.try_recv().is_err(),
            "expired request was never accepted"
        );
        let (next_id, _receiver) = ctx.state.waiter.alloc(Duration::from_secs(1));
        assert_eq!(
            next_id, 1,
            "closure must not restart an exhausted response budget"
        );
    }

    #[tokio::test]
    async fn accepted_request_connection_failure_is_not_retried() {
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        let ctx = connected_context(sender);
        let client = Client {
            max_retries: 10,
            ..Default::default()
        };
        let mut request = Box::pin(client.ruapc_request::<(), (), Error>(
            &ctx,
            &(),
            ReadAttachment::new(None, None),
            None,
            "Test/accepted",
        ));
        assert!(
            request
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(
            queue.try_recv().is_ok(),
            "request reached transport acceptance"
        );
        let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
            unreachable!();
        };
        ctx.state.waiter.fail_connection(
            socket.conn_id().unwrap(),
            &Error::kind(ErrorKind::ConnectionClosed),
        );
        let error = request.await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        assert!(queue.try_recv().is_err());
        let (next_id, _receiver) = ctx.state.waiter.alloc(Duration::from_secs(1));
        assert_eq!(
            next_id, 1,
            "accepted requests must never enter the retry path"
        );
    }

    #[tokio::test]
    async fn cancelling_queue_admission_removes_waiter_without_sending() {
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        sender
            .send(bytes::Bytes::from_static(b"occupied"))
            .await
            .unwrap();
        let ctx = connected_context(sender);
        let client = Client::default();
        let mut request = Box::pin(client.ruapc_request::<(), (), Error>(
            &ctx,
            &(),
            ReadAttachment::new(None, None),
            None,
            "Test/cancel_admission",
        ));
        assert!(
            request
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(ctx.state.waiter.pending_count(), 1);
        drop(request);
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        assert_eq!(queue.recv().await.unwrap(), b"occupied"[..]);
        assert!(queue.try_recv().is_err());
    }

    #[tokio::test]
    async fn cancelling_a_pending_request_settles_its_metrics() {
        let base = Context::create(&SocketPoolConfig::default()).unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let socket = Socket::TCP(TcpSocket::new(sender));
        let ctx = Context::server_ctx(
            &base.state,
            socket,
            MsgMeta {
                timeout_ms: 30_000,
                ..Default::default()
            },
        );
        let recorder = DebuggingRecorder::new();
        let snapshot = recorder.snapshotter();
        let client = Client::default();
        let mut request = Box::pin(client.ruapc_request::<(), (), Error>(
            &ctx,
            &(),
            ReadAttachment::new(None, None),
            None,
            "Test/pending",
        ));
        metrics::with_local_recorder(&recorder, || {
            assert!(matches!(
                request
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop())),
                Poll::Pending
            ));
            drop(request);
        });
        let values = snapshot.snapshot().into_vec();
        let value = |name| {
            &values
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap()
                .3
        };
        assert!(matches!(
            value("ruapc_client_requests_total"),
            DebugValue::Counter(1)
        ));
        assert!(
            matches!(value("ruapc_client_inflight"), DebugValue::Gauge(value) if value.0 == 0.0)
        );
        assert!(
            matches!(value("ruapc_client_latency_seconds"), DebugValue::Histogram(values) if values.len() == 1)
        );
        assert_eq!(ctx.state.waiter.pending_count(), 0);
    }
}
