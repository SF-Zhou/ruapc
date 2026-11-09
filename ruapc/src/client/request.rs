//! Request execution: validate, acquire/send with retries, receive exactly once.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(feature = "rdma")]
use crate::Buffer;
use ruapc_bufpool::RemoteBufferInfo;
use ruapc_memory::BorrowedSource;
use thin_vec::ThinVec;

use crate::{
    Context, Receiver, Socket,
    core::ContextEndpoint,
    error::{Error, ErrorKind},
    msg::{MsgFlags, MsgMeta},
    remote_memory::{SourceRequests, SourceSender, WriteTarget},
};

use super::attempt::{
    AttemptCycle, AttemptEndpoint, AttemptFailure, AttemptOptions, acquire_for_attempt,
    capped_deadline, export_attached_regions, wire_timeout_ms,
};

use super::Client;

#[derive(Clone, Copy)]
pub(crate) struct ReadAttachment<'a> {
    source: ReadAttachmentSource<'a>,
    charge_bytes: u64,
    confirm_source: bool,
}

#[derive(Clone, Copy)]
enum ReadAttachmentSource<'a> {
    Shared(Option<&'a BorrowedSource<'a>>),
    /// A direct remote_write exports only metadata; no waiter or local reader
    /// may retain access to these caller-owned buffers.
    #[cfg(feature = "rdma")]
    Remote(&'a [Buffer]),
}

impl<'a> ReadAttachment<'a> {
    pub(crate) fn new(source: Option<&'a BorrowedSource<'a>>, charge_bytes: Option<u64>) -> Self {
        Self {
            source: ReadAttachmentSource::Shared(source),
            charge_bytes: charge_bytes
                .unwrap_or_else(|| source.map_or(0, |source| source.layout().total())),
            confirm_source: true,
        }
    }

    pub(crate) fn with_confirmation(mut self, confirm: bool) -> Self {
        self.confirm_source = confirm;
        self
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn remote_source(source: &'a [Buffer], charge_bytes: u64) -> Self {
        Self {
            source: ReadAttachmentSource::Remote(source),
            charge_bytes,
            confirm_source: false,
        }
    }

    fn shared_source(self) -> Option<&'a BorrowedSource<'a>> {
        match self.source {
            ReadAttachmentSource::Shared(source) => source,
            #[cfg(feature = "rdma")]
            ReadAttachmentSource::Remote(_) => None,
        }
    }

    fn export_regions(
        self,
        socket: &Socket,
        target: Option<&Arc<WriteTarget>>,
    ) -> std::result::Result<(ThinVec<RemoteBufferInfo>, ThinVec<RemoteBufferInfo>), AttemptFailure>
    {
        match self.source {
            ReadAttachmentSource::Shared(source) => {
                export_attached_regions(socket, source.map_or(&[], BorrowedSource::buffers), target)
            }
            #[cfg(feature = "rdma")]
            ReadAttachmentSource::Remote(source) => {
                // Only read_into_target consumes this form. Streams use an
                // owned inline payload rather than delayed source access.
                if !matches!(socket, Socket::RDMA(_)) {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "metadata-only read sources require RDMA".into(),
                    )
                    .into());
                }
                export_attached_regions(socket, source, target)
            }
        }
    }
}

impl Client {
    /// Executes one logical call, including pre-wire retries and metrics.
    /// Settles destination access, or leaves owned destinations with the runtime
    /// after their recovery grace expires.
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
        let mut result = self
            .request_inner(ctx, req, read_attachment, write_target, method_name)
            .await;
        // The waiter has been removed on every terminal path. A reverse RPC
        // may still own the destinations. Strict borrowed storage waits for the
        // exact buffers; owning adapters may abandon recovery after the grace.
        if let Some(target) = write_target {
            let closed = target.close().await;
            if result.is_ok() {
                if let Err(error) = closed {
                    result = Err(error.into());
                } else if let Some(error) = target.failure() {
                    result = Err(error.into());
                }
            }
        }
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

        let SentRequest {
            receiver,
            msgid,
            source_requests,
        } = self
            .send_with_retries(ctx, req, read_attachment, write_target, method_name)
            .await?;
        // Never retry after send. Source access stays in this same future: a
        // forgotten call can retain its mailbox, but cannot answer commands.
        let response = if let Some(mut requests) = source_requests {
            let source = read_attachment
                .shared_source()
                .expect("source mailbox has a borrowed source");
            let response = receiver.recv();
            tokio::pin!(response);
            loop {
                tokio::select! {
                    biased;
                    result = response.as_mut() => break result?,
                    available = requests.serve_next(source, &ctx.state.waiter, msgid) => {
                        if !available {
                            break response.await?;
                        }
                    }
                }
            }
        } else {
            receiver.recv().await?
        };
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
    async fn send_with_retries<'a, Req>(
        &self,
        ctx: &'a Context,
        req: &Req,
        read_attachment: ReadAttachment<'_>,
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
    ) -> Result<SentRequest<'a>, Error>
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
    async fn try_send<'a, Req>(
        &self,
        ctx: &'a Context,
        req: &Req,
        read_attachment: ReadAttachment<'_>,
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
        attempt: AttemptOptions<'_>,
    ) -> std::result::Result<SentRequest<'a>, AttemptFailure>
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
            read_attachment.export_regions(&socket, write_target)?;
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

        // Response waiting uses the periodic sweep. A full transport queue
        // separately bounds admission by this same absolute deadline.
        let (msgid, receiver) = ctx.state.waiter.alloc_until(response_deadline);
        let source_requests = read_attachment.shared_source().and_then(|source| {
            // Empty sources cannot be accessed; avoid a mailbox for destination-only calls.
            if source.layout().total() == 0 {
                return None;
            }
            let (source, requests) =
                SourceSender::channel_with_confirmation(read_attachment.confirm_source);
            ctx.state.waiter.bind_read_source(msgid, source);
            Some(requests)
        });
        if let Some(target) = write_target {
            // Pin the write buffers to the pending request so remote-memory
            // handlers can reach (and keep alive) the destination memory.
            ctx.state.waiter.bind_write_target(msgid, target.clone());
        }
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
        if let Err(err) = socket.send(&mut meta, req, &ctx.state).await {
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
        Ok(SentRequest {
            receiver,
            msgid,
            source_requests,
        })
    }
}

/// A successfully sent request attempt, waiting for its response.
struct SentRequest<'a> {
    receiver: Receiver<'a>,
    msgid: u64,
    source_requests: Option<SourceRequests>,
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
        let source = BorrowedSource::new(&buffers).unwrap();
        let empty = BorrowedSource::new(&[]).unwrap();

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
    async fn rejected_send_retries_are_bounded_and_release_read_sources() {
        let (sender, queue) = tokio::sync::mpsc::channel(1);
        drop(queue);
        let ctx = connected_context(sender);
        let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"read");
        let buffers = [buffer];
        let source = BorrowedSource::new(&buffers).unwrap();
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
        assert_eq!(&buffers[0][..], b"read");
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
