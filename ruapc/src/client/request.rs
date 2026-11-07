//! Request execution: validate, acquire/send with retries, receive exactly once.

use bytes::Bytes;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    Buffer, BufferAvailability, Context, Message, Receiver, SocketTrait,
    core::ContextEndpoint,
    error::{Error, ErrorKind},
    msg::{MsgFlags, MsgMeta},
    remote_memory::{ReadSource, WriteTarget},
};

use super::attempt::{
    AttemptCycle, AttemptEndpoint, AttemptFailure, AttemptOptions, acquire_for_attempt,
    capped_deadline, export_attached_regions, wire_timeout_ms,
};

use super::Client;

#[derive(Clone, Copy)]
pub(crate) struct ReadAttachment<'a> {
    source: Option<&'a Arc<ReadSource>>,
    charge_bytes: u64,
}

impl<'a> ReadAttachment<'a> {
    pub(crate) fn new(source: Option<&'a Arc<ReadSource>>, charge_bytes: Option<u64>) -> Self {
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

pub(crate) enum RequestPayload<'a, P> {
    Serialized(&'a P),
    Raw { control: &'a P, data: &'a [Bytes] },
}

/// Local ownership accompanying one logical call, shared by all attempts.
struct Attachments<'a> {
    read: ReadAttachment<'a>,
    write: &'a mut Option<Arc<WriteTarget>>,
    returned: Option<&'a mut BufferAvailability>,
}

impl Client {
    /// Internal raw-memory calls share acquisition, queue admission, waiter
    /// cleanup and the same no-retry-after-acceptance rule as typed calls.
    pub(crate) async fn request_message<Req: Serialize, Rsp>(
        &self,
        ctx: &Context,
        payload: RequestPayload<'_, Req>,
        method: &str,
        decode: impl FnOnce(Message) -> Result<Rsp, Error>,
    ) -> Result<Rsp, Error> {
        self.execute_request(
            ctx,
            payload,
            Attachments {
                read: ReadAttachment::new(None, None),
                write: &mut None,
                returned: None,
            },
            method,
            decode,
        )
        .await
    }

    /// Executes one logical call, including pre-wire retries and metrics.
    /// `write_buffers_slot` receives the target only if it is available and
    /// uniquely held when the response arrives.
    pub(crate) async fn ruapc_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        read_attachment: ReadAttachment<'_>,
        write_target: &mut Option<Arc<WriteTarget>>,
        write_buffers_slot: Option<&mut BufferAvailability>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        self.execute_request(
            ctx,
            RequestPayload::Serialized(req),
            Attachments {
                read: read_attachment,
                write: write_target,
                returned: write_buffers_slot,
            },
            method_name,
            |response| {
                response
                    .payload
                    .deserialize(&response.meta)
                    .map_err(E::from)?
            },
        )
        .await
    }

    async fn execute_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        payload: RequestPayload<'_, Req>,
        attachments: Attachments<'_>,
        method_name: &str,
        decode: impl FnOnce(Message) -> std::result::Result<Rsp, E>,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize,
        E: From<Error>,
    {
        let metrics = ctx.state.metrics.client_method(method_name).start();
        let result = async {
            if ctx.is_expired() {
                return Err(Error::new(
                    ErrorKind::Timeout,
                    "request deadline already expired".to_string(),
                )
                .into());
            }

            let sent = self
                .send_with_retries(
                    ctx,
                    &payload,
                    attachments.read,
                    attachments.write.as_ref(),
                    method_name,
                )
                .await?;
            // Never retry after send: a missing response cannot tell us whether
            // the peer executed the request.
            let (response, returned_target) = sent.receiver.recv().await?;
            // Drop our clone before trying to recover the target. A handler still
            // holding it, or DMA still owning its buffers, prevents recovery.
            let had_attachment = attachments.write.take().is_some();
            if let Some(slot) = attachments.returned {
                *slot = match returned_target.and_then(WriteTarget::try_into_buffers) {
                    Some(buffers) => BufferAvailability::Available(buffers),
                    None if had_attachment => BufferAvailability::Unavailable,
                    None => BufferAvailability::NotAttached,
                };
            }
            if response.meta.flags.contains(MsgFlags::IsFrameworkError) {
                let error = response
                    .payload
                    .deserialize::<Result<(), Error>>(&response.meta)?
                    .err()
                    .unwrap_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidArgument,
                            "framework error response contains a success value".into(),
                        )
                    });
                return Err(error.into());
            }
            decode(response)
        }
        .await;
        if result.is_err() {
            metrics.failed();
        }
        result
    }

    /// Acquires a socket and sends the request, retrying only failures that
    /// provably occur before the request reaches the wire.
    async fn send_with_retries<'a, Req>(
        &self,
        ctx: &'a Context,
        payload: &RequestPayload<'_, Req>,
        read_attachment: ReadAttachment<'_>,
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
    ) -> Result<SentRequest<'a>, Error>
    where
        Req: Serialize,
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
                    payload,
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
        payload: &RequestPayload<'_, Req>,
        read_attachment: ReadAttachment<'_>,
        write_target: Option<&Arc<WriteTarget>>,
        method_name: &str,
        attempt: AttemptOptions<'_>,
    ) -> std::result::Result<SentRequest<'a>, AttemptFailure>
    where
        Req: Serialize,
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
            export_attached_regions(&socket, &ctx.state, read_attachment.buffers(), write_target)?;
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
        if let Some(source) = read_attachment.source {
            ctx.state.waiter.bind_read_source(msgid, source.clone());
        }
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
        let send_result = match payload {
            RequestPayload::Raw { control, data } => {
                socket.send_raw(&mut meta, control, data, &ctx.state).await
            }
            RequestPayload::Serialized(req) => socket.send(&mut meta, req, &ctx.state).await,
        };
        if let Err(err) = send_result {
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
        Ok(SentRequest { receiver })
    }
}

/// A successfully sent request attempt, waiting for its response.
struct SentRequest<'a> {
    receiver: Receiver<'a>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Socket, SocketPoolConfig, msg::frame::SendFrame, tcp::TcpSocket};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use ruapc_bufpool::{BufferPool, EmptyDevices};
    use std::{
        future::Future,
        task::{Context as TaskContext, Poll, Waker},
        time::Duration,
    };

    #[test]
    fn read_bandwidth_charge_uses_logical_lengths_unless_overridden() {
        let pool = BufferPool::new(Arc::new(EmptyDevices));
        let buffers = [4, 0, 7]
            .into_iter()
            .map(|len| {
                let mut buffer = pool.allocate(64).unwrap();
                buffer.set_len(len);
                buffer
            })
            .collect();
        let source = Arc::new(ReadSource::try_new(buffers).unwrap());
        let empty = Arc::new(ReadSource::try_new(Vec::new()).unwrap());

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

    fn connected_context(sender: tokio::sync::mpsc::Sender<SendFrame>) -> Context {
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
    async fn raw_requests_use_selected_codec_and_the_common_response_path() {
        for use_msgpack in [false, true] {
            for framework_error in [false, true] {
                let (sender, mut queue) = tokio::sync::mpsc::channel(1);
                let ctx = connected_context(sender);
                let client = Client {
                    use_msgpack,
                    ..Default::default()
                };
                let parts = [Bytes::from_static(b"raw data")];
                let mut call = Box::pin(client.request_message(
                    &ctx,
                    RequestPayload::Raw {
                        control: &42u64,
                        data: &parts,
                    },
                    "Test/raw",
                    |message| {
                        assert!(
                            !framework_error,
                            "framework errors precede application decoding"
                        );
                        message
                            .payload
                            .deserialize::<Result<u64, Error>>(&message.meta)?
                    },
                ));
                assert!(futures_util::poll!(call.as_mut()).is_pending());
                let frame = queue.try_recv().unwrap().into_bytes();
                let request = Message::parse(frame.slice(ruapc_wire::frame::HEADER_LEN..)).unwrap();
                assert_eq!(
                    request.meta.flags.contains(MsgFlags::UseMessagePack),
                    use_msgpack
                );
                let (control, data) =
                    ruapc_wire::raw::decode::<u64>(&request.payload, request.meta.flags).unwrap();
                assert_eq!(control, 42);
                assert_eq!(data, b"raw data");

                let mut meta = MsgMeta {
                    msgid: request.meta.msgid,
                    flags: MsgFlags::IsRsp | (request.meta.flags & MsgFlags::UseMessagePack),
                    ..Default::default()
                };
                let response = if framework_error {
                    meta.flags.insert(MsgFlags::IsFrameworkError);
                    Err(Error::kind(ErrorKind::Overloaded))
                } else {
                    Ok(7u64)
                };
                let bytes = ruapc_wire::frame::encode(&meta, &response).unwrap();
                ctx.state.waiter.post(
                    meta.msgid,
                    Message::parse(bytes.slice(ruapc_wire::frame::HEADER_LEN..)).unwrap(),
                );
                let result = call.await;
                if framework_error {
                    assert_eq!(result.unwrap_err().kind, ErrorKind::Overloaded);
                } else {
                    assert_eq!(result.unwrap(), 7);
                }
                assert_eq!(ctx.state.waiter.pending_count(), 0);
                assert!(
                    queue.try_recv().is_err(),
                    "accepted calls must not be retried"
                );
            }
        }
    }

    #[tokio::test]
    async fn rejected_send_retries_are_bounded_and_release_read_sources() {
        let (sender, queue) = tokio::sync::mpsc::channel(1);
        drop(queue);
        let ctx = connected_context(sender);
        let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"read");
        let source = Arc::new(ReadSource::try_new(vec![buffer]).unwrap());
        let client = Client {
            max_retries: 2,
            ..Default::default()
        };
        let mut target = None;
        let error = client
            .ruapc_request::<(), (), Error>(
                &ctx,
                &(),
                ReadAttachment::new(Some(&source), None),
                &mut target,
                None,
                "Test/rejected",
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        let (next_id, _receiver) = ctx.state.waiter.alloc(Duration::from_secs(1));
        assert_eq!(next_id, 3, "two retries permit exactly three send attempts");
        let source = Arc::try_unwrap(source).expect("failed attempts must release their source");
        assert_eq!(&source.buffers()[0][..], b"read");
    }

    #[tokio::test]
    async fn full_send_queue_observes_parent_deadline_without_retrying() {
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        sender
            .send(bytes::Bytes::from_static(b"occupied").into())
            .await
            .unwrap();
        let mut ctx = connected_context(sender);
        ctx.deadline = Some(std::time::Instant::now() + Duration::from_millis(30));
        let client = Client {
            timeout: Duration::from_secs(30),
            max_retries: 10,
            ..Default::default()
        };
        let mut target = None;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.ruapc_request::<(), (), Error>(
                &ctx,
                &(),
                ReadAttachment::new(None, None),
                &mut target,
                None,
                "Test/queue_timeout",
            ),
        )
        .await
        .expect("full queue must not hide the request deadline");
        assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(ctx.state.waiter.pending_count(), 0);
        assert_eq!(queue.recv().await.unwrap().into_bytes(), b"occupied"[..]);
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
        let mut target = None;
        let error = client
            .ruapc_request::<_, (), Error>(
                &ctx,
                &payload,
                ReadAttachment::new(None, None),
                &mut target,
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
        let mut target = None;
        let mut request = Box::pin(client.ruapc_request::<(), (), Error>(
            &ctx,
            &(),
            ReadAttachment::new(None, None),
            &mut target,
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
            .send(bytes::Bytes::from_static(b"occupied").into())
            .await
            .unwrap();
        let ctx = connected_context(sender);
        let client = Client::default();
        let mut target = None;
        let mut request = Box::pin(client.ruapc_request::<(), (), Error>(
            &ctx,
            &(),
            ReadAttachment::new(None, None),
            &mut target,
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
        assert_eq!(queue.recv().await.unwrap().into_bytes(), b"occupied"[..]);
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
        let mut target = None;
        let mut request = Box::pin(client.ruapc_request::<(), (), Error>(
            &ctx,
            &(),
            ReadAttachment::new(None, None),
            &mut target,
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

    #[tokio::test]
    async fn framework_error_recovers_write_buffers_before_converting_custom_error() {
        #[derive(Debug, Deserialize)]
        struct ApplicationError {
            framework: Error,
        }

        impl std::fmt::Display for ApplicationError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.framework.fmt(f)
            }
        }

        impl std::error::Error for ApplicationError {}

        impl From<Error> for ApplicationError {
            fn from(framework: Error) -> Self {
                Self { framework }
            }
        }

        for use_msgpack in [false, true] {
            let (sender, mut queue) = tokio::sync::mpsc::channel(1);
            let ctx = connected_context(sender);
            let mut buffer = ctx.state.buffer_pool.allocate(8).unwrap();
            buffer.set_len(8);
            buffer.copy_from_slice(b"original");
            let pointer = buffer.as_ptr();
            let client = Client {
                use_msgpack,
                ..Default::default()
            };
            let attached = client.with_write_buffers(vec![buffer]);
            let mut call = Box::pin(attached.ruapc_request::<(), (), ApplicationError>(
                &ctx,
                &(),
                None,
                "Test/framework_with_target",
            ));
            assert!(futures_util::poll!(call.as_mut()).is_pending());
            let frame = queue.try_recv().unwrap().into_bytes();
            let request = Message::parse(frame.slice(ruapc_wire::frame::HEADER_LEN..)).unwrap();
            assert_eq!(request.meta.write_regions.len(), 1);
            assert_eq!(ctx.state.waiter.pending_count(), 1);

            let meta = MsgMeta {
                msgid: request.meta.msgid,
                flags: MsgFlags::IsRsp
                    | MsgFlags::IsFrameworkError
                    | (request.meta.flags & MsgFlags::UseMessagePack),
                ..Default::default()
            };
            let response = Err::<(), Error>(Error::new(
                ErrorKind::Overloaded,
                "admission refused".into(),
            ));
            let frame = ruapc_wire::frame::encode(&meta, &response).unwrap();
            ctx.state.waiter.post(
                meta.msgid,
                Message::parse(frame.slice(ruapc_wire::frame::HEADER_LEN..)).unwrap(),
            );
            let error = call.await.unwrap_err().framework;
            assert_eq!(error.kind, ErrorKind::Overloaded);
            assert_eq!(error.msg, "admission refused");

            let BufferAvailability::Available(buffers) = attached.take_write_buffers() else {
                panic!("framework failure must return an available write target");
            };
            assert_eq!(buffers.len(), 1);
            assert_eq!(buffers[0].as_ptr(), pointer);
            assert_eq!(&buffers[0][..], b"original");
            assert!(matches!(
                attached.take_write_buffers(),
                BufferAvailability::NotAttached
            ));
            assert_eq!(ctx.state.waiter.pending_count(), 0);
            assert!(
                queue.try_recv().is_err(),
                "accepted calls must not be retried"
            );
        }
    }
}
