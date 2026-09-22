//! Request execution: validate, acquire/send with retries, receive exactly once.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    Buffer, Context, MAX_REGIONS, SocketTrait,
    core::{ContextEndpoint, EndpointState},
    error::{Error, ErrorKind},
    msg::{MsgFlags, MsgMeta},
    remote_memory::{ReadSource, WriteTarget},
    sockets::AcquireOptions,
};

use super::attempt::{
    AcquiredSocket, AttemptCycle, AttemptEndpoint, AttemptFailure, AttemptOptions, acquire_direct,
    acquire_for_attempt, capped_deadline, export_attached_regions, is_connection_failure,
    wire_timeout_ms,
};

use super::Client;

#[derive(Clone, Copy)]
pub(crate) struct ReadAttachment<'a> {
    source: Option<&'a Arc<ReadSource>>,
    charge_bytes: Option<u64>,
}

impl<'a> ReadAttachment<'a> {
    pub(crate) const fn new(
        source: Option<&'a Arc<ReadSource>>,
        charge_bytes: Option<u64>,
    ) -> Self {
        Self {
            source,
            charge_bytes,
        }
    }

    fn buffers(self) -> &'a [Buffer] {
        self.source.map_or(&[], |source| source.buffers.as_slice())
    }
}

impl Client {
    /// Executes one logical call, including pre-wire retries and metrics.
    /// `write_buffers_slot` receives the target only if it is available and
    /// uniquely held when the response arrives.
    pub(crate) async fn ruapc_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        read_attachment: ReadAttachment<'_>,
        write_target: &mut Option<Arc<WriteTarget>>,
        write_buffers_slot: Option<&mut Vec<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<crate::Error> + for<'c> Deserialize<'c>,
    {
        let metrics = ctx.state.metrics.client_method(method_name).start();
        let result = self
            .request_inner(
                ctx,
                req,
                read_attachment,
                write_target,
                write_buffers_slot,
                method_name,
            )
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
        write_target: &mut Option<Arc<WriteTarget>>,
        write_buffers_slot: Option<&mut Vec<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<crate::Error> + for<'c> Deserialize<'c>,
    {
        if ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "request deadline already expired".to_string(),
            )
            .into());
        }

        if read_attachment.buffers().len() > MAX_REGIONS {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!(
                    "too many read buffers: {} (limit {MAX_REGIONS})",
                    read_attachment.buffers().len()
                ),
            )
            .into());
        }

        let (sent, endpoint_state) = self
            .send_with_retries(
                ctx,
                req,
                read_attachment,
                write_target.as_ref(),
                method_name,
            )
            .await?;
        // Never retry after send: a missing response cannot tell us whether
        // the peer executed the request.
        let (response, returned_target) = match sent.receiver.recv().await {
            Ok(response) => response,
            Err(err) => {
                if matches!(err.kind, ErrorKind::ConnectionClosed)
                    && let Some(state) = &endpoint_state
                    && let Some(conn_id) = sent.conn_id
                {
                    state.record_connection_failure(conn_id);
                }
                return Err(err.into());
            }
        };
        if let Some(state) = &endpoint_state
            && let Some(conn_id) = sent.conn_id
        {
            state.record_request_success(conn_id);
        }
        // Drop our clone before trying to recover the target. A handler still
        // holding it, or DMA still owning its buffers, prevents recovery.
        drop(write_target.take());
        if let Some(slot) = write_buffers_slot {
            *slot = returned_target
                .and_then(WriteTarget::try_into_buffers)
                .unwrap_or_default();
        }
        response.payload.deserialize(&response.meta)?
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
    ) -> Result<(SentRequest<'a>, Option<Arc<EndpointState>>), Error>
    where
        Req: Serialize + JsonSchema,
    {
        let connect_deadline = capped_deadline(
            std::time::Instant::now(),
            self.connect_timeout,
            ctx.deadline,
        );
        let mut cycle = AttemptCycle::new(
            self.initial_candidates(ctx)?,
            u64::from(self.max_retries) + 1,
        );
        loop {
            let endpoint_state = cycle.next_candidate();
            let endpoint = match (&ctx.endpoint, endpoint_state.clone()) {
                (ContextEndpoint::Connected(socket), _) => {
                    AttemptEndpoint::Connected(socket.clone())
                }
                (ContextEndpoint::Endpoints(_), Some(state)) => AttemptEndpoint::Endpoint(state),
                (ContextEndpoint::Endpoints(_), None) => {
                    unreachable!("an endpoint candidate was selected before try_send")
                }
                (ContextEndpoint::Invalid, _) => {
                    unreachable!("request endpoint was validated before try_send")
                }
            };
            let attempted_addr = endpoint_state.as_ref().map(|state| state.endpoint().addr());
            let result = self
                .try_send(
                    ctx,
                    req,
                    read_attachment,
                    write_target,
                    method_name,
                    AttemptOptions {
                        endpoint,
                        connect_deadline,
                        remaining_acquire_attempts: cycle.remaining_attempts(),
                        avoided_remote_nics: cycle.avoided_remote_nics(attempted_addr),
                    },
                )
                .await;
            match result {
                Ok(sent) => return Ok((sent, endpoint_state)),
                Err(failure) => {
                    let attempt = cycle.attempt_index();
                    if !cycle.note_failure(endpoint_state.as_ref(), &failure) {
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

    /// Validates the context's destination and resolves the ranked
    /// endpoint candidates (empty for an already-connected context);
    /// alternatives beyond the first start connecting in the background.
    fn initial_candidates(&self, ctx: &Context) -> Result<Vec<Arc<EndpointState>>, Error> {
        match &ctx.endpoint {
            ContextEndpoint::Invalid => Err(Error::new(
                ErrorKind::InvalidArgument,
                "client context without address".to_string(),
            )),
            ContextEndpoint::Connected(_) => Ok(Vec::new()),
            ContextEndpoint::Endpoints(set) => {
                let candidates = set.candidates(&ctx.state);
                if candidates.is_empty() {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "client context with empty endpoint set".to_string(),
                    ));
                }
                self.preconnect(ctx, &candidates[1..]);
                Ok(candidates)
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
        let AcquiredSocket {
            socket,
            endpoint_state,
        } = acquire_for_attempt(
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
        let timeout = response_deadline.saturating_duration_since(now);
        if timeout.is_zero() {
            return Err(AttemptFailure::deadline());
        }

        let (read_regions, write_regions) =
            export_attached_regions(&socket, &ctx.state, read_attachment.buffers(), write_target)?;
        let read_bytes = read_attachment
            .charge_bytes
            .unwrap_or_else(|| read_regions.iter().map(|region| region.len).sum::<u64>());
        if let Err(err) = socket
            .reserve_rdma_send_bandwidth(read_bytes, Some(timeout))
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

        // The waiter entry expires after `timeout` (coarse, swept
        // periodically); no per-request timer is registered.
        let (msgid, receiver) = ctx.state.waiter.alloc(timeout);
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
        if let Err(err) = socket.send(&mut meta, req, &ctx.state).await {
            if is_connection_failure(&err)
                && let Some(state) = &endpoint_state
                && let Some(conn_id) = socket.conn_id()
            {
                state.record_connection_failure(conn_id);
            }
            return Err(AttemptFailure::send(
                err,
                socket.rdma_remote_device().map(str::to_owned),
            ));
        }
        Ok(SentRequest {
            receiver,
            conn_id: socket.conn_id(),
        })
    }

    /// Starts background connections to alternative endpoints so a later
    /// failover finds them established.
    fn preconnect(&self, ctx: &Context, candidates: &[Arc<EndpointState>]) {
        let supervisor = ctx.state.socket_pool.task_supervisor_handle();
        for endpoint_state in candidates {
            let Some(activity) = endpoint_state.try_begin_preconnect() else {
                continue;
            };
            let state = ctx.state.clone();
            let endpoint_state = endpoint_state.clone();
            let _ = supervisor.try_spawn(async move {
                let result =
                    acquire_direct(&state, endpoint_state.endpoint(), AcquireOptions::default())
                        .await;
                match &result {
                    Ok(socket) => activity.record_connection(socket),
                    Err(err) if is_connection_failure(err) => activity.record_failure(),
                    Err(_) => {}
                }
                if let Err(err) = result {
                    tracing::debug!(
                        endpoint = %endpoint_state.endpoint(),
                        %err,
                        "background connection failed"
                    );
                }
            });
        }
    }
}

/// A successfully sent request attempt, waiting for its response.
struct SentRequest<'a> {
    receiver: crate::Receiver<'a>,
    conn_id: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::{
        future::Future,
        task::{Context as TaskContext, Poll, Waker},
    };

    #[tokio::test]
    async fn cancelling_a_pending_request_settles_its_metrics() {
        let base = Context::create(&crate::SocketPoolConfig::default()).unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let socket = crate::Socket::TCP(crate::tcp::TcpSocket::new(sender));
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
}
