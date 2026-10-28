//! Connection state and the RDMA socket send path.

mod encode;
mod large;
mod read;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use ruapc_rdma::ibv_send_flags;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::{RdmaBandwidthLimiter, RdmaPathInfo, RdmaState, SendPermit};
use crate::{
    Buffer, BufferPool, Context, CopyOp, Error, RemoteIoError, RemoteSpace, SocketTrait, State,
    error::{ErrorKind, Result},
    msg::MsgMeta,
    rdma::{
        poller::{PollerWaker, ReservedQueuePair},
        rdma_service::is_payload_method,
    },
    sockets::{SendRequest, reserve_send},
    task::next_conn_id,
};

pub(crate) struct RdmaSocketConfig {
    pub(crate) max_msg_size: usize,
    pub(crate) max_large_msg_size: usize,
    pub(crate) max_inflight_large_bytes: usize,
    pub(crate) send_window: u32,
    pub(crate) path: RdmaPathInfo,
    pub(crate) read_timeout: Option<Duration>,
    pub(crate) read_permits: Arc<tokio::sync::Semaphore>,
    pub(crate) bandwidth_limiter: Arc<RdmaBandwidthLimiter>,
    pub(crate) sq_read_cap: u32,
}

pub(crate) use read::ReadCredits;

#[derive(Debug)]
pub struct RdmaSocket {
    /// The QP owns posted memory through completion and successful destruction.
    pub(crate) queue_pair: ReservedQueuePair,
    // The provider QP must be destroyed before unconfirmed remote READ
    // sources are released. Never move this field ahead of queue_pair.
    large: large::LargeTransfers,
    /// Field order ensures unpolled READ permits return only after QP destruction.
    pub(crate) read_credits: ReadCredits,
    pending_reads: read::PendingReads,
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    pub(crate) state: RdmaState,
    /// Window-blocked framed sends, flushed by the poll thread once
    /// credits free up.
    pub(crate) pending_sender: Sender<Buffer>,
    /// Wakes the device poll thread (pending sends, error teardown).
    pub(crate) poller_waker: PollerWaker,
    /// Negotiated maximum serialized message size (= the peer's receive
    /// buffer size).
    pub(crate) max_msg_size: usize,
    max_large_msg_size: usize,
    /// The (local NIC, remote NIC) pair this connection runs on.
    pub(crate) path: RdmaPathInfo,
    /// Process-wide unique connection id (see [`crate::task::next_conn_id`]).
    pub(crate) conn_id: u64,
    /// Initiator asks the poll thread to send one accounted immediate-only
    /// SEND after control-plane confirmation.
    activation_requested: AtomicBool,
    /// Published by the initiator, or confirmed and activated by the acceptor.
    probe_ready: AtomicBool,
    /// Server-side accept lease notified by the first successful receive.
    accept_lease_id: AtomicU64,
    /// Shared bandwidth shaper for the local RDMA port.
    bandwidth_limiter: Arc<RdmaBandwidthLimiter>,
    /// Software budget for READ admission and completion. `None` disables
    /// this budget, but not request deadlines. Completion expiry uses the
    /// poll thread's sweep; only contended admission creates a timer.
    read_timeout: Option<Duration>,
}

impl RdmaSocket {
    pub(crate) fn new(
        queue_pair: ReservedQueuePair,
        rdmabuf_pool: Arc<BufferPool>,
        pending_sender: Sender<Buffer>,
        poller_waker: PollerWaker,
        config: RdmaSocketConfig,
    ) -> Self {
        Self {
            queue_pair,
            large: large::LargeTransfers::new(config.max_inflight_large_bytes),
            read_credits: ReadCredits::new(config.read_permits, config.sq_read_cap),
            pending_reads: read::PendingReads::default(),
            rdmabuf_pool,
            state: RdmaState::new(config.send_window.max(1)),
            pending_sender,
            poller_waker,
            max_msg_size: config.max_msg_size,
            max_large_msg_size: config.max_large_msg_size,
            path: config.path,
            conn_id: next_conn_id(),
            activation_requested: AtomicBool::new(false),
            probe_ready: AtomicBool::new(false),
            accept_lease_id: AtomicU64::new(0),
            bandwidth_limiter: config.bandwidth_limiter,
            read_timeout: config.read_timeout,
        }
    }

    /// Start an outbound connection after publication has disarmed rollback.
    pub(crate) fn activate_published(&self) {
        self.request_activation();
        self.enable_probes();
    }

    pub(crate) fn request_activation(&self) {
        self.activation_requested.store(true, Ordering::Release);
        self.poller_waker.wake();
    }

    pub(crate) fn enable_probes(&self) {
        self.probe_ready.store(true, Ordering::Release);
    }

    pub(crate) fn is_probe_ready(&self) -> bool {
        self.probe_ready.load(Ordering::Acquire)
    }

    pub(crate) fn take_activation_request(&self) -> bool {
        self.activation_requested.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn set_accept_lease(&self, connection_id: u64) {
        debug_assert_ne!(connection_id, 0);
        self.accept_lease_id.store(connection_id, Ordering::Release);
    }

    pub(crate) fn take_accept_lease(&self) -> Option<u64> {
        if self.accept_lease_id.load(Ordering::Acquire) == 0 {
            return None;
        }
        match self.accept_lease_id.swap(0, Ordering::AcqRel) {
            0 => None,
            connection_id => Some(connection_id),
        }
    }

    pub fn set_error(&self) {
        if !self.state.set_error() {
            return;
        }
        self.read_credits.close();
        // Notify callers even if the provider never produces flush CQEs.
        // Failure notification preserves posted buffers and READ permits.
        self.fail_read_batches();
        self.large.close();
        // The poll thread moves the QP to ERR, serialized with receive
        // reposting. A receive posted after the provider's ERR flush can
        // otherwise remain outstanding indefinitely (notably on RXE).
        self.poller_waker.wake();
    }

    /// Reserves SEND bandwidth before advertising local read buffers to the
    /// peer, which is expected to read the complete logical space.
    pub(crate) async fn reserve_send_bandwidth(
        &self,
        bytes: u64,
        request_deadline: Option<Instant>,
    ) -> Result<()> {
        self.bandwidth_limiter
            .reserve_send(bytes, request_deadline)
            .await
    }
}

impl Drop for RdmaSocket {
    fn drop(&mut self) {
        // No posting or completion can still execute through this socket.
        // Snapshot only: refunding here would release admission before the QP
        // is destroyed. Fields then drop in order: QP, large transfers, credits.
        self.read_credits
            .record_unpolled(self.queue_pair.pending_read_count());
    }
}

impl SocketTrait for Arc<RdmaSocket> {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
        // Only the transport can promise that its READs have settled. Public
        // raw calls must not fabricate offer/finish requests on a live QP.
        if meta.is_req() && is_payload_method(&meta.method) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA payload RPCs may only be sent by the transport".into(),
            ));
        }
        let request = SendRequest::bind(meta, &state.waiter, self.conn_id);
        if !self.state.is_ok() {
            return Err(ErrorKind::ConnectionClosed.into());
        }
        let request = request?;
        let encoded = self.serialize_msg(meta, payload)?;
        if !self.state.is_ok() {
            return Err(ErrorKind::ConnectionClosed.into());
        }
        if encoded.buffer.len() > self.max_msg_size {
            request.check()?;
            // Keep the rendezvous state out of every inline send's future.
            return Box::pin(self.send_large(meta, encoded, &request, state)).await;
        }
        self.send_buffer(meta, encoded, &request).await
    }

    async fn remote_read(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
        remote: &RemoteSpace<'_>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        self.read_remote(ctx, ops, local, remote).await
    }

    async fn remote_write(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        self.write_remote(ctx, ops, local).await
    }
}

impl RdmaSocket {
    /// On error no bytes have been accepted. After success all subsequent
    /// failures belong to the connection/waiter, never the retryable send path.
    async fn send_buffer(
        &self,
        meta: &mut MsgMeta,
        mut encoded: encode::Encoded,
        request: &SendRequest<'_>,
    ) -> Result<()> {
        // Do not consume a send-window credit for an already expired request.
        request.check()?;
        match self.state.try_acquire() {
            SendPermit::Granted { window_tail } => {
                // Invariant: a fully consumed send window must contain at
                // least one signaled WR, otherwise its slots stay stranded
                // (unsignaled completions are only swept by later signaled
                // ones) and the connection stalls until the 5s keepalive.
                // Direct sends within the window make the window-tail send
                // signaled; pending flushes (the other way credits get
                // consumed) are always signaled by the poll thread.
                let posted = if window_tail {
                    self.queue_pair
                        .send_signaled(encoded.buffer, ibv_send_flags::IBV_SEND_SIGNALED)
                } else {
                    self.queue_pair
                        .send(encoded.buffer, ibv_send_flags::IBV_SEND_SIGNALED)
                };
                posted.map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
                Ok(())
            }
            SendPermit::Full => {
                // Window exhausted: hand the framed message to the poll
                // thread, which flushes (and opportunistically aggregates)
                // pending sends as credits free up.
                let slot = reserve_send(&self.pending_sender, request.deadline(), || {
                    self.read_credits.closed()
                })
                .await?;
                if !self.state.is_ok() {
                    return Err(ErrorKind::ConnectionClosed.into());
                }
                let old_timeout = meta.timeout_ms;
                request.refresh(meta)?;
                if meta.timeout_ms != old_timeout {
                    encoded.refresh_timeout(meta)?;
                }
                request.check()?;
                if !self.state.is_ok() {
                    return Err(ErrorKind::ConnectionClosed.into());
                }
                slot.send(encoded.buffer);
                // The poll thread may be sleeping; enqueueing a pending send
                // produces no completion event, so wake it explicitly.
                self.poller_waker.wake();
                Ok(())
            }
            SendPermit::Error => Err(ErrorKind::ConnectionClosed.into()),
        }
    }
}

#[cfg(test)]
mod tests;
