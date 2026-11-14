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
    Buffer, BufferPool, Error, State,
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
    poller_waker: PollerWaker,
    /// Set while this connection is queued for poll-thread maintenance.
    pub(crate) maintenance_queued: AtomicBool,
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
    /// Set by the poll thread on the first successful SEND or receive
    /// completion. RTS programs only local state, so a connection that fails
    /// before this point usually ran on an unreachable NIC pair.
    path_verified: AtomicBool,
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
            maintenance_queued: AtomicBool::new(false),
            max_msg_size: config.max_msg_size,
            max_large_msg_size: config.max_large_msg_size,
            path: config.path,
            conn_id: next_conn_id(),
            activation_requested: AtomicBool::new(false),
            probe_ready: AtomicBool::new(false),
            accept_lease_id: AtomicU64::new(0),
            path_verified: AtomicBool::new(false),
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
        self.wake_poller();
    }

    /// Schedules poll-thread maintenance of this connection. Use after any
    /// change that produces no completion: pending sends, activation, errors.
    pub(crate) fn wake_poller(&self) {
        self.poller_waker
            .wake_conn(self.queue_pair.qp_num(), &self.maintenance_queued);
    }

    pub(crate) fn enable_probes(&self) {
        self.probe_ready.store(true, Ordering::Release);
    }

    pub(crate) fn is_probe_ready(&self) -> bool {
        self.probe_ready.load(Ordering::Acquire)
    }

    /// Runs on every maintenance pass; the plain load keeps the common
    /// no-request case free of a locked read-modify-write.
    pub(crate) fn take_activation_request(&self) -> bool {
        self.activation_requested.load(Ordering::Acquire)
            && self.activation_requested.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn set_accept_lease(&self, connection_id: u64) {
        debug_assert_ne!(connection_id, 0);
        self.accept_lease_id.store(connection_id, Ordering::Release);
    }

    pub(crate) fn mark_path_verified(&self) {
        if !self.path_verified.load(Ordering::Relaxed) {
            self.path_verified.store(true, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(crate) fn reset_path_verified(&self) {
        self.path_verified.store(false, Ordering::Release);
    }

    /// Whether traffic has completed successfully on this NIC pair.
    pub(crate) fn is_path_verified(&self) -> bool {
        self.path_verified.load(Ordering::Acquire)
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
        self.wake_poller();
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

impl RdmaSocket {
    pub(crate) async fn send<P: Serialize>(
        self: &Arc<Self>,
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
                // The window credit cannot be refunded: a rejected WR never
                // completes or gets acknowledged, so the window would shrink
                // permanently. Fail the connection like the poller's posts do;
                // the request was not accepted and remains retryable.
                posted.map_err(|e| {
                    self.set_error();
                    Error::new(ErrorKind::RdmaSendFailed, e.to_string())
                })?;
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
                self.wake_poller();
                Ok(())
            }
            SendPermit::Error => Err(ErrorKind::ConnectionClosed.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Semaphore;

    #[tokio::test]
    async fn queue_reservation_wakes_on_connection_close_without_closing_shared_nic() {
        let device = Arc::new(Semaphore::new(2));
        let credits = ReadCredits::new(device.clone(), 1);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender.try_send(1).unwrap();
        let acquiring = reserve_send(&sender, None, || credits.closed());
        tokio::pin!(acquiring);
        assert!(futures_util::poll!(&mut acquiring).is_pending());

        credits.close();
        assert_eq!(
            acquiring.await.unwrap_err().kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(receiver.try_recv().unwrap(), 1);
        assert!(receiver.try_recv().is_err());
        assert!(!device.is_closed());
        assert_eq!(device.available_permits(), 2);
    }

    #[tokio::test]
    async fn queue_reservation_observes_connection_closed_before_first_poll() {
        let credits = ReadCredits::new(Arc::new(Semaphore::new(2)), 1);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender.try_send(1).unwrap();
        let acquiring = reserve_send(&sender, None, || credits.closed());
        credits.close();

        assert_eq!(
            acquiring.await.unwrap_err().kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(receiver.try_recv().unwrap(), 1);
        assert!(receiver.try_recv().is_err());
    }
}
