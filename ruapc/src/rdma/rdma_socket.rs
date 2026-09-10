//! Connection state and the RDMA socket send path.

mod read;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use ruapc_rdma::ibv_send_flags;
use serde::Serialize;
use tokio::sync::{Notify, Semaphore, mpsc::Sender};

use super::{RdmaBandwidthLimiter, RdmaPathInfo, RdmaState, SendPermit};
use crate::{
    Buffer, BufferPool, Context, CopyOp, Error, RemoteIoError, RemoteSpace, SocketTrait, State,
    error::{ErrorKind, Result},
    msg::MsgMeta,
    rdma::{
        frame::FramedBuffer,
        poller::{PollerWaker, ReservedQueuePair},
    },
};

pub(crate) struct RdmaSocketConfig {
    pub(crate) max_msg_size: usize,
    pub(crate) send_window: u32,
    pub(crate) path: RdmaPathInfo,
    pub(crate) read_timeout: Option<Duration>,
    pub(crate) read_permits: Arc<tokio::sync::Semaphore>,
    pub(crate) bandwidth_limiter: Arc<RdmaBandwidthLimiter>,
    pub(crate) sq_read_cap: u32,
}

/// Owns this QP's READ admission and its share of the NIC-wide limit.
///
/// The socket must declare this owner after its QP. Normal completions return
/// both permits immediately; final destruction returns only the NIC permits
/// for WRs whose completions were never processed, after destroying the QP.
#[derive(Debug)]
pub(crate) struct ReadCredits {
    device: Arc<Semaphore>,
    sq: Semaphore,
    cap: usize,
    closed: Notify,
    /// Captured from the QP during exclusive socket destruction. SQ occupancy
    /// is insufficient: an unposted task may hold SQ capacity while awaiting
    /// the NIC, and forgetting that future need not keep the socket alive.
    unpolled_on_drop: usize,
}

impl ReadCredits {
    fn new(device: Arc<Semaphore>, cap: u32) -> Self {
        let cap = cap.max(1) as usize;
        Self {
            device,
            sq: Semaphore::new(cap),
            cap,
            closed: Notify::new(),
            unpolled_on_drop: 0,
        }
    }

    /// Called once for each authentic READ completion, including errors.
    pub(crate) fn complete(&self) {
        self.device.add_permits(1);
        self.sq.add_permits(1);
    }

    pub(crate) fn close(&self) {
        self.sq.close();
        self.closed.notify_waiters();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.sq.is_closed()
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.sq.available_permits() == self.cap
    }
}

impl Drop for ReadCredits {
    fn drop(&mut self) {
        // The QP's successful destruction has ended DMA. Forgotten unposted
        // futures may leak their own permits, but must never invent NIC credit.
        if self.unpolled_on_drop != 0 {
            self.device.add_permits(self.unpolled_on_drop);
        }
    }
}

#[derive(Debug)]
pub struct RdmaSocket {
    /// The QP owns posted memory through completion and successful destruction.
    pub(crate) queue_pair: ReservedQueuePair,
    /// Field order ensures unpolled READ permits return only after QP destruction.
    pub(crate) read_credits: ReadCredits,
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
    /// The (local NIC, remote NIC) pair this connection runs on.
    pub(crate) path: RdmaPathInfo,
    /// Process-wide unique connection id (see [`crate::task::next_conn_id`]).
    pub(crate) conn_id: u64,
    /// Aggregate health of the outbound peer this stripe belongs to.
    peer_health: std::sync::OnceLock<std::sync::Weak<super::RdmaPeerHealth>>,
    /// Initiator asks the poll thread to send one accounted immediate-only
    /// SEND after control-plane confirmation.
    activation_requested: AtomicBool,
    /// Server-side accept lease notified by the first successful receive.
    accept_lease_id: AtomicU64,
    /// Shared bandwidth shaper for the local RDMA port.
    bandwidth_limiter: Arc<RdmaBandwidthLimiter>,
    /// Software deadline for RDMA READ completions; `None` disables the
    /// timeout. Enforced by the poll thread's periodic sweep, not by
    /// per-operation timers.
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
            read_credits: ReadCredits::new(config.read_permits, config.sq_read_cap),
            rdmabuf_pool,
            state: RdmaState::new(config.send_window.max(1)),
            pending_sender,
            poller_waker,
            max_msg_size: config.max_msg_size,
            path: config.path,
            conn_id: crate::task::next_conn_id(),
            peer_health: std::sync::OnceLock::new(),
            activation_requested: AtomicBool::new(false),
            accept_lease_id: AtomicU64::new(0),
            bandwidth_limiter: config.bandwidth_limiter,
            read_timeout: config.read_timeout,
        }
    }

    pub(crate) fn set_peer_health(&self, health: &Arc<super::RdmaPeerHealth>) {
        let _ = self.peer_health.set(Arc::downgrade(health));
    }

    pub(crate) fn peer_health(&self) -> Option<std::sync::Weak<super::RdmaPeerHealth>> {
        self.peer_health.get().cloned()
    }

    pub(crate) fn request_activation(&self) {
        self.activation_requested.store(true, Ordering::Release);
        self.poller_waker.wake();
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

    /// Serializes a message into a right-sized framed buffer.
    ///
    /// The serialized size is unknown upfront, so try increasingly larger
    /// buffers (4 KiB → 64 KiB → 256 KiB → negotiated `max_msg_size`).
    /// Typical RPC messages fit the first rung; larger payloads should use
    /// the remote read/write paths.
    fn serialize_msg<P: Serialize>(&self, meta: &MsgMeta, payload: &P) -> Result<Buffer> {
        let mut last_err = None;
        for size in [4 * 1024, 64 * 1024, 256 * 1024, self.max_msg_size] {
            let size = size.min(self.max_msg_size);
            let mut buf = self.rdmabuf_pool.allocate(size)?;
            match meta.serialize_to(payload, &mut FramedBuffer(&mut buf)) {
                Ok(()) => return Ok(buf),
                Err(e) => last_err = Some(e),
            }
            if size == self.max_msg_size {
                break;
            }
        }
        Err(last_err.unwrap_or_else(|| {
            Error::new(
                ErrorKind::SerializeFailed,
                format!(
                    "message exceeds negotiated max_msg_size ({}); use remote read/write for large payloads",
                    self.max_msg_size
                ),
            )
        }))
    }

    pub fn set_error(&self) {
        if !self.state.set_error() {
            return;
        }
        self.read_credits.close();
        let mut attr = ruapc_rdma::ibv_qp_attr {
            qp_state: ruapc_rdma::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        let mask = ruapc_rdma::ibv_qp_attr_mask::IBV_QP_STATE;
        if let Err(err) = self.queue_pair.modify(&mut attr, mask.0 as _) {
            tracing::warn!(conn_id = self.conn_id, local_qp = self.queue_pair.qp_num(), %err,
                "failed to move RDMA queue pair to ERR");
        }
        // Ensure the poll thread notices the error even when the QP had no
        // outstanding work requests to flush.
        self.poller_waker.wake();
    }

    /// Reserves SEND bandwidth before advertising local read buffers to the
    /// peer, which is expected to read the complete logical space.
    pub(crate) async fn reserve_send_bandwidth(
        &self,
        bytes: u64,
        request_remaining: Option<Duration>,
    ) -> Result<()> {
        self.bandwidth_limiter
            .reserve_send(bytes, request_remaining)
            .await
    }
}

impl Drop for RdmaSocket {
    fn drop(&mut self) {
        // No posting or completion can still execute through this socket.
        // Snapshot only: refunding here would release admission before the QP
        // is destroyed. Fields then drop in order: QP, followed by credits.
        self.read_credits.unpolled_on_drop = self.queue_pair.pending_read_count();
    }
}

impl SocketTrait for RdmaSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
        let buf = self.serialize_msg(meta, payload)?;

        // Bind the pending request to this connection so it fails eagerly
        // if the connection dies before the response arrives.
        if meta.is_req() {
            state.waiter.bind_connection(meta.msgid, self.conn_id);
        }

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
                        .send_signaled(buf, ibv_send_flags::IBV_SEND_SIGNALED)
                } else {
                    self.queue_pair.send(buf, ibv_send_flags::IBV_SEND_SIGNALED)
                };
                posted.map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
                Ok(())
            }
            SendPermit::Full => {
                // Window exhausted: hand the framed message to the poll
                // thread, which flushes (and opportunistically aggregates)
                // pending sends as credits free up.
                self.pending_sender
                    .send(buf)
                    .await
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
                // The poll thread may be sleeping; enqueueing a pending send
                // produces no completion event, so wake it explicitly.
                self.poller_waker.wake();
                Ok(())
            }
            SendPermit::Error => Err(ErrorKind::RdmaSendFailed.into()),
        }
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
