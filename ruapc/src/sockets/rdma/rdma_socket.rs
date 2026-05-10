use std::sync::Arc;

use ruapc_rdma_sys::{QueuePair, WRID, ibv_send_flags, ibv_wc_status};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::{RdmaBufferRef, RdmaState};
use crate::{
    Context, Device, Error, RemoteBufferInfo, SocketTrait, State,
    error::{ErrorKind, Result},
    memory::{Buffer, BufferPool, MemoryKey},
    msg::MsgMeta,
};

/// Payload delivered through the oneshot channel when a one-sided RDMA
/// operation (read/write) completes.
pub(crate) type RdmaCompletion = (ibv_wc_status, Option<RdmaBufferRef>);

#[derive(Debug)]
pub struct RdmaSocket {
    /// Pending RDMA one-sided operation completions (wr_id -> status sender).
    ///
    /// When a one-sided RDMA verb is posted via the tracked QP API, the
    /// buffer's ownership is transferred into the QP's internal `wrs` map.
    /// Upon completion, the event loop retrieves the buffer from the QP and
    /// sends it back through the oneshot channel together with the status.
    pub(crate) rdma_completions:
        dashmap::DashMap<WRID, tokio::sync::oneshot::Sender<RdmaCompletion>>,
    /// Buffers waiting to be sent (not yet posted to QP due to flow control).
    pub(crate) pending_buffers: dashmap::DashMap<u64, RdmaBufferRef>,
    /// Shared buffer pool — its Arc is decremented before QP destruction,
    /// but actual pool cleanup waits until *all* Arcs (from Buffers) are gone.
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    /// The RDMA queue pair — destroyed after rdmabuf_pool Arcs.
    pub(crate) queue_pair: QueuePair<RdmaBufferRef>,
    /// The RDMA device (as the Device enum) this QP belongs to.
    pub(crate) device: Arc<Device>,
    pub(crate) state: RdmaState,
    pub(crate) pending_sender: Sender<u64>,
}

impl RdmaSocket {
    pub fn new(
        queue_pair: QueuePair<RdmaBufferRef>,
        device: Arc<Device>,
        rdmabuf_pool: Arc<BufferPool>,
        pending_sender: Sender<u64>,
    ) -> Self {
        Self {
            rdma_completions: dashmap::DashMap::default(),
            pending_buffers: dashmap::DashMap::default(),
            rdmabuf_pool,
            queue_pair,
            device,
            state: RdmaState::new(32),
            pending_sender,
        }
    }

    pub fn set_error(&self) {
        self.state.set_error();
        // Put QP into error state via modify.
        let mut attr = ruapc_rdma_sys::ibv_qp_attr {
            qp_state: ruapc_rdma_sys::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        let mask = ruapc_rdma_sys::ibv_qp_attr_mask::IBV_QP_STATE;
        let _ = self.queue_pair.modify(&mut attr, mask.0 as _);
    }

    /// Creates an `RdmaBufferRef` for the given buffer on this socket's device.
    pub(crate) fn make_buffer_ref(&self, buffer: Buffer) -> Option<RdmaBufferRef> {
        RdmaBufferRef::new(buffer, &self.device)
    }

    /// Posts a one-sided RDMA read or write verb using the **tracked** QP API.
    ///
    /// The buffer's ownership is transferred into the QP's internal work-request
    /// map. When the completion arrives, the event loop retrieves the buffer and
    /// sends it back through the returned oneshot channel.
    fn post_rdma_verb(
        &self,
        buf_ref: RdmaBufferRef,
        remote_addr: u64,
        rkey: u32,
        is_read: bool,
    ) -> Result<tokio::sync::oneshot::Receiver<RdmaCompletion>> {
        let wr_id = if is_read {
            self.queue_pair.read(
                buf_ref,
                remote_addr,
                rkey,
                ibv_send_flags::IBV_SEND_SIGNALED,
            )
        } else {
            self.queue_pair.write(
                buf_ref,
                remote_addr,
                rkey,
                ibv_send_flags::IBV_SEND_SIGNALED,
            )
        }
        .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.rdma_completions.insert(wr_id, tx);
        Ok(rx)
    }

    /// Awaits a one-sided RDMA operation completion and returns the buffer.
    ///
    /// The buffer ownership was previously transferred to the RDMA device
    /// (QP's `wrs` map) and is now returned after the operation completes.
    ///
    /// On failure the buffer is still recovered (and will be dropped, returning
    /// it to the pool) to avoid resource leaks.
    async fn await_completion(
        rx: tokio::sync::oneshot::Receiver<RdmaCompletion>,
    ) -> Result<RdmaBufferRef> {
        let (status, buffer) = rx.await.map_err(|_| {
            Error::new(
                ErrorKind::RdmaSendFailed,
                "RDMA completion channel closed".into(),
            )
        })?;
        // Always take the buffer back first to avoid leaking it on error.
        let buf_ref = buffer.ok_or_else(|| {
            Error::new(
                ErrorKind::RdmaSendFailed,
                "RDMA completion did not return buffer".into(),
            )
        })?;
        if status != ibv_wc_status::IBV_WC_SUCCESS {
            // buf_ref is dropped here → Buffer returns to the pool.
            return Err(Error::new(
                ErrorKind::RdmaSendFailed,
                format!("RDMA operation failed with status {status:?}"),
            ));
        }
        Ok(buf_ref)
    }
}

impl RdmaSocket {
    /// Performs a one-sided RDMA operation (read or write).
    ///
    /// Transfers the local buffer's ownership to the QP, posts the verb,
    /// awaits completion, and returns the buffer.
    async fn remote_op(
        &self,
        local: Buffer,
        remote: &RemoteBufferInfo,
        is_read: bool,
    ) -> Result<Buffer> {
        let rkey = match remote.key {
            MemoryKey::Rdma { rkey, .. } => rkey,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "RDMA remote_{} requires an RDMA memory key",
                        if is_read { "read" } else { "write" }
                    ),
                ));
            }
        };

        let buf_ref = self
            .make_buffer_ref(local)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::RdmaSendFailed,
                    "buffer not registered on RDMA device".into(),
                )
            })?
            .with_len(remote.len as usize);

        let rx = self.post_rdma_verb(buf_ref, remote.addr, rkey, is_read)?;
        Ok(Self::await_completion(rx).await?.into_buffer())
    }
}

impl SocketTrait for RdmaSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        _: &Arc<State>,
    ) -> Result<()> {
        let mut buf = self.rdmabuf_pool.allocate()?;
        meta.serialize_to(payload, &mut buf)?;

        let buf_ref = self.make_buffer_ref(buf).ok_or_else(|| {
            Error::new(
                ErrorKind::RdmaSendFailed,
                "buffer not registered on QP device".into(),
            )
        })?;

        let index = self.state.apply_send_index();
        if index.is_ok() {
            if self.state.ready_to_send(index) {
                self.queue_pair
                    .send(buf_ref, ibv_send_flags::IBV_SEND_SIGNALED)
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
            } else {
                // Store the buffer for later sending when flow control allows.
                self.pending_buffers.insert(meta.msgid, buf_ref);
                self.pending_sender
                    .send(meta.msgid)
                    .await
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
            }
            Ok(())
        } else {
            Err(ErrorKind::RdmaSendFailed.into())
        }
    }

    async fn remote_read(
        &self,
        _ctx: &Context,
        local: Buffer,
        remote: &RemoteBufferInfo,
    ) -> Result<Buffer> {
        self.remote_op(local, remote, true).await
    }

    async fn remote_write(
        &self,
        _ctx: &Context,
        local: Buffer,
        remote: &RemoteBufferInfo,
    ) -> Result<Buffer> {
        self.remote_op(local, remote, false).await
    }
}
