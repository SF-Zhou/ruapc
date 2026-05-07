use std::sync::Arc;

use ruapc_rdma_sys::{QueuePair, WRID, ibv_send_flags, ibv_wc_status};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::{RdmaBufferRef, RdmaState};
use crate::{
    Device, Error, SocketTrait, State,
    error::{ErrorKind, Result},
    memory::{Buffer, BufferPool},
    msg::{MsgMeta, SendMsg},
};

#[derive(Debug)]
pub struct RdmaSocket {
    /// Pending RDMA one-sided operation completions (wr_id -> status sender).
    pub(crate) rdma_completions:
        dashmap::DashMap<WRID, tokio::sync::oneshot::Sender<ibv_wc_status>>,
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
    pub(crate) fn make_buffer_ref(&self, buffer: Arc<Buffer>) -> Option<RdmaBufferRef> {
        RdmaBufferRef::new(buffer, &self.device)
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

        let buf = Arc::new(buf);
        let buf_ref = self.make_buffer_ref(buf.clone()).ok_or_else(|| {
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
}

impl SendMsg for Buffer {
    fn size(&self) -> usize {
        self.len()
    }

    fn prepare(&mut self) -> Result<()> {
        self.set_len(0);
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        const S: usize = std::mem::size_of::<u32>();
        let meta_len = u32::try_from(payload_offset - meta_offset - S)?;
        self[meta_offset..meta_offset + S].copy_from_slice(&meta_len.to_be_bytes());
        Ok(())
    }

    fn writer(&mut self) -> impl std::io::Write {
        #[repr(transparent)]
        struct Writer<'a>(&'a mut Buffer);

        impl std::io::Write for Writer<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                self.0.extend_from_slice(buf).map_err(std::io::Error::other)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        Writer(self)
    }
}
