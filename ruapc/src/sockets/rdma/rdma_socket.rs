use std::sync::Arc;

use ruapc_rdma::{QueuePair, verbs::WRID};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::{QueuePairExt, RdmaState};
use crate::{
    Error, SocketTrait, State,
    error::{ErrorKind, Result},
    memory::{Buffer, BufferPool},
    msg::{MsgMeta, SendMsg},
};

#[derive(Debug)]
pub struct RdmaSocket {
    /// Pending RDMA one-sided operation completions (wr_id -> status sender).
    pub(crate) rdma_completions:
        dashmap::DashMap<WRID, tokio::sync::oneshot::Sender<ruapc_rdma::verbs::ibv_wc_status>>,
    /// Buffers for in-flight sends — dropped before QP so that any Arc<Buffer>
    /// references are released before we destroy the queue pair.
    pub(crate) send_buffers: dashmap::DashMap<u64, Arc<Buffer>>,
    /// Shared buffer pool — its Arc is decremented before QP destruction,
    /// but actual pool cleanup waits until *all* Arcs (from Buffers) are gone.
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    /// The RDMA queue pair — destroyed after send_buffers and rdmabuf_pool Arcs.
    pub(crate) queue_pair: QueuePair,
    pub(crate) state: RdmaState,
    pub(crate) pending_sender: Sender<u64>,
}

impl RdmaSocket {
    pub fn new(
        queue_pair: QueuePair,
        rdmabuf_pool: Arc<BufferPool>,
        pending_sender: Sender<u64>,
    ) -> Self {
        Self {
            rdma_completions: dashmap::DashMap::default(),
            send_buffers: dashmap::DashMap::default(),
            rdmabuf_pool,
            queue_pair,
            state: RdmaState::new(32),
            pending_sender,
        }
    }

    pub fn set_error(&self) {
        self.state.set_error();
        self.queue_pair.set_error();
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
        self.send_buffers.insert(meta.msgid, buf.clone());

        let index = self.state.apply_send_index();
        if index.is_ok() {
            if self.state.ready_to_send(index) {
                self.queue_pair.send(WRID::send_data(meta.msgid), &buf)?;
            } else {
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
