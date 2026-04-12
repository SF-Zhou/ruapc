use std::sync::Arc;

use ruapc_rdma::{QueuePair, verbs::WRID};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::RdmaState;
use crate::{
    Buffer, BufferPool, Device, MemoryKey, State,
    error::{Error, ErrorKind, Result},
    msg::{MsgMeta, SendMsg},
};

#[derive(Debug)]
pub struct RdmaSocket {
    pub(crate) queue_pair: QueuePair,
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    pub(crate) rdma_device: Arc<Device>,
    pub(crate) send_buffers: dashmap::DashMap<u64, (Arc<Buffer>, u32)>,
    pub(crate) state: RdmaState,
    pub(crate) pending_sender: Sender<u64>,
    /// Pending RDMA one-sided operation completions (wr_id -> status sender).
    pub(crate) rdma_completions:
        dashmap::DashMap<WRID, tokio::sync::oneshot::Sender<ruapc_rdma::verbs::ibv_wc_status>>,
}

impl RdmaSocket {
    pub fn new(
        queue_pair: QueuePair,
        rdmabuf_pool: Arc<BufferPool>,
        rdma_device: Arc<Device>,
        pending_sender: Sender<u64>,
    ) -> Self {
        Self {
            queue_pair,
            rdmabuf_pool,
            rdma_device,
            send_buffers: dashmap::DashMap::default(),
            state: RdmaState::new(32),
            pending_sender,
            rdma_completions: dashmap::DashMap::default(),
        }
    }

    pub fn set_error(&self) {
        self.state.set_error();
        self.queue_pair.set_error();
    }
}

/// A cursor-based wrapper around `Buffer` implementing `SendMsg`.
///
/// `ruapc::Buffer` is pre-allocated at a fixed size; this wrapper tracks
/// how many bytes have been written so far and provides an append-style
/// `Write` implementation over the pre-allocated slice.
struct RdmaSendBuf {
    buf: Buffer,
    cursor: usize,
}

impl RdmaSendBuf {
    fn new(buf: Buffer) -> Self {
        Self { buf, cursor: 0 }
    }

    fn written_len(&self) -> usize {
        self.cursor
    }

    fn into_inner(self) -> Buffer {
        self.buf
    }
}

impl SendMsg for RdmaSendBuf {
    fn size(&self) -> usize {
        self.cursor
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        const S: usize = std::mem::size_of::<u32>();
        let meta_len = u32::try_from(payload_offset - meta_offset - S)?;
        self.buf[meta_offset..meta_offset + S].copy_from_slice(&meta_len.to_be_bytes());
        Ok(())
    }

    fn writer(&mut self) -> impl std::io::Write {
        struct Writer<'a>(&'a mut RdmaSendBuf);

        impl std::io::Write for Writer<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                let end = self.0.cursor + buf.len();
                if end > self.0.buf.len() {
                    return Err(std::io::Error::other("rdma send buffer overflow"));
                }
                self.0.buf[self.0.cursor..end].copy_from_slice(buf);
                self.0.cursor = end;
                Ok(())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        Writer(self)
    }
}

impl crate::SocketTrait for RdmaSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        _: &Arc<State>,
    ) -> Result<()> {
        let raw_buf = self.rdmabuf_pool.allocate()?;
        let mut send_buf = RdmaSendBuf::new(raw_buf);
        meta.serialize_to(payload, &mut send_buf)?;

        let written = send_buf.written_len();
        let raw_buf = send_buf.into_inner();

        let rbi = raw_buf.remote_buffer_info(&self.rdma_device)?;
        let lkey = match rbi.key {
            MemoryKey::Rdma { lkey, .. } => lkey,
            _ => {
                return Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    "send buffer not registered on RDMA device".into(),
                ));
            }
        };

        let buf = Arc::new(raw_buf);
        self.send_buffers
            .insert(meta.msgid, (buf.clone(), written as u32));

        let index = self.state.apply_send_index();
        if index.is_ok() {
            if self.state.ready_to_send(index) {
                self.queue_pair.send_raw(
                    WRID::send_data(meta.msgid),
                    buf.as_ptr() as u64,
                    written as u32,
                    lkey,
                )?;
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
