use std::sync::Arc;

use ruapc_rdma::{Buffer, BufferPool, QueuePair, verbs::WRID};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::RdmaState;
use crate::{
    Error, MsgFlags, Receiver, State,
    error::{ErrorKind, Result},
    msg::{MsgMeta, SendMsg},
};

#[derive(Debug)]
pub struct RdmaSocket {
    pub(crate) queue_pair: QueuePair,
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    pub(crate) send_buffers: dashmap::DashMap<u64, Arc<Buffer>>,
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
            queue_pair,
            rdmabuf_pool,
            send_buffers: dashmap::DashMap::default(),
            state: RdmaState::new(32),
            pending_sender,
        }
    }

    pub async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &State,
    ) -> Result<Receiver> {
        // receiver recall.
        let receiver = if meta.flags.contains(MsgFlags::IsReq) {
            let (msgid, rx) = state.waiter.alloc();
            meta.msgid = msgid;
            Receiver::OneShotRx(rx)
        } else {
            Receiver::None
        };

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
        } else {
            return Err(ErrorKind::RdmaSendFailed.into());
        }

        Ok(receiver)
    }

    pub fn set_error(&self) {
        self.state.set_error();
        self.queue_pair.set_error();
    }
}

impl SendMsg for Buffer {
    fn size(&self) -> usize {
        self.len()
    }

    fn prepare(&mut self) -> Result<()> {
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
