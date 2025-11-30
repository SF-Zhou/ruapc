use std::sync::Arc;

use ruapc_rdma::{Buffer, BufferPool, QueuePair, RemoteBuffer, verbs::WRID};
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use super::RdmaState;
use crate::{
    Error, State,
    error::{ErrorKind, Result},
    msg::{MsgMeta, SendMsg},
};

/// Represents an in-progress RDMA Read operation.
pub struct RdmaReadRequest {
    /// The local buffer that will receive the read data
    pub local_buffer: Buffer,
    /// The channel to signal completion
    pub completion_tx: oneshot::Sender<Result<Buffer>>,
}

impl std::fmt::Debug for RdmaReadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaReadRequest")
            .field("local_buffer_len", &self.local_buffer.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct RdmaSocket {
    pub(crate) queue_pair: QueuePair,
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    pub(crate) send_buffers: dashmap::DashMap<u64, Arc<Buffer>>,
    /// Pending RDMA Read requests indexed by wr_id
    pub(crate) rdma_read_requests: dashmap::DashMap<u64, RdmaReadRequest>,
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
            rdma_read_requests: dashmap::DashMap::default(),
            state: RdmaState::new(32),
            pending_sender,
        }
    }

    pub async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        _: &State,
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

    /// Initiates an RDMA Read operation to read data from a remote buffer.
    ///
    /// This method allocates a local buffer and posts an RDMA Read work request
    /// to read data from the remote peer's memory. The caller should first verify
    /// that the remote peer is still waiting (buffer is valid) using `is_message_waiting`.
    ///
    /// # Arguments
    ///
    /// * `remote_buffer` - Information about the remote buffer to read from
    /// * `request_id` - A unique ID for this RDMA Read request (usually msgid)
    ///
    /// # Returns
    ///
    /// A receiver that will receive the completed buffer with the read data,
    /// or an error if the operation fails.
    pub fn rdma_read(
        &self,
        remote_buffer: &RemoteBuffer,
        request_id: u64,
    ) -> Result<oneshot::Receiver<Result<Buffer>>> {
        // Allocate local buffer to receive the data
        let mut local_buf = self.rdmabuf_pool.allocate()?;

        // Set the expected length so the completion handler knows the size
        local_buf.set_len(remote_buffer.len as usize);

        // Extract buffer pointer and lkey before moving the buffer
        let buf_ptr = local_buf.as_ptr() as u64;
        let buf_lkey = local_buf.lkey(&self.queue_pair.device);

        let (tx, rx) = oneshot::channel();

        // Store the request for completion handling
        let request = RdmaReadRequest {
            local_buffer: local_buf,
            completion_tx: tx,
        };

        self.rdma_read_requests.insert(request_id, request);

        // Post the RDMA Read
        let mut sge = ruapc_rdma::verbs::ibv_sge {
            addr: buf_ptr,
            length: remote_buffer.len,
            lkey: buf_lkey,
        };
        let mut wr = ruapc_rdma::verbs::ibv_send_wr {
            wr_id: WRID::rdma_read(request_id),
            sg_list: &mut sge as *mut _,
            num_sge: 1,
            opcode: ruapc_rdma::verbs::ibv_wr_opcode::IBV_WR_RDMA_READ,
            send_flags: ruapc_rdma::verbs::ibv_send_flags::IBV_SEND_SIGNALED.0,
            wr: ruapc_rdma::verbs::ibv_send_wr__bindgen_ty_2 {
                rdma: ruapc_rdma::verbs::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 {
                    remote_addr: remote_buffer.addr,
                    rkey: remote_buffer.rkey,
                },
            },
            ..Default::default()
        };

        match self.queue_pair.post_send(&mut wr) {
            0 => Ok(rx),
            _ => {
                // Remove the request on failure
                self.rdma_read_requests.remove(&request_id);
                Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    format!("RDMA Read post failed: {}", std::io::Error::last_os_error()),
                ))
            }
        }
    }

    /// Handles completion of an RDMA Read operation.
    ///
    /// This is called by the event loop when an RDMA Read work completion is received.
    ///
    /// # Arguments
    ///
    /// * `request_id` - The ID of the completed request
    /// * `success` - Whether the operation completed successfully
    pub fn complete_rdma_read(&self, request_id: u64, success: bool) {
        if let Some((_, request)) = self.rdma_read_requests.remove(&request_id) {
            let result = if success {
                Ok(request.local_buffer)
            } else {
                Err(Error::kind(ErrorKind::RdmaRecvFailed))
            };
            let _ = request.completion_tx.send(result);
        }
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
