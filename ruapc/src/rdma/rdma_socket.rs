use std::sync::Arc;

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_rdma::{QueuePair, WRID, ibv_send_flags, ibv_wc_status};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::{RdmaPathInfo, RdmaState, SendPermit};
use crate::{
    Buffer, BufferPool, Context, Error, RemoteIoError, RemoteReadOptions, SocketTrait, State,
    error::{ErrorKind, Result},
    msg::MsgMeta,
    rdma::poller::{FRAME_HEADER, PollerWaker},
    services::{MemoryService, MetaService},
};

pub(crate) type RdmaCompletion = (ibv_wc_status, Option<Buffer>);

/// Serializes a message as one wire frame: `[4B frame_len][4B meta_len]
/// [meta][payload]`.
///
/// Every RDMA send is a sequence of such frames (usually one). The frame
/// header makes messages self-delimiting, so the poll thread can aggregate
/// window-blocked sends by plain concatenation and the receive side always
/// walks the same frame loop — no aggregation magic, no special cases.
struct FramedBuffer<'a>(&'a mut Buffer);

impl crate::msg::SendMsg for FramedBuffer<'_> {
    fn size(&self) -> usize {
        self.0.len()
    }

    fn prepare(&mut self) -> Result<()> {
        self.0.set_len(0);
        // Reserve the frame length header; patched in `finish`.
        self.0.extend_from_slice(&0u32.to_be_bytes())?;
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        let meta_len = u32::try_from(payload_offset - meta_offset - FRAME_HEADER)?;
        self.0[meta_offset..meta_offset + FRAME_HEADER].copy_from_slice(&meta_len.to_be_bytes());
        let frame_len = u32::try_from(self.0.len() - FRAME_HEADER)?;
        self.0[..FRAME_HEADER].copy_from_slice(&frame_len.to_be_bytes());
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

        Writer(self.0)
    }
}

#[derive(Debug)]
pub struct RdmaSocket {
    pub(crate) rdma_completions:
        dashmap::DashMap<WRID, tokio::sync::oneshot::Sender<RdmaCompletion>>,
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    pub(crate) queue_pair: QueuePair,
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
}

impl RdmaSocket {
    pub fn new(
        queue_pair: QueuePair,
        rdmabuf_pool: Arc<BufferPool>,
        pending_sender: Sender<Buffer>,
        poller_waker: PollerWaker,
        max_msg_size: usize,
        send_window: u32,
        path: RdmaPathInfo,
    ) -> Self {
        Self {
            rdma_completions: dashmap::DashMap::default(),
            rdmabuf_pool,
            queue_pair,
            state: RdmaState::new(send_window.max(1)),
            pending_sender,
            poller_waker,
            max_msg_size,
            path,
            conn_id: crate::task::next_conn_id(),
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
                ErrorKind::RdmaSendFailed,
                format!(
                    "message exceeds negotiated max_msg_size ({}); use remote read/write for large payloads",
                    self.max_msg_size
                ),
            )
        }))
    }

    pub fn set_error(&self) {
        self.state.set_error();
        let mut attr = ruapc_rdma::ibv_qp_attr {
            qp_state: ruapc_rdma::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        let mask = ruapc_rdma::ibv_qp_attr_mask::IBV_QP_STATE;
        let _ = self.queue_pair.modify(&mut attr, mask.0 as _);
        // Ensure the poll thread notices the error even when the QP had no
        // outstanding work requests to flush.
        self.poller_waker.wake();
    }

    fn post_rdma_read(
        &self,
        buffer: Buffer,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<tokio::sync::oneshot::Receiver<RdmaCompletion>> {
        let wr_id = self
            .queue_pair
            .read(buffer, remote_addr, rkey, ibv_send_flags::IBV_SEND_SIGNALED)
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.rdma_completions.insert(wr_id, tx);
        Ok(rx)
    }

    async fn await_completion(
        rx: tokio::sync::oneshot::Receiver<RdmaCompletion>,
    ) -> std::result::Result<Buffer, RemoteIoError> {
        // The buffer is only lost when the completion never surfaces
        // (channel closed / missing buffer); a failed work completion still
        // hands it back for reuse.
        let (status, buffer) = rx.await.map_err(|_| {
            Error::new(
                ErrorKind::RdmaSendFailed,
                "RDMA completion channel closed".into(),
            )
        })?;
        let buf = buffer.ok_or_else(|| {
            Error::new(
                ErrorKind::RdmaSendFailed,
                "RDMA completion did not return buffer".into(),
            )
        })?;
        if status != ibv_wc_status::IBV_WC_SUCCESS {
            return Err(RemoteIoError::new(
                Error::new(
                    ErrorKind::RdmaSendFailed,
                    format!("RDMA operation failed with status {status:?}"),
                ),
                Some(buf),
            ));
        }
        Ok(buf)
    }
}

impl RdmaSocket {
    async fn remote_read_op(
        &self,
        mut local: Buffer,
        remote: &RemoteBufferInfo,
    ) -> std::result::Result<Buffer, RemoteIoError> {
        let rkey = remote.key.rkey;
        // `remote.len` is the number of valid data bytes on the peer; read
        // exactly that many. The returned buffer's logical length is set to
        // the transferred size.
        if remote.len as usize > local.capacity() {
            return Err(RemoteIoError::new(
                Error::new(
                    ErrorKind::BufferTooSmall,
                    format!(
                        "remote buffer has {} bytes but local buffer capacity is {}",
                        remote.len,
                        local.capacity()
                    ),
                ),
                Some(local),
            ));
        }
        local.set_len(remote.len as usize);
        // On a post failure the buffer is owned by the in-flight WR table
        // and cannot be recovered here.
        let rx = self.post_rdma_read(local, remote.addr, rkey)?;
        Self::await_completion(rx).await
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
        local: Buffer,
        remote: &RemoteBufferInfo,
        options: &RemoteReadOptions,
    ) -> std::result::Result<Buffer, RemoteIoError> {
        let local = self.remote_read_op(local, remote).await?;

        if options.skip_verify {
            return Ok(local);
        }

        // After RDMA Read completes, verify the client's original request is still
        // alive. If the client has timed out, the buffer may have been reclaimed
        // and the data read via RDMA could be invalid.
        let msgid = ctx.msg_meta.msgid;
        let client = crate::Client::default();
        let still_waiting: bool = match client.is_message_waiting(ctx, &msgid).await {
            Ok(w) => w,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if !still_waiting {
            return Err(RemoteIoError::new(
                Error::new(
                    ErrorKind::Timeout,
                    "RDMA read completed but client request has already timed out".into(),
                ),
                Some(local),
            ));
        }

        Ok(local)
    }

    async fn remote_write(
        &self,
        ctx: &Context,
        local: Buffer,
    ) -> std::result::Result<Buffer, RemoteIoError> {
        // Instead of one-sided RDMA WRITE (unsafe for client buffer lifetime),
        // send a reverse RPC to the client with our buffer attached. The client
        // will perform an RDMA READ from our buffer into its own local buffer.
        let req = crate::services::MemoryPullReq {
            msgid: ctx.msg_meta.msgid,
            len: local.len() as u64,
        };
        let client = crate::Client::default();
        match client.with_read_buffer(&local).rdma_pull(ctx, &req).await {
            Ok(()) => Ok(local),
            Err(e) => Err(RemoteIoError::new(e, Some(local))),
        }
    }
}
