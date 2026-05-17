use std::sync::Arc;

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_rdma::{QueuePair, WRID, ibv_send_flags, ibv_wc_status};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::RdmaState;
use crate::{
    Buffer, BufferPool, Context, Error, RemoteReadOptions, SocketTrait, State,
    error::{ErrorKind, Result},
    msg::MsgMeta,
    rdma::event_loop::SendMsg,
    services::{MemoryService, MetaService},
};

pub(crate) type RdmaCompletion = (ibv_wc_status, Option<Buffer>);

#[derive(Debug)]
pub struct RdmaSocket {
    pub(crate) rdma_completions:
        dashmap::DashMap<WRID, tokio::sync::oneshot::Sender<RdmaCompletion>>,
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    pub(crate) queue_pair: QueuePair,
    pub(crate) state: RdmaState,
    pub(crate) pending_sender: Sender<SendMsg>,
}

impl RdmaSocket {
    pub fn new(
        queue_pair: QueuePair,
        rdmabuf_pool: Arc<BufferPool>,
        pending_sender: Sender<SendMsg>,
    ) -> Self {
        Self {
            rdma_completions: dashmap::DashMap::default(),
            rdmabuf_pool,
            queue_pair,
            state: RdmaState::new(32),
            pending_sender,
        }
    }

    pub fn set_error(&self) {
        self.state.set_error();
        let mut attr = ruapc_rdma::ibv_qp_attr {
            qp_state: ruapc_rdma::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        let mask = ruapc_rdma::ibv_qp_attr_mask::IBV_QP_STATE;
        let _ = self.queue_pair.modify(&mut attr, mask.0 as _);
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
    ) -> Result<Buffer> {
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
            return Err(Error::new(
                ErrorKind::RdmaSendFailed,
                format!("RDMA operation failed with status {status:?}"),
            ));
        }
        Ok(buf)
    }
}

impl RdmaSocket {
    async fn remote_read_op(&self, mut local: Buffer, remote: &RemoteBufferInfo) -> Result<Buffer> {
        let rkey = remote.key.rkey;
        local.set_len(remote.len as usize);
        let rx = self.post_rdma_read(local, remote.addr, rkey)?;
        Self::await_completion(rx).await
    }
}

impl SocketTrait for RdmaSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        _: &Arc<State>,
    ) -> Result<()> {
        let mut buf = self.rdmabuf_pool.allocate(1024 * 1024)?;
        meta.serialize_to(payload, &mut buf)?;

        let index = self.state.apply_send_index();
        if index.is_ok() {
            if self.state.ready_to_send(index) {
                self.queue_pair
                    .send(buf, ibv_send_flags::IBV_SEND_SIGNALED)
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
            } else {
                self.pending_sender
                    .send(SendMsg {
                        id: meta.msgid,
                        buf,
                    })
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
        ctx: &Context,
        local: Buffer,
        remote: &RemoteBufferInfo,
        options: &RemoteReadOptions,
    ) -> Result<Buffer> {
        let local = self.remote_read_op(local, remote).await?;

        if options.skip_verify {
            return Ok(local);
        }

        // After RDMA Read completes, verify the client's original request is still
        // alive. If the client has timed out, the buffer may have been reclaimed
        // and the data read via RDMA could be invalid.
        let msgid = ctx.msg_meta.msgid;
        let client = crate::Client::default();
        let still_waiting: bool = client.is_message_waiting(ctx, &msgid).await?;
        if !still_waiting {
            return Err(Error::new(
                ErrorKind::Timeout,
                "RDMA read completed but client request has already timed out".into(),
            ));
        }

        Ok(local)
    }

    async fn remote_write(&self, ctx: &Context, local: Buffer) -> Result<Buffer> {
        // Instead of one-sided RDMA WRITE (unsafe for client buffer lifetime),
        // send a reverse RPC to the client with our buffer attached. The client
        // will perform an RDMA READ from our buffer into its own local buffer.
        let req = crate::services::MemoryPullReq {
            msgid: ctx.msg_meta.msgid,
            len: local.len() as u64,
        };
        let client = crate::Client::default();
        client.with_read_buffer(&local).rdma_pull(ctx, &req).await?;
        Ok(local)
    }
}
