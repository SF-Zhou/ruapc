use std::sync::{Arc, Weak};

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_rdma::{QueuePair, ibv_send_flags};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::RdmaState;
use super::cq_poller::{RdmaCompletion, SendMsg, WrContext, WrOp, WrRegistry};
use crate::{
    Buffer, BufferPool, Context, Error, RemoteReadOptions, SocketTrait, State,
    error::{ErrorKind, Result},
    msg::MsgMeta,
    services::{MemoryService, MetaService},
};

#[derive(Debug)]
pub struct RdmaSocket {
    /// Weak self-handle, used to tag work requests with their owning connection
    /// so the shared-CQ poller can route completions back here. Set once at
    /// construction via `Arc::new_cyclic`.
    pub(crate) weak_self: Weak<RdmaSocket>,
    /// The shared CQ's work-request registry this connection posts into.
    pub(crate) registry: Arc<WrRegistry>,
    pub(crate) rdmabuf_pool: Arc<BufferPool>,
    pub(crate) queue_pair: QueuePair,
    pub(crate) state: RdmaState,
    pub(crate) pending_sender: Sender<SendMsg>,
    /// Maximum size of a send buffer. Bounded by the peer's pre-posted recv
    /// buffer size so an inbound SEND never overruns the receiver.
    pub(crate) send_buffer_size: usize,
}

impl RdmaSocket {
    /// Creates an `Arc<RdmaSocket>` whose work requests are registered into
    /// `registry` (the shared CQ this connection is pinned to).
    pub(crate) fn new(
        registry: Arc<WrRegistry>,
        queue_pair: QueuePair,
        rdmabuf_pool: Arc<BufferPool>,
        pending_sender: Sender<SendMsg>,
        send_buffer_size: usize,
        max_send_wr: u32,
    ) -> Arc<Self> {
        // The QP send queue is shared by data SENDs, ACK SEND_WITH_IMM, and
        // RDMA READs. Reserve half of the send-queue depth for those auxiliary
        // operations so data sends never starve them or overflow the SQ.
        let max_send_limit = (max_send_wr / 2).max(1);
        Arc::new_cyclic(|weak_self| Self {
            weak_self: weak_self.clone(),
            registry,
            rdmabuf_pool,
            queue_pair,
            state: RdmaState::new(max_send_limit),
            pending_sender,
            send_buffer_size,
        })
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

    /// Posts a pre-posted receive buffer, registering it in the shared registry.
    pub(crate) fn post_recv(&self, buf: Buffer) -> Result<()> {
        self.registry.register_and_post(
            WrContext {
                socket: self.weak_self.clone(),
                buf: Some(buf),
                op: WrOp::Recv,
            },
            |id, buf| {
                self.queue_pair
                    .recv(id, buf.expect("recv buffer present"))
                    .map_err(|e| Error::new(ErrorKind::RdmaRecvFailed, e.to_string()))
            },
        )
    }

    /// Posts a data SEND for `buf`. Always signaled so the completion advances
    /// flow-control bookkeeping and returns the buffer to the pool.
    pub(crate) fn post_data_send(&self, buf: Buffer) -> Result<()> {
        self.registry.register_and_post(
            WrContext {
                socket: self.weak_self.clone(),
                buf: Some(buf),
                op: WrOp::DataSend,
            },
            |id, buf| {
                self.queue_pair
                    .send(
                        id,
                        buf.expect("send buffer present"),
                        ibv_send_flags::IBV_SEND_SIGNALED,
                    )
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
            },
        )
    }

    /// Posts a zero-length flow-control ACK (SEND_WITH_IMM, no buffer).
    pub(crate) fn post_ack(&self, imm: u32) -> Result<()> {
        self.registry.register_and_post(
            WrContext {
                socket: self.weak_self.clone(),
                buf: None,
                op: WrOp::Ack,
            },
            |id, _buf| {
                self.queue_pair
                    .send_imm_only(id, imm, ibv_send_flags::IBV_SEND_SIGNALED)
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
            },
        )
    }

    fn post_rdma_read(
        &self,
        buffer: Buffer,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<tokio::sync::oneshot::Receiver<RdmaCompletion>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.registry.register_and_post(
            WrContext {
                socket: self.weak_self.clone(),
                buf: Some(buffer),
                op: WrOp::RdmaRead(tx),
            },
            |id, buf| {
                self.queue_pair
                    .read(
                        id,
                        buf.expect("read buffer present"),
                        remote_addr,
                        rkey,
                        ibv_send_flags::IBV_SEND_SIGNALED,
                    )
                    .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
            },
        )?;
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
        if status != ruapc_rdma::ibv_wc_status::IBV_WC_SUCCESS {
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
        let mut buf = self.rdmabuf_pool.allocate(self.send_buffer_size)?;
        meta.serialize_to(payload, &mut buf)?;

        let index = self.state.apply_send_index();
        if index.is_ok() {
            if self.state.ready_to_send(index) {
                self.post_data_send(buf)?;
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
