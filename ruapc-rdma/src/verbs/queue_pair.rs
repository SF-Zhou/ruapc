use std::{os::raw::c_int, ptr, sync::Arc};

use ruapc_bufpool::{Buffer, DeviceIndex};

use super::{completion_queue::CompletionQueue, protection_domain::ProtectionDomain};
use crate::{
    Error, ErrorKind, LinkLayer, Result, WRID, ibv_gid, ibv_mtu, ibv_qp_attr, ibv_qp_attr_mask,
    ibv_qp_state,
};

pub struct QueuePair {
    ptr: *mut crate::ibv_qp,
    _pd: Arc<ProtectionDomain>,
    send_cq: Arc<CompletionQueue>,
    recv_cq: Arc<CompletionQueue>,
    pub device_index: DeviceIndex,
}

impl QueuePair {
    pub fn create(
        pd: &Arc<ProtectionDomain>,
        send_cq: &Arc<CompletionQueue>,
        recv_cq: &Arc<CompletionQueue>,
        init_attr: &mut crate::ibv_qp_init_attr,
        device_index: DeviceIndex,
    ) -> Result<Self> {
        init_attr.send_cq = send_cq.as_ptr();
        init_attr.recv_cq = recv_cq.as_ptr();
        let ptr = unsafe { crate::ibv_create_qp(pd.as_ptr(), init_attr) };
        if ptr.is_null() {
            return Err(ErrorKind::IBCreateQueuePairFail.with_errno());
        }
        Ok(Self {
            ptr,
            _pd: Arc::clone(pd),
            send_cq: Arc::clone(send_cq),
            recv_cq: Arc::clone(recv_cq),
            device_index,
        })
    }

    /// Returns the send completion queue this QP is attached to.
    pub fn send_cq(&self) -> &Arc<CompletionQueue> {
        &self.send_cq
    }

    /// Returns the receive completion queue this QP is attached to.
    pub fn recv_cq(&self) -> &Arc<CompletionQueue> {
        &self.recv_cq
    }

    fn lkey(&self, buffer: &Buffer) -> Result<u32> {
        buffer
            .memory_key(&self.device_index)
            .map(|k| k.lkey)
            .map_err(|e| Error::new(ErrorKind::IBRegMemoryRegionFail, e.to_string()))
    }

    pub fn as_ptr(&self) -> *mut crate::ibv_qp {
        self.ptr
    }

    pub fn qp_num(&self) -> u32 {
        unsafe { (*self.ptr).qp_num }
    }

    /// Posts a SEND work request for `buffer` with the caller-assigned `wr_id`.
    ///
    /// The buffer's memory must remain valid and pinned until the matching
    /// completion is reaped. Ownership of the buffer is tracked by the caller
    /// (the per-CQ work-request registry); this method only borrows it to read
    /// its address / length / lkey, so the caller must register the buffer
    /// under `wr_id` **before** calling this (and unregister on error).
    pub fn send(&self, wr_id: u64, buffer: &Buffer, flags: crate::ibv_send_flags) -> Result<()> {
        let addr = buffer.as_ptr() as u64;
        let len = buffer.len() as u32;
        let lkey = self.lkey(buffer)?;
        self.post_send_verb(
            WRID::new(wr_id),
            addr,
            len,
            lkey,
            crate::ibv_wr_opcode::IBV_WR_SEND,
            flags.0,
            None,
        )
    }

    /// Posts a SEND_WITH_IMM carrying `buffer` and `imm`, with `wr_id`.
    ///
    /// See [`send`](Self::send) for the buffer-ownership contract.
    pub fn send_imm(
        &self,
        wr_id: u64,
        buffer: &Buffer,
        imm: u32,
        flags: crate::ibv_send_flags,
    ) -> Result<()> {
        let addr = buffer.as_ptr() as u64;
        let len = buffer.len() as u32;
        let lkey = self.lkey(buffer)?;
        let mut sge = crate::ibv_sge {
            addr,
            length: len,
            lkey,
        };
        let mut wr = crate::ibv_send_wr {
            wr_id: WRID::new(wr_id),
            sg_list: &mut sge,
            num_sge: 1,
            opcode: crate::ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
            send_flags: flags.0,
            __bindgen_anon_1: crate::ibv_send_wr__bindgen_ty_1 {
                imm_data: imm.to_be(),
            },
            ..Default::default()
        };
        unsafe { self.post_send(&mut wr) }.map_err(|(_, err)| err)
    }

    /// Posts a zero-length SEND_WITH_IMM (used for flow-control ACKs) with
    /// `wr_id`. No buffer is associated.
    pub fn send_imm_only(&self, wr_id: u64, imm: u32, flags: crate::ibv_send_flags) -> Result<()> {
        let mut wr = crate::ibv_send_wr {
            wr_id: WRID::new(wr_id),
            sg_list: ptr::null_mut(),
            num_sge: 0,
            opcode: crate::ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
            send_flags: flags.0,
            __bindgen_anon_1: crate::ibv_send_wr__bindgen_ty_1 {
                imm_data: imm.to_be(),
            },
            ..Default::default()
        };
        unsafe { self.post_send(&mut wr) }.map_err(|(_, err)| err)
    }

    /// Posts an RDMA READ into `buffer` from `remote_addr`/`rkey`, with `wr_id`.
    ///
    /// See [`send`](Self::send) for the buffer-ownership contract.
    pub fn read(
        &self,
        wr_id: u64,
        buffer: &Buffer,
        remote_addr: u64,
        rkey: u32,
        flags: crate::ibv_send_flags,
    ) -> Result<()> {
        let addr = buffer.as_ptr() as u64;
        let len = buffer.len() as u32;
        let lkey = self.lkey(buffer)?;
        self.post_send_verb(
            WRID::new(wr_id),
            addr,
            len,
            lkey,
            crate::ibv_wr_opcode::IBV_WR_RDMA_READ,
            flags.0,
            Some((remote_addr, rkey)),
        )
    }

    fn post_send_verb(
        &self,
        wr_id: WRID,
        addr: u64,
        len: u32,
        lkey: u32,
        opcode: crate::ibv_wr_opcode,
        send_flags: u32,
        remote: Option<(u64, u32)>,
    ) -> Result<()> {
        let mut sge = crate::ibv_sge {
            addr,
            length: len,
            lkey,
        };
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: &mut sge,
            num_sge: 1,
            opcode,
            send_flags,
            ..Default::default()
        };
        if let Some((remote_addr, rkey)) = remote {
            wr.wr = crate::ibv_send_wr__bindgen_ty_2 {
                rdma: crate::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 { remote_addr, rkey },
            };
        }
        unsafe { self.post_send(&mut wr) }.map_err(|(_, err)| err)
    }

    /// Posts a RECV work request for `buffer` with the caller-assigned `wr_id`.
    ///
    /// See [`send`](Self::send) for the buffer-ownership contract; the buffer is
    /// posted at its full capacity to receive an inbound message.
    pub fn recv(&self, wr_id: u64, buffer: &Buffer) -> Result<()> {
        let addr = buffer.as_ptr() as u64;
        let len = buffer.capacity() as u32;
        let lkey = self.lkey(buffer)?;
        let mut sge = crate::ibv_sge {
            addr,
            length: len,
            lkey,
        };
        let mut wr = crate::ibv_recv_wr {
            wr_id: WRID::new(wr_id),
            sg_list: &mut sge,
            num_sge: 1,
            ..Default::default()
        };
        unsafe { self.post_recv(&mut wr) }.map_err(|(_, err)| err)
    }

    pub fn modify(&self, attr: &mut crate::ibv_qp_attr, attr_mask: c_int) -> Result<()> {
        let ret = unsafe { crate::ibv_modify_qp(self.ptr, attr, attr_mask) };
        if ret != 0 {
            return Err(ErrorKind::IBModifyQueuePairFail.with_errno());
        }
        Ok(())
    }

    const ACCESS_FLAGS: u32 = crate::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
        | crate::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
        | crate::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
        | crate::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING.0;

    pub fn init(&self, port_num: u8, pkey_index: u16) -> Result<()> {
        let mut attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_INIT,
            pkey_index,
            port_num,
            qp_access_flags: Self::ACCESS_FLAGS,
            ..Default::default()
        };
        let mask = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        self.modify(&mut attr, mask.0 as _)
    }

    pub fn ready_to_recv(
        &self,
        remote_qp_num: u32,
        remote_gid: ibv_gid,
        remote_lid: u16,
        local_port_num: u8,
        local_gid_index: u8,
        link_layer: LinkLayer,
        path_mtu: ibv_mtu,
    ) -> Result<()> {
        let mut ah_attr = crate::ibv_ah_attr {
            sl: 0,
            src_path_bits: 0,
            static_rate: 0,
            port_num: local_port_num,
            ..Default::default()
        };

        match link_layer {
            LinkLayer::InfiniBand => {
                ah_attr.dlid = remote_lid;
                ah_attr.is_global = 0;
            }
            LinkLayer::Ethernet => {
                ah_attr.grh = crate::ibv_global_route {
                    dgid: remote_gid,
                    flow_label: 0,
                    sgid_index: local_gid_index,
                    hop_limit: 0xff,
                    traffic_class: 0,
                };
                ah_attr.is_global = 1;
            }
            LinkLayer::Unspecified => {
                return Err(Error::new(
                    ErrorKind::IBModifyQueuePairFail,
                    "RDMA link layer is unspecified".into(),
                ));
            }
        }

        let mut attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_RTR,
            path_mtu,
            dest_qp_num: remote_qp_num,
            rq_psn: 0,
            max_dest_rd_atomic: 1,
            min_rnr_timer: 0x12,
            ah_attr,
            ..Default::default()
        };
        let mask = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_AV
            | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN
            | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
            | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        self.modify(&mut attr, mask.0 as _)
    }

    pub fn ready_to_send(&self) -> Result<()> {
        let mut attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_RTS,
            timeout: 0x12,
            retry_cnt: 6,
            rnr_retry: 6,
            sq_psn: 0,
            max_rd_atomic: 1,
            ..Default::default()
        };
        let mask = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        self.modify(&mut attr, mask.0 as _)
    }

    pub fn connect(
        &self,
        local_port_num: u8,
        local_gid_index: u8,
        pkey_index: u16,
        link_layer: LinkLayer,
        path_mtu: ibv_mtu,
        remote_qp_num: u32,
        remote_gid: ibv_gid,
        remote_lid: u16,
    ) -> Result<()> {
        self.init(local_port_num, pkey_index)?;
        self.ready_to_recv(
            remote_qp_num,
            remote_gid,
            remote_lid,
            local_port_num,
            local_gid_index,
            link_layer,
            path_mtu,
        )?;
        self.ready_to_send()
    }

    pub(crate) unsafe fn post_send(
        &self,
        wr: *mut crate::ibv_send_wr,
    ) -> std::result::Result<(), (*mut crate::ibv_send_wr, Error)> {
        let mut bad_wr: *mut crate::ibv_send_wr = ptr::null_mut();
        let ret = unsafe {
            (*(*self.ptr).context).ops.post_send.unwrap_unchecked()(self.ptr, wr, &mut bad_wr)
        };
        if ret != 0 {
            return Err((bad_wr, ErrorKind::IBPostSendFail.with_errno()));
        }
        Ok(())
    }

    pub(crate) unsafe fn post_recv(
        &self,
        wr: *mut crate::ibv_recv_wr,
    ) -> std::result::Result<(), (*mut crate::ibv_recv_wr, Error)> {
        let mut bad_wr: *mut crate::ibv_recv_wr = ptr::null_mut();
        let ret = unsafe {
            (*(*self.ptr).context).ops.post_recv.unwrap_unchecked()(self.ptr, wr, &mut bad_wr)
        };
        if ret != 0 {
            return Err((bad_wr, ErrorKind::IBPostRecvFail.with_errno()));
        }
        Ok(())
    }
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        let _ = unsafe { crate::ibv_destroy_qp(self.ptr) };
    }
}
impl std::fmt::Debug for QueuePair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuePair")
            .field("ptr", &self.ptr)
            .field("qp_num", &self.qp_num())
            .finish()
    }
}
unsafe impl Send for QueuePair {}
unsafe impl Sync for QueuePair {}
