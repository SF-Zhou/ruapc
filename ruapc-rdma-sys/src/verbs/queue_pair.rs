//! [`QueuePair`]: RAII wrapper for `ibv_qp` with typed buffer management.

use std::{
    collections::HashMap,
    os::raw::c_int,
    ptr,
    sync::Arc,
    sync::Mutex,
    sync::atomic::{AtomicU64, Ordering},
};

use super::{completion_queue::CompletionQueue, protection_domain::ProtectionDomain};
use crate::{
    Error, ErrorKind, RdmaBuffer, Result, WRID, ibv_access_flags, ibv_gid, ibv_mtu, ibv_qp_attr,
    ibv_qp_attr_mask, ibv_qp_state, ibv_wc,
};

/// A work completion paired with its buffer.
pub struct Completion<B> {
    /// The work completion status and metadata.
    pub wc: ibv_wc,
    /// The buffer that was associated with this work request,
    /// if one was provided (always `Some` for high-level operations).
    pub buffer: Option<B>,
}

/// A queue pair (QP) — the fundamental RDMA communication endpoint.
///
/// Maintains shared references to the [`ProtectionDomain`] and both
/// send/recv [`CompletionQueue`]s. The type parameter `B` is the buffer
/// type used for send/recv operations.
///
/// Buffers posted via the high-level API are stored in an internal map
/// keyed by `wr_id`, protected by a `Mutex`. When a completion arrives,
/// [`poll_send`](Self::poll_send) / [`poll_recv`](Self::poll_recv) look up
/// and return the corresponding buffer.
///
/// # High-level API
///
/// [`send`](Self::send) / [`recv`](Self::recv) / [`send_imm`](Self::send_imm)
/// / [`read`](Self::read) take ownership of a buffer and return it via
/// [`Completion`] once the corresponding work completion arrives.
///
/// # Low-level API
///
/// [`post_send`](Self::post_send) / [`post_recv`](Self::post_recv) accept
/// raw work request pointers for advanced use cases (scatter/gather, chaining).
/// These methods do not interact with the buffer map.
///
/// # QP state machine
///
/// [`init`](Self::init) → [`ready_to_recv`](Self::ready_to_recv) →
/// [`ready_to_send`](Self::ready_to_send) transition the QP through the
/// RESET → INIT → RTR → RTS states.  [`connect`](Self::connect) performs
/// all three in one call.
pub struct QueuePair<B: RdmaBuffer> {
    ptr: *mut crate::ibv_qp,
    /// Prevents the PD (and transitively the context) from being freed.
    _pd: Arc<ProtectionDomain>,
    send_cq: Arc<CompletionQueue>,
    recv_cq: Arc<CompletionQueue>,
    /// Maps monotonically-allocated `wr_id`s to in-flight buffers.
    wrs: Mutex<HashMap<u64, B>>,
    /// Monotonically-allocating ID shared by send, send_imm, read, and recv.
    next_id: AtomicU64,
}

impl<B: RdmaBuffer> QueuePair<B> {
    /// Creates a new queue pair.
    ///
    /// The `send_cq` and `recv_cq` fields of `init_attr` will be overwritten
    /// with the provided CQ pointers.
    pub fn create(
        pd: &Arc<ProtectionDomain>,
        send_cq: &Arc<CompletionQueue>,
        recv_cq: &Arc<CompletionQueue>,
        init_attr: &mut crate::ibv_qp_init_attr,
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
            wrs: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        })
    }

    /// Returns the raw QP pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_qp {
        self.ptr
    }

    /// Returns the QP number.
    pub fn qp_num(&self) -> u32 {
        unsafe { (*self.ptr).qp_num }
    }

    // ------------------------------------------------------------------
    // High-level typed operations
    // ------------------------------------------------------------------

    /// Sends a buffer.
    ///
    /// Takes ownership of `buffer`; it will be returned in a
    /// [`Completion`] when the send completes.
    pub fn send(&self, buffer: B, flags: crate::ibv_send_flags) -> Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wr_id = WRID::send_data(id);

        let addr = buffer.addr();
        let len = buffer.len();
        let lkey = buffer.lkey();

        self.wrs.lock().unwrap().insert(id, buffer);

        let mut sge = crate::ibv_sge {
            addr: addr as u64,
            length: len as u32,
            lkey,
        };
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: &mut sge,
            num_sge: 1,
            opcode: crate::ibv_wr_opcode::IBV_WR_SEND,
            send_flags: flags.0,
            ..Default::default()
        };

        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            self.wrs.lock().unwrap().remove(&id);
            return Err(err);
        }
        Ok(())
    }

    /// Sends a buffer with immediate data.
    pub fn send_imm(&self, buffer: B, imm: u32, flags: crate::ibv_send_flags) -> Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wr_id = WRID::send_imm(id);

        let addr = buffer.addr();
        let len = buffer.len();
        let lkey = buffer.lkey();

        self.wrs.lock().unwrap().insert(id, buffer);

        let mut sge = crate::ibv_sge {
            addr: addr as u64,
            length: len as u32,
            lkey,
        };
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: &mut sge,
            num_sge: 1,
            opcode: crate::ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
            send_flags: flags.0,
            __bindgen_anon_1: crate::ibv_send_wr__bindgen_ty_1 {
                imm_data: imm.to_be(),
            },
            ..Default::default()
        };

        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            self.wrs.lock().unwrap().remove(&id);
            return Err(err);
        }
        Ok(())
    }

    /// Sends only immediate data without a buffer payload.
    ///
    /// This is useful for lightweight signaling (e.g. ACK messages in
    /// flow control) where no data needs to be transferred.
    pub fn send_imm_only(&self, imm: u32, flags: crate::ibv_send_flags) -> Result<()> {
        let _id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wr_id = WRID::send_imm(_id);

        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: ptr::null_mut(),
            num_sge: 0,
            opcode: crate::ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
            send_flags: flags.0,
            __bindgen_anon_1: crate::ibv_send_wr__bindgen_ty_1 {
                imm_data: imm.to_be(),
            },
            ..Default::default()
        };

        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            return Err(err);
        }
        Ok(())
    }

    /// Reads data from a remote memory region into a local buffer.
    ///
    /// `remote_addr` and `rkey` identify the remote memory region.
    /// The buffer is returned via [`Completion`] on the send CQ.
    ///
    /// Returns the [`WRID`] assigned to this operation, which can be used
    /// to correlate completions.
    pub fn read(
        &self,
        buffer: B,
        remote_addr: u64,
        rkey: u32,
        flags: crate::ibv_send_flags,
    ) -> Result<WRID> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wr_id = WRID::read(id);

        let addr = buffer.addr();
        let len = buffer.len();
        let lkey = buffer.lkey();

        self.wrs.lock().unwrap().insert(id, buffer);

        let mut sge = crate::ibv_sge {
            addr: addr as u64,
            length: len as u32,
            lkey,
        };
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: &mut sge,
            num_sge: 1,
            opcode: crate::ibv_wr_opcode::IBV_WR_RDMA_READ,
            send_flags: flags.0,
            wr: crate::ibv_send_wr__bindgen_ty_2 {
                rdma: crate::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 { remote_addr, rkey },
            },
            ..Default::default()
        };

        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            self.wrs.lock().unwrap().remove(&id);
            return Err(err);
        }
        Ok(wr_id)
    }

    /// Writes data from a local buffer into a remote memory region.
    ///
    /// `remote_addr` and `rkey` identify the remote memory region.
    /// The buffer is returned via [`Completion`] on the send CQ.
    ///
    /// Returns the [`WRID`] assigned to this operation.
    pub fn write(
        &self,
        buffer: B,
        remote_addr: u64,
        rkey: u32,
        flags: crate::ibv_send_flags,
    ) -> Result<WRID> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wr_id = WRID::send_data(id);

        let addr = buffer.addr();
        let len = buffer.len();
        let lkey = buffer.lkey();

        self.wrs.lock().unwrap().insert(id, buffer);

        let mut sge = crate::ibv_sge {
            addr: addr as u64,
            length: len as u32,
            lkey,
        };
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: &mut sge,
            num_sge: 1,
            opcode: crate::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            send_flags: flags.0,
            wr: crate::ibv_send_wr__bindgen_ty_2 {
                rdma: crate::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 { remote_addr, rkey },
            },
            ..Default::default()
        };

        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            self.wrs.lock().unwrap().remove(&id);
            return Err(err);
        }
        Ok(wr_id)
    }

    /// Reads data from a remote memory region without buffer ownership tracking.
    ///
    /// Unlike [`read`](Self::read), this method does not store the buffer in
    /// the internal map. The caller must ensure the memory described by
    /// `local` remains valid until the completion arrives.
    ///
    /// Returns the [`WRID`] assigned to this operation.
    pub fn read_untracked(
        &self,
        local: &impl RdmaBuffer,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<WRID> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wr_id = WRID::read(id);

        let mut sge = crate::ibv_sge {
            addr: local.addr() as u64,
            length: local.len() as u32,
            lkey: local.lkey(),
        };
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: &mut sge,
            num_sge: 1,
            opcode: crate::ibv_wr_opcode::IBV_WR_RDMA_READ,
            send_flags: crate::ibv_send_flags::IBV_SEND_SIGNALED.0,
            wr: crate::ibv_send_wr__bindgen_ty_2 {
                rdma: crate::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 { remote_addr, rkey },
            },
            ..Default::default()
        };

        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            return Err(err);
        }
        Ok(wr_id)
    }

    /// Writes data to a remote memory region without buffer ownership tracking.
    ///
    /// Unlike [`write`](Self::write), this method does not store the buffer in
    /// the internal map. The caller must ensure the memory described by
    /// `local` remains valid until the completion arrives.
    ///
    /// Returns the [`WRID`] assigned to this operation.
    pub fn write_untracked(
        &self,
        local: &impl RdmaBuffer,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<WRID> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wr_id = WRID::send_data(id);

        let mut sge = crate::ibv_sge {
            addr: local.addr() as u64,
            length: local.len() as u32,
            lkey: local.lkey(),
        };
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: &mut sge,
            num_sge: 1,
            opcode: crate::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            send_flags: crate::ibv_send_flags::IBV_SEND_SIGNALED.0,
            wr: crate::ibv_send_wr__bindgen_ty_2 {
                rdma: crate::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 { remote_addr, rkey },
            },
            ..Default::default()
        };

        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            return Err(err);
        }
        Ok(wr_id)
    }

    /// Posts a receive buffer.
    ///
    /// The buffer's memory will be written to by the remote peer.
    /// It is returned in a [`Completion`] once data arrives.
    pub fn recv(&self, buffer: B) -> Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wr_id = WRID::recv(id);

        let addr = buffer.addr();
        let len = buffer.len();
        let lkey = buffer.lkey();

        self.wrs.lock().unwrap().insert(id, buffer);

        let mut sge = crate::ibv_sge {
            addr: addr as u64,
            length: len as u32,
            lkey,
        };
        let mut wr = crate::ibv_recv_wr {
            wr_id,
            sg_list: &mut sge,
            num_sge: 1,
            ..Default::default()
        };

        let result = unsafe { self.post_recv(&mut wr) };
        if let Err((_, err)) = result {
            self.wrs.lock().unwrap().remove(&id);
            return Err(err);
        }
        Ok(())
    }

    /// Polls the send completion queue.
    ///
    /// Returns work completions paired with their buffers.
    pub fn poll_send(&self, wc: &mut [ibv_wc]) -> Result<Vec<Completion<B>>> {
        let n = self.send_cq.poll(wc)?;
        Ok(self.take_buffers(&wc[..n]))
    }

    /// Polls the receive completion queue.
    pub fn poll_recv(&self, wc: &mut [ibv_wc]) -> Result<Vec<Completion<B>>> {
        let n = self.recv_cq.poll(wc)?;
        Ok(self.take_buffers(&wc[..n]))
    }

    fn take_buffers(&self, wc: &[ibv_wc]) -> Vec<Completion<B>> {
        let mut completions = Vec::with_capacity(wc.len());
        let mut wrs = self.wrs.lock().unwrap();
        for wc in wc {
            let id = wc.wr_id.get_id();
            let buffer = wrs.remove(&id);
            completions.push(Completion { wc: *wc, buffer });
        }
        completions
    }

    /// Retrieves the buffer associated with the given work request ID.
    ///
    /// This is useful in event-driven mode where you poll the CQ directly
    /// (via `CompletionQueue::poll`) and need to look up the buffer afterwards.
    pub fn take_buffer(&self, wr_id: &WRID) -> Option<B> {
        self.wrs.lock().unwrap().remove(&wr_id.get_id())
    }

    // ------------------------------------------------------------------
    // Modifying QP state
    // ------------------------------------------------------------------

    /// Modifies the queue pair state/attributes.
    pub fn modify(&self, attr: &mut crate::ibv_qp_attr, attr_mask: c_int) -> Result<()> {
        let ret = unsafe { crate::ibv_modify_qp(self.ptr, attr, attr_mask) };
        if ret != 0 {
            return Err(ErrorKind::IBModifyQueuePairFail.with_errno());
        }
        Ok(())
    }

    const ACCESS_FLAGS: u32 = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
        | ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING.0;

    /// RESET → INIT.
    pub fn init(&self, port_num: u8) -> Result<()> {
        let mut attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_INIT,
            pkey_index: 0,
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

    /// INIT → RTR.
    pub fn ready_to_recv(&self, remote_qp_num: u32, gid: ibv_gid, lid: u16) -> Result<()> {
        let mut attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_RTR,
            path_mtu: ibv_mtu::IBV_MTU_512,
            dest_qp_num: remote_qp_num,
            rq_psn: 0,
            max_dest_rd_atomic: 1,
            min_rnr_timer: 0x12,
            ah_attr: crate::ibv_ah_attr {
                grh: crate::ibv_global_route {
                    dgid: gid,
                    flow_label: 0,
                    sgid_index: 1,
                    hop_limit: 0xff,
                    traffic_class: 0,
                },
                dlid: lid,
                sl: 0,
                src_path_bits: 0,
                static_rate: 0,
                is_global: 1,
                port_num: 1,
            },
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

    /// RTR → RTS.
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

    /// Convenience: RESET → INIT → RTR → RTS in one call.
    pub fn connect(&self, remote_qp_num: u32, gid: ibv_gid, lid: u16) -> Result<()> {
        self.init(1)?;
        self.ready_to_recv(remote_qp_num, gid, lid)?;
        self.ready_to_send()
    }

    // ------------------------------------------------------------------
    // Low-level unsafe operations (do not touch the wrs map)
    // ------------------------------------------------------------------

    /// Posts a raw send work request.
    ///
    /// # Safety
    ///
    /// Caller must ensure the WR chain and referenced memory are valid.
    /// This method does NOT manage buffer ownership — use [`send`](Self::send)
    /// for safe buffer tracking.
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

    /// Posts a raw receive work request.
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

impl<B: RdmaBuffer> Drop for QueuePair<B> {
    fn drop(&mut self) {
        let _ = unsafe { crate::ibv_destroy_qp(self.ptr) };
    }
}

impl<B: RdmaBuffer> std::fmt::Debug for QueuePair<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuePair")
            .field("ptr", &self.ptr)
            .field("qp_num", &self.qp_num())
            .finish()
    }
}

unsafe impl<B: RdmaBuffer> Send for QueuePair<B> {}
unsafe impl<B: RdmaBuffer> Sync for QueuePair<B> {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_utils::{TestBuffer, open_device, test_buffer, test_buffer_with};
    use crate::*;

    #[test]
    fn test_queue_pair_create() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = Arc::clone(dev.pd());
        let send_cq = CompletionQueue::create(&ctx, 16, None).unwrap();
        let recv_cq = CompletionQueue::create(&ctx, 16, None).unwrap();

        let _buf = test_buffer(&pd);

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 1,
                max_recv_wr: 1,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };

        let qp: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &send_cq, &recv_cq, &mut init_attr).unwrap();
        assert!(qp.qp_num() > 0);
    }

    #[test]
    fn test_queue_pair_modify_to_init() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = Arc::clone(dev.pd());
        let cq = CompletionQueue::create(&ctx, 16, None).unwrap();

        let _buf = test_buffer(&pd);

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 1,
                max_recv_wr: 1,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };

        let qp: QueuePair<TestBuffer> = QueuePair::create(&pd, &cq, &cq, &mut init_attr).unwrap();

        qp.init(1).unwrap();
    }

    #[test]
    fn test_queue_pair_debug() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = Arc::clone(dev.pd());
        let cq = CompletionQueue::create(&ctx, 16, None).unwrap();

        let _buf = test_buffer(&pd);

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 1,
                max_recv_wr: 1,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };

        let qp: QueuePair<TestBuffer> = QueuePair::create(&pd, &cq, &cq, &mut init_attr).unwrap();
        let debug = format!("{:?}", qp);
        assert!(debug.contains("QueuePair"));
        assert!(debug.contains("qp_num"));
    }

    #[test]
    fn test_read() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = Arc::clone(dev.pd());

        let port = &dev.info().ports[0];
        let port_attr = &port.port_attr;
        let gid = port.gids[1].gid;

        let send_cq = CompletionQueue::create(&ctx, 128, None).unwrap();
        let recv_cq = CompletionQueue::create(&ctx, 128, None).unwrap();

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 64,
                max_recv_wr: 64,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let qp_a: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &send_cq, &send_cq, &mut init_attr).unwrap();
        let qp_b: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &recv_cq, &recv_cq, &mut init_attr).unwrap();

        qp_a.connect(qp_b.qp_num(), gid, port_attr.lid).unwrap();
        qp_b.connect(qp_a.qp_num(), gid, port_attr.lid).unwrap();

        // Remote buffer — registered with REMOTE_READ access
        let mut remote_buf = test_buffer(&pd);
        remote_buf.as_mut_slice().fill(0xAB);

        // Read into a local buffer via qp_a
        let local_buf = test_buffer(&pd);
        qp_a.read(
            local_buf,
            remote_buf.addr() as u64,
            remote_buf.rkey(),
            ibv_send_flags::IBV_SEND_SIGNALED,
        )
        .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(100));

        let mut wc = [ibv_wc::default(); 1];
        let comps = qp_a.poll_send(&mut wc).unwrap();
        assert_eq!(comps.len(), 1);
        let buf = comps.into_iter().next().unwrap().buffer.unwrap();
        assert_eq!(buf.as_slice(), &[0xABu8; 64]);
    }

    /// Validates buffer ownership transfer for tracked RDMA Read.
    ///
    /// The tracked `read()` API takes ownership of the buffer.
    /// After completion, the buffer is returned via `poll_send()` /
    /// `take_buffer()`. This is the pattern used by `RdmaSocket::remote_read`
    /// to guarantee the local buffer stays alive during the RDMA operation.
    #[test]
    fn test_read_ownership_transfer() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = Arc::clone(dev.pd());

        let port = &dev.info().ports[0];
        let port_attr = &port.port_attr;
        let gid = port.gids[1].gid;

        let send_cq = CompletionQueue::create(&ctx, 128, None).unwrap();
        let recv_cq = CompletionQueue::create(&ctx, 128, None).unwrap();

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 64,
                max_recv_wr: 64,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let qp_a: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &send_cq, &send_cq, &mut init_attr).unwrap();
        let qp_b: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &recv_cq, &recv_cq, &mut init_attr).unwrap();

        qp_a.connect(qp_b.qp_num(), gid, port_attr.lid).unwrap();
        qp_b.connect(qp_a.qp_num(), gid, port_attr.lid).unwrap();

        // Remote buffer with known data pattern
        let mut remote_buf = test_buffer(&pd);
        remote_buf.as_mut_slice().fill(0xCD);

        // Local buffer — transferred into the QP via tracked `read()`
        let local_buf = test_buffer(&pd);
        let local_addr = local_buf.addr() as usize; // remember address for verification

        let wr_id = qp_a
            .read(
                local_buf, // ownership transferred here
                remote_buf.addr() as u64,
                remote_buf.rkey(),
                ibv_send_flags::IBV_SEND_SIGNALED,
            )
            .unwrap();

        // At this point, `local_buf` is consumed — the QP holds it.
        // Verify we can retrieve it via take_buffer after completion.
        std::thread::sleep(std::time::Duration::from_millis(100));

        let mut wc = [ibv_wc::default(); 1];
        let comps = qp_a.poll_send(&mut wc).unwrap();
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].wc.status, ibv_wc_status::IBV_WC_SUCCESS);

        // Buffer is returned with the completion — ownership recovered.
        let returned_buf = comps.into_iter().next().unwrap().buffer.unwrap();
        assert_eq!(returned_buf.addr() as usize, local_addr);
        assert_eq!(returned_buf.as_slice(), &[0xCDu8; 64]);

        // Verify take_buffer returns None (buffer was already taken via poll_send)
        assert!(qp_a.take_buffer(&wr_id).is_none());
    }

    /// Validates buffer ownership transfer for tracked RDMA Write.
    #[test]
    fn test_write_ownership_transfer() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = Arc::clone(dev.pd());

        let port = &dev.info().ports[0];
        let port_attr = &port.port_attr;
        let gid = port.gids[1].gid;

        let send_cq = CompletionQueue::create(&ctx, 128, None).unwrap();
        let recv_cq = CompletionQueue::create(&ctx, 128, None).unwrap();

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 64,
                max_recv_wr: 64,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let qp_a: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &send_cq, &send_cq, &mut init_attr).unwrap();
        let qp_b: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &recv_cq, &recv_cq, &mut init_attr).unwrap();

        qp_a.connect(qp_b.qp_num(), gid, port_attr.lid).unwrap();
        qp_b.connect(qp_a.qp_num(), gid, port_attr.lid).unwrap();

        // Remote buffer — will be written into (needs REMOTE_WRITE access)
        let remote_buf = test_buffer_with(
            &pd,
            vec![0u8; 64],
            ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ,
        );
        assert_eq!(remote_buf.as_slice(), &[0u8; 64]);

        // Local buffer with data to write — ownership transferred to QP
        let mut local_buf = test_buffer(&pd);
        local_buf.as_mut_slice().fill(0xEF);
        let local_addr = local_buf.addr() as usize;

        let _wr_id = qp_a
            .write(
                local_buf, // ownership transferred here
                remote_buf.addr() as u64,
                remote_buf.rkey(),
                ibv_send_flags::IBV_SEND_SIGNALED,
            )
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(100));

        let mut wc = [ibv_wc::default(); 1];
        let comps = qp_a.poll_send(&mut wc).unwrap();
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].wc.status, ibv_wc_status::IBV_WC_SUCCESS);

        // Buffer ownership recovered after completion
        let returned_buf = comps.into_iter().next().unwrap().buffer.unwrap();
        assert_eq!(returned_buf.addr() as usize, local_addr);

        // Verify the remote buffer received the data
        assert_eq!(remote_buf.as_slice(), &[0xEFu8; 64]);
    }

    /// Simple send / recv test using high-level typed API.
    #[test]
    fn test_send() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = Arc::clone(dev.pd());

        let port = &dev.info().ports[0];
        let port_attr = &port.port_attr;
        let gid = port.gids[1].gid;

        let send_cq = CompletionQueue::create(&ctx, 128, None).unwrap();
        let recv_cq = CompletionQueue::create(&ctx, 128, None).unwrap();

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 64,
                max_recv_wr: 64,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let qp_a: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &send_cq, &send_cq, &mut init_attr).unwrap();
        let qp_b: QueuePair<TestBuffer> =
            QueuePair::create(&pd, &recv_cq, &recv_cq, &mut init_attr).unwrap();

        qp_a.connect(qp_b.qp_num(), gid, port_attr.lid).unwrap();
        qp_b.connect(qp_a.qp_num(), gid, port_attr.lid).unwrap();

        // Post recv on qp_b, send on qp_a using high-level API
        let rbuf = test_buffer(&pd);
        qp_b.recv(rbuf).unwrap();

        let sbuf = test_buffer(&pd);
        qp_a.send(sbuf, ibv_send_flags::IBV_SEND_SIGNALED).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(100));

        let mut wc = [ibv_wc::default(); 1];
        let comps = qp_a.poll_send(&mut wc).unwrap();
        assert_eq!(comps.len(), 1);
        assert!(comps[0].buffer.is_some());

        let comps = qp_b.poll_recv(&mut wc).unwrap();
        assert_eq!(comps.len(), 1);
        assert!(comps[0].buffer.is_some());
    }
}
