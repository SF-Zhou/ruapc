use std::{
    os::raw::c_int,
    ptr,
    sync::{Arc, Mutex},
};

use ruapc_bufpool::{Buffer, DeviceIndex};

use super::{
    completion_queue::CompletionQueue, protection_domain::ProtectionDomain, wr_slots::WrSlots,
};
use crate::{
    Error, ErrorKind, LinkLayer, Result, WRID, WRType, ibv_gid, ibv_mtu, ibv_qp_attr,
    ibv_qp_attr_mask, ibv_qp_state, ibv_wc,
};

/// Maximum gather-list length accepted by [`QueuePair::send_gather`];
/// bounds its stack-allocated SGE array.
pub const MAX_GATHER_SGE: usize = 32;

/// Buffers owned by one in-flight work request: receives, reads and plain
/// sends hold one buffer; gather-list sends hold several.
#[derive(Debug)]
pub enum WrBuffers {
    One(Buffer),
    Many(Box<[Buffer]>),
}

impl WrBuffers {
    /// Returns the buffer of a single-buffer work request; `None` for a
    /// gather list.
    pub fn into_single(self) -> Option<Buffer> {
        match self {
            Self::One(buffer) => Some(buffer),
            Self::Many(_) => None,
        }
    }
}

impl From<Buffer> for WrBuffers {
    fn from(buffer: Buffer) -> Self {
        Self::One(buffer)
    }
}

pub struct Completion {
    pub wc: ibv_wc,
    pub buffer: Option<WrBuffers>,
}

pub struct QueuePair {
    ptr: *mut crate::ibv_qp,
    _pd: Arc<ProtectionDomain>,
    send_cq: Arc<CompletionQueue>,
    recv_cq: Arc<CompletionQueue>,
    /// In-flight buffers of send-queue work requests (send/send_imm/read).
    send_wrs: WrSlots,
    /// In-flight buffers of receive-queue work requests.
    recv_wrs: WrSlots,
    /// Selective signaling interval for data sends posted via [`send`].
    ///
    /// `0` or `1` signals every work request. With interval `N > 1`, only
    /// data sends whose SQ id is a multiple of `N` carry
    /// `IBV_SEND_SIGNALED`; completions of the unsignaled ones are inferred
    /// from later signaled completions (RC SQs complete in order) and their
    /// buffers are reclaimed via [`take_send_buffer`](Self::take_send_buffer).
    ///
    /// [`send`]: Self::send
    send_signal_interval: u64,
    /// Opaque connection tag stamped into every [`WRID`] this QP posts, so
    /// completion consumers can map a `wr_id` back to the owning
    /// connection without a `qp_num` lookup. Must be set (via
    /// [`set_wr_tag`](Self::set_wr_tag)) before any work request is posted.
    wr_tag: u32,
    /// Negotiated gather-list capability (`init_attr.cap.max_send_sge`
    /// after creation).
    max_send_sge: usize,
    /// Serializes send-queue posts so that SQ ids are allocated in post
    /// order.
    ///
    /// Selective signaling infers completion of unsignaled sends from later
    /// signaled completions, which requires the SQ id order to match the
    /// hardware post order exactly. Without this lock, two concurrent
    /// posters could allocate ids in one order and post in the other,
    /// letting the completion sweep reclaim the buffer of a still-in-flight
    /// work request. The completion path stays lock-free.
    sq_post_lock: Mutex<()>,
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
        // `ibv_create_qp` updates `init_attr.cap` with the actual (possibly
        // larger) queue depths; size the slot arrays from those.
        Ok(Self {
            ptr,
            _pd: Arc::clone(pd),
            send_cq: Arc::clone(send_cq),
            recv_cq: Arc::clone(recv_cq),
            send_wrs: WrSlots::new(init_attr.cap.max_send_wr),
            recv_wrs: WrSlots::new(init_attr.cap.max_recv_wr),
            max_send_sge: init_attr.cap.max_send_sge as usize,
            send_signal_interval: 1,
            wr_tag: 0,
            sq_post_lock: Mutex::new(()),
            device_index,
        })
    }

    /// Sets the connection tag embedded in every posted `wr_id`.
    ///
    /// Must be called before any work request is posted: completions of
    /// work requests posted with a different tag would be attributed to
    /// the wrong (or no) connection.
    pub fn set_wr_tag(&mut self, tag: u32) {
        assert!(tag <= WRID::TAG_MAX, "wr tag too large");
        self.wr_tag = tag;
    }

    /// Sets the selective signaling interval for data sends.
    ///
    /// The interval is clamped to `max(1, max_send_wr / 2)` so that the send
    /// queue can always be reclaimed by polling a signaled completion.
    pub fn set_send_signal_interval(&mut self, interval: u32, max_send_wr: u32) {
        let limit = (max_send_wr / 2).max(1);
        self.send_signal_interval = u64::from(interval.clamp(1, limit));
    }

    /// Computes the send flags for a data send with the given SQ id,
    /// applying the selective signaling policy.
    fn data_send_flags(&self, id: u64, flags: crate::ibv_send_flags) -> crate::ibv_send_flags {
        if self.send_signal_interval > 1 && !id.is_multiple_of(self.send_signal_interval) {
            crate::ibv_send_flags(flags.0 & !crate::ibv_send_flags::IBV_SEND_SIGNALED.0)
        } else {
            flags | crate::ibv_send_flags::IBV_SEND_SIGNALED
        }
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

    /// Posts a data send and returns its send-queue id.
    ///
    /// Subject to the selective signaling policy: the completion of an
    /// unsignaled send is only observed (and its send-window slot only
    /// released) when a *later* signaled work request completes. Callers
    /// posting a send that may be the last one for a while — in particular
    /// one that consumes the tail of the send window — must use
    /// [`send_signaled`](Self::send_signaled) instead, or the window slots
    /// stay stranded until an unrelated signaled WR (e.g. a keepalive ACK)
    /// happens to sweep them.
    pub fn send(&self, buffer: Buffer, flags: crate::ibv_send_flags) -> Result<u64> {
        self.send_data(buffer, flags, false)
    }

    /// Posts a data send with `IBV_SEND_SIGNALED` enforced, bypassing the
    /// selective signaling policy.
    pub fn send_signaled(&self, buffer: Buffer, flags: crate::ibv_send_flags) -> Result<u64> {
        self.send_data(buffer, flags, true)
    }

    fn send_data(
        &self,
        buffer: Buffer,
        flags: crate::ibv_send_flags,
        force_signal: bool,
    ) -> Result<u64> {
        let addr = buffer.as_ptr() as u64;
        let len = buffer.len() as u32;
        let lkey = self.lkey(&buffer)?;
        let _guard = self.sq_post_lock.lock().unwrap();
        let id = self.send_wrs.alloc_id();
        let flags = if force_signal {
            flags | crate::ibv_send_flags::IBV_SEND_SIGNALED
        } else {
            self.data_send_flags(id, flags)
        };
        let wr_id = WRID::send_data(self.wr_tag, id);
        self.send_wrs.insert(id, buffer.into());
        self.post_send_verb(
            wr_id,
            addr,
            len,
            lkey,
            crate::ibv_wr_opcode::IBV_WR_SEND,
            flags.0,
            None,
        )
        .inspect_err(|_e| {
            self.send_wrs.take(id);
        })?;
        Ok(id)
    }

    /// Longest gather list this QP can post (negotiated `max_send_sge`,
    /// bounded by [`MAX_GATHER_SGE`]).
    pub fn gather_limit(&self) -> usize {
        self.max_send_sge.min(MAX_GATHER_SGE)
    }

    /// Posts one data send gathering several framed buffers into a single
    /// wire message (zero-copy aggregation) and returns its send-queue id.
    ///
    /// The receiver sees the plain concatenation of the buffers, exactly
    /// as if they had been copied into one; the gather list is purely a
    /// local property of the send WQE. With `imm` the send carries the
    /// value as immediate data (piggybacked ACK) and is posted as an
    /// immediate work request.
    ///
    /// Always posted signaled, bypassing selective signaling: gather
    /// aggregates are flushed from backlogs and may be the last WR for a
    /// while, so their completion (and the buffers it releases) must not
    /// wait for an unrelated signaled send.
    pub fn send_gather(&self, buffers: Box<[Buffer]>, imm: Option<u32>) -> Result<u64> {
        if buffers.is_empty() || buffers.len() > self.gather_limit() {
            return Err(Error::new(
                ErrorKind::IBPostSendFail,
                format!(
                    "invalid gather list length {} (limit {})",
                    buffers.len(),
                    self.gather_limit()
                ),
            ));
        }
        let mut sges = [crate::ibv_sge::default(); MAX_GATHER_SGE];
        for (sge, buffer) in sges.iter_mut().zip(buffers.iter()) {
            *sge = crate::ibv_sge {
                addr: buffer.as_ptr() as u64,
                length: buffer.len() as u32,
                lkey: self.lkey(buffer)?,
            };
        }
        let num_sge = buffers.len() as c_int;

        let _guard = self.sq_post_lock.lock().unwrap();
        let id = self.send_wrs.alloc_id();
        let (wr_id, opcode, imm_data) = match imm {
            Some(imm) => (
                WRID::send_imm(self.wr_tag, id),
                crate::ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
                imm.to_be(),
            ),
            None => (
                WRID::send_data(self.wr_tag, id),
                crate::ibv_wr_opcode::IBV_WR_SEND,
                0,
            ),
        };
        self.send_wrs.insert(id, WrBuffers::Many(buffers));
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: sges.as_mut_ptr(),
            num_sge,
            opcode,
            send_flags: crate::ibv_send_flags::IBV_SEND_SIGNALED.0,
            __bindgen_anon_1: crate::ibv_send_wr__bindgen_ty_1 { imm_data },
            ..Default::default()
        };
        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            self.send_wrs.take(id);
            return Err(err);
        }
        Ok(id)
    }

    /// Posts a send with immediate data and returns its send-queue id.
    pub fn send_imm(&self, buffer: Buffer, imm: u32, flags: crate::ibv_send_flags) -> Result<u64> {
        let addr = buffer.as_ptr() as u64;
        let len = buffer.len() as u32;
        let lkey = self.lkey(&buffer)?;
        let _guard = self.sq_post_lock.lock().unwrap();
        let id = self.send_wrs.alloc_id();
        let wr_id = WRID::send_imm(self.wr_tag, id);
        self.send_wrs.insert(id, buffer.into());
        let mut sge = crate::ibv_sge {
            addr,
            length: len,
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
            self.send_wrs.take(id);
            return Err(err);
        }
        Ok(id)
    }

    pub fn send_imm_only(&self, imm: u32, flags: crate::ibv_send_flags) -> Result<()> {
        // Allocates an ID (the WR consumes a hardware SQ slot) but stores no
        // buffer; the slot stays empty and `take` will return `None`.
        let _guard = self.sq_post_lock.lock().unwrap();
        let id = self.send_wrs.alloc_id();
        let wr_id = WRID::send_imm(self.wr_tag, id);
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

    pub fn read(
        &self,
        buffer: Buffer,
        remote_addr: u64,
        rkey: u32,
        flags: crate::ibv_send_flags,
    ) -> Result<WRID> {
        let addr = buffer.as_ptr() as u64;
        let len = buffer.len() as u32;
        let lkey = self.lkey(&buffer)?;
        let _guard = self.sq_post_lock.lock().unwrap();
        let id = self.send_wrs.alloc_id();
        let wr_id = WRID::read(self.wr_tag, id);
        self.send_wrs.insert(id, buffer.into());
        self.post_send_verb(
            wr_id,
            addr,
            len,
            lkey,
            crate::ibv_wr_opcode::IBV_WR_RDMA_READ,
            flags.0,
            Some((remote_addr, rkey)),
        )
        .inspect_err(|_e| {
            self.send_wrs.take(id);
        })?;
        Ok(wr_id)
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
        let result = unsafe { self.post_send(&mut wr) };
        if let Err((_, err)) = result {
            return Err(err);
        }
        Ok(())
    }

    pub fn recv(&self, buffer: Buffer) -> Result<()> {
        let id = self.recv_wrs.alloc_id();
        let wr_id = WRID::recv(self.wr_tag, id);
        let addr = buffer.as_ptr() as u64;
        let len = buffer.capacity() as u32;
        let lkey = self.lkey(&buffer)?;
        self.recv_wrs.insert(id, buffer.into());
        let mut sge = crate::ibv_sge {
            addr,
            length: len,
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
            self.recv_wrs.take(id);
            return Err(err);
        }
        Ok(())
    }

    pub fn poll_send(&self, wc: &mut [ibv_wc]) -> Result<Vec<Completion>> {
        let n = self.send_cq.poll(wc)?;
        Ok(self.take_buffers(&wc[..n]))
    }

    pub fn poll_recv(&self, wc: &mut [ibv_wc]) -> Result<Vec<Completion>> {
        let n = self.recv_cq.poll(wc)?;
        Ok(self.take_buffers(&wc[..n]))
    }

    fn take_buffers(&self, wc: &[ibv_wc]) -> Vec<Completion> {
        let mut completions = Vec::with_capacity(wc.len());
        for wc in wc {
            let buffer = self.take_buffer(&wc.wr_id);
            completions.push(Completion { wc: *wc, buffer });
        }
        completions
    }

    pub fn take_buffer(&self, wr_id: &WRID) -> Option<WrBuffers> {
        match wr_id.get_type() {
            WRType::Recv => self.recv_wrs.take(wr_id.get_id()),
            WRType::SendData | WRType::SendImm | WRType::Read => self.send_wrs.take(wr_id.get_id()),
        }
    }

    /// Takes the buffer(s) of a send-queue work request by raw SQ id.
    ///
    /// Used by the completion handler to reclaim buffers of *unsignaled*
    /// data sends: when a signaled completion with id `X` is polled, all SQ
    /// work requests with ids below `X` are guaranteed complete (RC SQs
    /// complete in order). Returns `None` for ids without stored buffers
    /// (buffer-less immediate sends or failed posts).
    pub fn take_send_buffer(&self, id: u64) -> Option<WrBuffers> {
        self.send_wrs.take(id)
    }

    /// Reclaims every in-flight send buffer, returning how many were taken.
    ///
    /// Only safe to call after the QP has transitioned to the error state
    /// and its outstanding completions have been drained: buffers of
    /// unsignaled data sends that completed successfully never produce a
    /// CQE, so teardown must reclaim them explicitly.
    pub fn reclaim_send_buffers(&self) -> usize {
        self.send_wrs.reclaim_all()
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
        rq_psn: u32,
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
            rq_psn: rq_psn & 0xFF_FFFF,
            max_dest_rd_atomic: 1,
            // 0.01ms (encoding 1): an RNR NAK costs ~10µs instead of the
            // common-default 1.28ms (0x12). The receive ring is normally
            // pre-posted ahead of the peer's send window, so RNR is a rare
            // transient (repost lag); a short backoff keeps it invisible
            // instead of quantizing the connection's RTT in 1.28ms steps.
            min_rnr_timer: 0x01,
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

    pub fn ready_to_send(&self, sq_psn: u32) -> Result<()> {
        let mut attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_RTS,
            timeout: 0x12,
            retry_cnt: 6,
            rnr_retry: 6,
            sq_psn: sq_psn & 0xFF_FFFF,
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
        local_psn: u32,
        remote_psn: u32,
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
            remote_psn,
        )?;
        self.ready_to_send(local_psn)
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
