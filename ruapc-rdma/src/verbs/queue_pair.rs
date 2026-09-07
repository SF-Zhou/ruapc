use std::{os::raw::c_int, ptr, sync::Arc};

mod read;
pub use read::{ReadFailure, ReadPosting, ReadReceiver, ReadRequest, ReadSegment};

use ruapc_bufpool::{Buffer, DeviceIndex};

use super::{
    completion_queue::{Completion, CompletionIdentity, CompletionQueue, IdentityLease},
    protection_domain::ProtectionDomain,
    wr_slots::WrSlots,
};
use crate::{
    Error, ErrorKind, QpConnectionConfig, Result, WRID, WRType, ibv_qp_attr, ibv_qp_attr_mask,
};

/// Maximum gather-list length accepted by [`QueuePair::send_gather`];
/// bounds its stack-allocated SGE array.
pub const MAX_GATHER_SGE: usize = 32;

/// Buffers owned by one in-flight receive or send work request.
/// Owned RDMA READ destinations are tracked separately in the QP's read state.
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

/// Progress of the selective SEND sweep for one QP. Keep one cursor per
/// completion consumer; it avoids rescanning IDs already covered by a CQE.
/// The cursor provides no reclamation authority without a completion proof.
#[derive(Debug, Default)]
pub struct CompletionCursor {
    sq_swept: u64,
}

impl CompletionCursor {
    #[inline]
    fn sweep_to(&mut self, first_sequence: u64, completed: u64) -> std::ops::Range<u64> {
        let start = self.sq_swept.max(first_sequence);
        self.sq_swept = self.sq_swept.max(completed + 1);
        start..completed
    }
}

/// Metadata and owned buffers released by a verified completion.
#[derive(Debug)]
pub struct CompletedWork<'a> {
    pub wc: &'a crate::ibv_wc,
    pub buffer: Option<WrBuffers>,
    /// Earlier data SENDs reclaimed under RC's ordered SQ completion rule.
    pub swept_sends: usize,
}

/// One local scatter segment of an RDMA READ posted via
/// [`QueuePair::read_sges`]: a raw (address, length, lkey) triple. The
/// caller owns the memory and must keep it alive until the read's
/// completion is observed.
#[derive(Debug, Clone, Copy)]
pub struct ReadSge {
    /// Local destination address.
    pub addr: u64,
    /// Segment length in bytes.
    pub len: u32,
    /// Local memory key covering the segment on this QP's device.
    pub lkey: u32,
}

/// Signaling is explicit at each entry point: window-tail and backlog sends
/// must complete independently; ordinary data sends may share a later CQE.
#[derive(Clone, Copy)]
enum SendSignaling {
    Selective,
    Always,
    Explicit,
}

impl SendSignaling {
    #[inline]
    fn flags(self, id: u64, interval: u64, flags: crate::ibv_send_flags) -> crate::ibv_send_flags {
        let signaled = crate::ibv_send_flags::IBV_SEND_SIGNALED;
        match self {
            Self::Explicit => flags,
            Self::Selective if interval > 1 && !id.is_multiple_of(interval) => {
                crate::ibv_send_flags(flags.0 & !signaled.0)
            }
            Self::Selective | Self::Always => flags | signaled,
        }
    }
}

pub struct QueuePair {
    ptr: *mut crate::ibv_qp,
    _pd: Arc<ProtectionDomain>,
    /// CQ-owned QPN identity and sequence space. A shared CQ uses one lease for
    /// both directions; separate CQs each register their own identity.
    send_identity: IdentityLease,
    recv_identity: Option<IdentityLease>,
    /// In-flight SEND buffers; READs share the identity's SQ sequence counter.
    send_wrs: WrSlots,
    /// In-flight buffers of receive-queue work requests.
    recv_wrs: WrSlots,
    /// Ownership of destinations is internal to this QP through completion.
    read_state: read::ReadState,
    /// Selective signaling interval for data sends posted via [`send`].
    ///
    /// `0` or `1` signals every work request. With interval `N > 1`, only
    /// data sends whose SQ sequence offset from this QP's first sequence is
    /// a multiple of `N` carry
    /// `IBV_SEND_SIGNALED`; completions of the unsignaled ones are inferred
    /// from later signaled completions (RC SQs complete in order) and their
    /// buffers are reclaimed by [`complete`](Self::complete).
    ///
    /// [`send`]: Self::send
    send_signal_interval: u64,
    /// Negotiated gather-list capability (`init_attr.cap.max_send_sge`
    /// after creation).
    max_send_sge: usize,
    pub device_index: DeviceIndex,
}

/// Setup can register one CQ before another rejects a reused QPN. Drop must
/// destroy the provider QP before Rust releases either registered identity.
struct CreatingQueuePair {
    ptr: *mut crate::ibv_qp,
    send_identity: Option<IdentityLease>,
    recv_identity: Option<IdentityLease>,
}

impl CreatingQueuePair {
    fn register_identities(
        &mut self,
        send_cq: &Arc<CompletionQueue>,
        recv_cq: &Arc<CompletionQueue>,
    ) -> Result<()> {
        let qp_num = unsafe { (*self.ptr).qp_num };
        self.send_identity = Some(send_cq.register_qp(qp_num)?);
        if !Arc::ptr_eq(send_cq, recv_cq) {
            self.recv_identity = Some(recv_cq.register_qp(qp_num)?);
        }
        Ok(())
    }
}

impl Drop for CreatingQueuePair {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            destroy_provider_qp(self.ptr);
        }
    }
}

fn destroy_provider_qp(ptr: *mut crate::ibv_qp) {
    if unsafe { crate::ruapc_ibv_destroy_qp(ptr) } != 0 {
        std::process::abort();
    }
}

impl QueuePair {
    pub fn create(
        pd: &Arc<ProtectionDomain>,
        send_cq: &Arc<CompletionQueue>,
        recv_cq: &Arc<CompletionQueue>,
        init_attr: &mut crate::ibv_qp_init_attr,
        device_index: DeviceIndex,
    ) -> Result<Self> {
        // Completion of a later SQ request only authorizes the selective
        // SEND sweep for reliable-connected queue pairs.
        if init_attr.qp_type != crate::ibv_qp_type::IBV_QPT_RC
            || !init_attr.srq.is_null()
            || !init_attr.qp_context.is_null()
            || !Arc::ptr_eq(pd.context(), send_cq.context())
            || !Arc::ptr_eq(pd.context(), recv_cq.context())
        {
            return Err(ErrorKind::InvalidQueuePairConfig.into());
        }
        init_attr.send_cq = send_cq.as_ptr();
        init_attr.recv_cq = recv_cq.as_ptr();
        let ptr = unsafe { crate::ruapc_ibv_create_qp(pd.as_ptr(), init_attr) };
        if ptr.is_null() {
            let source = std::io::Error::last_os_error();
            return Err(Error::new(
                ErrorKind::IBCreateQueuePairFail,
                format!(
                    "ibv_create_qp failed: type={:?}, requested_cap={:?}: {source}",
                    init_attr.qp_type, init_attr.cap,
                ),
            ));
        }
        // QPN identity can only be claimed after the provider assigns it.
        // The guard destroys this QP before releasing any partial registration.
        let mut creating = CreatingQueuePair {
            ptr,
            send_identity: None,
            recv_identity: None,
        };
        creating.register_identities(send_cq, recv_cq)?;
        // `ibv_create_qp` updates `init_attr.cap` with the actual (possibly
        // larger) queue depths; size the slot arrays from those.
        let send_wrs = WrSlots::new(init_attr.cap.max_send_wr);
        let recv_wrs = WrSlots::new(init_attr.cap.max_recv_wr);
        let read_state = read::ReadState::new();
        let qp = Self {
            ptr,
            _pd: Arc::clone(pd),
            send_identity: creating.send_identity.take().unwrap(),
            recv_identity: creating.recv_identity.take(),
            send_wrs,
            recv_wrs,
            read_state,
            max_send_sge: init_attr.cap.max_send_sge as usize,
            send_signal_interval: 1,
            device_index,
        };
        creating.ptr = ptr::null_mut();
        Ok(qp)
    }

    /// This QP's send completion identity, registered after provider creation.
    #[inline]
    pub fn send_identity(&self) -> CompletionIdentity {
        self.send_identity.identity()
    }

    /// This QP's receive completion identity. With a shared CQ it equals
    /// [`send_identity`](Self::send_identity).
    #[inline]
    pub fn recv_identity(&self) -> CompletionIdentity {
        self.receive_identity().identity()
    }

    pub fn send_cq(&self) -> &Arc<CompletionQueue> {
        self.send_identity.cq()
    }

    pub fn recv_cq(&self) -> &Arc<CompletionQueue> {
        self.receive_identity().cq()
    }

    #[inline]
    fn receive_identity(&self) -> &IdentityLease {
        self.recv_identity.as_ref().unwrap_or(&self.send_identity)
    }

    /// Sets the selective signaling interval for data sends.
    ///
    /// The interval is clamped to `max(1, max_send_wr / 2)` so that the send
    /// queue can always be reclaimed by polling a signaled completion.
    pub fn set_send_signal_interval(&mut self, interval: u32, max_send_wr: u32) {
        let limit = (max_send_wr / 2).max(1);
        self.send_signal_interval = u64::from(interval.clamp(1, limit));
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
        self.send_buffer(buffer, None, flags, SendSignaling::Selective)
    }

    /// Posts a data send with `IBV_SEND_SIGNALED` enforced, bypassing the
    /// selective signaling policy.
    pub fn send_signaled(&self, buffer: Buffer, flags: crate::ibv_send_flags) -> Result<u64> {
        self.send_buffer(buffer, None, flags, SendSignaling::Always)
    }

    /// Builds the one-buffer SGE before transferring ownership to the SQ.
    #[inline]
    fn send_buffer(
        &self,
        buffer: Buffer,
        imm: Option<u32>,
        flags: crate::ibv_send_flags,
        signaling: SendSignaling,
    ) -> Result<u64> {
        let mut sge = [crate::ibv_sge {
            addr: buffer.as_ptr() as u64,
            length: buffer.len() as u32,
            lkey: self.lkey(&buffer)?,
        }];
        self.post_send_buffers(Some(buffer.into()), &mut sge, imm, flags, signaling)
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
        let count = buffers.len();
        self.post_send_buffers(
            Some(WrBuffers::Many(buffers)),
            &mut sges[..count],
            imm,
            crate::ibv_send_flags::IBV_SEND_SIGNALED,
            SendSignaling::Always,
        )
    }

    /// Posts a send with immediate data and returns its send-queue id.
    pub fn send_imm(&self, buffer: Buffer, imm: u32, flags: crate::ibv_send_flags) -> Result<u64> {
        self.send_buffer(buffer, Some(imm), flags, SendSignaling::Explicit)
    }

    pub fn send_imm_only(&self, imm: u32, flags: crate::ibv_send_flags) -> Result<()> {
        self.post_send_buffers(None, &mut [], Some(imm), flags, SendSignaling::Explicit)
            .map(|_| ())
    }

    /// The common SEND transaction: allocate an SQ id in hardware post order,
    /// install the memory hold before posting, and undo it on post failure.
    ///
    /// READs use a separate transaction because their memory belongs to a batch,
    /// while a standalone ACK intentionally reserves an id without a buffer.
    #[inline]
    fn post_send_buffers(
        &self,
        buffers: Option<WrBuffers>,
        sges: &mut [crate::ibv_sge],
        imm: Option<u32>,
        flags: crate::ibv_send_flags,
        signaling: SendSignaling,
    ) -> Result<u64> {
        // The sequence guard serializes allocation, ownership registration
        // and provider posting. SQ IDs must follow hardware post order for
        // the selective completion sweep to be safe.
        let posting = self.send_identity.lock_send()?;
        let id = posting.sequence();
        let route = self.send_identity();
        let (wr_id, opcode, imm_data) = match imm {
            Some(imm) => (
                route.encode(WRType::SendImm, id),
                crate::ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
                imm.to_be(),
            ),
            None => (
                route.encode(WRType::SendData, id),
                crate::ibv_wr_opcode::IBV_WR_SEND,
                0,
            ),
        };
        if let Some(buffers) = buffers {
            self.send_wrs
                .insert(id, buffers)
                .map_err(|_| ErrorKind::WorkRequestSlotsExhausted)?;
        }
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: if sges.is_empty() {
                ptr::null_mut()
            } else {
                sges.as_mut_ptr()
            },
            num_sge: sges.len() as c_int,
            opcode,
            // Signaling cadence starts with this QP, independently of the
            // QPN's previous incarnations and their receive-queue traffic.
            send_flags: signaling
                .flags(
                    id - self.send_identity().first_sequence(),
                    self.send_signal_interval,
                    flags,
                )
                .0,
            __bindgen_anon_1: crate::ibv_send_wr__bindgen_ty_1 { imm_data },
            ..Default::default()
        };
        // SAFETY: the slot table owns every SGE's buffer before the NIC can
        // observe the WR. The stack-allocated descriptor is consumed by post.
        unsafe { self.post_send(&mut wr) }.map_err(|(_, err)| {
            self.send_wrs.take(id);
            err
        })?;
        Ok(id)
    }

    /// Posts one RDMA READ from a contiguous remote region into the local
    /// scatter list `sges`, always signaled.
    ///
    /// Unlike sends, no buffer ownership is stored in the WR slot table:
    /// the caller guarantees the memory behind `sges` stays alive (and is
    /// not recycled) until the read's work completion — success, error or
    /// flush — has been observed. `register` runs with the allocated
    /// [`WRID`] *before* the work request is posted (under the SQ post
    /// lock), so a completion can never race the caller's bookkeeping; on
    /// post failure `unregister` undoes it.
    ///
    /// # Safety
    ///
    /// Every SGE must describe writable registered memory on this QP's device.
    /// Keep that memory alive and exclusively available for DMA until the WR's
    /// success, error, or flush completion is observed (or the QP is destroyed).
    pub unsafe fn read_sges(
        &self,
        sges: &[ReadSge],
        remote_addr: u64,
        rkey: u32,
        register: impl FnOnce(WRID),
        unregister: impl FnOnce(WRID),
    ) -> Result<WRID> {
        if sges.is_empty() || sges.len() > self.gather_limit() {
            return Err(Error::new(
                ErrorKind::IBPostSendFail,
                format!(
                    "invalid read scatter list length {} (limit {})",
                    sges.len(),
                    self.gather_limit()
                ),
            ));
        }
        let mut raw = [crate::ibv_sge::default(); MAX_GATHER_SGE];
        for (dst, sge) in raw.iter_mut().zip(sges.iter()) {
            *dst = crate::ibv_sge {
                addr: sge.addr,
                length: sge.len,
                lkey: sge.lkey,
            };
        }
        let num_sge = sges.len() as c_int;

        let posting = self.send_identity.lock_send()?;
        let id = posting.sequence();
        let wr_id = self.send_identity().encode(WRType::Read, id);
        register(wr_id);
        let mut wr = crate::ibv_send_wr {
            wr_id,
            sg_list: raw.as_mut_ptr(),
            num_sge,
            opcode: crate::ibv_wr_opcode::IBV_WR_RDMA_READ,
            send_flags: crate::ibv_send_flags::IBV_SEND_SIGNALED.0,
            wr: crate::ibv_send_wr__bindgen_ty_2 {
                rdma: crate::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 { remote_addr, rkey },
            },
            ..Default::default()
        };
        unsafe { self.post_send(&mut wr) }.map_err(|(_, err)| {
            unregister(wr_id);
            err
        })?;
        Ok(wr_id)
    }

    pub fn recv(&self, buffer: Buffer) -> Result<()> {
        let addr = buffer.as_ptr() as u64;
        let len = buffer.capacity() as u32;
        let lkey = self.lkey(&buffer)?;
        let id = self.receive_identity().alloc_recv()?;
        let wr_id = self.recv_identity().encode(WRType::Recv, id);
        self.recv_wrs
            .insert(id, buffer.into())
            .map_err(|_| ErrorKind::WorkRequestSlotsExhausted)?;
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
        unsafe { self.post_recv(&mut wr) }.map_err(|(_, err)| {
            self.recv_wrs.take(id);
            err
        })
    }

    /// Consumes a CQ-issued completion proof and releases the completed memory.
    ///
    /// Validates the CQ, QP number and incarnation's sequence floor before
    /// accessing any ownership table. For SQ completions, RC ordering also proves
    /// that preceding unsignaled data SENDs no longer access their buffers.
    /// READ ownership is settled here; callers receive only completion metadata.
    #[inline]
    pub fn complete<'a>(
        &self,
        completion: Completion<'a>,
        cursor: &mut CompletionCursor,
    ) -> Result<CompletedWork<'a>> {
        let wc = completion.info();
        let route = if wc.is_recv() {
            self.receive_identity()
        } else {
            &self.send_identity
        };
        if !completion.belongs_to(route.cq(), self.qp_num(), route.identity()) {
            return Err(ErrorKind::InvalidCompletion.into());
        }
        let id = completion.sequence();
        let mut swept_sends = 0;
        let buffer = if wc.is_recv() {
            self.recv_wrs.take(id)
        } else {
            // A replacement QP can start far above zero. Never scan the
            // sequence space preceding this QPN incarnation.
            for swept in cursor.sweep_to(route.identity().first_sequence(), id) {
                if self.send_wrs.take(swept).is_some() {
                    swept_sends += 1;
                }
            }
            if wc.wr_id.get_type() == WRType::Read {
                self.complete_read(wc.wr_id, wc.succ());
            }
            self.send_wrs.take(id)
        };
        Ok(CompletedWork {
            wc,
            buffer,
            swept_sends,
        })
    }

    /// Applies raw QP attributes, preserving the provider's returned errno.
    pub fn modify(&self, attr: &mut ibv_qp_attr, attr_mask: c_int) -> Result<()> {
        let ret = unsafe { crate::ruapc_ibv_modify_qp(self.ptr, attr, attr_mask) };
        if ret != 0 {
            return Err(modify_error(self.qp_num(), attr, attr_mask, ret));
        }
        Ok(())
    }

    /// Connects a freshly created reliable-connected QP: RESET → INIT → RTR → RTS.
    ///
    /// All parameters are validated before the first transition. A provider
    /// failure leaves the QP at the last successful stage; callers must discard
    /// it instead of retrying this method on the same QP. Receive buffers must
    /// be posted before the peer is allowed to send application traffic.
    pub fn connect(&self, config: &QpConnectionConfig) -> Result<()> {
        config.validate()?;
        for (transition, (mut attr, mask)) in [
            ("RESET -> INIT", config.init_attributes()),
            ("INIT -> RTR", config.receive_attributes()),
            ("RTR -> RTS", config.send_attributes()),
        ] {
            self.modify(&mut attr, mask.0 as _).map_err(|error| {
                Error::new(
                    error.kind,
                    format!("{transition}: {}; connection={config:?}", error.msg),
                )
            })?;
        }
        Ok(())
    }

    /// Posts a raw send work request chain.
    ///
    /// On failure returns the offending work request (`bad_wr`) alongside
    /// the error.
    ///
    /// # Safety
    ///
    /// `wr` must point to a valid, properly linked `ibv_send_wr` chain
    /// whose SG lists reference registered memory that stays alive until
    /// the corresponding completions are observed.
    pub(crate) unsafe fn post_send(
        &self,
        wr: *mut crate::ibv_send_wr,
    ) -> std::result::Result<(), (*mut crate::ibv_send_wr, Error)> {
        let mut bad_wr: *mut crate::ibv_send_wr = ptr::null_mut();
        let ret = unsafe { crate::ruapc_ibv_post_send(self.ptr, wr, &mut bad_wr) };
        if ret != 0 {
            return Err((bad_wr, ErrorKind::IBPostSendFail.with_errno()));
        }
        Ok(())
    }

    /// Posts a raw receive work request chain.
    ///
    /// On failure returns the offending work request (`bad_wr`) alongside
    /// the error.
    ///
    /// # Safety
    ///
    /// `wr` must point to a valid, properly linked `ibv_recv_wr` chain
    /// whose SG lists reference registered memory that stays alive until
    /// the corresponding completions are observed.
    pub(crate) unsafe fn post_recv(
        &self,
        wr: *mut crate::ibv_recv_wr,
    ) -> std::result::Result<(), (*mut crate::ibv_recv_wr, Error)> {
        let mut bad_wr: *mut crate::ibv_recv_wr = ptr::null_mut();
        let ret = unsafe { crate::ruapc_ibv_post_recv(self.ptr, wr, &mut bad_wr) };
        if ret != 0 {
            return Err((bad_wr, ErrorKind::IBPostRecvFail.with_errno()));
        }
        Ok(())
    }
}

// ibv_modify_qp returns an errno value directly; last_os_error() can be stale.
fn modify_error(qp_num: u32, attr: &ibv_qp_attr, attr_mask: c_int, errno: c_int) -> Error {
    let target = if attr_mask & ibv_qp_attr_mask::IBV_QP_STATE.0 as c_int != 0 {
        format!("{:?}", attr.qp_state)
    } else {
        "unchanged".to_owned()
    };
    Error::new(
        ErrorKind::IBModifyQueuePairFail,
        format!(
            "ibv_modify_qp failed: qp_num={qp_num}, target_state={target}, \
             attr_mask={attr_mask:#x}, errno={errno}: {}",
            std::io::Error::from_raw_os_error(errno),
        ),
    )
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        // Field destruction releases SEND/RECV buffers, READ destinations and
        // their PD/MR ownership. If the provider cannot destroy the QP, DMA may
        // still access them; unwinding would release those holds as well.
        destroy_provider_qp(self.ptr);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selective_sweep_skips_previous_incarnations_and_already_completed_work() {
        let first = 1 << 47;
        let mut cursor = CompletionCursor::default();
        assert_eq!(cursor.sweep_to(first, first + 2), first..first + 2);
        assert_eq!(cursor.sweep_to(first, first + 4), first + 3..first + 4);
        // A CQ batch may be consumed out of order. Its old token must neither
        // rewind the cursor nor make a subsequent completion scan old IDs.
        assert!(cursor.sweep_to(first, first + 1).is_empty());
        assert_eq!(cursor.sweep_to(first, first + 7), first + 5..first + 7);
    }

    fn init_attr() -> crate::ibv_qp_init_attr {
        crate::ibv_qp_init_attr {
            qp_type: crate::ibv_qp_type::IBV_QPT_RC,
            cap: crate::ibv_qp_cap {
                max_send_wr: 4,
                max_recv_wr: 4,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn qp_identities_are_registered_after_creation_and_retire_after_destruction() {
        let device = crate::test_utils::open_device();
        let cq = CompletionQueue::create(device.context(), 16, None).unwrap();
        let qp = QueuePair::create(
            device.pd(),
            &cq,
            &cq,
            &mut init_attr(),
            DeviceIndex::default(),
        )
        .unwrap();
        let identity = qp.send_identity();
        assert_eq!(identity, qp.recv_identity());
        assert_eq!(identity.qp_num(), qp.qp_num());
        assert!(Arc::ptr_eq(qp.send_cq(), &cq));
        assert!(Arc::ptr_eq(qp.recv_cq(), &cq));
        assert_eq!(cq.qp_registry_stats().active, 1);
        for _ in 0..300 {
            qp.send_identity.lock_send().unwrap().sequence();
        }
        qp.receive_identity().alloc_recv().unwrap();
        drop(qp);
        assert_eq!(cq.qp_registry_stats().active, 0);
        assert_eq!(cq.qp_registry_stats().next_sequence_floor, 300);
        let replacement = QueuePair::create(
            device.pd(),
            &cq,
            &cq,
            &mut init_attr(),
            DeviceIndex::default(),
        )
        .unwrap();
        assert_eq!(replacement.send_identity().first_sequence(), 300);
        assert_eq!(
            replacement.send_identity.lock_send().unwrap().sequence(),
            300
        );
        assert_eq!(replacement.receive_identity().alloc_recv().unwrap(), 300);
    }

    #[test]
    fn cq_capacity_does_not_limit_qps_with_no_posted_work() {
        let device = crate::test_utils::open_device();
        let cq = CompletionQueue::create(device.context(), 1, None).unwrap();
        let qps: Vec<_> = (0..cq.capacity() + 2)
            .map(|_| {
                QueuePair::create(
                    device.pd(),
                    &cq,
                    &cq,
                    &mut init_attr(),
                    DeviceIndex::default(),
                )
                .unwrap()
            })
            .collect();
        assert!(qps.len() > cq.capacity() as usize);
        assert_eq!(cq.qp_registry_stats().active, qps.len());
        assert!(
            qps.iter()
                .all(|qp| qp.send_identity().first_sequence() == 0)
        );
        drop(qps);
        assert_eq!(cq.qp_registry_stats().active, 0);
        assert_eq!(cq.qp_registry_stats().next_sequence_floor, 1);
    }

    #[test]
    fn separate_cqs_preserve_independent_retired_floors() {
        let device = crate::test_utils::open_device();
        let send_cq = CompletionQueue::create(device.context(), 16, None).unwrap();
        let recv_cq = CompletionQueue::create(device.context(), 1024, None).unwrap();
        let qp = QueuePair::create(
            device.pd(),
            &send_cq,
            &recv_cq,
            &mut init_attr(),
            DeviceIndex::default(),
        )
        .unwrap();
        assert!(qp.recv_identity.is_some());
        assert!(Arc::ptr_eq(qp.send_cq(), &send_cq));
        assert!(Arc::ptr_eq(qp.recv_cq(), &recv_cq));
        for _ in 0..7 {
            qp.send_identity.lock_send().unwrap().sequence();
        }
        for _ in 0..11 {
            qp.receive_identity().alloc_recv().unwrap();
        }
        drop(qp);
        assert_eq!(send_cq.qp_registry_stats().next_sequence_floor, 7);
        assert_eq!(recv_cq.qp_registry_stats().next_sequence_floor, 11);
        let next = QueuePair::create(
            device.pd(),
            &send_cq,
            &recv_cq,
            &mut init_attr(),
            DeviceIndex::default(),
        )
        .unwrap();
        assert_eq!(next.send_identity().first_sequence(), 7);
        assert_eq!(next.recv_identity().first_sequence(), 11);
    }

    #[test]
    fn failed_second_cq_registration_releases_only_its_own_partial_lease() {
        let device = crate::test_utils::open_device();
        let send_cq = CompletionQueue::create(device.context(), 16, None).unwrap();
        let recv_cq = CompletionQueue::create(device.context(), 16, None).unwrap();
        let mut attrs = init_attr();
        attrs.send_cq = send_cq.as_ptr();
        attrs.recv_cq = recv_cq.as_ptr();
        let ptr = unsafe { crate::ruapc_ibv_create_qp(device.pd().as_ptr(), &mut attrs) };
        assert!(!ptr.is_null());
        let qp_num = unsafe { (*ptr).qp_num };
        let mut creating = CreatingQueuePair {
            ptr,
            send_identity: None,
            recv_identity: None,
        };
        // Model provider QPN reuse before the previous Rust lease was released.
        let old = recv_cq.register_qp(qp_num).unwrap();
        assert_eq!(
            creating
                .register_identities(&send_cq, &recv_cq)
                .unwrap_err()
                .kind,
            ErrorKind::CompletionIdentityInUse
        );
        assert_eq!(send_cq.qp_registry_stats().active, 1);
        assert_eq!(recv_cq.qp_registry_stats().active, 1);
        drop(creating); // Its Drop destroys the provider QP before its fields.
        assert_eq!(send_cq.qp_registry_stats().active, 0);
        assert_eq!(send_cq.qp_registry_stats().next_sequence_floor, 1);
        assert_eq!(recv_cq.qp_registry_stats().active, 1);
        assert_eq!(old.lock_send().unwrap().sequence(), 0);
        drop(old);
        assert_eq!(recv_cq.qp_registry_stats().next_sequence_floor, 1);
    }

    #[test]
    fn qp_creation_rejects_unowned_resources_and_foreign_contexts() {
        let device = crate::test_utils::open_device();
        let cq = CompletionQueue::create(device.context(), 16, None).unwrap();
        let mut attrs = init_attr();
        attrs.srq = ptr::dangling_mut();
        assert_eq!(
            QueuePair::create(device.pd(), &cq, &cq, &mut attrs, DeviceIndex::default())
                .unwrap_err()
                .kind,
            ErrorKind::InvalidQueuePairConfig
        );
        attrs.srq = ptr::null_mut();
        attrs.qp_context = ptr::dangling_mut();
        assert!(
            QueuePair::create(device.pd(), &cq, &cq, &mut attrs, DeviceIndex::default()).is_err()
        );
        let foreign = crate::test_utils::open_device();
        let foreign_cq = CompletionQueue::create(foreign.context(), 16, None).unwrap();
        assert_eq!(
            QueuePair::create(
                device.pd(),
                &cq,
                &foreign_cq,
                &mut init_attr(),
                DeviceIndex::default()
            )
            .unwrap_err()
            .kind,
            ErrorKind::InvalidQueuePairConfig
        );
    }

    #[test]
    fn signaling_preserves_flags_and_forces_window_tail_completion() {
        use crate::ibv_send_flags as Flags;
        let flags = Flags::IBV_SEND_INLINE | Flags::IBV_SEND_SIGNALED;
        for id in 0..16 {
            let selective = SendSignaling::Selective.flags(id, 4, flags);
            assert_eq!(
                selective.0 & Flags::IBV_SEND_INLINE.0,
                Flags::IBV_SEND_INLINE.0
            );
            assert_eq!(selective.0 & Flags::IBV_SEND_SIGNALED.0 != 0, id % 4 == 0);
            assert_eq!(SendSignaling::Always.flags(id, 4, flags).0, flags.0);
            assert_eq!(SendSignaling::Selective.flags(id, 1, flags).0, flags.0);
        }
        assert_eq!(
            SendSignaling::Explicit
                .flags(0, 4, Flags::IBV_SEND_INLINE)
                .0,
            Flags::IBV_SEND_INLINE.0
        );
        assert_eq!(
            SendSignaling::Always.flags(1, 4, Flags::IBV_SEND_INLINE).0,
            flags.0
        );
    }

    #[test]
    fn modify_error_reports_provider_errno_and_target_state() {
        let attr = ibv_qp_attr {
            qp_state: crate::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        let error = modify_error(
            42,
            &attr,
            ibv_qp_attr_mask::IBV_QP_STATE.0 as _,
            libc::EINVAL,
        );
        assert_eq!(error.kind, ErrorKind::IBModifyQueuePairFail);
        assert!(error.msg.contains("qp_num=42"));
        assert!(error.msg.contains("target_state=IBV_QPS_ERR"));
        assert!(error.msg.contains(&format!("errno={}", libc::EINVAL)));
        assert!(
            error
                .msg
                .contains(&std::io::Error::from_raw_os_error(libc::EINVAL).to_string())
        );
    }
}
