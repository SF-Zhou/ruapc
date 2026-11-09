//! Revocable QP ownership. No provider operation borrows across an await.

use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicBool, Ordering},
};

use ruapc_rdma::{
    CompletedWork, Completion, CompletionCursor, CompletionIdentity, CompletionQueue, Error,
    ErrorKind, QpConnectionConfig, QueuePair, ReadCompletion, ReadPosting, ReadRequest, Result,
    ibv_qp_attr, ibv_send_flags,
};
use tokio::sync::Notify;

use super::{ConnReservation, PollerShared};
use crate::Buffer;
use ruapc_bufpool::WriteBuffer;

#[derive(Debug)]
struct LiveQp {
    qp: Option<QueuePair>,
    posting_open: bool,
}

/// The provider QP can be destroyed while cached socket handles still exist.
/// The gate excludes provider calls from shutdown and rejects later posting.
/// Fields preserve provider destruction before CQ-capacity retirement on setup
/// errors too; explicit retirement follows the same order.
#[derive(Debug)]
pub(crate) struct ReservedQueuePair {
    live: RwLock<LiveQp>,
    reservation: Mutex<Option<ConnReservation>>,
    shared: Arc<PollerShared>,
    cq: Arc<CompletionQueue>,
    identity: CompletionIdentity,
    gather_limit: usize,
    pub(crate) device_index: usize,
    destroyed: AtomicBool,
    destruction: Notify,
}

impl ReservedQueuePair {
    pub(super) fn new(qp: QueuePair, reservation: ConnReservation) -> Self {
        Self {
            cq: qp.cq().clone(),
            identity: qp.identity(),
            gather_limit: qp.gather_limit(),
            device_index: qp.device_index,
            shared: reservation.shared.clone(),
            live: RwLock::new(LiveQp {
                qp: Some(qp),
                posting_open: true,
            }),
            reservation: Mutex::new(Some(reservation)),
            destroyed: AtomicBool::new(false),
            destruction: Notify::new(),
        }
    }

    fn closed() -> Error {
        Error::new(ErrorKind::IBPostSendFail, "queue pair is closed".into())
    }

    fn with_live<T>(&self, operation: impl FnOnce(&QueuePair) -> Result<T>) -> Result<T> {
        let live = self.live.read().unwrap();
        operation(live.qp.as_ref().ok_or_else(Self::closed)?)
    }

    fn post<T>(&self, operation: impl FnOnce(&QueuePair) -> Result<T>) -> Result<T> {
        let live = self.live.read().unwrap();
        if !live.posting_open {
            return Err(Self::closed());
        }
        operation(live.qp.as_ref().ok_or_else(Self::closed)?)
    }

    pub(super) fn belongs_to(&self, shared: &Arc<PollerShared>) -> bool {
        Arc::ptr_eq(&self.shared, shared)
    }

    pub(crate) fn cq(&self) -> &Arc<CompletionQueue> {
        &self.cq
    }

    pub(crate) fn identity(&self) -> CompletionIdentity {
        self.identity
    }

    pub(crate) fn qp_num(&self) -> u32 {
        self.identity.qp_num()
    }

    pub(crate) fn gather_limit(&self) -> usize {
        self.gather_limit
    }

    /// Wait for synchronous posting already in progress, then reject every new
    /// SEND, READ and receive repost before the provider transitions to ERR.
    pub(crate) fn stop_posting(&self) {
        self.live.write().unwrap().posting_open = false;
    }

    pub(crate) fn is_destroyed(&self) -> bool {
        self.destroyed.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_destroyed(&self) {
        let notified = self.destruction.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_destroyed() {
            notified.await;
        }
    }

    /// Called by the poller after ERR and READ completion accounting settle.
    /// Cached socket handles retain only this closed shell, never the provider.
    pub(super) fn destroy(&self) {
        let qp = {
            let mut live = self.live.write().unwrap();
            live.posting_open = false;
            if let Some(qp) = &live.qp {
                assert!(
                    !qp.has_pending_reads(),
                    "destroy requires all READ completions"
                );
            }
            live.qp.take()
        };
        let Some(qp) = qp else { return };
        drop(qp);
        // Provider destruction precedes identity retirement in QueuePair::drop,
        // and CQ capacity retirement here. The poll-to-empty rule still applies.
        let reservation = self.reservation.lock().unwrap().take();
        drop(reservation);
        self.destroyed.store(true, Ordering::Release);
        self.destruction.notify_waiters();
    }

    pub(crate) fn connect(&self, config: &QpConnectionConfig) -> Result<()> {
        self.post(|qp| qp.connect(config))
    }

    pub(crate) fn modify(&self, attr: &mut ibv_qp_attr, mask: i32) -> Result<()> {
        self.with_live(|qp| qp.modify(attr, mask))
    }

    pub(crate) fn send(&self, buffer: Buffer, flags: ibv_send_flags) -> Result<u64> {
        self.post(|qp| qp.send(buffer, flags))
    }

    pub(crate) fn send_signaled(&self, buffer: Buffer, flags: ibv_send_flags) -> Result<u64> {
        self.post(|qp| qp.send_signaled(buffer, flags))
    }

    pub(crate) fn send_imm(&self, buffer: Buffer, imm: u32, flags: ibv_send_flags) -> Result<u64> {
        self.post(|qp| qp.send_imm(buffer, imm, flags))
    }

    pub(crate) fn send_imm_only(&self, imm: u32, flags: ibv_send_flags) -> Result<()> {
        self.post(|qp| qp.send_imm_only(imm, flags))
    }

    pub(crate) fn send_gather(&self, buffers: Box<[Buffer]>, imm: Option<u32>) -> Result<u64> {
        self.post(|qp| qp.send_gather(buffers, imm))
    }

    pub(crate) fn recv(&self, buffer: Buffer) -> Result<()> {
        self.post(|qp| qp.recv(buffer))
    }

    pub(crate) fn prepare_reads(
        &self,
        buffers: Vec<WriteBuffer>,
        requests: Vec<ReadRequest>,
    ) -> std::result::Result<(ReadPosting, ReadCompletion), (Error, Vec<WriteBuffer>)> {
        let live = self.live.read().unwrap();
        match &live.qp {
            Some(qp) if live.posting_open => qp.prepare_reads(buffers, requests),
            _ => Err((Self::closed(), buffers)),
        }
    }

    pub(crate) fn post_read(&self, posting: &mut ReadPosting) -> Result<()> {
        self.post(|qp| qp.post_read(posting))
    }

    pub(crate) fn has_pending_reads(&self) -> bool {
        self.live
            .read()
            .unwrap()
            .qp
            .as_ref()
            .is_some_and(QueuePair::has_pending_reads)
    }

    pub(crate) fn pending_read_count(&self) -> usize {
        self.live
            .read()
            .unwrap()
            .qp
            .as_ref()
            .map_or(0, QueuePair::pending_read_count)
    }

    pub(crate) fn complete<'a>(
        &self,
        completion: Completion<'a>,
        cursor: &mut CompletionCursor,
    ) -> Result<CompletedWork<'a>> {
        self.with_live(|qp| qp.complete(completion, cursor))
    }
}
