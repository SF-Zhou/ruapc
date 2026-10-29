//! Connection-scoped rendezvous transfers using ordinary framed RPCs.
//! Sources remain owned until a peer finish RPC or provider QP destruction.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use bytes::Bytes;
use ruapc_memory::SpaceLayout;
use ruapc_wire::frame::{HEADER_LEN, MAX_MSG_SIZE};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{RdmaSocket, encode::Encoded};
use crate::{
    Buffer, BufferPool, Error, ErrorKind, Message, MsgFlags, MsgMeta, Result, Socket, State,
    metrics::MethodCall,
    rdma::rdma_service::{FinishPayloadRequest, LargePayloadOffer},
    sockets::{SendRequest, reserve_send},
};

// Cleanup is independent of the business deadline. A peer that disappears
// forces connection retirement, never a TTL-based release of DMA sources.
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_TRANSFERS: usize = 128;
const FINISHED_CACHE: usize = 256;

#[derive(Debug)]
struct Source {
    _buffer: Buffer,
    _permit: OwnedSemaphorePermit,
    request_id: Option<u64>,
    deadline: Option<Instant>,
    cleanup_at: Instant,
}

#[derive(Debug, Default)]
struct Sources {
    next: u64,
    active: HashMap<u64, Source>,
    finished: VecDeque<(u64, bool)>,
}

/// Compressed, permanent replay rejection. Concurrent dispatch can observe
/// offers out of order, so a single high-water mark would reject valid offers.
/// Sequential traffic collapses into one interval; excessive disorder closes
/// the connection rather than growing memory without bound.
#[derive(Debug, Default)]
struct Seen {
    ranges: BTreeMap<u64, u64>,
}

impl Seen {
    fn insert(&mut self, id: u64) -> Result<()> {
        if id == 0 {
            return Err(protocol_error("zero transfer ID"));
        }
        let left = self.ranges.range(..=id).next_back().map(|(&a, &b)| (a, b));
        if left.is_some_and(|(_, end)| id <= end) {
            return Err(protocol_error("replayed large-payload offer"));
        }
        let right = self.ranges.range(id..).next().map(|(&a, &b)| (a, b));
        let mut start = id;
        let mut end = id;
        if let Some((a, b)) = left
            && b.checked_add(1) == Some(id)
        {
            start = a;
            self.ranges.remove(&a);
        }
        if let Some((a, b)) = right
            && id.checked_add(1) == Some(a)
        {
            end = b;
            self.ranges.remove(&a);
        }
        self.ranges.insert(start, end);
        if self.ranges.len() > MAX_TRANSFERS {
            return Err(protocol_error("too many out-of-order large-payload offers"));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Receivers {
    seen: Seen,
    active: HashSet<u64>,
}

#[derive(Debug)]
pub(super) struct LargeTransfers {
    pub(super) send_bytes: Arc<Semaphore>,
    recv_bytes: Arc<Semaphore>,
    sources: Mutex<Sources>,
    receivers: Mutex<Receivers>,
    // A finish can retire its source before the offer ACK arrives. Bound
    // ACK supervision separately so completed transfers cannot bypass it.
    offers: Arc<Semaphore>,
    // A failed READ may still own its destination in the QP. These byte
    // reservations are retired after provider destruction along with sources.
    failed_receives: Mutex<Vec<OwnedSemaphorePermit>>,
}

impl LargeTransfers {
    pub(super) fn new(byte_limit: usize) -> Self {
        Self {
            send_bytes: Arc::new(Semaphore::new(byte_limit)),
            recv_bytes: Arc::new(Semaphore::new(byte_limit)),
            sources: Mutex::default(),
            receivers: Mutex::default(),
            offers: Arc::new(Semaphore::new(MAX_TRANSFERS)),
            failed_receives: Mutex::default(),
        }
    }

    pub(super) fn close(&self) {
        self.send_bytes.close();
        self.recv_bytes.close();
        self.receivers.lock().unwrap().active.clear();
        self.offers.close();
        // Sources stay immutable and owned until the QP is destroyed.
    }

    fn finish(&self, id: u64, state: &State, conn_id: u64, healthy: bool) -> bool {
        let mut sources = self.sources.lock().unwrap();
        if let Some(source) = sources.active.remove(&id) {
            let valid = healthy
                && source
                    .deadline
                    .is_none_or(|deadline| Instant::now() < deadline)
                && source
                    .request_id
                    .is_none_or(|msgid| state.waiter.pending_on_connection(msgid, conn_id));
            // This path is called only by a finish RPC received on the
            // source's QP, after the peer has settled every READ. Even a
            // rejected/expired transfer can now release its source.
            drop(source);
            sources.finished.push_back((id, valid));
            if sources.finished.len() > FINISHED_CACHE {
                sources.finished.pop_front();
            }
            valid
        } else {
            sources
                .finished
                .iter()
                .find(|(old, _)| *old == id)
                .is_some_and(|(_, valid)| *valid)
        }
    }
}

fn protocol_error(message: &str) -> Error {
    Error::new(ErrorKind::RdmaRecvFailed, message.into())
}

impl RdmaSocket {
    pub(super) async fn send_large(
        self: &Arc<Self>,
        meta: &mut MsgMeta,
        encoded: Encoded,
        request: &SendRequest<'_>,
        state: &Arc<State>,
    ) -> Result<()> {
        let payload_len = encoded.buffer.len() - encoded.payload_offset;
        let business_deadline = request.deadline();
        self.reserve_send_bandwidth(payload_len as u64, business_deadline)
            .await?;
        let permit = self.large.offers.clone().try_acquire_owned().map_err(|_| {
            if self.state.is_ok() {
                Error::kind(ErrorKind::Overloaded)
            } else {
                Error::kind(ErrorKind::ConnectionClosed)
            }
        })?;
        // Reserve a queue slot before publishing the source. After this await,
        // ownership installation and transport acceptance are one synchronous
        // transaction; cancelling send cannot leave a half-published offer.
        let cleanup_deadline = Instant::now() + TRANSFER_TIMEOUT;
        let queue_deadline =
            business_deadline.map_or(cleanup_deadline, |deadline| deadline.min(cleanup_deadline));
        let slot = reserve_send(&self.pending_sender, Some(queue_deadline), || {
            self.read_credits.closed()
        })
        .await?;
        if !self.state.is_ok() {
            return Err(ErrorKind::ConnectionClosed.into());
        }
        request.refresh(meta)?;
        let mut region = encoded
            .buffer
            .remote_buffer_info(&self.queue_pair.device_index)?;
        region.addr = region
            .addr
            .checked_add(encoded.payload_offset as u64)
            .ok_or_else(|| protocol_error("payload address overflow"))?;
        region.len = payload_len as u64;
        let (msgid, response) = state.waiter.alloc_owned_until(cleanup_deadline);
        state.waiter.bind_connection(msgid, self.conn_id);
        {
            let mut sources = self.large.sources.lock().unwrap();
            if sources.active.len() >= MAX_TRANSFERS {
                return Err(ErrorKind::Overloaded.into());
            }
            let id = sources
                .next
                .checked_add(1)
                .ok_or_else(|| protocol_error("transfer IDs exhausted"))?;
            let offer = LargePayloadOffer {
                transfer_id: id,
                payload_len: payload_len as u64,
                regions: vec![region],
                meta: meta.clone(),
            };
            let offer_meta = MsgMeta {
                method: "RdmaService/offer".into(),
                flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
                msgid,
                timeout_ms: remaining_ms(cleanup_deadline),
                ..Default::default()
            };
            // Offers must fit inline. Never recursively rendezvous a descriptor.
            let descriptor = self.serialize_msg(&offer_meta, &offer)?;
            if descriptor.buffer.len() > self.max_msg_size {
                return Err(Error::new(
                    ErrorKind::SerializeFailed,
                    "RDMA large-payload descriptor exceeds receive buffer".into(),
                ));
            }
            request.check()?;
            if !self.state.is_ok() {
                return Err(ErrorKind::ConnectionClosed.into());
            }
            sources.next = id;
            sources.active.insert(
                id,
                Source {
                    _buffer: encoded.buffer,
                    _permit: encoded
                        .permit
                        .expect("large serialization reserves its source bytes"),
                    request_id: meta.is_req().then_some(meta.msgid),
                    deadline: business_deadline,
                    cleanup_at: Instant::now() + TRANSFER_TIMEOUT,
                },
            );
            slot.send(descriptor.buffer);
        }
        self.poller_waker.wake();
        // Acceptance is irrevocable. Supervise the ACK independently so an
        // already delivered response cannot keep its business handler alive.
        // Failure (including shutdown before the task starts) closes the
        // connection, while its source table keeps the advertised bytes.
        let guard = OfferAck {
            socket: Arc::downgrade(self),
            _permit: permit,
            metrics: state.metrics.client_method("RdmaService/offer").start(),
            acknowledged: false,
        };
        state
            .socket_pool
            .task_supervisor_handle()
            .try_spawn(async move {
                let mut guard = guard;
                let acknowledged = async {
                    let (message, _) = response.recv().await?;
                    message.payload.deserialize::<Result<()>>(&message.meta)?
                }
                .await;
                match acknowledged {
                    Ok(()) => guard.acknowledged = true,
                    Err(error) => {
                        tracing::warn!(%error, "RDMA payload offer RPC failed after acceptance")
                    }
                }
            });
        Ok(())
    }

    /// The RPC handler selects the receiving RDMA connection.
    pub(crate) fn start_large_receive(
        self: &Arc<Self>,
        offer: LargePayloadOffer,
        state: &Arc<State>,
        began: Instant,
    ) -> Result<()> {
        let result = self.start_large_receive_inner(offer, state, began);
        if result.is_err() {
            // A rejected offer must not leave replay tracking or remote READ
            // sources indefinitely live, even if its peer ignores RPC errors.
            self.set_error();
        }
        result
    }

    fn start_large_receive_inner(
        self: &Arc<Self>,
        offer: LargePayloadOffer,
        state: &Arc<State>,
        began: Instant,
    ) -> Result<()> {
        self.check_large_connection()?;
        self.validate_offer(&offer)?;
        let mut receivers = self.large.receivers.lock().unwrap();
        if receivers.active.len() >= MAX_TRANSFERS {
            return Err(ErrorKind::Overloaded.into());
        }
        receivers.seen.insert(offer.transfer_id)?;
        receivers.active.insert(offer.transfer_id);
        drop(receivers);
        let guard = ReceiveGuard {
            socket: self.clone(),
            id: offer.transfer_id,
            settled: false,
        };
        let state = state.clone();
        let supervisor = state.socket_pool.task_supervisor_handle();
        // Release RPC handler admission promptly. READ/confirmation work has
        // its own transfer-count, byte and cleanup budgets.
        supervisor
            .try_spawn(async move {
                let mut guard = guard;
                let result = tokio::time::timeout(
                    TRANSFER_TIMEOUT,
                    guard.socket.receive_large(offer, &state, began),
                )
                .await;
                match result {
                    Ok(Ok(())) => guard.settled = true,
                    Ok(Err(error)) => tracing::warn!(%error, "RDMA large-payload receive failed"),
                    Err(_) => tracing::warn!("RDMA large-payload confirmation timed out"),
                }
            })
            .ok_or_else(|| Error::kind(ErrorKind::ConnectionClosed))?;
        Ok(())
    }

    pub(crate) fn finish_large(&self, id: u64, state: &State) -> Result<bool> {
        self.check_large_connection()?;
        Ok(self
            .large
            .finish(id, state, self.conn_id, self.state.is_ok()))
    }

    fn check_large_connection(&self) -> Result<()> {
        if self.max_large_msg_size == 0 {
            return Err(protocol_error("large payloads were not negotiated"));
        }
        if !self.state.is_ok() {
            return Err(ErrorKind::ConnectionClosed.into());
        }
        Ok(())
    }

    fn validate_offer(&self, offer: &LargePayloadOffer) -> Result<()> {
        if offer.meta.is_req() == offer.meta.is_rsp() {
            return Err(protocol_error("invalid large-payload message flags"));
        }
        let meta_len = rmp_serde::to_vec_named(&offer.meta)?.len() as u64;
        if offer.payload_len == 0
            || offer
                .payload_len
                .checked_add(meta_len)
                .and_then(|len| len.checked_add((HEADER_LEN + 4) as u64))
                .is_none_or(|len| {
                    len > self.max_large_msg_size as u64 || len >= MAX_MSG_SIZE as u64
                })
        {
            return Err(protocol_error(
                "large payload exceeds negotiated message limit",
            ));
        }
        let layout = SpaceLayout::from_lens(offer.regions.iter().map(|region| region.len))?;
        if layout.total() != offer.payload_len
            || offer
                .regions
                .iter()
                .any(|region| region.len == 0 || region.addr.checked_add(region.len).is_none())
        {
            return Err(protocol_error("invalid large-payload region layout"));
        }
        Ok(())
    }

    async fn receive_large(
        self: &Arc<Self>,
        mut offer: LargePayloadOffer,
        state: &Arc<State>,
        began: Instant,
    ) -> Result<()> {
        let deadline = offer
            .meta
            .is_req()
            .then(|| began + Duration::from_millis(u64::from(offer.meta.timeout_ms)));
        let unwanted = deadline.is_some_and(|deadline| deadline <= Instant::now())
            || (offer.meta.is_rsp()
                && !state
                    .waiter
                    .pending_on_connection(offer.meta.msgid, self.conn_id));
        if unwanted {
            self.confirm_payload(offer.transfer_id, state).await?;
            return Ok(());
        }
        let len = usize::try_from(offer.payload_len)?;
        let capacity = BufferPool::allocation_capacity(len)?;
        let permit = self
            .large
            .recv_bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(capacity)?)
            .map_err(|_| Error::kind(ErrorKind::Overloaded))?;
        let mut buffer = self.rdmabuf_pool.allocate(len)?;
        buffer.set_len(len);
        let mut destination_budget = DestinationBudget {
            socket: self.clone(),
            permit: Some(permit),
        };
        let buffer = self.read_payload(&offer.regions, buffer, deadline).await?;
        // Successful owned READ completion has returned the buffer. Its budget
        // can now follow the received payload instead of the QP's failure holds.
        let received = ReceivedPayload {
            buffer,
            _permit: destination_budget
                .permit
                .take()
                .expect("destination byte reservation"),
        };
        let confirmed = self.confirm_payload(offer.transfer_id, state).await?;
        if !confirmed {
            return Ok(());
        }
        if let Some(deadline) = deadline {
            offer.meta.timeout_ms = remaining_ms(deadline);
            if offer.meta.timeout_ms == 0 {
                return Ok(());
            }
        }
        let payload = Bytes::from_owner(received);
        state.handle_recv(
            &Socket::RDMA(self.clone()),
            Message::new(offer.meta, payload.into()),
        )
    }

    async fn confirm_payload(self: &Arc<Self>, id: u64, state: &Arc<State>) -> Result<bool> {
        // Every submitted READ has completed (or none was posted). Cleanup has
        // its own budget even if the business request expired while reading.
        let metrics = state.metrics.client_method("RdmaService/finish").start();
        let result = self.finish_rpc(id, state).await;
        if result.is_err() {
            metrics.failed();
        }
        result
    }

    // Bypass only the public send authorization check, using the same framed
    // serializer, queue admission, response waiter and result codec as RPCs.
    async fn finish_rpc(&self, id: u64, state: &Arc<State>) -> Result<bool> {
        self.check_large_connection()?;
        let (msgid, receiver) = state.waiter.alloc(TRANSFER_TIMEOUT);
        let mut meta = MsgMeta {
            method: "RdmaService/finish".into(),
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            msgid,
            timeout_ms: TRANSFER_TIMEOUT.as_millis() as u32,
            ..Default::default()
        };
        let request = SendRequest::bind(&mut meta, &state.waiter, self.conn_id)?;
        let encoded = self.serialize_msg(&meta, &FinishPayloadRequest { transfer_id: id })?;
        if encoded.buffer.len() > self.max_msg_size {
            return Err(protocol_error("payload finish RPC exceeds receive buffer"));
        }
        self.send_buffer(&mut meta, encoded, &request).await?;
        let (message, _) = receiver.recv().await?;
        message.payload.deserialize(&message.meta)?
    }

    pub(crate) fn sweep_large_transfers(&self, now: Instant) {
        if self
            .large
            .sources
            .lock()
            .unwrap()
            .active
            .values()
            .any(|source| now >= source.cleanup_at)
        {
            self.set_error();
        }
    }
}

fn remaining_ms(deadline: Instant) -> u32 {
    u32::try_from(
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis(),
    )
    .unwrap_or(u32::MAX)
}

struct OfferAck {
    // ACK supervision must not delay provider QP destruction.
    socket: Weak<RdmaSocket>,
    _permit: OwnedSemaphorePermit,
    metrics: MethodCall,
    acknowledged: bool,
}

impl Drop for OfferAck {
    fn drop(&mut self) {
        if !self.acknowledged {
            self.metrics.failed();
            if let Some(socket) = self.socket.upgrade() {
                socket.set_error();
            }
        }
    }
}

struct ReceivedPayload {
    buffer: Buffer,
    _permit: OwnedSemaphorePermit,
}

struct DestinationBudget {
    socket: Arc<RdmaSocket>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for DestinationBudget {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            self.socket.set_error();
            self.socket
                .large
                .failed_receives
                .lock()
                .unwrap()
                .push(permit);
        }
    }
}
impl AsRef<[u8]> for ReceivedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

struct ReceiveGuard {
    socket: Arc<RdmaSocket>,
    id: u64,
    settled: bool,
}
impl Drop for ReceiveGuard {
    fn drop(&mut self) {
        self.socket
            .large
            .receivers
            .lock()
            .unwrap()
            .active
            .remove(&self.id);
        if !self.settled {
            self.socket.set_error();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{Context, SocketPoolConfig};

    const SOURCE_ID: u64 = 7;
    const CONNECTION_ID: u64 = 41;
    const BYTE_LIMIT: usize = 16 * 1024;

    fn context() -> Context {
        Context::create(&SocketPoolConfig {
            rdma: None,
            ..Default::default()
        })
        .unwrap()
    }

    fn install_source(
        large: &LargeTransfers,
        state: &State,
        id: u64,
        request_id: Option<u64>,
        deadline: Option<Instant>,
    ) {
        let mut buffer = state.buffer_pool.allocate(BYTE_LIMIT).unwrap();
        assert_eq!(buffer.capacity(), BYTE_LIMIT);
        buffer.set_len(6);
        buffer.copy_from_slice(b"source");
        let permit = large
            .send_bytes
            .clone()
            .try_acquire_many_owned(BYTE_LIMIT as u32)
            .unwrap();
        assert!(
            large
                .sources
                .lock()
                .unwrap()
                .active
                .insert(
                    id,
                    Source {
                        _buffer: buffer,
                        _permit: permit,
                        request_id,
                        deadline,
                        cleanup_at: Instant::now() + TRANSFER_TIMEOUT,
                    },
                )
                .is_none()
        );
    }

    #[tokio::test]
    async fn valid_source_finishes_once_and_repeated_confirmation_is_idempotent() {
        let ctx = context();
        let large = LargeTransfers::new(BYTE_LIMIT);
        let (msgid, receiver) = ctx.state.waiter.alloc(Duration::from_secs(30));
        ctx.state.waiter.bind_connection(msgid, CONNECTION_ID);
        install_source(
            &large,
            &ctx.state,
            SOURCE_ID,
            Some(msgid),
            Some(Instant::now() + Duration::from_secs(30)),
        );
        assert_eq!(large.send_bytes.available_permits(), 0);
        assert!(large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
        assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
        assert!(large.sources.lock().unwrap().active.is_empty());

        // Once the original READ was confirmed, the result is independent of the
        // later business waiter lifetime. A retransmitted finish cannot release
        // memory or return its budget twice.
        drop(receiver);
        assert!(large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
        assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
        assert_eq!(large.sources.lock().unwrap().finished.len(), 1);
    }

    #[tokio::test]
    async fn cancelled_request_keeps_source_until_peer_finishes_reading() {
        let ctx = context();
        let large = LargeTransfers::new(BYTE_LIMIT);
        let (msgid, receiver) = ctx.state.waiter.alloc(Duration::from_secs(30));
        ctx.state.waiter.bind_connection(msgid, CONNECTION_ID);
        install_source(&large, &ctx.state, SOURCE_ID, Some(msgid), None);
        drop(receiver);

        assert!(!ctx.state.waiter.pending_on_connection(msgid, CONNECTION_ID));
        assert_eq!(large.send_bytes.available_permits(), 0);
        assert_eq!(
            &large.sources.lock().unwrap().active[&SOURCE_ID]._buffer[..],
            b"source"
        );
        assert!(!large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
        assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
        assert!(!large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
        assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
    }

    #[tokio::test]
    async fn source_deadline_and_unswept_waiter_deadline_both_reject_expired_payloads() {
        let ctx = context();
        for source_expired in [true, false] {
            let large = LargeTransfers::new(BYTE_LIMIT);
            let waiter_timeout = if source_expired {
                Duration::from_secs(30)
            } else {
                Duration::ZERO
            };
            let (msgid, receiver) = ctx.state.waiter.alloc(waiter_timeout);
            ctx.state.waiter.bind_connection(msgid, CONNECTION_ID);
            let deadline = if source_expired {
                Instant::now() - Duration::from_secs(1)
            } else {
                Instant::now() + Duration::from_secs(30)
            };
            install_source(&large, &ctx.state, SOURCE_ID, Some(msgid), Some(deadline));

            // Expiry only invalidates acceptance. It is never completion evidence
            // for the source buffer, even before the periodic waiter sweep runs.
            assert_eq!(large.send_bytes.available_permits(), 0);
            assert_eq!(large.sources.lock().unwrap().active.len(), 1);
            assert!(!large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
            assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
            drop(receiver);
        }
    }

    #[tokio::test]
    async fn source_confirmation_requires_matching_waiter_connection_and_health() {
        let ctx = context();
        for (connection_id, healthy) in [(CONNECTION_ID + 1, true), (CONNECTION_ID, false)] {
            let large = LargeTransfers::new(BYTE_LIMIT);
            let (msgid, receiver) = ctx.state.waiter.alloc(Duration::from_secs(30));
            ctx.state.waiter.bind_connection(msgid, CONNECTION_ID);
            install_source(&large, &ctx.state, SOURCE_ID, Some(msgid), None);
            assert!(!large.finish(SOURCE_ID, &ctx.state, connection_id, healthy));
            assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
            assert!(ctx.state.waiter.pending_on_connection(msgid, CONNECTION_ID));
            drop(receiver);
        }
    }

    #[tokio::test]
    async fn response_sources_do_not_require_a_request_waiter_and_unknown_ids_do_not_release_them()
    {
        let ctx = context();
        let large = LargeTransfers::new(BYTE_LIMIT);
        install_source(&large, &ctx.state, SOURCE_ID, None, None);
        assert!(!large.finish(SOURCE_ID + 1, &ctx.state, CONNECTION_ID, true));
        assert_eq!(large.send_bytes.available_permits(), 0);
        assert!(large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
        assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
    }

    #[tokio::test]
    async fn connection_close_clears_receivers_but_preserves_unconfirmed_sources() {
        let ctx = context();
        let large = LargeTransfers::new(BYTE_LIMIT);
        let send_budget = large.send_bytes.clone();
        install_source(&large, &ctx.state, SOURCE_ID, None, None);
        large.receivers.lock().unwrap().active.insert(11);
        large.receivers.lock().unwrap().active.insert(12);

        large.close();
        large.close();
        assert!(large.send_bytes.is_closed());
        assert!(large.recv_bytes.is_closed());
        assert!(large.receivers.lock().unwrap().active.is_empty());
        assert_eq!(send_budget.available_permits(), 0);
        assert_eq!(
            &large.sources.lock().unwrap().active[&SOURCE_ID]._buffer[..],
            b"source"
        );

        // The socket drops this owner only after destroying its provider QP.
        drop(large);
        assert_eq!(send_budget.available_permits(), BYTE_LIMIT);
    }

    #[tokio::test]
    async fn finished_confirmation_cache_is_bounded_without_releasing_unrelated_sources() {
        let ctx = context();
        let large = LargeTransfers::new(BYTE_LIMIT);
        for id in 1..=FINISHED_CACHE as u64 + 1 {
            install_source(&large, &ctx.state, id, None, None);
            assert!(large.finish(id, &ctx.state, CONNECTION_ID, true));
        }
        assert_eq!(large.sources.lock().unwrap().finished.len(), FINISHED_CACHE);
        install_source(&large, &ctx.state, 999, None, None);
        assert!(!large.finish(1, &ctx.state, CONNECTION_ID, true));
        assert!(large.finish(2, &ctx.state, CONNECTION_ID, true));
        assert_eq!(large.send_bytes.available_permits(), 0);
        assert_eq!(large.sources.lock().unwrap().active.len(), 1);
    }

    #[test]
    fn seen_merges_disordered_ids_and_permanently_rejects_replays() {
        let mut seen = Seen::default();
        for id in [4, 2, 7, 3, 1, 6, 5] {
            seen.insert(id).unwrap();
        }
        assert_eq!(seen.ranges, BTreeMap::from([(1, 7)]));
        for id in 1..=7 {
            let error = seen.insert(id).unwrap_err();
            assert_eq!(error.kind, ErrorKind::RdmaRecvFailed);
        }
        assert_eq!(seen.ranges, BTreeMap::from([(1, 7)]));
        assert!(seen.insert(0).is_err());
    }

    #[test]
    fn seen_handles_maximum_id_without_wraparound() {
        let mut seen = Seen::default();
        seen.insert(u64::MAX).unwrap();
        seen.insert(1).unwrap();
        seen.insert(u64::MAX - 2).unwrap();
        seen.insert(u64::MAX - 1).unwrap();
        assert_eq!(
            seen.ranges,
            BTreeMap::from([(1, 1), (u64::MAX - 2, u64::MAX)])
        );
        assert!(seen.insert(u64::MAX).is_err());
        assert!(seen.insert(0).is_err());
    }

    #[test]
    fn seen_bounds_disorder_without_limiting_sequential_transfers() {
        let mut disordered = Seen::default();
        for id in (1..).step_by(2).take(MAX_TRANSFERS) {
            disordered.insert(id).unwrap();
        }
        assert!(disordered.insert((MAX_TRANSFERS * 2 + 1) as u64).is_err());

        let mut ordered = Seen::default();
        for id in 1..=(MAX_TRANSFERS * 10) as u64 {
            ordered.insert(id).unwrap();
        }
        assert_eq!(ordered.ranges.len(), 1);
        assert!(ordered.insert(1).is_err());
    }

    #[tokio::test]
    async fn received_payload_keeps_its_budget_until_all_payload_views_are_dropped() {
        let ctx = context();
        let large = LargeTransfers::new(BYTE_LIMIT);
        let mut buffer = ctx.state.buffer_pool.allocate(BYTE_LIMIT).unwrap();
        buffer.set_len(7);
        buffer.copy_from_slice(b"payload");
        let permit = large
            .recv_bytes
            .clone()
            .try_acquire_many_owned(BYTE_LIMIT as u32)
            .unwrap();
        let bytes = Bytes::from_owner(ReceivedPayload {
            buffer,
            _permit: permit,
        });
        let cloned = bytes.clone();
        let slice = bytes.slice(1..4);
        assert_eq!(slice.as_ref(), b"ayl");
        assert_eq!(large.recv_bytes.available_permits(), 0);
        drop(bytes);
        drop(cloned);
        large.close();
        assert_eq!(large.recv_bytes.available_permits(), 0);
        assert_eq!(slice.as_ref(), b"ayl");
        drop(slice);
        assert_eq!(large.recv_bytes.available_permits(), BYTE_LIMIT);
    }
}

#[cfg(test)]
mod lifecycle_tests {
    //! Provider-QP ownership tests and large-response admission regression coverage.
    //!
    //! The fixture reserves a live CQ and creates a registered-memory pool, but
    //! never registers its socket with the poller. Large sends still cross the
    //! normal transport-acceptance boundary; their descriptors remain in the
    //! owned pending queue instead of being posted on the unconnected QP.

    use ruapc_wire::frame::decode_frames;

    use super::*;
    use crate::{
        Client, Context, Endpoint, MsgFlags, Payload, RawCall, Receiver, Router, Server,
        SocketPoolConfig, SocketTrait, Transport,
        core::ContextEndpoint,
        rdma::{
            DevicePollers, PollerConfig, RdmaBandwidthLimiter, RdmaConnectionConfig, RdmaNicInfo,
            RdmaPathInfo, RdmaQueuePairConfig, RdmaSocketConfig, SendPermit,
            test_utils::{make_rdma_devices, small_rdma_config},
        },
        spawn_handler,
    };
    use ruapc_bufpool::Device as _;
    use ruapc_rdma::{CompletionQueue, QueuePair};
    use std::sync::atomic::Ordering;
    use tokio::sync::Notify;

    #[crate::service]
    trait AckOrderingService {
        async fn large(&self, ctx: &Context, request: &()) -> Result<String>;
        async fn small(&self, ctx: &Context, request: &()) -> Result<()>;
    }

    impl AckOrderingService for () {
        async fn large(&self, _ctx: &Context, _: &()) -> Result<String> {
            Ok("x".repeat(64 * 1024))
        }

        async fn small(&self, _ctx: &Context, _: &()) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_large_reply_releases_handler_before_offer_ack() {
        let mut config = small_rdma_config();
        config.max_inflight_requests = 2;
        config
            .rdma
            .as_mut()
            .unwrap()
            .peers
            .min_connections_per_remote_nic = 0;
        let mut router = Router::default();
        AckOrderingService::ruapc_export(Arc::new(()), &mut router);
        let server = Server::create(router, &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let allow_ack = Arc::new(Notify::new());
        let ack_gate = allow_ack.clone();
        let mut router = Router::default();
        router.wrap_method("RdmaService/offer", move |_original| {
            Box::new(move |mut ctx, payload| {
                let offer = payload.deserialize(&ctx.msg_meta)?;
                let ContextEndpoint::Connected(Socket::RDMA(socket)) = &ctx.endpoint else {
                    unreachable!();
                };
                // Preserve the real handler's ordering, but hold its unit ACK
                // until the test has completed READ/finish and the next call.
                socket.start_large_receive(offer, &ctx.state, Instant::now())?;
                let gate = ack_gate.clone();
                tokio::spawn(async move {
                    gate.notified().await;
                    ctx.send_rsp::<(), Error>(Ok(())).await;
                });
                Ok(())
            })
        });
        let context = Context::create_with_router(router, &config)
            .unwrap()
            .with_endpoint(Endpoint::rdma(Transport::TCP, [address]).unwrap());
        let client = Client {
            max_retries: 0,
            ..Default::default()
        };
        let reply = tokio::time::timeout(Duration::from_secs(5), client.large(&context, &()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply, "x".repeat(64 * 1024));
        // The finish response can arrive before its handler drops its admission
        // guard. All server handlers must settle while the offer ACK is held.
        tokio::time::timeout(Duration::from_secs(2), async {
            while server
                .state()
                .metrics
                .server_inflight
                .load(Ordering::Relaxed)
                != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed replies must release handler capacity before offer ACK");
        client.small(&context, &()).await.unwrap();
        allow_ack.notify_one();
        context.state.socket_pool.stop();
        context.state.socket_pool.join().await;
        server.stop();
        server.join().await;
    }

    const INLINE_BYTES: usize = 16 * 1024;
    const LARGE_BYTES: usize = 1024 * 1024;
    const BYTE_BUDGET: usize = 4 * 1024 * 1024;

    fn decode_offer(descriptor: Buffer) -> (MsgMeta, LargePayloadOffer) {
        let mut decoded = None;
        decode_frames(&Bytes::from_owner(descriptor), |body| -> Result<()> {
            let message = Message::parse(body)?;
            assert_eq!(message.meta.method, "RdmaService/offer");
            assert!(message.meta.is_req());
            let offer = message.payload.deserialize(&message.meta)?;
            assert!(decoded.replace((message.meta, offer)).is_none());
            Ok(())
        })
        .unwrap();
        decoded.unwrap()
    }

    struct Fixture {
        socket: Arc<RdmaSocket>,
        pending: tokio::sync::mpsc::Receiver<Buffer>,
        cq: Arc<CompletionQueue>,
        _pollers: DevicePollers,
        state: Arc<State>,
        _stop: tokio_util::sync::DropGuard,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_handler_limit(SocketPoolConfig::default().max_inflight_requests)
        }

        fn with_handler_limit(max_inflight_requests: usize) -> Self {
            let devices = make_rdma_devices();
            let device = &devices.devices()[0];
            let pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
            let connection = RdmaConnectionConfig {
                qp: RdmaQueuePairConfig {
                    max_send_wr: 4,
                    max_recv_wr: 4,
                    max_send_sge: 1,
                    max_recv_sge: 1,
                },
                recv_queue_len: 2,
                max_msg_size: INLINE_BYTES as u32,
                max_large_msg_size: LARGE_BYTES as u32,
                traffic_class: 0,
            };
            let pollers = DevicePollers::default();
            let (poller, reservation) = pollers
                .reserve(
                    device,
                    PollerConfig {
                        cq_len: 64,
                        read_limit: 2,
                        spin_us: 0,
                        dispatch_workers: 1,
                    },
                    1,
                    &connection,
                )
                .unwrap();
            let mut attrs = ruapc_rdma::ibv_qp_init_attr {
                qp_type: ruapc_rdma::ibv_qp_type::IBV_QPT_RC,
                cap: ruapc_rdma::ibv_qp_cap {
                    max_send_wr: 4,
                    max_recv_wr: 4,
                    max_send_sge: 1,
                    max_recv_sge: 1,
                    ..Default::default()
                },
                ..Default::default()
            };
            let qp =
                QueuePair::create(device.pd(), poller.cq(), &mut attrs, device.index()).unwrap();
            let nic = RdmaNicInfo {
                device: device.info().name.clone(),
                port_num: device.info().ports[0].port_num,
                gid_index: 0,
                ip: None,
            };
            let (pending_sender, pending) = tokio::sync::mpsc::channel(4);
            let socket = Arc::new(RdmaSocket::new(
                reservation.bind(qp),
                pool,
                pending_sender,
                poller.waker(),
                RdmaSocketConfig {
                    max_msg_size: INLINE_BYTES,
                    max_large_msg_size: LARGE_BYTES,
                    max_inflight_large_bytes: BYTE_BUDGET,
                    send_window: 1,
                    path: RdmaPathInfo {
                        local: nic.clone(),
                        remote: nic.clone(),
                        same_connectivity_domain: true,
                    },
                    read_timeout: None,
                    read_permits: Arc::new(Semaphore::new(2)),
                    bandwidth_limiter: Arc::new(RdmaBandwidthLimiter::new(
                        nic.device,
                        nic.port_num,
                        0,
                        Duration::ZERO,
                        Duration::ZERO,
                    )),
                    sq_read_cap: 2,
                },
            ));
            let (state, stop) = State::create(
                Router::default(),
                &SocketPoolConfig {
                    rdma: None,
                    max_inflight_requests,
                    ..Default::default()
                },
            )
            .unwrap();
            Self {
                socket,
                pending,
                cq: poller.cq().clone(),
                _pollers: pollers,
                state,
                _stop: stop,
            }
        }

        async fn publish_request(&mut self, msgid: u64) -> (LargePayloadOffer, usize) {
            let mut meta = MsgMeta {
                msgid,
                method: "Lifecycle/large".into(),
                flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
                timeout_ms: 30_000,
                ..Default::default()
            };
            let payload = "x".repeat(64 * 1024);
            self.socket
                .send(&mut meta, &payload, &self.state)
                .await
                .unwrap();
            let descriptor = self.pending.try_recv().expect("accepted offer is queued");
            let (offer_meta, offer) = decode_offer(descriptor);
            assert_ne!(offer_meta.msgid, msgid, "offer ACK has its own RPC waiter");
            let reply = Message::new(
                MsgMeta {
                    msgid: offer_meta.msgid,
                    flags: MsgFlags::IsRsp | MsgFlags::UseMessagePack,
                    ..Default::default()
                },
                Bytes::from(rmp_serde::to_vec_named(&Result::<()>::Ok(())).unwrap()).into(),
            );
            self.state.waiter.post(offer_meta.msgid, reply);
            self.wait_for_offer_acks().await;
            assert!(
                self.state
                    .waiter
                    .pending_on_connection(msgid, self.socket.conn_id)
            );
            let capacity = self.socket.large.sources.lock().unwrap().active[&offer.transfer_id]
                ._buffer
                .capacity();
            (offer, capacity)
        }

        async fn wait_for_offer_acks(&self) {
            tokio::time::timeout(Duration::from_secs(2), async {
                while self.socket.large.offers.available_permits() != MAX_TRANSFERS {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("offer ACK supervision must settle");
        }

        fn block_inline_queue(&self) {
            // Exhaust the software window without posting on the fixture's
            // unconnected QP, then fill every pending queue slot.
            assert!(matches!(
                self.socket.state.try_acquire(),
                SendPermit::Granted { .. }
            ));
            for _ in 0..4 {
                let mut queued = self.socket.rdmabuf_pool.allocate(1).unwrap();
                queued.set_len(0);
                self.socket.pending_sender.try_send(queued).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn payload_handlers_share_the_application_capacity() {
        let mut fixture = Fixture::with_handler_limit(1);
        let state = fixture.state.clone();
        // Keep rejection replies queued instead of posting on the unconnected QP.
        assert!(matches!(
            fixture.socket.state.try_acquire(),
            SendPermit::Granted { .. }
        ));
        let release = Arc::new(Semaphore::new(0));
        for (method, rejected) in [
            ("Application/call", "RdmaService/offer"),
            ("Application/call", "RdmaService/finish"),
            ("RdmaService/offer", "Application/call"),
            ("RdmaService/finish", "Application/call"),
        ] {
            let mut context = Context::server_ctx(
                &state,
                Socket::RDMA(fixture.socket.clone()),
                MsgMeta {
                    method: method.into(),
                    flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
                    timeout_ms: 30_000,
                    ..Default::default()
                },
            );
            let held = release.clone();
            spawn_handler(context.clone(), method, Payload::Empty, |_, _| async move {
                let permit = held.acquire().await.unwrap();
                permit.forget();
            });
            assert_eq!(state.metrics.server_inflight.load(Ordering::Relaxed), 1);
            context.msg_meta.method = rejected.into();
            spawn_handler(context, rejected, Payload::Empty, |_, _| async {
                panic!("all handlers must share the same capacity");
            });
            let response = tokio::time::timeout(Duration::from_secs(2), fixture.pending.recv())
                .await
                .unwrap()
                .unwrap();
            decode_frames(&Bytes::from_owner(response), |body| -> Result<()> {
                let message = Message::parse(body)?;
                let result: Result<()> = message.payload.deserialize(&message.meta)?;
                assert_eq!(result.unwrap_err().kind, ErrorKind::Overloaded);
                Ok(())
            })
            .unwrap();
            assert_eq!(state.metrics.server_inflight.load(Ordering::Relaxed), 1);
            release.add_permits(1);
            tokio::time::timeout(Duration::from_secs(2), async {
                while state.metrics.server_inflight.load(Ordering::Relaxed) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        drop(fixture);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn public_socket_send_and_raw_calls_cannot_forge_payload_rpcs() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let (offer, capacity) = fixture.publish_request(msgid).await;
        let budget = fixture.socket.large.send_bytes.clone();
        let socket = Socket::RDMA(fixture.socket.clone());
        let context = Context::server_ctx(
            &state,
            socket.clone(),
            MsgMeta {
                timeout_ms: 30_000,
                ..Default::default()
            },
        );
        let client = Client {
            max_retries: 0,
            ..Default::default()
        };
        for (method, request) in [
            ("RdmaService/offer", serde_json::to_value(&offer).unwrap()),
            (
                "RdmaService/finish",
                serde_json::to_value(FinishPayloadRequest {
                    transfer_id: offer.transfer_id,
                })
                .unwrap(),
            ),
        ] {
            let mut meta = MsgMeta {
                method: method.into(),
                msgid: u64::MAX,
                flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
                timeout_ms: 30_000,
                ..Default::default()
            };
            let error = socket.send(&mut meta, &request, &state).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidArgument);
            let error = client
                .ruapc_raw_call::<_, serde_json::Value, Error>(&context, &request, None, method)
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidArgument);
            assert!(fixture.pending.is_empty());
            assert_eq!(state.waiter.pending_count(), 1);
            assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 1);
            assert_eq!(budget.available_permits(), BYTE_BUDGET - capacity);
            assert!(
                fixture
                    .socket
                    .large
                    .receivers
                    .lock()
                    .unwrap()
                    .active
                    .is_empty()
            );
            assert!(fixture.socket.state.is_ok());
        }
        assert_eq!(
            fixture.socket.state.try_acquire(),
            SendPermit::Granted { window_tail: true },
            "rejected payload RPCs must not consume a SEND credit"
        );

        drop(response);
        drop(context);
        drop(socket);
        drop(fixture);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn another_connection_cannot_finish_a_published_source() {
        let mut fixture = Fixture::new();
        let other = Fixture::new();
        let state = fixture.state.clone();
        let other_state = other.state.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let (offer, capacity) = fixture.publish_request(msgid).await;
        let budget = fixture.socket.large.send_bytes.clone();

        assert!(
            !other
                .socket
                .finish_large(offer.transfer_id, &state)
                .unwrap()
        );
        assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 1);
        assert_eq!(budget.available_permits(), BYTE_BUDGET - capacity);
        assert!(
            fixture
                .socket
                .finish_large(offer.transfer_id, &state)
                .unwrap()
        );
        assert_eq!(budget.available_permits(), BYTE_BUDGET);

        drop(response);
        drop(fixture);
        drop(other);
        state.socket_pool.join().await;
        other_state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn rejected_offer_ack_is_not_a_retryable_send_failure() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let budget = fixture.socket.large.send_bytes.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let mut meta = MsgMeta {
            msgid,
            method: "Lifecycle/large".into(),
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 30_000,
            ..Default::default()
        };
        let payload = "x".repeat(64 * 1024);
        fixture
            .socket
            .send(&mut meta, &payload, &state)
            .await
            .expect("send completes at acceptance without waiting for the ACK");
        let (offer_meta, offer) = decode_offer(fixture.pending.try_recv().unwrap());
        let retained = budget.available_permits();
        assert!(retained < BYTE_BUDGET);
        state.waiter.post(
            offer_meta.msgid,
            Message::new(
                MsgMeta {
                    msgid: offer_meta.msgid,
                    flags: MsgFlags::IsRsp | MsgFlags::UseMessagePack,
                    ..Default::default()
                },
                Bytes::from(
                    rmp_serde::to_vec_named(&Result::<()>::Err(ErrorKind::Overloaded.into()))
                        .unwrap(),
                )
                .into(),
            ),
        );

        fixture.wait_for_offer_acks().await;
        assert!(!fixture.socket.state.is_ok());
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .contains_key(&offer.transfer_id)
        );
        assert_eq!(budget.available_permits(), retained);
        assert!(fixture.pending.is_empty());

        drop(response);
        drop(fixture);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn cancelling_ack_supervision_retains_source_until_qp_destruction() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let budget = fixture.socket.large.send_bytes.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let mut meta = MsgMeta {
            msgid,
            method: "Lifecycle/large".into(),
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 30_000,
            ..Default::default()
        };
        let payload = "x".repeat(64 * 1024);
        fixture
            .socket
            .send(&mut meta, &payload, &state)
            .await
            .unwrap();
        let (offer_meta, offer) = decode_offer(fixture.pending.try_recv().unwrap());
        let retained = budget.available_permits();
        assert!(retained < BYTE_BUDGET);
        drop(response);
        assert!(state.waiter.contains_message_id(offer_meta.msgid));
        state.socket_pool.stop();
        state.socket_pool.join().await;
        assert!(!state.waiter.contains_message_id(offer_meta.msgid));
        assert!(!fixture.socket.state.is_ok());
        assert_eq!(
            fixture.socket.large.offers.available_permits(),
            MAX_TRANSFERS
        );
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .contains_key(&offer.transfer_id)
        );
        assert_eq!(budget.available_permits(), retained);
        drop(fixture);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn missing_offer_ack_does_not_extend_the_business_deadline() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let budget = fixture.socket.large.send_bytes.clone();
        let mut meta = MsgMeta {
            method: "Lifecycle/large".into(),
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 100,
            ..Default::default()
        };
        let payload = "x".repeat(64 * 1024);
        // Registration and serialization are setup, outside the business budget.
        // Parallel hardware tests can make a first pool registration expensive.
        let encoded = fixture.socket.serialize_msg(&meta, &payload).unwrap();
        let (msgid, response) = state.waiter.alloc(Duration::from_millis(100));
        meta.msgid = msgid;
        let request = SendRequest::bind(&mut meta, &state.waiter, fixture.socket.conn_id).unwrap();
        fixture
            .socket
            .send_large(&mut meta, encoded, &request, &state)
            .await
            .expect("an accepted request cannot report a retryable send failure");
        let (offer_meta, offer) = decode_offer(fixture.pending.try_recv().unwrap());
        let retained = budget.available_permits();
        assert!(retained < BYTE_BUDGET);

        let error = tokio::time::timeout(Duration::from_secs(1), response.recv())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(state.waiter.contains_message_id(offer_meta.msgid));
        assert!(fixture.socket.state.is_ok());
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .contains_key(&offer.transfer_id)
        );
        assert_eq!(budget.available_permits(), retained);

        drop(fixture);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn finished_sources_do_not_bypass_pending_offer_limit() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let payload = "x".repeat(64 * 1024);
        let mut meta = MsgMeta {
            flags: MsgFlags::IsRsp | MsgFlags::UseMessagePack,
            ..Default::default()
        };
        for _ in 0..MAX_TRANSFERS {
            fixture
                .socket
                .send(&mut meta, &payload, &state)
                .await
                .unwrap();
            let (_, offer) = decode_offer(fixture.pending.try_recv().unwrap());
            assert!(
                fixture
                    .socket
                    .finish_large(offer.transfer_id, &state)
                    .unwrap()
            );
        }
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        assert_eq!(
            fixture.socket.large.send_bytes.available_permits(),
            BYTE_BUDGET
        );
        assert_eq!(fixture.socket.large.offers.available_permits(), 0);
        let error = fixture
            .socket
            .send(&mut meta, &payload, &state)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Overloaded);
        assert!(fixture.pending.is_empty());
        assert_eq!(state.waiter.pending_count(), MAX_TRANSFERS);
        assert!(fixture.socket.state.is_ok());

        // Missing ACKs still close the connection even after all sources finish.
        state.waiter.expire(Instant::now() + TRANSFER_TIMEOUT);
        fixture.wait_for_offer_acks().await;
        assert!(!fixture.socket.state.is_ok());
        assert_eq!(state.waiter.pending_count(), 0);
        drop(fixture);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn rejected_ack_supervision_closes_after_acceptance_and_retains_source() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        state.socket_pool.stop();
        state.socket_pool.join().await;
        let mut meta = MsgMeta {
            flags: MsgFlags::IsRsp | MsgFlags::UseMessagePack,
            ..Default::default()
        };
        fixture
            .socket
            .send(&mut meta, &"x".repeat(64 * 1024), &state)
            .await
            .expect("accepted bytes cannot become a retryable send failure");
        let (offer_meta, offer) = decode_offer(fixture.pending.try_recv().unwrap());
        assert!(!fixture.socket.state.is_ok());
        assert!(!state.waiter.contains_message_id(offer_meta.msgid));
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .contains_key(&offer.transfer_id)
        );
        let budget = fixture.socket.large.send_bytes.clone();
        assert!(budget.available_permits() < BYTE_BUDGET);
        drop(fixture);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
    }

    #[tokio::test]
    async fn accepted_sources_survive_waiter_cancellation_expiry_and_socket_error() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let budget = fixture.socket.large.send_bytes.clone();
        let cq = fixture.cq.clone();
        let socket = Arc::downgrade(&fixture.socket);
        let (cancelled_id, cancelled) = state.waiter.alloc(Duration::from_secs(30));
        let (_, first_capacity) = fixture.publish_request(cancelled_id).await;
        let (expired_id, expired) = state.waiter.alloc(Duration::from_secs(30));
        let (_, second_capacity) = fixture.publish_request(expired_id).await;
        let retained = BYTE_BUDGET - first_capacity - second_capacity;
        assert_eq!(budget.available_permits(), retained);

        drop(cancelled);
        assert!(!state.waiter.contains_message_id(cancelled_id));
        state
            .waiter
            .expire(Instant::now() + Duration::from_secs(60));
        assert!(!state.waiter.contains_message_id(expired_id));
        assert_eq!(budget.available_permits(), retained);
        assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 2);

        fixture.socket.set_error();
        assert!(!fixture.socket.state.is_ok());
        assert!(budget.is_closed());
        assert_eq!(budget.available_permits(), retained);
        assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 2);
        assert_eq!(cq.qp_registry_stats().active, 1);

        drop(expired);
        drop(fixture);
        assert!(socket.upgrade().is_none());
        assert_eq!(cq.qp_registry_stats().active, 0);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn abandoning_a_receive_closes_the_qp_without_releasing_accepted_sources() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let (_, capacity) = fixture.publish_request(msgid).await;
        let budget = fixture.socket.large.send_bytes.clone();
        let cq = fixture.cq.clone();
        let transfer_id = 71;
        fixture
            .socket
            .large
            .receivers
            .lock()
            .unwrap()
            .active
            .insert(transfer_id);

        drop(ReceiveGuard {
            socket: fixture.socket.clone(),
            id: transfer_id,
            settled: false,
        });
        assert!(!fixture.socket.state.is_ok());
        assert!(
            fixture
                .socket
                .large
                .receivers
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 1);
        assert_eq!(budget.available_permits(), BYTE_BUDGET - capacity);
        assert_eq!(cq.qp_registry_stats().active, 1);

        drop(response);
        drop(fixture);
        assert_eq!(cq.qp_registry_stats().active, 0);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn a_replayed_offer_cannot_start_a_second_receiver() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let (mut offer, capacity) = fixture.publish_request(msgid).await;
        let budget = fixture.socket.large.send_bytes.clone();
        // The spawned receiver will skip READ because no waiter wants this
        // response. This allows receive admission on an unconnected QP.
        offer.meta.flags = MsgFlags::IsRsp | MsgFlags::UseMessagePack;
        offer.meta.msgid = u64::MAX;
        fixture
            .socket
            .start_large_receive(offer.clone(), &state, Instant::now())
            .unwrap();
        let owners = Arc::strong_count(&fixture.socket);
        let error = fixture
            .socket
            .start_large_receive(offer, &state, Instant::now())
            .unwrap_err();
        assert!(error.msg.contains("replayed"), "{error}");
        assert_eq!(Arc::strong_count(&fixture.socket), owners);
        assert!(!fixture.socket.state.is_ok());
        assert!(
            fixture
                .socket
                .large
                .receivers
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 1);
        assert_eq!(budget.available_permits(), BYTE_BUDGET - capacity);
        assert_eq!(
            fixture.socket.large.recv_bytes.available_permits(),
            BYTE_BUDGET
        );

        // Cancel and join the one supervised receiver before dropping its socket.
        state.socket_pool.stop();
        state.socket_pool.join().await;
        assert_eq!(Arc::strong_count(&fixture.socket), 1);
        assert!(!fixture.socket.state.is_ok());
        assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 1);
        assert_eq!(budget.available_permits(), BYTE_BUDGET - capacity);
        let cq = fixture.cq.clone();
        drop(response);
        drop(fixture);
        assert_eq!(cq.qp_registry_stats().active, 0);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
    }

    #[tokio::test]
    async fn excessive_offer_disorder_closes_connection_and_bounds_replay_tracking() {
        let mut fixture = Fixture::new();
        let state = fixture.state.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let (mut offer, capacity) = fixture.publish_request(msgid).await;
        let budget = fixture.socket.large.send_bytes.clone();
        {
            let mut receivers = fixture.socket.large.receivers.lock().unwrap();
            for id in (1..).step_by(2).take(MAX_TRANSFERS) {
                receivers.seen.insert(id).unwrap();
            }
        }
        offer.transfer_id = (2 * MAX_TRANSFERS + 1) as u64;
        let error = fixture
            .socket
            .start_large_receive(offer.clone(), &state, Instant::now())
            .unwrap_err();
        assert!(error.msg.contains("too many out-of-order"), "{error}");
        assert!(!fixture.socket.state.is_ok());
        assert!(
            fixture
                .socket
                .large
                .receivers
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        let ranges = fixture
            .socket
            .large
            .receivers
            .lock()
            .unwrap()
            .seen
            .ranges
            .len();
        assert_eq!(ranges, MAX_TRANSFERS + 1);

        // A peer ignoring the RPC rejection cannot keep growing the interval map.
        for _ in 0..4 {
            offer.transfer_id += 2;
            let error = fixture
                .socket
                .start_large_receive(offer.clone(), &state, Instant::now())
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::ConnectionClosed);
            assert_eq!(
                fixture
                    .socket
                    .large
                    .receivers
                    .lock()
                    .unwrap()
                    .seen
                    .ranges
                    .len(),
                ranges
            );
        }
        assert_eq!(fixture.socket.large.sources.lock().unwrap().active.len(), 1);
        assert_eq!(budget.available_permits(), BYTE_BUDGET - capacity);
        let cq = fixture.cq.clone();
        assert_eq!(cq.qp_registry_stats().active, 1);

        drop(response);
        drop(fixture);
        assert_eq!(cq.qp_registry_stats().active, 0);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn abandoned_destination_budget_is_held_until_provider_qp_destruction() {
        let fixture = Fixture::new();
        let state = fixture.state.clone();
        let budget = fixture.socket.large.recv_bytes.clone();
        let cq = fixture.cq.clone();
        let permit = budget
            .clone()
            .try_acquire_many_owned(LARGE_BYTES as u32)
            .unwrap();
        drop(DestinationBudget {
            socket: fixture.socket.clone(),
            permit: Some(permit),
        });

        assert!(!fixture.socket.state.is_ok());
        assert!(budget.is_closed());
        assert_eq!(
            fixture.socket.large.failed_receives.lock().unwrap().len(),
            1
        );
        assert_eq!(budget.available_permits(), BYTE_BUDGET - LARGE_BYTES);
        assert_eq!(cq.qp_registry_stats().active, 1);
        // Repeated closure must not reclaim permits for an unobserved READ.
        fixture.socket.set_error();
        assert_eq!(budget.available_permits(), BYTE_BUDGET - LARGE_BYTES);

        drop(fixture);
        assert_eq!(cq.qp_registry_stats().active, 0);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn exhausted_transfer_ids_reject_before_acceptance_without_wrapping() {
        let fixture = Fixture::new();
        let state = fixture.state.clone();
        let budget = fixture.socket.large.send_bytes.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        fixture.socket.large.sources.lock().unwrap().next = u64::MAX;
        let mut meta = MsgMeta {
            msgid,
            method: "Lifecycle/large".into(),
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 30_000,
            ..Default::default()
        };

        let error = fixture
            .socket
            .send(&mut meta, &"x".repeat(64 * 1024), &state)
            .await
            .unwrap_err();
        assert!(error.msg.contains("transfer IDs exhausted"), "{error}");
        assert_eq!(fixture.socket.large.sources.lock().unwrap().next, u64::MAX);
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        assert!(fixture.pending.is_empty());
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        assert!(fixture.socket.state.is_ok());

        drop(response);
        drop(fixture);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn cancelling_a_queue_blocked_send_releases_unpublished_source_and_budget() {
        let fixture = Fixture::new();
        let state = fixture.state.clone();
        let budget = fixture.socket.large.send_bytes.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        for _ in 0..4 {
            let mut queued = fixture.socket.rdmabuf_pool.allocate(1).unwrap();
            queued.set_len(0);
            fixture.socket.pending_sender.try_send(queued).unwrap();
        }
        let mut meta = MsgMeta {
            msgid,
            method: "Lifecycle/large".into(),
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 30_000,
            ..Default::default()
        };
        let payload = "x".repeat(64 * 1024);
        let mut sending = Box::pin(fixture.socket.send(&mut meta, &payload, &state));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        assert!(budget.available_permits() < BYTE_BUDGET);
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        assert_eq!(fixture.socket.large.sources.lock().unwrap().next, 0);
        assert_eq!(fixture.pending.len(), 4);

        drop(sending);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        assert_eq!(fixture.pending.len(), 4);
        assert!(fixture.socket.state.is_ok());

        drop(response);
        drop(fixture);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn queue_blocked_send_obeys_business_deadline_without_publishing_source() {
        let fixture = Fixture::new();
        let state = fixture.state.clone();
        let budget = fixture.socket.large.send_bytes.clone();
        for _ in 0..4 {
            let mut queued = fixture.socket.rdmabuf_pool.allocate(1).unwrap();
            queued.set_len(0);
            fixture.socket.pending_sender.try_send(queued).unwrap();
        }
        let payload = "x".repeat(64 * 1024);
        // Start the business deadline after device setup, registration, and queue
        // preparation, so only the blocked send consumes this short budget.
        let (msgid, response) = state.waiter.alloc(Duration::from_millis(100));
        let mut meta = MsgMeta {
            msgid,
            method: "Lifecycle/large".into(),
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 100,
            ..Default::default()
        };
        let mut sending = Box::pin(fixture.socket.send(&mut meta, &payload, &state));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        assert!(budget.available_permits() < BYTE_BUDGET);
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        assert_eq!(fixture.socket.large.sources.lock().unwrap().next, 0);
        assert_eq!(fixture.pending.len(), 4);

        let error = tokio::time::timeout(Duration::from_secs(2), sending.as_mut())
            .await
            .expect("queue admission must finish within the request's short deadline")
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        drop(sending);
        assert!(
            fixture
                .socket
                .large
                .sources
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
        assert_eq!(fixture.socket.large.sources.lock().unwrap().next, 0);
        assert_eq!(budget.available_permits(), BYTE_BUDGET);
        assert_eq!(fixture.pending.len(), 4);
        assert!(fixture.socket.state.is_ok());

        drop(response);
        drop(fixture);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn inline_queue_blocked_send_times_out_without_accepting_a_message() {
        let fixture = Fixture::new();
        let state = fixture.state.clone();
        fixture.block_inline_queue();
        let (msgid, response) = state.waiter.alloc(Duration::from_millis(100));
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 100,
            ..Default::default()
        };
        let mut sending = Box::pin(fixture.socket.send(&mut meta, &27, &state));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        let error = tokio::time::timeout(Duration::from_secs(2), sending.as_mut())
            .await
            .expect("inline pending admission must honor the request deadline")
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        drop(sending);
        assert_eq!(fixture.pending.len(), 4);
        assert!(fixture.socket.state.is_ok());
        assert_eq!(
            fixture.socket.large.send_bytes.available_permits(),
            BYTE_BUDGET
        );

        drop(response);
        drop(fixture);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn inline_queue_blocked_send_wakes_when_connection_closes() {
        let fixture = Fixture::new();
        let state = fixture.state.clone();
        fixture.block_inline_queue();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 30_000,
            ..Default::default()
        };
        let mut sending = Box::pin(fixture.socket.send(&mut meta, &27, &state));
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        fixture.socket.set_error();
        let error = tokio::time::timeout(Duration::from_secs(2), sending.as_mut())
            .await
            .expect("connection closure must wake blocked inline admission")
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        drop(sending);
        assert_eq!(fixture.pending.len(), 4);

        drop(response);
        drop(fixture);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn expired_inline_request_does_not_consume_a_direct_send_credit() {
        let fixture = Fixture::new();
        let state = fixture.state.clone();
        let (msgid, response) = state.waiter.alloc(Duration::ZERO);
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            ..Default::default()
        };
        let error = fixture
            .socket
            .send(&mut meta, &27, &state)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(fixture.pending.is_empty());
        assert_eq!(
            fixture.socket.state.try_acquire(),
            SendPermit::Granted { window_tail: true }
        );
        assert!(fixture.socket.state.is_ok());

        drop(response);
        drop(fixture);
        state.socket_pool.join().await;
    }

    #[tokio::test]
    async fn inline_request_cancelled_during_serialization_keeps_direct_send_credit() {
        struct CancelRequest<'a> {
            response: std::cell::RefCell<Option<Receiver<'a>>>,
            serializations: std::cell::Cell<usize>,
        }

        impl serde::Serialize for CancelRequest<'_> {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                self.serializations.set(self.serializations.get() + 1);
                drop(self.response.borrow_mut().take());
                serializer.serialize_u32(27)
            }
        }

        let fixture = Fixture::new();
        let state = fixture.state.clone();
        let (msgid, response) = state.waiter.alloc(Duration::from_secs(30));
        let payload = CancelRequest {
            response: std::cell::RefCell::new(Some(response)),
            serializations: std::cell::Cell::new(0),
        };
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 30_000,
            ..Default::default()
        };

        let error = fixture
            .socket
            .send(&mut meta, &payload, &state)
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(payload.serializations.get(), 1);
        assert!(!state.waiter.contains_message_id(msgid));
        assert!(fixture.pending.is_empty());
        assert_eq!(
            fixture.socket.state.try_acquire(),
            SendPermit::Granted { window_tail: true }
        );
        assert!(fixture.socket.state.is_ok());

        drop(payload);
        drop(fixture);
        state.socket_pool.join().await;
    }
}
