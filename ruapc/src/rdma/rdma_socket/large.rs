//! Connection-scoped rendezvous transfers using ordinary framed RPCs.
//! Sources remain owned until a peer finish RPC or provider QP destruction.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use bytes::Bytes;
use ruapc_wire::frame::{HEADER_LEN, MAX_MSG_SIZE};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{RdmaSocket, encode::Encoded};
use crate::{
    Buffer, BufferPool, Error, ErrorKind, Message, MsgFlags, MsgMeta, Result, Socket, State,
    metrics::MethodCall,
    rdma::rdma_service::{FinishPayloadRequest, LargePayloadOffer},
    remote_memory::scatter::SpaceLayout,
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
mod tests;

#[cfg(test)]
mod lifecycle_tests;
