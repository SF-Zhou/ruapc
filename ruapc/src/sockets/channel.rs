//! Shared send-queue ownership for TCP, WebSocket and HTTP/2 connections.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc};

use super::{ConnectionLifecycle, Outbound, SendRequest, reserve_send};
use crate::{Error, ErrorKind, MsgMeta, Result, Waiter};

/// Most data-frame bytes one connection may hold queued ahead of its send
/// loop.
///
/// The queue is entry-counted, and one data frame can own about 1 MiB of
/// freshly gathered storage, so entries alone would let a slow reader pin
/// almost a gigabyte. A full transfer keeps at most this much frame storage in
/// flight; the send loop releases it as it writes each frame.
const MAX_QUEUED_DATA_BYTES: usize = 8 << 20;

/// Most responses of one connection that may wait in tasks of their own for
/// send-queue capacity.
///
/// A receive loop answers the last data frame of a transfer without waiting
/// for its own send queue, so an answer that finds the queue full is handed
/// to a task. A peer that reads its socket empties the queue faster than it
/// can ask for answers; one that does not could otherwise make this side
/// spawn a task per frame, without bound.
pub(crate) const MAX_DEFERRED_RESPONSES: usize = 1024;

#[derive(Debug)]
pub(crate) struct ChannelConnection {
    sender: mpsc::Sender<Outbound>,
    lifecycle: ConnectionLifecycle,
    /// The transport's name in error messages.
    transport: &'static str,
    /// Data-frame bytes that may still be queued, one permit per byte. A
    /// frame that has to wait keeps its place: it takes the bytes that are
    /// free and those released next, so no later frame, however small, is
    /// admitted before it. A frame that fits at once does not wait.
    data_bytes: Arc<Semaphore>,
    /// Responses that may still be handed to a task; see
    /// [`MAX_DEFERRED_RESPONSES`].
    deferred_responses: Arc<Semaphore>,
}

impl ChannelConnection {
    pub(crate) fn new(sender: mpsc::Sender<Outbound>, transport: &'static str) -> Self {
        Self {
            sender,
            lifecycle: ConnectionLifecycle::new(),
            transport,
            data_bytes: Arc::new(Semaphore::new(MAX_QUEUED_DATA_BYTES)),
            deferred_responses: Arc::new(Semaphore::new(MAX_DEFERRED_RESPONSES)),
        }
    }

    pub(crate) fn conn_id(&self) -> u64 {
        self.lifecycle.conn_id()
    }

    pub(crate) fn close_once(&self) -> bool {
        self.lifecycle.close_once()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.lifecycle.is_closed() || self.sender.is_closed()
    }

    /// Completes once teardown marked this connection closed.
    ///
    /// Transport loops race their I/O against this future so that one
    /// direction failing ends the other one. Each loop keeps the queue's only
    /// sender alive through its `Arc<ChannelConnection>`, so a loop cannot wait
    /// for the queue to close while it is the one that would have to drop the
    /// sender.
    pub(crate) async fn closed(&self) {
        self.lifecycle.closed().await
    }

    /// Reserves capacity within the request's budget, then encodes and commits
    /// without yielding. Responses have no local response waiter or deadline.
    pub(crate) async fn send(
        &self,
        meta: &mut MsgMeta,
        waiter: &Waiter,
        encode: impl FnOnce(&MsgMeta) -> Result<Bytes>,
    ) -> Result<()> {
        // Bind before checking closure: a concurrent teardown must see this
        // request, or this check must reject it. Reversing the order loses
        // the eager failure when the connection closes between the two.
        let request = SendRequest::bind(meta, waiter, self.conn_id());
        self.check_open()?;
        let request = request?;
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Closed(())) => {
                return Err(ErrorKind::ConnectionClosed.into());
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                let permit =
                    reserve_send(&self.sender, request.deadline(), || self.lifecycle.closed())
                        .await?;
                self.check_open()?;
                request.refresh(meta)?;
                permit
            }
        };
        let bytes = encode(meta)?;
        // Serialization can take time or run application code. Recheck both
        // conditions before synchronous submission, even on the fast path.
        self.check_open()?;
        request.check()?;
        permit.send(bytes.into());
        Ok(())
    }

    /// Queues a data frame, waiting for capacity until `deadline` or closure.
    ///
    /// A data frame belongs to no local waiter: its sender bounds the wait
    /// with the deadline of the transfer the frame is part of. Frames queued
    /// by one task keep their order.
    ///
    /// Admission counts both frame bytes and queue entries, so a slow reader
    /// bounds how many data-frame bytes the connection keeps queued. Bytes
    /// are reserved first: a frame that waits for them holds no queue entry
    /// that messages could use meanwhile. The frame keeps its bytes until the
    /// send loop has written all of it.
    pub(crate) async fn send_data(&self, frame: Outbound, deadline: Instant) -> Result<()> {
        self.check_open()?;
        if deadline <= Instant::now() {
            return Err(ErrorKind::Timeout.into());
        }
        // Dropped on any failure below, which returns the bytes.
        let queued = self.reserve_bytes(frame.byte_len(), deadline).await?;
        let permit = reserve_send(&self.sender, Some(deadline), || self.lifecycle.closed()).await?;
        self.check_open()?;
        permit.send(match queued {
            Some(queued) => frame.admitted(queued),
            None => frame,
        });
        Ok(())
    }

    /// Reserves queued-frame bytes until `deadline` or connection closure.
    ///
    /// Returns `None` for a frame without bytes, and for one larger than the
    /// whole budget, which the data frame encoders never produce and which
    /// could never be admitted.
    async fn reserve_bytes(
        &self,
        bytes: usize,
        deadline: Instant,
    ) -> Result<Option<OwnedSemaphorePermit>> {
        debug_assert!(
            bytes <= MAX_QUEUED_DATA_BYTES,
            "data frame exceeds the queued-byte budget"
        );
        let permits = match u32::try_from(bytes) {
            Ok(permits) if (1..=MAX_QUEUED_DATA_BYTES).contains(&bytes) => permits,
            _ => return Ok(None),
        };
        match self.data_bytes.clone().try_acquire_many_owned(permits) {
            Ok(queued) => Ok(Some(queued)),
            Err(TryAcquireError::Closed) => Err(ErrorKind::ConnectionClosed.into()),
            // Keep the timer out of every frame's future, as `reserve_send`
            // does: only a full budget waits.
            Err(TryAcquireError::NoPermits) => {
                Box::pin(self.wait_for_bytes(permits, deadline)).await
            }
        }
    }

    #[cold]
    async fn wait_for_bytes(
        &self,
        permits: u32,
        deadline: Instant,
    ) -> Result<Option<OwnedSemaphorePermit>> {
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline.into()) => Err(ErrorKind::Timeout.into()),
            () = self.lifecycle.closed() => Err(ErrorKind::ConnectionClosed.into()),
            queued = self.data_bytes.clone().acquire_many_owned(permits) => queued
                .map(Some)
                .map_err(|_| ErrorKind::ConnectionClosed.into()),
        }
    }

    /// Reserves room for one response that will wait in a task of its own for
    /// send-queue capacity, until the returned permit is dropped. `None`
    /// means the connection already has [`MAX_DEFERRED_RESPONSES`] of them.
    pub(crate) fn try_defer_response(&self) -> Option<OwnedSemaphorePermit> {
        self.deferred_responses.clone().try_acquire_owned().ok()
    }

    fn check_open(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                format!("{} connection is closed", self.transport),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, time::Duration};

    use bytes::BytesMut;

    use super::*;
    use crate::{
        Message, MsgFlags,
        msg::frame::{encode, parse_message},
    };

    fn full_queue() -> (ChannelConnection, mpsc::Receiver<Outbound>) {
        let (sender, receiver) = mpsc::channel(1);
        sender.try_send(occupied()).unwrap();
        (ChannelConnection::new(sender, "test"), receiver)
    }

    fn occupied() -> Outbound {
        Bytes::from_static(b"occupied").into()
    }

    /// The next queued frame, or `None` when the queue is empty.
    fn queued(receiver: &mut mpsc::Receiver<Outbound>) -> Option<Bytes> {
        receiver.try_recv().ok().map(Outbound::into_bytes)
    }

    fn request_meta(msgid: u64) -> MsgMeta {
        MsgMeta {
            msgid,
            flags: MsgFlags::IsReq,
            timeout_ms: 10_000,
            ..Default::default()
        }
    }

    fn data_frame(bytes: usize) -> Outbound {
        Outbound::from(Bytes::from(vec![7u8; bytes]))
    }

    fn in_five_seconds() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    /// A connection admits data frames by bytes as well as entries: the
    /// second oversized frame waits until the first has been written.
    #[tokio::test]
    async fn queued_data_bytes_limit_admission_until_written() {
        let half = MAX_QUEUED_DATA_BYTES / 2 + 1;
        let (sender, mut receiver) = mpsc::channel(4);
        let connection = ChannelConnection::new(sender, "test");
        let deadline = in_five_seconds();
        connection
            .send_data(data_frame(half), deadline)
            .await
            .unwrap();

        let sending = connection.send_data(data_frame(half), deadline);
        tokio::pin!(sending);
        assert!(futures_util::poll!(sending.as_mut()).is_pending());

        // The send loop taking the first frame releases its queued bytes.
        assert_eq!(receiver.recv().await.unwrap().byte_len(), half);
        sending.await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().byte_len(), half);
        assert_eq!(
            connection.data_bytes.available_permits(),
            MAX_QUEUED_DATA_BYTES
        );
    }

    /// A data frame's bytes stay reserved until its last segment is written,
    /// not just its head: a frame that shares its sender's buffers is still
    /// queued while those segments wait.
    #[tokio::test]
    async fn multi_segment_frames_release_their_bytes_after_the_last_segment() {
        let half = MAX_QUEUED_DATA_BYTES / 2 + 1;
        let (sender, mut receiver) = mpsc::channel(4);
        let connection = ChannelConnection::new(sender, "test");
        let deadline = in_five_seconds();
        let frame = Outbound::with_data(
            Bytes::from_static(b"head"),
            vec![Bytes::from(vec![7u8; half - 4])],
        );
        connection.send_data(frame, deadline).await.unwrap();

        let sending = connection.send_data(data_frame(half), deadline);
        tokio::pin!(sending);
        let mut segments: Vec<_> = receiver.recv().await.unwrap().segments().collect();
        // The head is written; the shared data still waits.
        segments.remove(0);
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        drop(segments);
        sending.await.unwrap();
    }

    /// A frame that waits for queued bytes holds no queue entry, so messages
    /// can still use the whole queue meanwhile.
    #[tokio::test]
    async fn frames_waiting_for_bytes_leave_queue_entries_to_messages() {
        let half = MAX_QUEUED_DATA_BYTES / 2 + 1;
        let (sender, mut receiver) = mpsc::channel(2);
        let connection = ChannelConnection::new(sender, "test");
        let deadline = in_five_seconds();
        connection
            .send_data(data_frame(half), deadline)
            .await
            .unwrap();
        let sending = connection.send_data(data_frame(half), deadline);
        tokio::pin!(sending);
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        // The second entry is still free while the frame waits for bytes.
        assert_eq!(connection.sender.capacity(), 1);
        drop(receiver.recv().await.unwrap());
        sending.await.unwrap();
        assert_eq!(connection.sender.capacity(), 1);
    }

    /// A waiting frame keeps its place. A large frame that waits for bytes
    /// is not overtaken by later small ones that would fit the bytes
    /// released so far, so it cannot starve behind a stream of them.
    #[tokio::test]
    async fn a_waiting_large_frame_is_not_overtaken_by_smaller_ones() {
        let third = MAX_QUEUED_DATA_BYTES / 3;
        let (sender, mut receiver) = mpsc::channel(8);
        let connection = ChannelConnection::new(sender, "test");
        let deadline = in_five_seconds();
        for _ in 0..3 {
            connection
                .send_data(data_frame(third), deadline)
                .await
                .unwrap();
        }
        // Needs two of the three queued frames to be written.
        let large = connection.send_data(data_frame(2 * third), deadline);
        tokio::pin!(large);
        assert!(futures_util::poll!(large.as_mut()).is_pending());
        // One written frame leaves room for the small frame, but not for the
        // large one that asked first.
        drop(receiver.recv().await.unwrap());
        let small = connection.send_data(data_frame(third), deadline);
        tokio::pin!(small);
        assert!(futures_util::poll!(small.as_mut()).is_pending());
        assert!(futures_util::poll!(large.as_mut()).is_pending());

        drop(receiver.recv().await.unwrap());
        large.await.unwrap();
        assert!(futures_util::poll!(small.as_mut()).is_pending());
        drop(receiver.recv().await.unwrap());
        small.await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().byte_len(), 2 * third);
        assert_eq!(receiver.recv().await.unwrap().byte_len(), third);
    }

    /// The same holds for bytes that were free before the large frame came:
    /// it reserves them while it waits, so a later frame that would have fit
    /// them waits behind it. This is how the semaphore's waiters behave, not
    /// something its nonblocking fast path promises; the fast path only ever
    /// runs ahead of a frame that is not waiting yet.
    #[tokio::test]
    async fn a_waiting_frame_reserves_the_bytes_that_were_already_free() {
        let half = MAX_QUEUED_DATA_BYTES / 2 + 1;
        let (sender, mut receiver) = mpsc::channel(8);
        let connection = ChannelConnection::new(sender, "test");
        let deadline = in_five_seconds();
        connection
            .send_data(data_frame(half), deadline)
            .await
            .unwrap();
        // Almost half the budget is free, and a small frame fits it.
        connection.send_data(data_frame(1), deadline).await.unwrap();
        assert!(connection.data_bytes.available_permits() > 1);

        let large = connection.send_data(data_frame(half), deadline);
        tokio::pin!(large);
        assert!(futures_util::poll!(large.as_mut()).is_pending());
        assert_eq!(connection.data_bytes.available_permits(), 0);
        let small = connection.send_data(data_frame(1), deadline);
        tokio::pin!(small);
        assert!(futures_util::poll!(small.as_mut()).is_pending());

        // The first frame's bytes complete the large frame, then the small.
        assert_eq!(receiver.recv().await.unwrap().byte_len(), half);
        large.await.unwrap();
        small.await.unwrap();
        let order: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok())
            .map(|frame| frame.byte_len())
            .collect();
        assert_eq!(order, [1, half, 1]);

        assert_eq!(
            connection.data_bytes.available_permits(),
            MAX_QUEUED_DATA_BYTES
        );
    }

    /// A waiting frame that gives up returns the bytes it had reserved.
    #[tokio::test]
    async fn a_frame_that_stops_waiting_returns_its_reserved_bytes() {
        let half = MAX_QUEUED_DATA_BYTES / 2 + 1;
        let (sender, _receiver) = mpsc::channel(8);
        let connection = ChannelConnection::new(sender, "test");
        let deadline = in_five_seconds();
        connection
            .send_data(data_frame(half), deadline)
            .await
            .unwrap();
        let free = MAX_QUEUED_DATA_BYTES - half;
        {
            let large = connection.send_data(data_frame(half), deadline);
            tokio::pin!(large);
            assert!(futures_util::poll!(large.as_mut()).is_pending());
            assert_eq!(connection.data_bytes.available_permits(), 0);
        }
        assert_eq!(connection.data_bytes.available_permits(), free);
    }

    /// Queued-byte admission observes its deadline and connection closure,
    /// and a frame that gave up leaves the budget as it found it.
    #[tokio::test]
    async fn queued_data_byte_admission_honors_deadline_and_closure() {
        let half = MAX_QUEUED_DATA_BYTES / 2 + 1;
        let (sender, mut receiver) = mpsc::channel(4);
        let connection = ChannelConnection::new(sender, "test");
        connection
            .send_data(data_frame(half), in_five_seconds())
            .await
            .unwrap();
        let available = connection.data_bytes.available_permits();

        // An expired deadline admits nothing, even a frame that would fit.
        let error = connection
            .send_data(data_frame(1), Instant::now())
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            connection.send_data(data_frame(half), Instant::now() + Duration::from_millis(20)),
        )
        .await
        .expect("queued-byte admission must observe its deadline")
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(connection.data_bytes.available_permits(), available);

        let sending = connection.send_data(data_frame(half), in_five_seconds());
        tokio::pin!(sending);
        assert!(futures_util::poll!(sending.as_mut()).is_pending());
        assert!(connection.close_once());
        assert_eq!(sending.await.unwrap_err().kind, ErrorKind::ConnectionClosed);
        assert_eq!(connection.data_bytes.available_permits(), available);
        assert_eq!(receiver.recv().await.unwrap().byte_len(), half);
    }

    /// A full entry queue holds a data frame back within its deadline, and
    /// the frame that gave up returns its bytes.
    #[tokio::test]
    async fn a_full_entry_queue_bounds_data_frames_by_their_deadline() {
        let (connection, mut receiver) = full_queue();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            connection.send_data(data_frame(16), Instant::now() + Duration::from_millis(20)),
        )
        .await
        .expect("entry admission must observe the frame's deadline")
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(
            connection.data_bytes.available_permits(),
            MAX_QUEUED_DATA_BYTES
        );
        assert_eq!(queued(&mut receiver).unwrap(), b"occupied"[..]);
        assert!(queued(&mut receiver).is_none());
    }

    /// Only so many responses may wait in tasks of their own; each slot
    /// comes back when its task ends.
    #[test]
    fn deferred_responses_are_bounded_per_connection() {
        let (sender, _receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender, "test");
        let mut slots: Vec<_> = (0..MAX_DEFERRED_RESPONSES)
            .map(|_| connection.try_defer_response().unwrap())
            .collect();
        assert!(connection.try_defer_response().is_none());
        slots.pop();
        assert!(connection.try_defer_response().is_some());
    }

    #[tokio::test]
    async fn full_queue_expires_without_waiter_sweeping_or_late_submission() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_millis(20));
        let mut meta = request_meta(msgid);
        let (connection, mut receiver) = full_queue();

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            connection.send(&mut meta, &waiter, |_| {
                panic!("an expired queued request must not be serialized")
            }),
        )
        .await
        .expect("send must observe its deadline without a sweep");

        assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);
        assert!(waiter.contains_message_id(msgid));
        assert_eq!(queued(&mut receiver).unwrap(), b"occupied"[..]);
        assert!(queued(&mut receiver).is_none());
    }

    #[tokio::test]
    async fn expired_request_is_rejected_even_with_available_capacity() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::ZERO);
        let mut meta = request_meta(msgid);
        let (sender, mut receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender, "test");

        let error = connection
            .send(&mut meta, &waiter, |_| {
                panic!("an expired request must not be serialized")
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(queued(&mut receiver).is_none());
    }

    #[tokio::test]
    async fn capacity_becoming_available_at_expiry_does_not_accept_request() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_millis(20));
        let mut meta = request_meta(msgid);
        let (connection, mut receiver) = full_queue();
        let sending = connection.send(&mut meta, &waiter, |_| {
            panic!("an expired request must not win the capacity race")
        });
        tokio::pin!(sending);
        assert!(futures_util::poll!(sending.as_mut()).is_pending());

        // Both the deadline and capacity are ready before send is polled again.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(queued(&mut receiver).unwrap(), b"occupied"[..]);
        assert_eq!(sending.await.unwrap_err().kind, ErrorKind::Timeout);
        assert!(queued(&mut receiver).is_none());
    }

    #[tokio::test]
    async fn closing_connection_wakes_sender_even_while_receiver_is_alive() {
        for release_capacity in [false, true] {
            let waiter = Waiter::default();
            let (msgid, _response) = waiter.alloc(Duration::from_secs(10));
            let mut meta = request_meta(msgid);
            let (connection, mut receiver) = full_queue();
            let sending = connection.send(&mut meta, &waiter, |_| {
                panic!("a closed connection must not serialize requests")
            });
            tokio::pin!(sending);
            assert!(futures_util::poll!(sending.as_mut()).is_pending());

            if release_capacity {
                // Test closure and capacity becoming ready in the same poll.
                assert_eq!(queued(&mut receiver).unwrap(), b"occupied"[..]);
            }
            assert!(connection.close_once());
            let result = tokio::time::timeout(Duration::from_secs(1), sending)
                .await
                .expect("logical closure must wake a full-queue send");
            assert_eq!(result.unwrap_err().kind, ErrorKind::ConnectionClosed);

            if !release_capacity {
                assert_eq!(queued(&mut receiver).unwrap(), b"occupied"[..]);
            }
            assert!(queued(&mut receiver).is_none());
        }
    }

    #[tokio::test]
    async fn metadata_budget_is_refreshed_after_reservation() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_secs(10));
        let mut meta = request_meta(msgid);
        let (connection, mut receiver) = full_queue();
        let serializations = Cell::new(0);
        {
            let sending = connection.send(&mut meta, &waiter, |meta| {
                serializations.set(serializations.get() + 1);
                encode(meta, &"payload").map_err(Into::into)
            });
            tokio::pin!(sending);
            assert!(futures_util::poll!(sending.as_mut()).is_pending());
            assert_eq!(serializations.get(), 0);

            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(queued(&mut receiver).unwrap(), b"occupied"[..]);
            sending.await.unwrap();
        }

        assert_eq!(serializations.get(), 1);
        let mut encoded = BytesMut::from(queued(&mut receiver).unwrap().as_ref());
        let message = Message::parse(parse_message(&mut encoded).unwrap().unwrap()).unwrap();
        assert_eq!(message.meta, meta);
        assert!(message.meta.timeout_ms > 0);
        assert!(message.meta.timeout_ms <= 9_980);
        assert_eq!(
            message
                .payload
                .deserialize::<String>(&message.meta)
                .unwrap(),
            "payload"
        );
    }

    #[tokio::test]
    async fn responses_do_not_use_local_waiter_deadlines() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::ZERO);
        let mut meta = MsgMeta {
            msgid,
            flags: MsgFlags::IsRsp,
            ..Default::default()
        };
        let (connection, mut receiver) = full_queue();
        {
            let sending = connection.send(&mut meta, &waiter, |meta| {
                encode(meta, &()).map_err(Into::into)
            });
            tokio::pin!(sending);
            assert!(futures_util::poll!(sending.as_mut()).is_pending());
            assert_eq!(queued(&mut receiver).unwrap(), b"occupied"[..]);
            sending.await.unwrap();
        }

        assert_eq!(meta.timeout_ms, 0);
        assert!(queued(&mut receiver).is_some());
        assert!(waiter.contains_message_id(msgid));
    }

    #[tokio::test]
    async fn request_expiring_during_serialization_is_not_submitted() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_millis(20));
        let mut meta = request_meta(msgid);
        let (sender, mut receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender, "test");

        let error = connection
            .send(&mut meta, &waiter, |_| {
                std::thread::sleep(Duration::from_millis(30));
                Ok(Bytes::from_static(b"expired"))
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(queued(&mut receiver).is_none());
    }

    #[tokio::test]
    async fn request_cancelled_during_serialization_is_not_submitted() {
        let waiter = Waiter::default();
        let (msgid, response) = waiter.alloc(Duration::from_secs(10));
        let mut meta = request_meta(msgid);
        let (sender, mut receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender, "test");

        let error = connection
            .send(&mut meta, &waiter, |_| {
                drop(response);
                Ok(Bytes::from_static(b"cancelled"))
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(!waiter.contains_message_id(msgid));
        assert!(queued(&mut receiver).is_none());
        assert_eq!(connection.sender.capacity(), 1);
    }

    #[tokio::test]
    async fn connection_closing_during_serialization_is_not_submitted() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_secs(10));
        let mut meta = request_meta(msgid);
        let (sender, mut receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender, "test");

        let error = connection
            .send(&mut meta, &waiter, |_| {
                assert!(connection.close_once());
                Ok(Bytes::from_static(b"closed"))
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert!(queued(&mut receiver).is_none());
    }
}
