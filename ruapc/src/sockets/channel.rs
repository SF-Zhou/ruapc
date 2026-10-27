//! Shared send-queue ownership for TCP, WebSocket and HTTP/2 connections.

use bytes::Bytes;
use tokio::sync::mpsc;

use super::{ConnectionLifecycle, SendRequest, reserve_send};
use crate::{Error, ErrorKind, MsgMeta, Result, Waiter};

#[derive(Debug)]
pub(crate) struct ChannelConnection {
    sender: mpsc::Sender<Bytes>,
    lifecycle: ConnectionLifecycle,
}

impl ChannelConnection {
    pub(crate) fn new(sender: mpsc::Sender<Bytes>) -> Self {
        Self {
            sender,
            lifecycle: ConnectionLifecycle::new(),
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

    /// Reserves capacity within the request's budget, then encodes and commits
    /// without yielding. Responses have no local response waiter or deadline.
    pub(crate) async fn send(
        &self,
        meta: &mut MsgMeta,
        waiter: &Waiter,
        transport: &'static str,
        encode: impl FnOnce(&MsgMeta) -> Result<Bytes>,
    ) -> Result<()> {
        // Bind before checking closure: a concurrent teardown must see this
        // request, or this check must reject it. Reversing the order loses
        // the eager failure when the connection closes between the two.
        let request = SendRequest::bind(meta, waiter, self.conn_id());
        self.check_open(transport)?;
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
                self.check_open(transport)?;
                request.refresh(meta)?;
                permit
            }
        };
        let bytes = encode(meta)?;
        // Serialization can take time or run application code. Recheck both
        // conditions before synchronous submission, even on the fast path.
        self.check_open(transport)?;
        request.check()?;
        permit.send(bytes);
        Ok(())
    }

    fn check_open(&self, transport: &'static str) -> Result<()> {
        if self.is_closed() {
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                format!("{transport} connection is closed"),
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

    fn full_queue() -> (ChannelConnection, mpsc::Receiver<Bytes>) {
        let (sender, receiver) = mpsc::channel(1);
        sender.try_send(Bytes::from_static(b"occupied")).unwrap();
        (ChannelConnection::new(sender), receiver)
    }

    fn request_meta(msgid: u64) -> MsgMeta {
        MsgMeta {
            msgid,
            flags: MsgFlags::IsReq,
            timeout_ms: 10_000,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn full_queue_expires_without_waiter_sweeping_or_late_submission() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_millis(20));
        let mut meta = request_meta(msgid);
        let (connection, mut receiver) = full_queue();

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            connection.send(&mut meta, &waiter, "test", |_| {
                panic!("an expired queued request must not be serialized")
            }),
        )
        .await
        .expect("send must observe its deadline without a sweep");

        assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);
        assert!(waiter.contains_message_id(msgid));
        assert_eq!(receiver.try_recv().unwrap(), b"occupied"[..]);
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn expired_request_is_rejected_even_with_available_capacity() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::ZERO);
        let mut meta = request_meta(msgid);
        let (sender, mut receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender);

        let error = connection
            .send(&mut meta, &waiter, "test", |_| {
                panic!("an expired request must not be serialized")
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn capacity_becoming_available_at_expiry_does_not_accept_request() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_millis(20));
        let mut meta = request_meta(msgid);
        let (connection, mut receiver) = full_queue();
        let sending = connection.send(&mut meta, &waiter, "test", |_| {
            panic!("an expired request must not win the capacity race")
        });
        tokio::pin!(sending);
        assert!(futures_util::poll!(sending.as_mut()).is_pending());

        // Both the deadline and capacity are ready before send is polled again.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(receiver.try_recv().unwrap(), b"occupied"[..]);
        assert_eq!(sending.await.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn closing_connection_wakes_sender_even_while_receiver_is_alive() {
        for release_capacity in [false, true] {
            let waiter = Waiter::default();
            let (msgid, _response) = waiter.alloc(Duration::from_secs(10));
            let mut meta = request_meta(msgid);
            let (connection, mut receiver) = full_queue();
            let sending = connection.send(&mut meta, &waiter, "test", |_| {
                panic!("a closed connection must not serialize requests")
            });
            tokio::pin!(sending);
            assert!(futures_util::poll!(sending.as_mut()).is_pending());

            if release_capacity {
                // Test closure and capacity becoming ready in the same poll.
                assert_eq!(receiver.try_recv().unwrap(), b"occupied"[..]);
            }
            assert!(connection.close_once());
            let result = tokio::time::timeout(Duration::from_secs(1), sending)
                .await
                .expect("logical closure must wake a full-queue send");
            assert_eq!(result.unwrap_err().kind, ErrorKind::ConnectionClosed);

            if !release_capacity {
                assert_eq!(receiver.try_recv().unwrap(), b"occupied"[..]);
            }
            assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
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
            let sending = connection.send(&mut meta, &waiter, "test", |meta| {
                serializations.set(serializations.get() + 1);
                encode(meta, &"payload").map_err(Into::into)
            });
            tokio::pin!(sending);
            assert!(futures_util::poll!(sending.as_mut()).is_pending());
            assert_eq!(serializations.get(), 0);

            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(receiver.try_recv().unwrap(), b"occupied"[..]);
            sending.await.unwrap();
        }

        assert_eq!(serializations.get(), 1);
        let mut encoded = BytesMut::from(receiver.try_recv().unwrap().as_ref());
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
            let sending = connection.send(&mut meta, &waiter, "test", |meta| {
                encode(meta, &()).map_err(Into::into)
            });
            tokio::pin!(sending);
            assert!(futures_util::poll!(sending.as_mut()).is_pending());
            assert_eq!(receiver.try_recv().unwrap(), b"occupied"[..]);
            sending.await.unwrap();
        }

        assert_eq!(meta.timeout_ms, 0);
        assert!(receiver.try_recv().is_ok());
        assert!(waiter.contains_message_id(msgid));
    }

    #[tokio::test]
    async fn request_expiring_during_serialization_is_not_submitted() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_millis(20));
        let mut meta = request_meta(msgid);
        let (sender, mut receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender);

        let error = connection
            .send(&mut meta, &waiter, "test", |_| {
                std::thread::sleep(Duration::from_millis(30));
                Ok(Bytes::from_static(b"expired"))
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn request_cancelled_during_serialization_is_not_submitted() {
        let waiter = Waiter::default();
        let (msgid, response) = waiter.alloc(Duration::from_secs(10));
        let mut meta = request_meta(msgid);
        let (sender, mut receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender);

        let error = connection
            .send(&mut meta, &waiter, "test", |_| {
                drop(response);
                Ok(Bytes::from_static(b"cancelled"))
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(!waiter.contains_message_id(msgid));
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        assert_eq!(connection.sender.capacity(), 1);
    }

    #[tokio::test]
    async fn connection_closing_during_serialization_is_not_submitted() {
        let waiter = Waiter::default();
        let (msgid, _response) = waiter.alloc(Duration::from_secs(10));
        let mut meta = request_meta(msgid);
        let (sender, mut receiver) = mpsc::channel(1);
        let connection = ChannelConnection::new(sender);

        let error = connection
            .send(&mut meta, &waiter, "test", |_| {
                assert!(connection.close_once());
                Ok(Bytes::from_static(b"closed"))
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }
}
