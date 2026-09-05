//! Shared send-queue ownership for TCP, WebSocket and HTTP/2 connections.

use bytes::Bytes;
use tokio::sync::mpsc;

use super::ConnectionLifecycle;
use crate::{Error, ErrorKind, MsgMeta, Result, State};

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

    /// Binds and validates a request before returning its connection's queue.
    /// Keeping this synchronous lets each transport await the queue directly.
    #[inline]
    pub(crate) fn prepare_send(
        &self,
        meta: &MsgMeta,
        state: &State,
        transport: &'static str,
    ) -> Result<&mpsc::Sender<Bytes>> {
        // Bind before checking closure: a concurrent teardown must see this
        // request, or this check must reject it. Reversing the order loses
        // the eager failure when the connection closes between the two.
        if meta.is_req() {
            state.waiter.bind_connection(meta.msgid, self.conn_id());
        }
        if self.is_closed() {
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                format!("{transport} connection is closed"),
            ));
        }
        Ok(&self.sender)
    }
}
