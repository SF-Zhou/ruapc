use foldhash::fast::RandomState;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

use crate::{Buffer, Error, Message, Receiver};

/// The response sent through the waiter channel, including the RPC response
/// message and an optional write buffer received from the server.
pub type WaiterResponse = (Message, Option<Buffer>);

/// The result delivered through the waiter channel: either a response, or an
/// eager error (e.g. the connection carrying the request was closed).
pub(crate) type WaiterResult = std::result::Result<WaiterResponse, Error>;

/// Allocates a process-wide unique connection id.
///
/// Every transport connection (TCP socket, WebSocket, HTTP/2 stream, ...)
/// gets one so that pending requests can be correlated with the connection
/// they were sent on and failed eagerly when the connection dies.
pub(crate) fn next_conn_id() -> u64 {
    static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Entry stored in the waiter's id_map for each pending request.
struct WaiterEntry {
    /// Channel to send the response through.
    sender: oneshot::Sender<WaiterResult>,
    /// Write buffer stored by `MemoryService::tcp_push` / `rdma_pull` during
    /// the request. Sent together with the response when `post` is called.
    write_buffer: Option<Buffer>,
    /// Id of the connection the request was sent on (see
    /// [`Waiter::bind_connection`]); used by [`Waiter::fail_connection`] to
    /// fail pending requests eagerly when the connection dies.
    conn_id: Option<u64>,
    /// Coarse expiry: the entry is dropped by the periodic sweep once the
    /// deadline passed, waking the waiting task with a timeout error.
    ///
    /// Per-request `tokio::time::timeout` is deliberately avoided: at
    /// hundreds of thousands of requests per second the timer wheel
    /// registration/cancellation lock becomes a process-wide bottleneck.
    /// RPC timeouts don't need millisecond precision; the sweep interval
    /// adds at most [`Waiter::SWEEP_INTERVAL`] of slack.
    deadline: Instant,
}

/// Response waiter for correlating RPC requests with responses.
///
/// The `Waiter` provides a mechanism for asynchronous RPC calls to wait for
/// their responses. It assigns unique message IDs to requests and stores
/// channels that will receive the corresponding responses.
///
/// Write buffers received during a request (via `store_write_buffer`) are
/// stored alongside the channel sender and delivered together with the
/// response message when `post` is called, ensuring atomicity.
#[derive(Default)]
pub struct Waiter {
    index: AtomicU64,
    id_map: dashmap::DashMap<u64, WaiterEntry, RandomState>,
}

/// RAII guard for automatic cleanup of waiter entries.
///
/// When a `WaiterCleaner` is dropped, it removes the associated message ID
/// from the waiter's map, preventing memory leaks for requests that don't
/// receive responses.
pub struct WaiterCleaner<'a> {
    waiter: &'a Waiter,
    msgid: u64,
}

impl Drop for WaiterCleaner<'_> {
    fn drop(&mut self) {
        self.waiter.remove(self.msgid);
    }
}

impl Waiter {
    /// Allocates a new message ID and receiver for waiting on a response.
    ///
    /// This method:
    /// 1. Generates a unique message ID
    /// 2. Creates a oneshot channel for the response
    /// 3. Stores the sender in the internal map
    /// 4. Returns the ID and a receiver with automatic cleanup
    ///
    /// # Returns
    ///
    /// Returns a tuple of (message_id, receiver). The receiver will automatically
    /// clean up the waiter entry when dropped. The entry expires `timeout`
    /// after allocation (with up to [`Self::SWEEP_INTERVAL`] of slack).
    pub fn alloc(&self, timeout: Duration) -> (u64, Receiver<'_>) {
        let msgid = self.index.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.id_map.insert(
            msgid,
            WaiterEntry {
                sender: tx,
                write_buffer: None,
                conn_id: None,
                deadline: Instant::now() + timeout,
            },
        );
        (
            msgid,
            Receiver::OneShotRx(
                rx,
                WaiterCleaner {
                    waiter: self,
                    msgid,
                },
            ),
        )
    }

    /// Posts a response message to the waiting receiver.
    ///
    /// Removes the entry from the map and sends the response message along
    /// with any write buffer that was stored during the request.
    ///
    /// If no waiter is found (e.g., because of timeout), a warning is logged.
    pub fn post(&self, msgid: u64, result: Message) {
        if let Some((_, entry)) = self.id_map.remove(&msgid) {
            let _ = entry.sender.send(Ok((result, entry.write_buffer)));
        } else {
            tracing::warn!("Waiter post failed for msgid: {}", msgid);
        }
    }

    /// Binds a pending request to the connection it is sent on.
    ///
    /// Called by the socket `send` implementations right before queueing a
    /// request. A no-op when the entry no longer exists (already completed
    /// or expired).
    pub fn bind_connection(&self, msgid: u64, conn_id: u64) {
        if let Some(mut entry) = self.id_map.get_mut(&msgid) {
            entry.conn_id = Some(conn_id);
        }
    }

    /// Fails every pending request bound to `conn_id` with `err`.
    ///
    /// Called when a connection dies so that in-flight requests fail
    /// eagerly instead of sitting until the coarse timeout sweep.
    pub fn fail_connection(&self, conn_id: u64, err: &Error) {
        // Collect first: sending consumes the oneshot sender, which cannot
        // be moved out of `retain`'s `&mut` closure.
        let msgids: Vec<u64> = self
            .id_map
            .iter()
            .filter(|entry| entry.conn_id == Some(conn_id))
            .map(|entry| *entry.key())
            .collect();
        for msgid in msgids {
            if let Some((_, entry)) = self.id_map.remove(&msgid) {
                let _ = entry.sender.send(Err(err.clone()));
            }
        }
    }

    /// Checks if a message ID is currently being waited on.
    pub fn contains_message_id(&self, msgid: u64) -> bool {
        self.id_map.contains_key(&msgid)
    }

    /// Number of requests currently waiting for a response.
    pub fn pending_count(&self) -> usize {
        self.id_map.len()
    }

    /// Stores a write buffer for a pending request.
    ///
    /// Called by `MemoryService` handlers when the server performs a
    /// `remote_write`. The buffer is stored in the same entry as the
    /// response channel, ensuring it is delivered atomically with the
    /// response when `post` is called.
    ///
    /// # Returns
    ///
    /// Returns `true` if the buffer was stored successfully (msgid still active),
    /// `false` if the request has already completed or timed out.
    pub fn store_write_buffer(&self, msgid: u64, buffer: Buffer) -> bool {
        if let Some(mut entry) = self.id_map.get_mut(&msgid) {
            entry.write_buffer = Some(buffer);
            true
        } else {
            false
        }
    }

    fn remove(&self, msgid: u64) {
        self.id_map.remove(&msgid);
    }

    /// Interval of the expiry sweep; bounds the timeout slack.
    pub const SWEEP_INTERVAL: Duration = Duration::from_millis(50);

    /// Drops all entries whose deadline has passed; their waiting tasks wake
    /// with a timeout error. Called periodically by the sweeper task.
    pub fn expire(&self, now: Instant) {
        self.id_map.retain(|_, entry| entry.deadline > now);
    }

    /// Spawns the periodic expiry sweeper for this waiter. The task exits
    /// once the waiter is dropped.
    pub fn spawn_sweeper(self: &std::sync::Arc<Self>) {
        let weak = std::sync::Arc::downgrade(self);
        tokio::spawn(async move {
            let pending = metrics::gauge!("ruapc_waiter_pending");
            let mut interval = tokio::time::interval(Self::SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(waiter) = weak.upgrade() else {
                    break;
                };
                waiter.expire(Instant::now());
                #[allow(clippy::cast_precision_loss)]
                pending.set(waiter.pending_count() as f64);
            }
        });
    }
}

impl std::fmt::Debug for Waiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Waiter").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_waiter() {
        let msg_waiter = Arc::new(Waiter::default());

        let (msgid, rx) = msg_waiter.alloc(std::time::Duration::from_secs(30));
        assert_eq!(msgid, 0);

        let handle = {
            let msg_waiter = Arc::clone(&msg_waiter);
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let mut msg = Message::default();
                msg.meta.method = "dummy".into();
                msg_waiter.post(msgid, msg);
            })
        };

        let (msg, write_buf) = rx.recv().await.unwrap();
        assert_eq!(msg.meta.method, "dummy");
        assert!(write_buf.is_none());
        handle.await.unwrap();

        let (msgid, rx) = msg_waiter.alloc(std::time::Duration::from_secs(30));
        drop(rx); // drop the receiver to trigger the cleaner's Drop
        assert!(msg_waiter.id_map.get(&msgid).is_none());
    }

    #[tokio::test]
    async fn test_waiter_post_nonexistent_msgid() {
        let waiter = Waiter::default();
        // Posting to a non-existent msgid should log a warning and not panic.
        waiter.post(42, Message::default());
    }

    #[tokio::test]
    async fn test_waiter_store_write_buffer() {
        use crate::Devices;

        let devices = Arc::new(Devices::default());
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices).build();

        let waiter = Waiter::default();
        let (msgid, rx) = waiter.alloc(std::time::Duration::from_secs(30));

        // Store a write buffer while the request is pending.
        let mut buf = pool.allocate(1024 * 1024).unwrap();
        buf[..5].copy_from_slice(b"hello");
        buf.set_len(5);
        assert!(waiter.store_write_buffer(msgid, buf));

        // Post the response.
        waiter.post(msgid, Message::default());

        // Receive should include the write buffer.
        let (_msg, write_buf) = rx.recv().await.unwrap();
        let write_buf = write_buf.expect("expected write buffer");
        assert_eq!(&write_buf[..], b"hello");
    }

    #[tokio::test]
    async fn test_waiter_store_write_buffer_after_timeout() {
        use crate::Devices;

        let devices = Arc::new(Devices::default());
        let pool = ruapc_bufpool::BufferPoolBuilder::new(devices).build();

        let waiter = Waiter::default();
        let (msgid, rx) = waiter.alloc(std::time::Duration::from_secs(30));

        // Drop receiver (simulates timeout cleanup).
        drop(rx);

        // Store should fail since the entry was removed.
        let buf = pool.allocate(1024 * 1024).unwrap();
        assert!(!waiter.store_write_buffer(msgid, buf));
    }

    #[tokio::test]
    async fn test_waiter_fail_connection() {
        let waiter = Waiter::default();
        let conn_id = super::next_conn_id();

        // Bound entry: fails eagerly with the given error.
        let (msgid_bound, rx_bound) = waiter.alloc(std::time::Duration::from_secs(30));
        waiter.bind_connection(msgid_bound, conn_id);

        // Unbound entry: must survive the connection failure.
        let (msgid_other, _rx_other) = waiter.alloc(std::time::Duration::from_secs(30));

        // Entry bound to a different connection: must also survive.
        let (msgid_diff, _rx_diff) = waiter.alloc(std::time::Duration::from_secs(30));
        waiter.bind_connection(msgid_diff, super::next_conn_id());

        let err = crate::Error::new(
            crate::ErrorKind::ConnectionClosed,
            "connection closed".into(),
        );
        waiter.fail_connection(conn_id, &err);

        let result = rx_bound.recv().await;
        assert_eq!(result.unwrap_err().kind, crate::ErrorKind::ConnectionClosed);
        assert!(!waiter.contains_message_id(msgid_bound));
        assert!(waiter.contains_message_id(msgid_other));
        assert!(waiter.contains_message_id(msgid_diff));
    }

    #[test]
    fn test_bind_connection_missing_entry_is_noop() {
        let waiter = Waiter::default();
        // Binding a non-existent msgid must not panic or create entries.
        waiter.bind_connection(12345, 1);
        assert!(!waiter.contains_message_id(12345));
    }

    #[test]
    fn test_waiter_debug_format() {
        let waiter = Waiter::default();
        let debug = format!("{waiter:?}");
        assert!(debug.contains("Waiter"));
    }
}
