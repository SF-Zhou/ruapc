use foldhash::fast::RandomState;
use std::sync::atomic::AtomicU64;
use tokio::sync::oneshot;

use crate::{Buffer, Message, Receiver};

/// The response sent through the waiter channel, including the RPC response
/// message and an optional write buffer received from the server.
pub type WaiterResponse = (Message, Option<Buffer>);

/// Entry stored in the waiter's id_map for each pending request.
struct WaiterEntry {
    /// Channel to send the response through.
    sender: oneshot::Sender<WaiterResponse>,
    /// Write buffer stored by `MemoryService::tcp_push` / `rdma_pull` during
    /// the request. Sent together with the response when `post` is called.
    write_buffer: Option<Buffer>,
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
    /// clean up the waiter entry when dropped.
    pub fn alloc(&self) -> (u64, Receiver<'_>) {
        let msgid = self.index.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.id_map.insert(
            msgid,
            WaiterEntry {
                sender: tx,
                write_buffer: None,
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
            let _ = entry.sender.send((result, entry.write_buffer));
        } else {
            tracing::warn!("Waiter post failed for msgid: {}", msgid);
        }
    }

    /// Checks if a message ID is currently being waited on.
    pub fn contains_message_id(&self, msgid: u64) -> bool {
        self.id_map.contains_key(&msgid)
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

        let (msgid, rx) = msg_waiter.alloc();
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

        let (msgid, rx) = msg_waiter.alloc();
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
        use crate::{BufferPool, Devices};

        let devices = Arc::new(Devices::default());
        let pool = BufferPool::new(devices, 4096, 4096, 0);

        let waiter = Waiter::default();
        let (msgid, rx) = waiter.alloc();

        // Store a write buffer while the request is pending.
        let mut buf = pool.allocate().unwrap();
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
        use crate::{BufferPool, Devices};

        let devices = Arc::new(Devices::default());
        let pool = BufferPool::new(devices, 4096, 4096, 0);

        let waiter = Waiter::default();
        let (msgid, rx) = waiter.alloc();

        // Drop receiver (simulates timeout cleanup).
        drop(rx);

        // Store should fail since the entry was removed.
        let buf = pool.allocate().unwrap();
        assert!(!waiter.store_write_buffer(msgid, buf));
    }

    #[test]
    fn test_waiter_debug_format() {
        let waiter = Waiter::default();
        let debug = format!("{waiter:?}");
        assert!(debug.contains("Waiter"));
    }
}
