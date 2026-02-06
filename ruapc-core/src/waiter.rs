use foldhash::fast::RandomState;
use std::sync::atomic::AtomicU64;
use tokio::sync::oneshot;

use crate::{Message, Receiver};

/// Response waiter for correlating RPC requests with responses.
///
/// The `Waiter` provides a mechanism for asynchronous RPC calls to wait for
/// their responses. It assigns unique message IDs to requests and stores
/// channels that will receive the corresponding responses.
#[derive(Default)]
pub struct Waiter {
    index: AtomicU64,
    id_map: dashmap::DashMap<u64, oneshot::Sender<Message>, RandomState>,
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
        self.id_map.insert(msgid, tx);
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
    /// This method looks up the message ID and sends the response through
    /// the corresponding channel. If no waiter is found (e.g., because of
    /// timeout), a warning is logged.
    ///
    /// # Arguments
    ///
    /// * `msgid` - The message ID to match
    /// * `result` - The response message to send
    pub fn post(&self, msgid: u64, result: Message) {
        if let Some((_, tx)) = self.id_map.remove(&msgid) {
            let _ = tx.send(result);
        } else {
            tracing::warn!("Waiter post failed for msgid: {}", msgid);
        }
    }

    /// Checks if a message ID is currently being waited on.
    ///
    /// # Arguments
    ///
    /// * `msgid` - The message ID to check
    ///
    /// # Returns
    ///
    /// Returns true if the message ID exists in the waiter's map.
    pub fn contains_message_id(&self, msgid: u64) -> bool {
        self.id_map.contains_key(&msgid)
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

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.meta.method, "dummy");
        handle.await.unwrap();

        let (msgid, rx) = msg_waiter.alloc();
        drop(rx); // drop the receiver to trigger the cleaner's Drop
        assert!(msg_waiter.id_map.get(&msgid).is_none());
    }
}
