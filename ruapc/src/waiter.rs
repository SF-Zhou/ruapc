use crate::RecvMsg;
use foldhash::fast::RandomState;
use std::sync::atomic::AtomicU64;
use tokio::sync::oneshot;

#[derive(Default)]
pub struct Waiter {
    index: AtomicU64,
    id_map: dashmap::DashMap<u64, oneshot::Sender<RecvMsg>, RandomState>,
}

impl Waiter {
    pub fn alloc(&self) -> (u64, oneshot::Receiver<RecvMsg>) {
        let msg_id = self.index.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.id_map.insert(msg_id, tx);
        (msg_id, rx)
    }

    pub fn post(&self, msg_id: u64, result: RecvMsg) {
        if let Some((_, tx)) = self.id_map.remove(&msg_id) {
            let _ = tx.send(result);
        } else {
            tracing::warn!("Waiter post failed for msg_id: {}", msg_id);
        }
    }

    pub fn set_timeout(&self, msg_id: u64) {
        self.id_map.remove(&msg_id);
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
        let msg_waiter = Waiter::default();
        let msg_waiter = Arc::new(msg_waiter);

        let (msg_id, rx) = msg_waiter.alloc();
        assert_eq!(msg_id, 0);

        let handle = {
            let msg_waiter = Arc::clone(&msg_waiter);
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let mut msg = RecvMsg::default();
                msg.meta.method = "dummy".into();
                msg_waiter.post(msg_id, msg);
            })
        };

        let msg = rx.await.unwrap();
        assert_eq!(msg.meta.method, "dummy");
        handle.await.unwrap();
    }
}
