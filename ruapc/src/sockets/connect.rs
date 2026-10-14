use std::{net::SocketAddr, sync::Arc};

/// Per-address connection-attempt locks. Entries intentionally live for the
/// pool lifetime: the address set is bounded by peers the pool has contacted,
/// and retaining them avoids lock-removal races between arriving waiters.
#[derive(Debug, Default)]
pub(crate) struct ConnectLocks {
    locks: dashmap::DashMap<SocketAddr, Arc<tokio::sync::Mutex<()>>>,
}

impl ConnectLocks {
    pub(crate) async fn lock(&self, addr: SocketAddr) -> tokio::sync::OwnedMutexGuard<()> {
        // Drop DashMap's shard guard before awaiting the per-address mutex.
        // Otherwise another lookup can synchronously block the runtime.
        let lock = self
            .locks
            .entry(addr)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        lock.lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waiting_for_address_does_not_hold_map_shard() {
        use std::{future::Future, task::Poll};

        let locks = ConnectLocks::default();
        let addr = "127.0.0.1:10001".parse().unwrap();
        let held = locks.lock(addr).await;
        let waiting = locks.lock(addr);
        tokio::pin!(waiting);
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;

        assert!(matches!(
            locks.locks.try_get(&addr),
            dashmap::try_result::TryResult::Present(_)
        ));
        drop(held);
        waiting.await;
    }

    #[tokio::test]
    async fn locks_only_serialize_the_same_address() {
        let locks = Arc::new(ConnectLocks::default());
        let first_addr = "127.0.0.1:10001".parse().unwrap();
        let second_addr = "127.0.0.1:10002".parse().unwrap();
        let first = locks.lock(first_addr).await;

        let same = {
            let locks = locks.clone();
            tokio::spawn(async move { locks.lock(first_addr).await })
        };
        tokio::task::yield_now().await;
        assert!(!same.is_finished());

        tokio::time::timeout(std::time::Duration::from_secs(1), locks.lock(second_addr))
            .await
            .expect("another address must not be blocked");
        drop(first);
        same.await.unwrap();
    }
}
