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
        self.locks
            .entry(addr)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
            .lock_owned()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
