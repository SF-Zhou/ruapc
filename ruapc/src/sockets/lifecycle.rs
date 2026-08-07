use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use foldhash::fast::RandomState;
use tokio::sync::RwLock;

/// Identity and close-once state shared by stream transports.
#[derive(Debug)]
pub(crate) struct ConnectionLifecycle {
    conn_id: u64,
    closed: std::sync::atomic::AtomicBool,
}

impl ConnectionLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            conn_id: crate::task::next_conn_id(),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn conn_id(&self) -> u64 {
        self.conn_id
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn close_once(&self) -> bool {
        !self.closed.swap(true, std::sync::atomic::Ordering::SeqCst)
    }
}

/// Operations required by the client-side one-connection-per-address maps.
/// This is a static generic bound; RuaPC never dispatches through `dyn`.
pub(crate) trait PoolConnection: Clone {
    fn is_closed(&self) -> bool;
    fn same_connection(&self, other: &Self) -> bool;
}

#[derive(Debug)]
pub(crate) struct ConnectionMap<S> {
    inner: Arc<RwLock<HashMap<SocketAddr, S, RandomState>>>,
}

impl<S> Clone for ConnectionMap<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> Default for ConnectionMap<S> {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
        }
    }
}

impl<S: PoolConnection> ConnectionMap<S> {
    pub(crate) fn try_get_live(&self, addr: &SocketAddr) -> Option<S> {
        self.inner
            .try_read()
            .ok()?
            .get(addr)
            .filter(|socket| !socket.is_closed())
            .cloned()
    }

    pub(crate) async fn get_live(&self, addr: &SocketAddr) -> Option<S> {
        self.inner
            .read()
            .await
            .get(addr)
            .filter(|socket| !socket.is_closed())
            .cloned()
    }

    pub(crate) async fn publish(&self, addr: SocketAddr, socket: S) {
        self.inner.write().await.insert(addr, socket);
    }

    /// Creates and publishes a socket while holding the map lock. Use this
    /// when creation starts teardown tasks that may immediately evict it.
    pub(crate) async fn try_publish_with<E>(
        &self,
        addr: SocketAddr,
        create: impl FnOnce() -> Result<S, E>,
    ) -> Result<S, E> {
        let mut sockets = self.inner.write().await;
        let socket = create()?;
        sockets.insert(addr, socket.clone());
        Ok(socket)
    }

    pub(crate) async fn evict_if_current(&self, addr: &SocketAddr, socket: &S) {
        let mut sockets = self.inner.write().await;
        if sockets
            .get(addr)
            .is_some_and(|current| current.same_connection(socket))
        {
            sockets.remove(addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[derive(Clone)]
    struct TestConnection {
        id: u64,
        closed: Arc<AtomicBool>,
    }

    impl TestConnection {
        fn new(id: u64) -> Self {
            Self {
                id,
                closed: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl PoolConnection for TestConnection {
        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::Acquire)
        }

        fn same_connection(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }

    #[test]
    fn lifecycle_closes_once() {
        let lifecycle = ConnectionLifecycle::new();
        assert!(!lifecycle.is_closed());
        assert!(lifecycle.close_once());
        assert!(lifecycle.is_closed());
        assert!(!lifecycle.close_once());
    }

    #[tokio::test]
    async fn stale_eviction_preserves_replacement() {
        let map = ConnectionMap::default();
        let addr = "127.0.0.1:10001".parse().unwrap();
        let old = TestConnection::new(1);
        let replacement = TestConnection::new(2);
        map.publish(addr, old.clone()).await;
        map.publish(addr, replacement.clone()).await;

        map.evict_if_current(&addr, &old).await;

        assert_eq!(map.get_live(&addr).await.unwrap().id, replacement.id);
    }

    #[tokio::test]
    async fn closed_connections_are_not_returned() {
        let map = ConnectionMap::default();
        let addr = "127.0.0.1:10001".parse().unwrap();
        let connection = TestConnection::new(1);
        map.publish(addr, connection.clone()).await;
        connection.closed.store(true, Ordering::Release);

        assert!(map.get_live(&addr).await.is_none());
    }
}
