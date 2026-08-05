use std::{
    future::Future,
    net::SocketAddr,
    sync::{Arc, LazyLock},
    time::Duration,
};

use foldhash::fast::RandomState;

use crate::error::{Error, ErrorKind, Result};

type ConnectLocks = dashmap::DashMap<SocketAddr, Arc<tokio::sync::Mutex<()>>, RandomState>;

/// Process-wide so independently-created pools and transport variants
/// cannot dial the same address concurrently.
static CONNECT_LOCKS: LazyLock<ConnectLocks> = LazyLock::new(ConnectLocks::default);

/// Serializes and time-bounds outgoing connection setup.
///
/// Two jobs:
///
/// - **Per-address serialization**: concurrent `acquire`s for the same
///   address take one process-wide lock, so exactly one of them dials while
///   the others wait. This includes independent socket pools and transport
///   variants. Different addresses connect independently — a stalled
///   connect to a dead peer must not block dialing healthy ones (a pool's
///   socket map is only locked for the brief lookup/insert, never across
///   the connect).
/// - **Connect timeout**: bounds connect + protocol handshake with
///   `connect_timeout_ms`, so an unreachable address (e.g. a downed
///   server NIC silently dropping SYNs) fails in bounded time instead of
///   the OS retransmission limit (minutes), letting multi-address clients
///   fail over quickly.
pub(crate) struct ConnectGate {
    timeout_ms: u64,
}

impl ConnectGate {
    pub(crate) fn new(timeout_ms: u64) -> Self {
        Self { timeout_ms }
    }

    /// Acquires the per-address connect lock. Hold the returned permit
    /// across the double-check, connect, and map insert.
    pub(crate) async fn lock(&self, addr: &SocketAddr) -> ConnectPermit {
        let lock = CONNECT_LOCKS
            .entry(*addr)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let guard = lock.clone().lock_owned().await;
        ConnectPermit {
            addr: *addr,
            lock,
            _guard: guard,
        }
    }

    /// Runs `fut` (connect + handshake) under the configured timeout.
    /// On expiry the future is dropped (tearing down any half-open
    /// connection) and an error of `kind` is returned.
    pub(crate) async fn with_timeout<T>(
        &self,
        addr: &SocketAddr,
        kind: ErrorKind,
        fut: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        if self.timeout_ms == 0 {
            return fut.await;
        }
        match tokio::time::timeout(Duration::from_millis(self.timeout_ms), fut).await {
            Ok(result) => result,
            Err(_) => Err(Error::new(
                kind,
                format!("connecting to {addr} timed out after {}ms", self.timeout_ms),
            )),
        }
    }
}

/// Exclusive permission to dial one address; see [`ConnectGate::lock`].
pub(crate) struct ConnectPermit {
    addr: SocketAddr,
    lock: Arc<tokio::sync::Mutex<()>>,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for ConnectPermit {
    fn drop(&mut self) {
        // Prune the map entry when no other task waits on it. The guard is
        // still held here (fields drop after this body), so a strong count
        // of exactly 3 — the map's clone, our `lock` field, and the owned
        // guard — proves there is no waiter; removing under the held lock
        // means late-comers atomically either reuse this entry or create a
        // fresh one, never dial concurrently.
        CONNECT_LOCKS.remove_if(&self.addr, |_, value| {
            Arc::ptr_eq(value, &self.lock) && Arc::strong_count(value) == 3
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gate_serializes_same_addr_and_prunes() {
        let gate = Arc::new(ConnectGate::new(0));
        let other_gate = Arc::new(ConnectGate::new(0));
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let permit = gate.lock(&addr).await;
        assert!(CONNECT_LOCKS.contains_key(&addr));

        // A second, independently-created gate for the same address must wait.
        let waiting = tokio::spawn({
            let gate = other_gate.clone();
            async move {
                let _permit = gate.lock(&addr).await;
            }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        // A different address is independent.
        let other: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let other_permit = gate.lock(&other).await;
        drop(other_permit);

        drop(permit);
        waiting.await.unwrap();
        // All permits released: this address is pruned from the global map.
        assert!(!CONNECT_LOCKS.contains_key(&addr));
    }

    #[tokio::test]
    async fn test_with_timeout_expires_and_zero_disables() {
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let gate = ConnectGate::new(20);
        let err = gate
            .with_timeout(&addr, ErrorKind::TcpConnectFailed, async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::TcpConnectFailed);
        assert!(err.msg.contains("timed out"), "msg={}", err.msg);

        // 0 disables the cap: the future runs to completion.
        let gate = ConnectGate::new(0);
        gate.with_timeout(&addr, ErrorKind::TcpConnectFailed, async { Ok(()) })
            .await
            .unwrap();
    }
}
