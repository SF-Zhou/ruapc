use std::{
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{Endpoint, Error, ErrorKind, Result};

/// Complete endpoint keys keep overlapping address groups independent.
/// Entries live for the pool lifetime to avoid removing a lock with waiters.
#[derive(Debug, Default)]
pub(crate) struct ConnectGroups {
    groups: dashmap::DashMap<Endpoint, Arc<Mutex<ConnectState>>>,
}

impl ConnectGroups {
    pub(crate) async fn lock(
        &self,
        endpoint: &Endpoint,
        deadline: Option<Instant>,
    ) -> Result<OwnedMutexGuard<ConnectState>> {
        let group = self
            .groups
            .entry(endpoint.clone())
            .or_insert_with(|| Arc::new(Mutex::new(ConnectState::new(endpoint))))
            .clone();
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), group.lock_owned())
                .await
                .map_err(|_| {
                    Error::new(ErrorKind::Timeout, "endpoint connect lock timed out".into())
                }),
            None => Ok(group.lock_owned().await),
        }
    }
}

#[derive(Debug)]
struct AddressState {
    addr: SocketAddr,
    failures: u32,
    retry_after: Option<Instant>,
}

/// Serializes establishment for one group, including address selection and
/// cooldown. A successful socket is published before releasing this guard.
#[derive(Debug)]
pub(crate) struct ConnectState {
    addresses: Vec<AddressState>,
    cursor: usize,
}

impl ConnectState {
    fn new(endpoint: &Endpoint) -> Self {
        Self {
            addresses: endpoint
                .addrs()
                .iter()
                .map(|&addr| AddressState {
                    addr,
                    failures: 0,
                    retry_after: None,
                })
                .collect(),
            cursor: 0,
        }
    }

    /// Tries every address at most once. An unresponsive handshake gets only
    /// its share of the remaining budget, leaving time for other addresses.
    /// Without a caller deadline, each address gets five seconds. Cooldown
    /// ranks candidates but never sleeps a caller when all addresses failed.
    pub(crate) async fn connect<T, F, Fut>(
        &mut self,
        deadline: Option<Instant>,
        mut dial: F,
    ) -> Result<T>
    where
        F: FnMut(SocketAddr) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let now = Instant::now();
        let len = self.addresses.len();
        let mut candidates: Vec<_> = (0..len).collect();
        candidates.sort_by_key(|&index| {
            let address = &self.addresses[index];
            (
                address.retry_after.is_some_and(|after| after > now),
                address.failures,
                (index + len - self.cursor) % len,
            )
        });
        let mut last_error = None;
        for (attempt, index) in candidates.into_iter().enumerate() {
            let now = Instant::now();
            let attempt_deadline = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(now);
                    if remaining.is_zero() {
                        return Err(Error::new(
                            ErrorKind::Timeout,
                            "connection deadline expired".into(),
                        ));
                    }
                    let divisor = u32::try_from(len - attempt).unwrap_or(u32::MAX);
                    now + remaining / divisor
                }
                None => now + Duration::from_secs(5),
            };
            let address = &mut self.addresses[index];
            let result = tokio::time::timeout_at(attempt_deadline.into(), dial(address.addr))
                .await
                .unwrap_or_else(|_| {
                    Err(Error::new(
                        ErrorKind::Timeout,
                        format!("connection to {} timed out", address.addr),
                    ))
                });
            self.cursor = (index + 1) % len;
            match result {
                Ok(socket) => {
                    address.failures = 0;
                    address.retry_after = None;
                    return Ok(socket);
                }
                Err(error) => {
                    address.failures = address.failures.saturating_add(1);
                    let backoff = Duration::from_millis(
                        (50_u64 << address.failures.saturating_sub(1).min(7)).min(5_000),
                    );
                    address.retry_after = Some(Instant::now() + backoff);
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.expect("endpoints have at least one address"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn normalized_groups_share_locks_but_overlapping_groups_do_not() {
        let groups = ConnectGroups::default();
        let group: Endpoint = "tcp://127.0.0.1:10001,127.0.0.1:10002".parse().unwrap();
        let reversed: Endpoint = "tcp://127.0.0.1:10002,127.0.0.1:10001".parse().unwrap();
        let subset: Endpoint = "tcp://127.0.0.1:10001".parse().unwrap();
        let first = groups.lock(&group, None).await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(10);
        assert!(groups.lock(&reversed, Some(deadline)).await.is_err());
        groups.lock(&subset, None).await.unwrap();
        drop(first);
        groups.lock(&reversed, None).await.unwrap();
    }

    #[tokio::test]
    async fn stalled_address_leaves_budget_for_an_alternative() {
        let endpoint: Endpoint = "ws://127.0.0.1:10001,127.0.0.1:10002".parse().unwrap();
        let mut group = ConnectState::new(&endpoint);
        let deadline = Instant::now() + Duration::from_millis(100);
        let socket = group
            .connect(Some(deadline), |addr| async move {
                if addr.port() == 10001 {
                    std::future::pending::<()>().await;
                }
                Ok(addr)
            })
            .await
            .unwrap();
        assert_eq!(socket.port(), 10002);
        assert!(Instant::now() < deadline);
        let next = group
            .connect(None, |addr| async move { Ok(addr) })
            .await
            .unwrap();
        assert_eq!(next.port(), 10002);
    }
}
