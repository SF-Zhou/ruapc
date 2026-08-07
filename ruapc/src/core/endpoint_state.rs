use std::{
    collections::{HashSet, VecDeque},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::{Endpoint, Socket, sockets::SocketHealth};

const MAX_FAILED_CONNECTION_HISTORY: usize = 1024;

#[derive(Debug)]
struct EndpointStatus {
    failures: u32,
    retry_after_ms: u64,
    health: Option<(u64, SocketHealth)>,
    connecting: usize,
    connection_generation: u64,
    failed_connections: HashSet<u64>,
    failed_connection_order: VecDeque<u64>,
}

/// Atomic selection view of one endpoint's current state.
#[cfg(test)]
pub(crate) struct EndpointSnapshot {
    pub(crate) failures: u32,
    pub(crate) connected: bool,
}

/// Health, backoff, and in-progress connection state for one endpoint.
#[derive(Debug)]
pub(crate) struct EndpointState {
    endpoint: Endpoint,
    status: Mutex<EndpointStatus>,
    failures: AtomicU32,
    retry_after_ms: AtomicU64,
    connected: AtomicBool,
    aggregate: AtomicBool,
    conn_id: AtomicU64,
}

impl EndpointState {
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            status: Mutex::new(EndpointStatus {
                failures: 0,
                retry_after_ms: 0,
                health: None,
                connecting: 0,
                connection_generation: 0,
                failed_connections: HashSet::new(),
                failed_connection_order: VecDeque::new(),
            }),
            failures: AtomicU32::new(0),
            retry_after_ms: AtomicU64::new(0),
            connected: AtomicBool::new(false),
            aggregate: AtomicBool::new(false),
            conn_id: AtomicU64::new(0),
        }
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    pub(crate) fn begin_connect(self: &Arc<Self>) -> ConnectActivity {
        let mut status = self.status.lock().unwrap();
        status.connecting += 1;
        let generation = status.connection_generation;
        drop(status);
        ConnectActivity {
            state: self.clone(),
            generation,
        }
    }

    pub(crate) fn try_begin_preconnect(self: &Arc<Self>) -> Option<ConnectActivity> {
        if self.connected.load(Ordering::Relaxed)
            || self.retry_after_ms.load(Ordering::Relaxed) > monotonic_ms()
        {
            return None;
        }
        let mut status = self.status.lock().unwrap();
        if status.connecting != 0
            || status.retry_after_ms > monotonic_ms()
            || status
                .health
                .as_ref()
                .is_some_and(|(_, health)| health.is_connected())
        {
            return None;
        }
        status.connecting = 1;
        let generation = status.connection_generation;
        drop(status);
        Some(ConnectActivity {
            state: self.clone(),
            generation,
        })
    }

    pub(crate) fn record_connection_failure(&self, conn_id: u64) {
        let mut status = self.status.lock().unwrap();
        let Some((current, health)) = status.health.as_ref() else {
            return;
        };
        let current = *current;
        let aggregate = health.is_aggregate();
        let connected = health.is_connected();
        if !aggregate && current != conn_id {
            return;
        }
        if aggregate && connected {
            self.connected.store(true, Ordering::Relaxed);
            return;
        }
        if !status.failed_connections.insert(conn_id) {
            return;
        }
        status.failed_connection_order.push_back(conn_id);
        if status.failed_connection_order.len() > MAX_FAILED_CONNECTION_HISTORY
            && let Some(expired) = status.failed_connection_order.pop_front()
        {
            status.failed_connections.remove(&expired);
        }
        if !aggregate {
            status.health = None;
        }
        status.connection_generation = status.connection_generation.wrapping_add(1);
        increment_failure(&mut status);
        self.publish_status(&status);
        self.connected.store(false, Ordering::Relaxed);
    }

    pub(crate) fn record_request_success(&self, conn_id: u64) {
        if self.failures.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut status = self.status.lock().unwrap();
        if status.health.as_ref().is_some_and(|(current, health)| {
            health.is_connected() && (*current == conn_id || health.is_aggregate())
        }) {
            status.failures = 0;
            status.retry_after_ms = 0;
            self.publish_status(&status);
        }
    }

    pub(crate) fn is_likely_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub(crate) fn is_current(&self, socket: &Socket) -> bool {
        self.is_likely_connected()
            && (self.aggregate.load(Ordering::Relaxed)
                || socket
                    .conn_id()
                    .is_some_and(|id| id == self.conn_id.load(Ordering::Relaxed)))
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> EndpointSnapshot {
        let status = self.status.lock().unwrap();
        EndpointSnapshot {
            failures: status.failures,
            connected: status
                .health
                .as_ref()
                .is_some_and(|(_, health)| health.is_connected()),
        }
    }

    pub(crate) fn selection_rank(&self) -> (bool, bool, u32, u64) {
        let retry_after_ms = self.retry_after_ms.load(Ordering::Relaxed);
        (
            retry_after_ms > monotonic_ms(),
            !self.connected.load(Ordering::Relaxed),
            self.failures.load(Ordering::Relaxed),
            retry_after_ms,
        )
    }

    fn publish_status(&self, status: &EndpointStatus) {
        self.failures.store(status.failures, Ordering::Relaxed);
        self.retry_after_ms
            .store(status.retry_after_ms, Ordering::Relaxed);
    }

    fn publish_connection(&self, conn_id: u64, health: &SocketHealth) {
        self.conn_id.store(conn_id, Ordering::Relaxed);
        self.aggregate
            .store(health.is_aggregate(), Ordering::Relaxed);
        self.connected.store(true, Ordering::Relaxed);
    }
}

/// One foreground or background connection attempt. Its starting generation
/// prevents a delayed failure from penalizing a newer connection.
pub(crate) struct ConnectActivity {
    state: Arc<EndpointState>,
    generation: u64,
}

impl ConnectActivity {
    pub(crate) fn record_connection(&self, socket: &Socket) {
        let (Some(conn_id), Some(health)) = (socket.conn_id(), socket.health()) else {
            return;
        };
        let mut status = self.state.status.lock().unwrap();
        if !health.is_connected() {
            if status.connection_generation == self.generation {
                status.health = None;
                status.connection_generation = status.connection_generation.wrapping_add(1);
                increment_failure(&mut status);
                self.state.publish_status(&status);
                self.state.connected.store(false, Ordering::Relaxed);
            }
            return;
        }
        if status.connection_generation != self.generation
            && status
                .health
                .as_ref()
                .is_some_and(|(_, health)| health.is_connected())
        {
            return;
        }
        if status
            .health
            .as_ref()
            .is_some_and(|(_, current)| current.same_scope(&health) && current.is_connected())
        {
            self.state.publish_connection(conn_id, &health);
            return;
        }
        status.health = Some((conn_id, health));
        status.connection_generation = status.connection_generation.wrapping_add(1);
        let (_, health) = status.health.as_ref().unwrap();
        self.state.publish_connection(conn_id, health);
    }

    pub(crate) fn record_failure(&self) {
        let mut status = self.state.status.lock().unwrap();
        if status.connection_generation != self.generation {
            return;
        }
        if status
            .health
            .as_ref()
            .is_some_and(|(_, health)| health.is_connected())
        {
            return;
        }
        status.health = None;
        increment_failure(&mut status);
        self.state.publish_status(&status);
        self.state.connected.store(false, Ordering::Relaxed);
    }
}

impl Drop for ConnectActivity {
    fn drop(&mut self) {
        self.state.status.lock().unwrap().connecting -= 1;
    }
}

fn increment_failure(status: &mut EndpointStatus) {
    status.failures = status.failures.saturating_add(1);
    let shift = status.failures.saturating_sub(1).min(8);
    let delay_ms = 100u64.checked_shl(shift).unwrap_or(u64::MAX).min(30_000);
    status.retry_after_ms = monotonic_ms().saturating_add(delay_ms);
}

pub(crate) fn monotonic_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn socket() -> (Socket, tokio::sync::mpsc::Receiver<bytes::Bytes>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        (Socket::TCP(crate::tcp::TcpSocket::new(sender)), receiver)
    }

    #[test]
    fn stale_attempt_failure_does_not_penalize_replacement() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        let stale = state.begin_connect();
        let replacement = state.begin_connect();
        let (socket, receiver) = socket();
        replacement.record_connection(&socket);
        drop(receiver);

        stale.record_failure();

        assert_eq!(state.snapshot().failures, 0);
    }

    #[test]
    fn stale_success_does_not_replace_live_connection() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        let stale = state.begin_connect();
        let current = state.begin_connect();
        let (current_socket, _current_receiver) = socket();
        let current_id = current_socket.conn_id().unwrap();
        current.record_connection(&current_socket);
        let (stale_socket, _stale_receiver) = socket();

        stale.record_connection(&stale_socket);
        state.record_connection_failure(current_id);

        assert_eq!(state.snapshot().failures, 1);
    }

    #[test]
    fn dead_success_does_not_clear_backoff() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        let failed = state.begin_connect();
        failed.record_failure();
        let attempt = state.begin_connect();
        let (socket, receiver) = socket();
        drop(receiver);

        attempt.record_connection(&socket);

        assert_eq!(state.snapshot().failures, 2);
    }

    #[test]
    fn connection_only_clears_backoff_after_response() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        state.begin_connect().record_failure();
        let (socket, _receiver) = socket();
        let conn_id = socket.conn_id().unwrap();

        state.begin_connect().record_connection(&socket);
        assert_eq!(state.snapshot().failures, 1);
        state.record_request_success(conn_id);
        assert_eq!(state.snapshot().failures, 0);
    }

    #[test]
    fn selection_health_is_refreshed_after_observed_failure() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        let (socket, receiver) = socket();
        let conn_id = socket.conn_id().unwrap();
        state.begin_connect().record_connection(&socket);
        drop(receiver);

        assert!(!state.selection_rank().1);
        state.record_connection_failure(conn_id);
        assert!(state.selection_rank().1);
    }
}
