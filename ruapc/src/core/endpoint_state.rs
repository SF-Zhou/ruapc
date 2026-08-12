use std::{
    collections::{HashSet, VecDeque},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use crate::{Endpoint, Socket, State, sockets::SocketHealth};

const MAX_FAILED_CONNECTION_HISTORY: usize = 1024;

/// Mutex-protected source of truth for one endpoint's health. Every field
/// the hot path needs is mirrored into the atomics of [`EndpointState`] by
/// [`EndpointState::publish`].
#[derive(Debug, Default)]
struct EndpointStatus {
    /// Consecutive connect/send failures since the last confirmed success.
    failures: u32,
    /// Monotonic-ms instant until which new attempts should back off.
    retry_after_ms: u64,
    /// The connection (or aggregate peer) currently representing this
    /// endpoint, keyed by its conn_id.
    health: Option<(u64, SocketHealth)>,
    /// Number of in-flight [`ConnectActivity`] spans; suppresses
    /// concurrent preconnects.
    connecting: usize,
    /// Bumped whenever `health` changes, so a delayed observation from an
    /// older connection attempt can detect that it is stale.
    connection_generation: u64,
    /// Recently failed conn_ids, so one dead connection is counted once.
    failed_connections: HashSet<u64>,
    failed_connection_order: VecDeque<u64>,
}

impl EndpointStatus {
    /// The tracked connection, if it is still alive.
    fn live_connection(&self) -> Option<(u64, &SocketHealth)> {
        self.health
            .as_ref()
            .and_then(|(conn_id, health)| health.is_connected().then_some((*conn_id, health)))
    }

    fn is_live(&self) -> bool {
        self.live_connection().is_some()
    }

    fn bump_generation(&mut self) {
        self.connection_generation = self.connection_generation.wrapping_add(1);
    }

    /// Counts one failure and extends the exponential backoff window
    /// (100ms doubling up to 30s).
    fn apply_failure(&mut self) {
        self.failures = self.failures.saturating_add(1);
        let shift = self.failures.saturating_sub(1).min(8);
        let delay_ms = 100u64.checked_shl(shift).unwrap_or(u64::MAX).min(30_000);
        self.retry_after_ms = monotonic_ms().saturating_add(delay_ms);
    }

    /// Remembers `conn_id` as failed; returns `false` when this failure
    /// was already counted (bounded dedup history).
    fn note_failed_connection(&mut self, conn_id: u64) -> bool {
        if !self.failed_connections.insert(conn_id) {
            return false;
        }
        self.failed_connection_order.push_back(conn_id);
        if self.failed_connection_order.len() > MAX_FAILED_CONNECTION_HISTORY
            && let Some(expired) = self.failed_connection_order.pop_front()
        {
            self.failed_connections.remove(&expired);
        }
        true
    }
}

/// Atomic selection view of one endpoint's current state.
#[cfg(test)]
pub(crate) struct EndpointSnapshot {
    pub(crate) failures: u32,
    pub(crate) connected: bool,
}

/// Lock-free ordering key for endpoint selection; smaller is better. The
/// derived `Ord` compares fields top to bottom.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SelectionRank {
    /// Endpoint is inside its retry-backoff window.
    pub(crate) cooling: bool,
    /// No live connection is known.
    pub(crate) disconnected: bool,
    /// Consecutive failures.
    pub(crate) failures: u32,
    /// When the backoff expires; orders cooling endpoints by soonest.
    pub(crate) retry_after_ms: u64,
}

/// Health, backoff, and in-progress connection state for one endpoint.
///
/// The [`Mutex<EndpointStatus>`] is the source of truth; the atomics are a
/// read-only mirror for lock-free selection, re-derived as a whole by
/// [`publish`](Self::publish) after every mutation.
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
            status: Mutex::default(),
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

    /// Starts a spanning connection attempt (foreground acquire). While
    /// any activity is alive, preconnects are suppressed; the activity's
    /// starting generation shields newer connections from its delayed
    /// failure report.
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

    /// Starts a background connection attempt, unless the endpoint is
    /// already connected, cooling down, or being connected.
    pub(crate) fn try_begin_preconnect(self: &Arc<Self>) -> Option<ConnectActivity> {
        if self.connected.load(Ordering::Relaxed)
            || self.retry_after_ms.load(Ordering::Relaxed) > monotonic_ms()
        {
            return None;
        }
        let mut status = self.status.lock().unwrap();
        if status.connecting != 0 || status.retry_after_ms > monotonic_ms() || status.is_live() {
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

    /// Records that an already-established socket serves this endpoint
    /// (e.g. it was acquired from the pool without a connect attempt).
    pub(crate) fn record_observed_connection(&self, socket: &Socket) {
        self.record_connection_guarded(socket, None);
    }

    /// Marks `conn_id` as failed. Counted once per connection; ignored for
    /// connections this endpoint no longer tracks, or when an aggregate
    /// peer (RDMA) still has live stripes.
    pub(crate) fn record_connection_failure(&self, conn_id: u64) {
        let mut status = self.status.lock().unwrap();
        let Some((current, health)) = status.health.as_ref() else {
            return;
        };
        let (current, aggregate, connected) =
            (*current, health.is_aggregate(), health.is_connected());
        if !aggregate && current != conn_id {
            return;
        }
        if aggregate && connected {
            // One stripe died but the peer stays reachable: refresh the
            // mirror (connected may have been cleared) without penalty.
            self.publish(&status);
            return;
        }
        if !status.note_failed_connection(conn_id) {
            return;
        }
        if !aggregate {
            status.health = None;
        }
        status.bump_generation();
        status.apply_failure();
        self.publish(&status);
    }

    /// A response arrived over `conn_id`: the connection provably works,
    /// so clear failures and backoff.
    pub(crate) fn record_request_success(&self, conn_id: u64) {
        if self.failures.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut status = self.status.lock().unwrap();
        let confirmed = status
            .live_connection()
            .is_some_and(|(current, health)| current == conn_id || health.is_aggregate());
        if confirmed {
            status.failures = 0;
            status.retry_after_ms = 0;
            self.publish(&status);
        }
    }

    pub(crate) fn is_likely_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Whether `socket` is the connection this endpoint currently tracks.
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
            connected: status.is_live(),
        }
    }

    pub(crate) fn selection_rank(&self) -> SelectionRank {
        let retry_after_ms = self.retry_after_ms.load(Ordering::Relaxed);
        SelectionRank {
            cooling: retry_after_ms > monotonic_ms(),
            disconnected: !self.connected.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            retry_after_ms,
        }
    }

    /// Re-derives the whole lock-free mirror from the source of truth.
    /// Call before releasing the lock after any mutation.
    fn publish(&self, status: &EndpointStatus) {
        self.failures.store(status.failures, Ordering::Relaxed);
        self.retry_after_ms
            .store(status.retry_after_ms, Ordering::Relaxed);
        match status.live_connection() {
            Some((conn_id, health)) => {
                self.conn_id.store(conn_id, Ordering::Relaxed);
                self.aggregate
                    .store(health.is_aggregate(), Ordering::Relaxed);
                self.connected.store(true, Ordering::Relaxed);
            }
            None => self.connected.store(false, Ordering::Relaxed),
        }
    }

    /// Records an established (or already-dead) socket for this endpoint.
    /// `attempt_generation` is `Some` for observations coming from a
    /// spanning [`ConnectActivity`]; a stale generation must not displace
    /// a live replacement connection.
    fn record_connection_guarded(&self, socket: &Socket, attempt_generation: Option<u64>) {
        let (Some(conn_id), Some(health)) = (socket.conn_id(), socket.health()) else {
            return;
        };
        let mut status = self.status.lock().unwrap();
        let stale =
            attempt_generation.is_some_and(|generation| generation != status.connection_generation);
        if !health.is_connected() {
            // The socket died before the observation landed: count it as a
            // failure unless a newer connection superseded this attempt.
            if !stale {
                status.health = None;
                status.bump_generation();
                status.apply_failure();
                self.publish(&status);
            }
            return;
        }
        if stale && status.is_live() {
            return;
        }
        // Replacing a connection invalidates older in-flight attempts;
        // re-observing the same connection scope must not.
        let same_scope = status
            .live_connection()
            .is_some_and(|(_, current)| current.same_scope(&health));
        status.health = Some((conn_id, health));
        if !same_scope {
            status.bump_generation();
        }
        self.publish(&status);
    }

    /// Records a failed connection attempt. A live connection outranks the
    /// failure; a stale generation means a newer attempt already resolved.
    fn record_failure_guarded(&self, attempt_generation: u64) {
        let mut status = self.status.lock().unwrap();
        if attempt_generation != status.connection_generation {
            return;
        }
        if status.is_live() {
            return;
        }
        status.health = None;
        status.apply_failure();
        self.publish(&status);
    }
}

/// One foreground or background connection attempt. Its starting generation
/// prevents a delayed failure from penalizing a newer connection; holding it
/// suppresses concurrent preconnects.
pub(crate) struct ConnectActivity {
    state: Arc<EndpointState>,
    generation: u64,
}

impl ConnectActivity {
    pub(crate) fn record_connection(&self, socket: &Socket) {
        self.state
            .record_connection_guarded(socket, Some(self.generation));
    }

    pub(crate) fn record_failure(&self) {
        self.state.record_failure_guarded(self.generation);
    }
}

impl Drop for ConnectActivity {
    fn drop(&mut self) {
        self.state.status.lock().unwrap().connecting -= 1;
    }
}

pub(crate) fn monotonic_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// A set of equivalent server endpoints with shared selection state.
#[derive(Debug)]
pub(crate) struct EndpointSet {
    endpoints: Vec<Endpoint>,
    states: OnceLock<Vec<Arc<EndpointState>>>,
    cursor: AtomicUsize,
}

impl EndpointSet {
    /// Creates an endpoint set. Empty sets are permitted but every request
    /// through them fails with `InvalidArgument`.
    #[must_use]
    pub(crate) fn new(endpoints: Vec<Endpoint>) -> Self {
        let mut seen = HashSet::with_capacity(endpoints.len());
        let endpoints = endpoints
            .into_iter()
            .filter(|endpoint| seen.insert(*endpoint))
            .collect();
        Self {
            endpoints,
            states: OnceLock::new(),
            cursor: AtomicUsize::new(0),
        }
    }

    /// Returns all endpoints ordered by current usability. Existing healthy
    /// connections win, then connectable endpoints with fewer failures;
    /// round-robin order breaks ties. Cooling addresses remain at the end so
    /// a fully degraded set can still be probed.
    pub(crate) fn candidates(&self, state: &State) -> Vec<Arc<EndpointState>> {
        let states = self.states.get_or_init(|| {
            self.endpoints
                .iter()
                .map(|endpoint| state.endpoint_state(*endpoint))
                .collect()
        });
        let len = states.len();
        if len == 0 {
            return Vec::new();
        }
        let base = self.cursor.fetch_add(1, Ordering::Relaxed) % len;
        let mut candidates: Vec<_> = states
            .iter()
            .enumerate()
            .map(|(index, endpoint_state)| {
                let round_robin = (index + len - base) % len;
                (
                    (endpoint_state.selection_rank(), round_robin),
                    endpoint_state.clone(),
                )
            })
            .collect();
        candidates.sort_unstable_by_key(|(rank, _)| *rank);
        candidates.into_iter().map(|(_, state)| state).collect()
    }
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

        assert!(!state.selection_rank().disconnected);
        state.record_connection_failure(conn_id);
        assert!(state.selection_rank().disconnected);
    }

    #[tokio::test]
    async fn endpoint_set_deduplicates_and_tracks_transports_separately() {
        let ctx = crate::Context::create(&crate::SocketPoolConfig::default()).unwrap();
        let first: std::net::SocketAddr = "127.0.0.1:10001".parse().unwrap();
        let set = EndpointSet::new(vec![
            Endpoint::new(crate::Transport::TCP, first),
            Endpoint::new(crate::Transport::TCP, first),
            Endpoint::new(crate::Transport::WS, first),
        ]);
        let endpoints = set.candidates(&ctx.state);
        assert_eq!(endpoints.len(), 2);
        assert_ne!(
            endpoints[0].endpoint().transport(),
            endpoints[1].endpoint().transport()
        );
    }

    #[tokio::test]
    async fn endpoint_health_is_shared_across_sets() {
        let ctx = crate::Context::create(&crate::SocketPoolConfig::default()).unwrap();
        let first: std::net::SocketAddr = "127.0.0.1:10001".parse().unwrap();
        let second: std::net::SocketAddr = "127.0.0.1:10002".parse().unwrap();
        let set = EndpointSet::new(vec![Endpoint::tcp(first), Endpoint::tcp(second)]);
        let candidates = set.candidates(&ctx.state);
        let failed = candidates[0].endpoint();
        candidates[0].begin_connect().record_failure();

        let other_set = EndpointSet::new(vec![Endpoint::tcp(first), Endpoint::tcp(second)]);
        let reordered = other_set.candidates(&ctx.state);
        assert_ne!(reordered[0].endpoint(), failed);
        assert!(Arc::ptr_eq(
            &candidates[0],
            &ctx.state.endpoint_state(failed)
        ));
    }

    #[test]
    fn foreground_connection_suppresses_preconnect() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        let foreground = state.begin_connect();
        assert!(state.try_begin_preconnect().is_none());
        drop(foreground);
        assert!(state.try_begin_preconnect().is_some());
    }

    #[test]
    fn endpoint_health_observes_closed_send_channel() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        let (socket, receiver) = socket();
        state.begin_connect().record_connection(&socket);
        assert!(state.snapshot().connected);
        drop(receiver);
        assert!(!state.snapshot().connected);
    }

    #[test]
    fn connection_failure_is_counted_once_per_connection() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        let (socket, _receiver) = socket();
        let conn_id = socket.conn_id().unwrap();
        state.begin_connect().record_connection(&socket);

        state.record_connection_failure(conn_id);
        state.record_connection_failure(conn_id);

        assert_eq!(state.snapshot().failures, 1);
    }

    #[test]
    fn stale_failure_does_not_clear_replacement_connection() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        let (old_socket, _old_receiver) = socket();
        let old_conn_id = old_socket.conn_id().unwrap();
        state.begin_connect().record_connection(&old_socket);

        let (new_socket, _new_receiver) = socket();
        state.begin_connect().record_connection(&new_socket);
        state.record_connection_failure(old_conn_id);

        let snapshot = state.snapshot();
        assert!(snapshot.connected);
        assert_eq!(snapshot.failures, 0);
    }

    #[test]
    fn observed_connection_applies_without_an_activity_span() {
        let state = Arc::new(EndpointState::new(Endpoint::tcp(
            "127.0.0.1:10001".parse().unwrap(),
        )));
        state.begin_connect().record_failure();
        assert_eq!(state.snapshot().failures, 1);

        let (socket, _receiver) = socket();
        state.record_observed_connection(&socket);
        assert!(state.snapshot().connected);
        assert!(state.is_current(&socket));

        // A live connection outranks a later attempt's failure.
        state.begin_connect().record_failure();
        assert_eq!(state.snapshot().failures, 1);
        assert!(state.snapshot().connected);
    }
}
