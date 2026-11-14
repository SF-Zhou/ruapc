//! State shared by one complete bootstrap address group: stripes and metadata.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use foldhash::fast::RandomState;

use crate::{Context, Endpoint, State};

use super::connect::CachedPeerAdvertisement;
use super::placement::next_connection_action;
use super::reconcile::RetryBackoff;
use super::{PathKey, RdmaSocket, Stripe};
use crate::rdma::RdmaPeerPoolConfig;

/// Connection stripes of one peer, guarded by a *synchronous* mutex that
/// only writers and bookkeeping take: critical sections must stay short and
/// must never await. Requests read the lock-free [`PeerState::active_view`]
/// snapshot, which [`StripesGuard`] republishes after every mutable access.
/// Structural mutations of `active` happen under the peer's `connect` lock
/// (admission) or in failure detection/pruning; `draining` is additionally
/// touched by the drain timers.
#[derive(Default)]
pub(super) struct PeerStripes {
    pub(super) active: Vec<Stripe>,
    pub(super) draining: Vec<Stripe>,
    /// Health detection keeps this peer desired until its targets recover,
    /// even when no user requests arrive and the idle TTL has elapsed.
    reconnect_requested: bool,
    /// A reconciliation pass ended below its targets. Probes, maintenance and
    /// slow-path acquisitions retry it under backoff, but unlike
    /// `reconnect_requested` it does not keep an idle peer alive: a target
    /// that stays unreachable must not pin the peer and its retries forever.
    fill_pending: bool,
}

impl PeerStripes {
    /// A reconciliation pass ended below a connection target.
    pub(super) fn fill_pending(&self) -> bool {
        self.fill_pending
    }

    /// Health detection requested recovery that has not completed.
    pub(super) fn recovery_requested(&self) -> bool {
        self.reconnect_requested
    }
}

/// Exclusive access to a peer's stripes. Mutable access republishes the
/// request-path snapshot before the mutex is released, so a published
/// snapshot never lags a completed mutation.
pub(super) struct StripesGuard<'a> {
    view: &'a ArcSwap<Vec<Stripe>>,
    inner: MutexGuard<'a, PeerStripes>,
    dirty: bool,
}

impl std::ops::Deref for StripesGuard<'_> {
    type Target = PeerStripes;
    fn deref(&self) -> &PeerStripes {
        &self.inner
    }
}

impl std::ops::DerefMut for StripesGuard<'_> {
    fn deref_mut(&mut self) -> &mut PeerStripes {
        self.dirty = true;
        &mut self.inner
    }
}

impl Drop for StripesGuard<'_> {
    fn drop(&mut self) {
        if self.dirty {
            self.view.store(Arc::new(self.inner.active.clone()));
        }
    }
}

/// Millisecond-granularity last-use time, written on every request. The
/// atomic avoids a mutex per request, and storing only when the millisecond
/// changes keeps concurrent requesters from bouncing its cache line.
pub(super) struct LastUsed {
    epoch: Instant,
    /// Signed milliseconds from `epoch`; `NEVER` before the first use.
    millis: AtomicI64,
}

impl LastUsed {
    const NEVER: i64 = i64::MIN;

    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            millis: AtomicI64::new(Self::NEVER),
        }
    }

    pub(super) fn set(&self, at: Instant) {
        let millis = match at.checked_duration_since(self.epoch) {
            Some(after) => i64::try_from(after.as_millis()).unwrap_or(i64::MAX),
            // Round away from `epoch` so the recorded time never exceeds `at`.
            None => -i64::try_from(self.epoch.duration_since(at).as_micros().div_ceil(1000))
                .unwrap_or(i64::MAX),
        };
        if self.millis.load(Ordering::Relaxed) != millis {
            self.millis.store(millis, Ordering::Relaxed);
        }
    }

    pub(super) fn get(&self) -> Option<Instant> {
        let millis = self.millis.load(Ordering::Relaxed);
        if millis == Self::NEVER {
            return None;
        }
        let offset = Duration::from_millis(millis.unsigned_abs());
        if millis >= 0 {
            self.epoch.checked_add(offset)
        } else {
            self.epoch.checked_sub(offset)
        }
    }
}

#[derive(Default)]
pub(super) struct PeerMeta {
    pub(super) device_cache: Option<CachedPeerAdvertisement>,
    pub(super) backoff: HashMap<String, RetryBackoff, RandomState>,
    pub(super) blacklist: HashMap<PathKey, Instant, RandomState>,
}

pub(crate) struct PeerState {
    pub(super) endpoint: Endpoint,
    /// Bootstrap endpoint over the stream transport selected by `endpoint`.
    pub(super) bootstrap_endpoint: Endpoint,
    pub(super) connect: tokio::sync::Mutex<()>,
    stripes: Mutex<PeerStripes>,
    /// Lock-free copy of `stripes.active` for the request path.
    active_view: ArcSwap<Vec<Stripe>>,
    pub(super) meta: Mutex<PeerMeta>,
    pub(super) last_used: LastUsed,
    /// One queued or running background reconciliation task for this group.
    reconcile_pending: AtomicBool,
}

/// Keeps a reconciliation task's claim until it finishes or is dropped.
/// Create this before spawning and move it into the future so rejection or
/// cancellation before the first poll also releases the claim.
#[must_use = "keep the guard alive for the entire reconciliation task"]
pub(super) struct ReconcileGuard {
    peer: Arc<PeerState>,
}

impl Drop for ReconcileGuard {
    fn drop(&mut self) {
        self.peer.reconcile_pending.store(false, Ordering::Release);
    }
}

impl PeerState {
    pub(super) fn new(endpoint: Endpoint) -> Self {
        let bootstrap_endpoint = endpoint
            .bootstrap_endpoint()
            .expect("RDMA peer endpoint declares a bootstrap transport");
        Self {
            endpoint,
            bootstrap_endpoint,
            connect: tokio::sync::Mutex::new(()),
            stripes: Mutex::new(PeerStripes::default()),
            active_view: ArcSwap::from_pointee(Vec::new()),
            meta: Mutex::new(PeerMeta::default()),
            last_used: LastUsed::new(),
            reconcile_pending: AtomicBool::new(false),
        }
    }

    /// Claims this group's background reconciliation slot. The connection mutex
    /// separately serializes actual handshakes with foreground acquisition.
    pub(super) fn try_reconcile(self: &Arc<Self>) -> Option<ReconcileGuard> {
        self.reconcile_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(ReconcileGuard { peer: self.clone() })
    }

    pub(super) fn lock_stripes(&self) -> StripesGuard<'_> {
        StripesGuard {
            view: &self.active_view,
            inner: self.stripes.lock().unwrap(),
            dirty: false,
        }
    }

    /// Lock-free snapshot of the selectable stripes.
    pub(super) fn active_view(&self) -> arc_swap::Guard<Arc<Vec<Stripe>>> {
        self.active_view.load()
    }

    pub(super) fn touch(&self, now: Instant) {
        self.last_used.set(now);
    }

    pub(super) fn contains_active(&self, socket: &Arc<RdmaSocket>) -> bool {
        self.lock_stripes()
            .active
            .iter()
            .any(|stripe| Arc::ptr_eq(&stripe.socket, socket))
    }

    /// Close only the matching active socket. Identity validation, failure
    /// and recovery intent share the replacement lock, so a late probe can
    /// neither close a draining socket nor lose its request for replenishment.
    pub(super) fn fail_active(&self, socket: &Arc<RdmaSocket>) -> bool {
        let mut stripes = self.lock_stripes();
        let Some(index) = stripes
            .active
            .iter()
            .position(|stripe| Arc::ptr_eq(&stripe.socket, socket))
        else {
            return false;
        };
        socket.set_error();
        stripes.active.remove(index);
        stripes.reconnect_requested = true;
        drop(stripes);
        self.blacklist_if_unverified(socket);
        true
    }

    /// Temporarily penalizes a NIC pair towards this peer.
    pub(super) fn blacklist(&self, path: &super::RdmaPathInfo) {
        self.meta.lock().unwrap().blacklist.insert(
            super::RdmaSocketPool::path_key(path),
            Instant::now() + super::RdmaSocketPool::PATH_BLACKLIST_TTL,
        );
    }

    /// A connection that failed before any successful completion most likely
    /// ran on an unreachable pair; avoid reconnecting it there right away.
    fn blacklist_if_unverified(&self, socket: &RdmaSocket) {
        if !socket.is_path_verified() {
            tracing::warn!(conn_id = socket.conn_id, path = ?socket.path,
                "RDMA connection failed before any successful completion; avoiding its NIC pair");
            self.blacklist(&socket.path);
        }
    }

    /// Remember failed active connections before pruning them, including
    /// when foreground acquisition wins the race with a health probe.
    pub(super) fn prune_dead(&self, request_reconnect: bool) {
        let mut stripes = self.lock_stripes();
        let mut removed_active = false;
        let mut unverified = Vec::new();
        stripes.active.retain(|stripe| {
            let healthy = stripe.socket.state.is_ok();
            removed_active |= !healthy;
            if !healthy && !stripe.socket.is_path_verified() {
                unverified.push(stripe.socket.clone());
            }
            healthy
        });
        // Use the retain decision itself: the poller can fail a socket at
        // any time, so a separate earlier health scan could miss a removal.
        stripes.reconnect_requested |= request_reconnect && removed_active;
        stripes
            .draining
            .retain(|stripe| stripe.socket.state.is_ok());
        drop(stripes);
        for socket in unverified {
            self.blacklist_if_unverified(&socket);
        }
    }

    pub(super) fn healthy_count(&self) -> usize {
        self.lock_stripes()
            .active
            .iter()
            .filter(|stripe| stripe.socket.state.is_ok())
            .count()
    }

    /// Whether connection work remains: health-detected recovery or an
    /// unfinished fill. Triggers retries; only the former pins the peer.
    pub(super) fn needs_connections(&self) -> bool {
        let stripes = self.lock_stripes();
        stripes.reconnect_requested || stripes.fill_pending
    }

    /// Whether a background connection fill is still pending. Unlike
    /// [`Self::needs_connections`] this does not include health-detected
    /// recovery, and it does not make the peer desired.
    #[cfg(test)]
    pub(super) fn needs_fill(&self) -> bool {
        self.lock_stripes().fill_pending()
    }

    pub(super) fn is_desired(&self, now: Instant, idle_ttl: Duration) -> bool {
        let stripes = self.lock_stripes();
        !stripes.active.is_empty()
            || stripes.reconnect_requested
            || self
                .last_used
                .get()
                .is_some_and(|last_used| now.saturating_duration_since(last_used) < idle_ttl)
    }

    /// Recompute connection intent from a current snapshot under the lock
    /// that publishes it. Any remaining action marks a pending fill, which
    /// probes, maintenance or a later slow-path acquisition retry. Reaching
    /// every target clears both the fill and health-detected recovery;
    /// backoff does not make a missing NIC healthy.
    pub(super) fn finish_recovery(&self, remote_names: &[String], config: &RdmaPeerPoolConfig) {
        let mut stripes = self.lock_stripes();
        let healthy_remotes: Vec<String> = stripes
            .active
            .iter()
            .filter(|stripe| stripe.socket.state.is_ok())
            .map(|stripe| stripe.socket.path.remote.device.clone())
            .collect();
        let pending = next_connection_action(
            remote_names,
            &healthy_remotes,
            &HashSet::new(),
            config.min_connections_per_remote_nic as usize,
            config.connections_per_peer as usize,
            config.preconnect_max_per_peer as usize,
        )
        .is_some();
        stripes.fill_pending = pending;
        if !pending {
            stripes.reconnect_requested = false;
        }
    }

    /// Marks an unfinished fill after a failed reconciliation pass. Retries
    /// follow, but the mark does not keep an idle peer from being evicted.
    pub(super) fn request_fill(&self) {
        self.lock_stripes().fill_pending = true;
    }

    /// Control RPCs use the same complete address group over the bootstrap
    /// transport selected when this peer was created. Entry-point failure can
    /// select another listener without changing this peer's QPs.
    pub(super) fn bootstrap_context(&self, state: &Arc<State>) -> Context {
        Context::create_with_state_and_endpoint(state, self.bootstrap_endpoint.clone())
    }

    pub(super) fn active_snapshot(&self) -> Vec<Stripe> {
        self.active_view().to_vec()
    }

    pub(super) fn all_sockets(&self) -> Vec<Arc<super::RdmaSocket>> {
        let stripes = self.lock_stripes();
        stripes
            .active
            .iter()
            .chain(&stripes.draining)
            .map(|stripe| stripe.socket.clone())
            .collect()
    }
}

impl std::fmt::Debug for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerState")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TaskSupervisor, Transport};

    fn peer() -> Arc<PeerState> {
        Arc::new(PeerState::new(
            Endpoint::rdma(
                Transport::TCP,
                [
                    "127.0.0.1:41001".parse().unwrap(),
                    "127.0.0.1:41002".parse().unwrap(),
                ],
            )
            .unwrap(),
        ))
    }

    #[test]
    fn idle_peer_with_pending_recovery_remains_desired() {
        let peer = peer();
        let now = Instant::now();
        let idle_ttl = Duration::from_secs(60);
        peer.touch(now - idle_ttl);
        assert!(!peer.is_desired(now, idle_ttl));

        peer.lock_stripes().reconnect_requested = true;
        assert!(peer.is_desired(now, idle_ttl));
        // Pruning an already empty peer must preserve its recovery intent.
        peer.prune_dead(false);
        assert!(peer.needs_connections());
        assert!(peer.is_desired(now, idle_ttl));
    }

    #[test]
    fn disabled_coverage_does_not_complete_an_unfilled_total_target() {
        let peer = peer();
        peer.lock_stripes().reconnect_requested = true;
        let config = RdmaPeerPoolConfig {
            min_connections_per_remote_nic: 0,
            ..Default::default()
        };
        peer.finish_recovery(&[], &config);
        assert!(peer.needs_connections());
    }

    #[test]
    fn below_target_pass_retries_without_pinning_an_idle_peer() {
        let peer = peer();
        let now = Instant::now();
        let idle_ttl = Duration::from_secs(60);
        peer.touch(now - idle_ttl);
        assert!(!peer.needs_connections());
        // A reconciliation pass that ends below the total target must leave
        // the fill pending, so probes or maintenance retry it.
        let config = RdmaPeerPoolConfig {
            connections_per_peer: 2,
            min_connections_per_remote_nic: 0,
            ..Default::default()
        };
        peer.finish_recovery(&[], &config);
        assert!(peer.needs_connections());
        assert!(peer.needs_fill());
        // An unreachable target must not keep an idle peer alive forever.
        assert!(!peer.is_desired(now, idle_ttl));
        peer.request_fill();
        assert!(peer.needs_fill());
        assert!(!peer.is_desired(now, idle_ttl));

        // Health-detected recovery still pins it until the targets recover.
        peer.lock_stripes().reconnect_requested = true;
        peer.finish_recovery(&[], &config);
        assert!(peer.is_desired(now, idle_ttl));

        // A pass that reaches every target clears both intents.
        let config = RdmaPeerPoolConfig {
            connections_per_peer: 0,
            min_connections_per_remote_nic: 0,
            ..Default::default()
        };
        peer.finish_recovery(&[], &config);
        assert!(!peer.needs_connections());
        assert!(!peer.needs_fill());
    }

    #[test]
    fn concurrent_reconcile_requests_share_one_claim() {
        let peer = peer();
        let barrier = std::sync::Barrier::new(8);
        let claims = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let peer = peer.clone();
                let barrier = &barrier;
                let claims = &claims;
                scope.spawn(move || {
                    barrier.wait();
                    let guard = peer.try_reconcile();
                    if guard.is_some() {
                        claims.fetch_add(1, Ordering::Relaxed);
                    }
                    // Keep the winning claim until every concurrent caller
                    // has tried; dropping it permits a later pass.
                    barrier.wait();
                    drop(guard);
                });
            }
        });
        assert_eq!(claims.load(Ordering::Relaxed), 1);
        let guard = peer.try_reconcile().expect("completed pass releases claim");
        assert!(peer.try_reconcile().is_none());
        drop(guard);
        assert!(peer.try_reconcile().is_some());
    }

    #[tokio::test]
    async fn rejected_reconcile_task_releases_claim_without_polling() {
        let peer = peer();
        let supervisor = TaskSupervisor::create();
        supervisor.stop();
        supervisor.all_stopped().await;

        let guard = peer.try_reconcile().unwrap();
        let task = supervisor.handle().try_spawn(async move {
            let _guard = guard;
            panic!("stopped supervisor must reject this future before polling");
        });
        assert!(task.is_none());
        assert!(peer.try_reconcile().is_some());
    }

    #[tokio::test]
    async fn cancelling_reconcile_releases_claim_and_connection_lock() {
        let peer = peer();
        let supervisor = TaskSupervisor::create();
        let guard = peer.try_reconcile().unwrap();
        let task_peer = peer.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = supervisor
            .handle()
            .try_spawn(async move {
                let _guard = guard;
                let _connect = task_peer.connect.lock().await;
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            })
            .unwrap();
        ready.await.unwrap();
        assert!(peer.try_reconcile().is_none());
        assert!(peer.connect.try_lock().is_err());

        supervisor.stop();
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("reconciliation task must stop")
            .unwrap();
        supervisor.all_stopped().await;
        assert!(peer.try_reconcile().is_some());
        assert!(peer.connect.try_lock().is_ok());
    }
}
