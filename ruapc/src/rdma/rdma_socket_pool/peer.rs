//! State shared by one complete bootstrap address group: stripes and metadata.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use foldhash::fast::RandomState;

use crate::{Context, Endpoint, State};

use super::connect::CachedPeerAdvertisement;
use super::placement::next_connection_action;
use super::reconcile::RetryBackoff;
use super::{PathKey, RdmaSocket, Stripe};
use crate::rdma::RdmaPeerPoolConfig;

/// Connection stripes of one peer, guarded by a *synchronous* lock:
/// critical sections must stay short and must never await. Structural
/// mutations of `active` happen under the peer's `connect` lock
/// (admission) or in failure detection/pruning; `draining` is additionally
/// touched by the drain timers.
#[derive(Default)]
pub(super) struct PeerStripes {
    pub(super) active: Vec<Stripe>,
    pub(super) draining: Vec<Stripe>,
    /// Health detection keeps this peer desired until its targets recover,
    /// even when no user requests arrive and the idle TTL has elapsed.
    reconnect_requested: bool,
}

#[derive(Default)]
pub(super) struct PeerMeta {
    pub(super) last_used: Option<Instant>,
    pub(super) device_cache: Option<CachedPeerAdvertisement>,
    pub(super) backoff: HashMap<String, RetryBackoff, RandomState>,
    pub(super) blacklist: HashMap<PathKey, Instant, RandomState>,
}

pub(crate) struct PeerState {
    pub(super) endpoint: Endpoint,
    pub(super) connect: tokio::sync::Mutex<()>,
    pub(super) stripes: RwLock<PeerStripes>,
    pub(super) meta: Mutex<PeerMeta>,
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
        Self {
            endpoint,
            connect: tokio::sync::Mutex::new(()),
            stripes: RwLock::new(PeerStripes::default()),
            meta: Mutex::new(PeerMeta::default()),
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

    pub(super) fn touch(&self, now: Instant) {
        self.meta.lock().unwrap().last_used = Some(now);
    }

    pub(super) fn contains_active(&self, socket: &Arc<RdmaSocket>) -> bool {
        self.stripes
            .read()
            .unwrap()
            .active
            .iter()
            .any(|stripe| Arc::ptr_eq(&stripe.socket, socket))
    }

    /// Close only the matching active socket. Identity validation, failure
    /// and recovery intent share the replacement lock, so a late probe can
    /// neither close a draining socket nor lose its request for replenishment.
    pub(super) fn fail_active(&self, socket: &Arc<RdmaSocket>) -> bool {
        let mut stripes = self.stripes.write().unwrap();
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
        true
    }

    /// Remember failed active connections before pruning them, including
    /// when foreground acquisition wins the race with a health probe.
    pub(super) fn prune_dead(&self, request_reconnect: bool) {
        let mut stripes = self.stripes.write().unwrap();
        let mut removed_active = false;
        stripes.active.retain(|stripe| {
            let healthy = stripe.socket.state.is_ok();
            removed_active |= !healthy;
            healthy
        });
        // Use the retain decision itself: the poller can fail a socket at
        // any time, so a separate earlier health scan could miss a removal.
        stripes.reconnect_requested |= request_reconnect && removed_active;
        stripes
            .draining
            .retain(|stripe| stripe.socket.state.is_ok());
    }

    pub(super) fn needs_reconnect(&self) -> bool {
        self.stripes.read().unwrap().reconnect_requested
    }

    pub(super) fn is_desired(&self, now: Instant, idle_ttl: Duration) -> bool {
        let stripes = self.stripes.read().unwrap();
        !stripes.active.is_empty()
            || stripes.reconnect_requested
            || self
                .meta
                .lock()
                .unwrap()
                .last_used
                .is_some_and(|last_used| now.saturating_duration_since(last_used) < idle_ttl)
    }

    /// Complete recovery only against a current snapshot under the same
    /// lock used to request it. Backoff does not make a missing NIC healthy.
    pub(super) fn finish_recovery(&self, remote_names: &[String], config: &RdmaPeerPoolConfig) {
        let mut stripes = self.stripes.write().unwrap();
        if !stripes.reconnect_requested {
            return;
        }
        let healthy_remotes: Vec<String> = stripes
            .active
            .iter()
            .filter(|stripe| stripe.socket.state.is_ok())
            .map(|stripe| stripe.socket.path.remote.device.clone())
            .collect();
        if next_connection_action(
            remote_names,
            &healthy_remotes,
            &HashSet::new(),
            config.min_connections_per_remote_nic as usize,
            config.connections_per_peer as usize,
            config.preconnect_max_per_peer as usize,
        )
        .is_none()
        {
            stripes.reconnect_requested = false;
        }
    }

    /// Control RPCs use the same complete address group over TCP. Entry-point
    /// failure can select another listener without changing this peer's QPs.
    pub(super) fn bootstrap_context(&self, state: &Arc<State>) -> Context {
        let endpoint = Endpoint::tcp(self.endpoint.addrs().iter().copied())
            .expect("RDMA peer endpoint is nonempty");
        Context::create_with_state_and_endpoint(state, endpoint)
    }

    pub(super) fn active_snapshot(&self) -> Vec<Stripe> {
        self.stripes.read().unwrap().active.clone()
    }

    pub(super) fn all_sockets(&self) -> Vec<Arc<super::RdmaSocket>> {
        let stripes = self.stripes.read().unwrap();
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
            Endpoint::new(
                Transport::RDMA,
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

        peer.stripes.write().unwrap().reconnect_requested = true;
        assert!(peer.is_desired(now, idle_ttl));
        // Pruning an already empty peer must preserve its recovery intent.
        peer.prune_dead(false);
        assert!(peer.needs_reconnect());
        assert!(peer.is_desired(now, idle_ttl));
    }

    #[test]
    fn disabled_coverage_does_not_complete_an_unfilled_total_target() {
        let peer = peer();
        peer.stripes.write().unwrap().reconnect_requested = true;
        let config = RdmaPeerPoolConfig {
            min_connections_per_remote_nic: 0,
            ..Default::default()
        };
        peer.finish_recovery(&[], &config);
        assert!(peer.needs_reconnect());
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
