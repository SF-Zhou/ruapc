//! Shared reconciliation for first-use coverage, periodic upkeep and recovery.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::MutexGuard;

use super::placement::{self, PathPreference, ReconcileAction};
use super::{PeerState, RdmaSocketPool, Stripe};
use crate::{Result, State};

#[derive(Clone, Copy)]
pub(super) struct RetryBackoff {
    pub(super) failures: u32,
    pub(super) retry_at: Instant,
}

pub(super) fn backoff_delay(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(8);
    Duration::from_millis((100u64 << exponent).min(30_000))
}

impl RdmaSocketPool {
    /// First use and detected failures share one queued reconciliation pass,
    /// independent of the periodic timer and the foreground request's deadline.
    pub(super) fn schedule_reconcile(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        if self.config.peers.min_connections_per_remote_nic == 0 && !peer.needs_reconnect() {
            return;
        }
        let Some(reconcile) = peer.try_reconcile() else {
            return;
        };
        let peer = peer.clone();
        let state = Arc::downgrade(state);
        let _ = self.task_supervisor.handle().try_spawn(async move {
            // Capture the guard before spawning so rejection or cancellation
            // of an unpolled future also releases the single-flight marker.
            let _reconcile = reconcile;
            let Some(state) = state.upgrade() else {
                return;
            };
            let Some(pool) = state.socket_pool.rdma_pool() else {
                return;
            };
            // Wait instead of dropping this one-shot trigger when an acquire
            // or periodic pass currently owns the peer. Recheck under lock.
            let connect = peer.connect.lock().await;
            pool.reconcile_peer(&peer, &state, connect).await;
        });
    }

    /// Periodic passes skip groups already reconciling or acquiring a
    /// connection. Once started, they yield between actions like first use.
    pub(super) async fn replenish_peer(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        let Ok(connect) = peer.connect.try_lock() else {
            return;
        };
        let Some(_reconcile) = peer.try_reconcile() else {
            return;
        };
        self.reconcile_peer(peer, state, connect).await;
    }

    /// Fills eligible remote-NIC coverage, then the total stripe target.
    /// The initial lock serializes discovery. Each action takes the lock
    /// afresh, holding it from the healthy snapshot through publication;
    /// queued foreground acquisitions run between those actions.
    async fn reconcile_peer(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        connect: MutexGuard<'_, ()>,
    ) {
        let endpoint = &peer.endpoint;
        const PEER_BACKOFF_KEY: &str = "";
        if !self.reconnect_ready(peer, PEER_BACKOFF_KEY) {
            return;
        }
        let result: Result<()> = async {
            // Fresh bootstrap budgets are independent of the request that
            // scheduled this task; the supervisor can cancel the whole pass.
            let plan = self.prepare_connect_plan(peer, state, None).await?;
            // Discovery can itself wait for RPCs. Yield its lock before the
            // first handshake as well, so queued acquisitions get a turn.
            drop(connect);
            let max_connections = self.config.peers.preconnect_max_per_peer as usize;
            let min_per_remote = self.config.peers.min_connections_per_remote_nic as usize;
            let avoided_remote_nics = HashSet::new();
            let remote_names = placement::eligible_remote_nics(
                &plan.candidates,
                self.config.path.subnet_policy,
            );
            let mut blocked: HashSet<String> = remote_names
                .iter()
                .filter(|remote| !self.reconnect_ready(peer, remote))
                .cloned()
                .collect();

            // Replan against actual admitted sockets after every attempt.
            // A failed or incompatible NIC must not reserve slots that keep
            // later eligible NICs out. Bound work even if admitted QPs fail
            // immediately; the periodic pass retries them under backoff.
            for _ in 0..max_connections.saturating_add(remote_names.len()) {
                // Tokio's FIFO mutex hands queued acquisitions the next turn
                // when the previous action releases its guard. Re-read all
                // placement state after reacquiring; foreground work may have
                // filled coverage or replaced a potential victim meanwhile.
                let _connect = peer.connect.lock().await;
                let existing: Vec<Stripe> = peer
                    .active_snapshot()
                    .into_iter()
                    .filter(|stripe| stripe.socket.state.is_ok())
                    .collect();
                let healthy_remotes: Vec<String> = existing
                    .iter()
                    .map(|stripe| stripe.socket.path.remote.device.clone())
                    .collect();
                let Some(action) = placement::next_connection_action(
                    &remote_names,
                    &healthy_remotes,
                    &blocked,
                    min_per_remote,
                    self.config.peers.connections_per_peer as usize,
                    max_connections,
                ) else {
                    break;
                };
                let (remote, victim) = match &action {
                    ReconcileAction::ConnectCoverage(remote) => (Some(remote), None),
                    ReconcileAction::ReplaceCoverage { remote, victim } => {
                        let victim = existing
                            .iter()
                            .find(|stripe| stripe.socket.path.remote.device == *victim)
                            .expect("coverage plan refers to an existing stripe");
                        (Some(remote), Some(victim.socket.clone()))
                    }
                    ReconcileAction::ConnectTarget => (None, None),
                };
                let preference = PathPreference {
                    remote_device: remote.map(String::as_str),
                    avoided_remote_nics: &avoided_remote_nics,
                };
                match self
                    .connect_with_failover(peer, state, &plan, preference, &existing)
                    .await
                {
                    Ok(established) => {
                        if let Some(remote) = remote {
                            self.clear_reconnect_failure(peer, remote);
                        }
                        if let Some(victim) = victim {
                            self.admit_replacing(peer, &victim, established);
                        } else {
                            self.admit_established(peer, established);
                        }
                    }
                    Err(err) => {
                        let Some(remote) = remote else {
                            return Err(err);
                        };
                        self.record_reconnect_failure(peer, remote);
                        blocked.insert(remote.clone());
                        tracing::debug!(peer = %endpoint, remote_device = %remote, %err, "RDMA coverage connection failed");
                    }
                }
            }
            peer.finish_recovery(&remote_names, &self.config.peers);
            Ok(())
        }
        .await;
        match result {
            Err(err) => {
                self.record_reconnect_failure(peer, PEER_BACKOFF_KEY);
                tracing::debug!("replenishing RDMA stripes to {endpoint} failed: {err}");
            }
            Ok(()) => {
                self.clear_reconnect_failure(peer, PEER_BACKOFF_KEY);
            }
        }
    }

    fn reconnect_ready(&self, peer: &PeerState, remote: &str) -> bool {
        peer.meta
            .lock()
            .unwrap()
            .backoff
            .get(remote)
            .is_none_or(|state| Instant::now() >= state.retry_at)
    }

    fn record_reconnect_failure(&self, peer: &PeerState, remote: &str) {
        let mut meta = peer.meta.lock().unwrap();
        let failures = meta
            .backoff
            .get(remote)
            .map_or(1, |state| state.failures.saturating_add(1));
        let base = backoff_delay(failures);
        let jitter =
            Duration::from_millis(self.pseudo_random() % (base.as_millis() as u64 / 2 + 1));
        meta.backoff.insert(
            remote.to_owned(),
            RetryBackoff {
                failures,
                retry_at: Instant::now() + base + jitter,
            },
        );
    }

    fn clear_reconnect_failure(&self, peer: &PeerState, remote: &str) {
        peer.meta.lock().unwrap().backoff.remove(remote);
    }
}

#[cfg(test)]
mod tests;
