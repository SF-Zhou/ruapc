//! Background upkeep: the maintenance task, dead-path pruning, peer
//! replenishment, preconnect backoff and rebalancing.

use std::{
    collections::HashSet,
    sync::atomic::Ordering,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use super::super::RdmaSocket;
use super::super::path::RdmaNicInfo;
use super::placement::{self, PathPreference, ReconcileAction};
use super::{PeerState, RdmaSocketPool, Stripe, pseudo_random};
use crate::{Result, State};

#[derive(Clone, Copy)]
pub(super) struct RetryBackoff {
    pub(super) failures: u32,
    pub(super) retry_at: Instant,
}

pub(super) fn preconnect_backoff_delay(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(8);
    Duration::from_millis((100u64 << exponent).min(30_000))
}

impl RdmaSocketPool {
    const DESIRED_PEER_IDLE_TTL: Duration = Duration::from_secs(60);

    /// Starts the background maintenance task on first use (when an
    /// `Arc<State>` is available). The task holds only a `Weak<State>`;
    /// it exits when the pool's supervisor stops or the state is dropped.
    pub(super) fn ensure_maintenance_task(&self, state: &Arc<State>) {
        let interval_ms = self.config.maintenance_interval_ms;
        if interval_ms == 0 || self.maintenance_started.swap(true, Ordering::Relaxed) {
            return;
        }
        let weak_state = Arc::downgrade(state);
        let guard = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            let mut seq = 0usize;
            loop {
                // 0.5x..1.5x jitter decorrelates clients that would
                // otherwise all rebalance against the same (cached) server
                // load snapshot.
                seq = seq.wrapping_add(1);
                let sleep_ms = interval_ms / 2 + pseudo_random(seq) % interval_ms.max(1);
                tokio::select! {
                    () = guard.stopped() => break,
                    () = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
                }
                let Some(state) = weak_state.upgrade() else {
                    break;
                };
                let Some(pool) = state.socket_pool.rdma_pool() else {
                    break;
                };
                pool.run_maintenance(&state).await;
            }
        });
    }

    /// One maintenance tick: fail connections on dead local ports, prune
    /// dead stripes, and replenish peers below `connections_per_peer`.
    pub(crate) async fn run_maintenance(&self, state: &Arc<State>) {
        self.fail_paths_on_dead_ports().await;
        self.prune_dead().await;
        let now = Instant::now();
        let snapshot: Vec<Arc<PeerState>> =
            self.peers.iter().map(|peer| peer.value().clone()).collect();
        let mut peers = Vec::new();
        for peer in snapshot {
            let has_stripes = !peer.stripes.read().unwrap().active.is_empty();
            let recently_used = peer
                .meta
                .lock()
                .unwrap()
                .last_used
                .is_some_and(|last_used| {
                    now.saturating_duration_since(last_used) < Self::DESIRED_PEER_IDLE_TTL
                });
            if has_stripes || recently_used {
                peers.push(peer);
                continue;
            }
            let Ok(_connect) = peer.connect.try_lock() else {
                continue;
            };
            if Arc::strong_count(&peer) == 2 {
                self.peers.remove_if(&peer.addr, |_, current| {
                    Arc::ptr_eq(current, &peer)
                        && Arc::strong_count(current) == 2
                        && current.stripes.read().unwrap().active.is_empty()
                });
            }
        }
        if peers.is_empty() {
            return;
        }
        for peer in &peers {
            self.replenish_peer(peer, state).await;
        }
        let target =
            peers[self.rebalance_cursor.fetch_add(1, Ordering::Relaxed) % peers.len()].clone();
        self.rebalance_peer(&target, state).await;
    }

    /// Whether the local NIC of `nic` can no longer carry traffic
    /// (device gone or port not usable, per the refresher's snapshot).
    fn local_nic_dead(&self, nic: &RdmaNicInfo) -> bool {
        let Some(device) = self
            .devices
            .rdma_devices()
            .iter()
            .find(|d| d.info().name == nic.device)
        else {
            return true;
        };
        !device
            .info()
            .ports
            .iter()
            .any(|port| port.port_num == nic.port_num && port.is_usable())
    }

    /// Proactively fails connections whose local port went down; the port
    /// state comes from the periodic device refresher, so failures are
    /// detected even on idle connections that see no completion errors.
    async fn fail_paths_on_dead_ports(&self) {
        let fail_if_dead = |socket: &Arc<RdmaSocket>| {
            if socket.state.is_ok() && self.local_nic_dead(&socket.path.local) {
                tracing::warn!(
                    device = %socket.path.local.device,
                    qp = socket.queue_pair.qp_num(),
                    "local RDMA port down; failing connection"
                );
                socket.set_error();
            }
        };
        for peer in self.peers.iter() {
            for socket in peer.value().all_sockets() {
                fail_if_dead(&socket);
            }
        }
        let inbound: Vec<Arc<RdmaSocket>> = self
            .inbound
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for socket in &inbound {
            fail_if_dead(socket);
        }
    }

    /// Removes dead stripes (and fully dead peers) from the socket map and
    /// prunes released inbound connections.
    async fn prune_dead(&self) {
        for peer in self.peers.iter() {
            let mut stripes = peer.value().stripes.write().unwrap();
            stripes.active.retain(|s| s.socket.state.is_ok());
            stripes.draining.retain(|s| s.socket.state.is_ok());
        }
        self.inbound
            .lock()
            .unwrap()
            .retain(|conn| conn.strong_count() > 0);
    }

    /// Tops a desired peer up to `connections_per_peer` healthy stripes.
    async fn replenish_peer(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        let addr = &peer.addr;
        const PEER_BACKOFF_KEY: &str = "";
        if !self.preconnect_ready(peer, PEER_BACKOFF_KEY) {
            return;
        }
        let Ok(guard) = peer.connect.try_lock() else {
            // An acquire is already connecting to this peer; retry next tick.
            return;
        };
        let result: Result<()> = async {
            let plan = self.prepare_connect_plan(peer, state, None).await?;
            let mut existing = peer.active_snapshot();
            let max_connections = self.config.preconnect_max_per_peer.max(1) as usize;
            let min_per_remote = self.config.min_connections_per_remote_nic as usize;
            let avoided_remote_nics = HashSet::new();
            let mut remote_names = Vec::new();
            for candidate in &plan.candidates {
                if !remote_names.contains(&candidate.path.remote.device) {
                    remote_names.push(candidate.path.remote.device.clone());
                }
            }
            let healthy_remotes: Vec<String> = existing
                .iter()
                .filter(|stripe| stripe.socket.state.is_ok())
                .map(|stripe| stripe.socket.path.remote.device.clone())
                .collect();
            let coverage_blocked: HashSet<String> = remote_names
                .iter()
                .filter(|remote| !self.preconnect_ready(peer, remote))
                .cloned()
                .collect();
            let actions = placement::plan_connections(
                &remote_names,
                &healthy_remotes,
                &coverage_blocked,
                min_per_remote,
                self.config.connections_per_peer.max(1) as usize,
                max_connections,
            );
            for action in actions {
                match action {
                    ReconcileAction::ConnectCoverage(remote) => {
                    if !self.preconnect_ready(peer, &remote) {
                            continue;
                    }
                    let preference = PathPreference {
                        remote_device: Some(&remote),
                        avoided_remote_nics: &avoided_remote_nics,
                    };
                    match self
                        .connect_with_failover(peer, state, &plan, preference, &existing)
                        .await
                    {
                        Ok(established) => {
                            self.clear_preconnect_failure(peer, &remote);
                            let stripe = self.admit_established(peer, established);
                            existing.push(stripe);
                        }
                        Err(err) => {
                            self.record_preconnect_failure(peer, &remote);
                            tracing::debug!(peer = %addr, remote_device = %remote, %err, "RDMA coverage connection failed");
                        }
                    }
                    }
                    ReconcileAction::ConnectTarget => {
                        let established = self
                            .connect_with_failover(
                                peer,
                                state,
                                &plan,
                                PathPreference {
                                    remote_device: None,
                                    avoided_remote_nics: &avoided_remote_nics,
                                },
                                &existing,
                            )
                            .await?;
                        let stripe = self.admit_established(peer, established);
                        existing.push(stripe);
                    }
                }
            }
            // Coverage actions may fail after the pure plan is built. Fill
            // the normal target from the resulting actual state rather than
            // assuming every planned action succeeded. The iteration bound
            // is computed upfront so connections that die right after
            // admission cannot keep this loop alive; the next maintenance
            // tick retries them (under backoff).
            let healthy_count = |stripes: &[Stripe]| {
                stripes
                    .iter()
                    .filter(|stripe| stripe.socket.state.is_ok())
                    .count()
            };
            let target = self.config.connections_per_peer.max(1) as usize;
            for _ in 0..target.saturating_sub(healthy_count(&existing)) {
                if healthy_count(&existing) >= max_connections {
                    break;
                }
                let established = self
                    .connect_with_failover(
                        peer,
                        state,
                        &plan,
                        PathPreference {
                            remote_device: None,
                            avoided_remote_nics: &avoided_remote_nics,
                        },
                        &existing,
                    )
                    .await?;
                let stripe = self.admit_established(peer, established);
                existing.push(stripe);
            }
            Ok(())
        }
        .await;
        if let Err(err) = result {
            self.record_preconnect_failure(peer, PEER_BACKOFF_KEY);
            tracing::debug!("replenishing RDMA stripes to {addr} failed: {err}");
        } else {
            self.clear_preconnect_failure(peer, PEER_BACKOFF_KEY);
        }
        drop(guard);
    }

    fn preconnect_ready(&self, peer: &PeerState, remote: &str) -> bool {
        peer.meta
            .lock()
            .unwrap()
            .backoff
            .get(remote)
            .is_none_or(|state| Instant::now() >= state.retry_at)
    }

    fn record_preconnect_failure(&self, peer: &PeerState, remote: &str) {
        let mut meta = peer.meta.lock().unwrap();
        let failures = meta
            .backoff
            .get(remote)
            .map_or(1, |state| state.failures.saturating_add(1));
        let base = preconnect_backoff_delay(failures);
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

    fn clear_preconnect_failure(&self, peer: &PeerState, remote: &str) {
        peer.meta.lock().unwrap().backoff.remove(remote);
    }

    async fn rebalance_peer(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        let Ok(plan) = self.prepare_connect_plan(peer, state, None).await else {
            return;
        };
        let advertised_remotes: HashSet<&str> = plan
            .remote_info
            .devices
            .iter()
            .map(|device| device.name.as_str())
            .collect();
        let stripes: Vec<Stripe> = peer
            .active_snapshot()
            .into_iter()
            .filter(|stripe| stripe.socket.state.is_ok())
            .collect();
        if stripes.is_empty() {
            return;
        }

        let remote_info = &plan.remote_info;
        let views: Vec<placement::Candidate<'_>> = plan
            .candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| placement::Candidate {
                index,
                local_index: candidate.local_device_index,
                remote: &candidate.path.remote.device,
                same_zone: candidate.has_same_zone(),
                class: candidate.class,
                blacklisted: self.is_blacklisted(peer, candidate),
                local_load: 0,
                remote_load: 0,
            })
            .collect();
        let indices = placement::eligible_paths(
            &views,
            &placement::Selection {
                required_remote: None,
                avoided_remotes: &HashSet::new(),
            },
            false,
        );
        if indices.is_empty() {
            return;
        }

        const GONE: u64 = u64::MAX / 4;
        let remote_count = |name: &str| -> u64 {
            remote_info
                .devices
                .iter()
                .find(|device| device.name == name)
                .map_or(GONE, |device| u64::from(device.active_connections))
        };
        let local_count_by_index = |index: usize| -> u64 {
            self.conn_counts
                .get(index)
                .map_or(0, |count| count.load(Ordering::Acquire) as u64)
        };
        let local_count = |name: &str| -> u64 {
            self.devices
                .rdma_devices()
                .iter()
                .enumerate()
                .find(|(_, device)| device.info().name == name)
                .map_or(GONE, |(index, _)| local_count_by_index(index))
        };
        let stripe_views: Vec<placement::ExistingStripe<'_>> = stripes
            .iter()
            .enumerate()
            .map(|(index, stripe)| placement::ExistingStripe {
                index,
                local: &stripe.socket.path.local.device,
                remote: &stripe.socket.path.remote.device,
                local_load: local_count(&stripe.socket.path.local.device),
                remote_load: remote_count(&stripe.socket.path.remote.device),
                remote_healthy: stripes
                    .iter()
                    .filter(|other| {
                        other.socket.path.remote.device == stripe.socket.path.remote.device
                    })
                    .count(),
                remote_advertised: advertised_remotes
                    .contains(stripe.socket.path.remote.device.as_str()),
            })
            .collect();
        let replacements: Vec<placement::Replacement<'_>> = indices
            .iter()
            .map(|&index| {
                let candidate = &plan.candidates[index];
                placement::Replacement {
                    index,
                    local: &candidate.path.local.device,
                    remote: &candidate.path.remote.device,
                    local_load: local_count_by_index(candidate.local_device_index),
                    remote_load: remote_count(&candidate.path.remote.device),
                }
            })
            .collect();
        let Some((victim_index, best_index)) = placement::choose_rebalance(
            &stripe_views,
            &replacements,
            self.config.min_connections_per_remote_nic as usize,
            u64::from(self.config.rebalance_threshold.max(1)),
            !self.pseudo_random().is_multiple_of(2),
        ) else {
            return;
        };
        let victim = &stripes[victim_index];
        let best = &plan.candidates[best_index];

        let Ok(guard) = peer.connect.try_lock() else {
            return;
        };
        match self
            .connect_stripe(peer, state, &plan.acquire_ctx, best)
            .await
        {
            Ok(established) => {
                self.admit_replacing(peer, &victim.socket, established);
            }
            Err(err) => tracing::debug!(peer = %peer.addr, %err, "RDMA rebalance failed"),
        }
        drop(guard);
    }

    pub(super) fn drain_then_close(&self, peer: &Arc<PeerState>, socket: Arc<RdmaSocket>) {
        let drain = Duration::from_millis(self.config.drain_timeout_ms);
        let guard = self.task_supervisor.start_async_task();
        let peer = peer.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = guard.stopped() => {}
                () = tokio::time::sleep(drain) => {}
            }
            socket.set_error();
            peer.stripes
                .write()
                .unwrap()
                .draining
                .retain(|stripe| !Arc::ptr_eq(&stripe.socket, &socket));
        });
    }
}
