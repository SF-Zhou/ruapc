//! Periodic upkeep: dead-path pruning, idle-peer eviction and rebalancing.
//! Connection replenishment is shared with first use and recovery in `reconcile`.

use std::{
    collections::HashSet,
    sync::atomic::Ordering,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use super::super::RdmaSocket;
use super::super::path::RdmaNicInfo;
use super::placement;
use super::{PeerState, RdmaSocketPool, Stripe, pseudo_random};
use crate::State;

impl RdmaSocketPool {
    const DESIRED_PEER_IDLE_TTL: Duration = Duration::from_secs(60);

    /// Starts the background maintenance task on first use (when an
    /// `Arc<State>` is available). The task holds only a `Weak<State>`;
    /// it exits when the pool's supervisor stops or the state is dropped.
    pub(super) fn ensure_maintenance_task(&self, state: &Arc<State>) {
        let interval_ms = self.config.maintenance.interval_ms;
        if interval_ms == 0 || self.maintenance_started.swap(true, Ordering::Relaxed) {
            return;
        }
        let weak_state = Arc::downgrade(state);
        let _ = self.task_supervisor.handle().try_spawn(async move {
            let mut seq = 0usize;
            loop {
                // 0.5x..1.5x jitter decorrelates clients that would
                // otherwise all rebalance against the same (cached) server
                // load snapshot.
                seq = seq.wrapping_add(1);
                let sleep_ms = interval_ms / 2 + pseudo_random(seq) % interval_ms.max(1);
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
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
        self.fail_paths_on_dead_ports();
        self.prune_dead();
        let now = Instant::now();
        let snapshot: Vec<Arc<PeerState>> =
            self.peers.iter().map(|peer| peer.value().clone()).collect();
        let mut peers = Vec::new();
        for peer in snapshot {
            if peer.is_desired(now, Self::DESIRED_PEER_IDLE_TTL) {
                peers.push(peer);
                continue;
            }
            let Ok(_connect) = peer.connect.try_lock() else {
                continue;
            };
            if Arc::strong_count(&peer) == 2 {
                self.peers.remove_if(&peer.endpoint, |_, current| {
                    Arc::ptr_eq(current, &peer)
                        && Arc::strong_count(current) == 2
                        && !current.is_desired(now, Self::DESIRED_PEER_IDLE_TTL)
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
            .devices()
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
    fn fail_paths_on_dead_ports(&self) {
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

    /// Prunes dead stripes while preserving recovery intent, and removes
    /// released inbound connections.
    fn prune_dead(&self) {
        for peer in self.peers.iter() {
            peer.value().prune_dead(self.config.probe.interval_ms != 0);
        }
        self.inbound
            .lock()
            .unwrap()
            .retain(|conn| conn.strong_count() > 0);
    }

    async fn rebalance_peer(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        // Coverage replacement must not change per-NIC counts between this
        // snapshot and publication, or rebalancing could remove sole coverage.
        let Ok(_connect) = peer.connect.try_lock() else {
            return;
        };
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
                same_connectivity_domain: candidate.path.same_connectivity_domain,
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
                subnet_policy: self.config.path.subnet_policy,
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
                .devices()
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
            self.config.peers.min_connections_per_remote_nic as usize,
            u64::from(self.config.maintenance.rebalance_threshold),
            !self.pseudo_random().is_multiple_of(2),
        ) else {
            return;
        };
        let victim = &stripes[victim_index];
        let best = &plan.candidates[best_index];

        match self
            .connect_stripe(peer, state, &plan.bootstrap_ctx, best)
            .await
        {
            Ok(established) => {
                self.admit_replacing(peer, &victim.socket, established);
            }
            Err(err) => tracing::debug!(peer = %peer.endpoint, %err, "RDMA rebalance failed"),
        }
    }

    pub(super) fn drain_then_close(&self, peer: &Arc<PeerState>, socket: Arc<RdmaSocket>) {
        let drain = Duration::from_millis(self.config.maintenance.drain_timeout_ms);
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
