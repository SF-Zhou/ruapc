//! Periodic health checks on each published connection, in both directions.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};

use super::{PeerState, RdmaSocket, RdmaSocketPool, pseudo_random};
use crate::{Error, ErrorKind, Result, State, rdma::probe_connection};

/// Sample a 0.5x..1.5x delay without truncating a 1ms interval to zero or
/// overflowing millisecond arithmetic for large configured intervals.
fn probe_delay(interval: Duration, sample: u64) -> Duration {
    interval / 2 + interval / 1_000 * (sample % 1_000) as u32
}

/// Only consecutive timeouts count toward the configured threshold.
fn record_result(count: &mut u32, result: &Result<()>, threshold: u32) -> bool {
    if matches!(
        result,
        Err(Error {
            kind: ErrorKind::Timeout | ErrorKind::RdmaReadTimeout,
            ..
        })
    ) {
        *count = count.saturating_add(1);
        *count >= threshold
    } else {
        *count = 0;
        false
    }
}

impl RdmaSocketPool {
    pub(super) fn ensure_probe_task(&self, state: &Arc<State>) {
        let config = self.config.probe.clone();
        if config.interval_ms == 0 || self.probe_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let state = Arc::downgrade(state);
        let _ = self.task_supervisor.handle().try_spawn(async move {
            let interval = Duration::from_millis(config.interval_ms);
            let mut seq = 0usize;
            let mut failures = HashMap::new();
            loop {
                // Spread pools across time from their very first round. Wait
                // after each completed round so slow probes never overlap or
                // trigger a burst of catch-up work.
                seq = seq.wrapping_add(1);
                tokio::time::sleep(probe_delay(interval, pseudo_random(seq))).await;
                // Neither State nor sockets are kept alive while sleeping.
                let Some(state) = state.upgrade() else { break };
                let Some(pool) = state.socket_pool.rdma_pool() else {
                    break;
                };
                pool.probe_round(&state, &mut failures).await;
            }
        });
    }

    async fn probe_round(&self, state: &Arc<State>, failures: &mut HashMap<u64, u32>) {
        let mut targets = Vec::new();
        let peers: Vec<_> = self.peers.iter().map(|peer| peer.value().clone()).collect();
        for peer in peers {
            for stripe in peer.active_snapshot() {
                if !stripe.socket.state.is_ok() {
                    self.probe_connection_closed(state, &stripe.socket, Some(&peer));
                } else if stripe.socket.is_probe_ready() {
                    targets.push((stripe.socket, Some(peer.clone())));
                }
            }
            // A failed reconnect remains desired and is retried here even
            // when maintenance is disabled and no user requests arrive.
            if peer.needs_reconnect() {
                self.schedule_reconcile(&peer, state);
            }
        }
        targets.extend(self.inbound.lock().unwrap().iter().filter_map(|weak| {
            let socket = weak.upgrade()?;
            (socket.state.is_ok() && socket.is_probe_ready()).then_some((socket, None))
        }));
        let live: HashSet<_> = targets.iter().map(|(socket, _)| socket.conn_id).collect();
        failures.retain(|conn_id, _| live.contains(conn_id));
        let mut probes = FuturesUnordered::new();
        for (socket, peer) in targets {
            probes.push(async move {
                let result = probe_connection(
                    &socket,
                    state,
                    Duration::from_millis(self.config.probe.timeout_ms),
                )
                .await;
                (socket, peer, result)
            });
        }
        // These are owned futures, not detached jobs. Supervisor cancellation
        // drops every probe waiter immediately; posted READ ownership remains
        // with the existing completion/QP machinery.
        while let Some((socket, peer, result)) = probes.next().await {
            if peer
                .as_ref()
                .is_some_and(|peer| !peer.contains_active(&socket))
            {
                // A replacement has already moved this socket to draining.
                // A late probe must not close it early or rebuild it again.
                failures.remove(&socket.conn_id);
                continue;
            }
            let mut count = failures.get(&socket.conn_id).copied().unwrap_or_default();
            let expired = record_result(
                &mut count,
                &result,
                self.config.probe.max_consecutive_timeouts,
            );
            if count == 0 {
                failures.remove(&socket.conn_id);
            } else {
                failures.insert(socket.conn_id, count);
            }
            if let Err(error) = &result {
                tracing::debug!(conn_id = socket.conn_id, path = ?socket.path, consecutive_timeouts = count, %error, "RDMA connection probe failed");
            }
            if expired {
                tracing::warn!(conn_id = socket.conn_id, path = ?socket.path, consecutive_timeouts = count, "RDMA connection probe timeout threshold reached; closing connection");
            }
            if expired || !socket.state.is_ok() {
                self.probe_connection_closed(state, &socket, peer.as_ref());
                failures.remove(&socket.conn_id);
            }
        }
    }

    fn probe_connection_closed(
        &self,
        state: &Arc<State>,
        socket: &Arc<RdmaSocket>,
        peer: Option<&Arc<PeerState>>,
    ) {
        if let Some(peer) = peer {
            // The peer checks identity and closes under the publication lock,
            // so a late result cannot interrupt an already draining socket.
            if !peer.fail_active(socket) {
                return;
            }
            self.schedule_reconcile(peer, state);
        } else {
            socket.set_error();
        }
        state.connection_closed(
            socket.conn_id,
            &Error::new(
                ErrorKind::ConnectionClosed,
                "RDMA connection failed its health probe".into(),
            ),
        );
    }
}

#[cfg(test)]
mod tests;
