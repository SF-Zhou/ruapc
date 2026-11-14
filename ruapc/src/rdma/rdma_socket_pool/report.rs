//! Path/connection introspection: the RDMA path report.

use std::{
    sync::atomic::Ordering,
    sync::{Arc, Weak},
};

use super::{RdmaSocket, RdmaSocketPool};
use crate::rdma::{RdmaConnDirection, RdmaDeviceLoad, RdmaPathEntry, RdmaPathReport, StripePhase};

impl RdmaSocketPool {
    /// Snapshot of every live connection with its NIC pair and phase, plus
    /// per-device connection counts.
    pub(crate) async fn path_report(&self) -> RdmaPathReport {
        let devices = self
            .devices
            .iter()
            .enumerate()
            .map(|(index, device)| RdmaDeviceLoad {
                device: device.info().name.clone(),
                connections: self
                    .conn_counts
                    .get(index)
                    .map_or(0, |count| count.load(Ordering::Acquire)),
            })
            .collect();

        let mut paths = Vec::new();
        let mut pending_fill_peers = Vec::new();
        let mut recovering_peers = Vec::new();
        for peer in self.peers.iter() {
            let peer = peer.value();
            // One lock: intents and stripes come from the same moment.
            let stripes = peer.lock_stripes();
            if stripes.fill_pending() {
                pending_fill_peers.push(peer.endpoint.clone());
            }
            if stripes.recovery_requested() {
                recovering_peers.push(peer.endpoint.clone());
            }
            for (phase, entries) in [
                (StripePhase::Active, &stripes.active),
                (StripePhase::Draining, &stripes.draining),
            ] {
                for stripe in entries {
                    paths.push(RdmaPathEntry {
                        peer: Some(peer.endpoint.clone()),
                        direction: RdmaConnDirection::Outbound,
                        path: stripe.socket.path.clone(),
                        qp_num: stripe.socket.queue_pair.qp_num(),
                        healthy: stripe.socket.state.is_ok(),
                        phase,
                    });
                }
            }
        }

        let inbound: Vec<Arc<RdmaSocket>> = self
            .inbound
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for socket in inbound {
            paths.push(RdmaPathEntry {
                peer: None,
                direction: RdmaConnDirection::Inbound,
                path: socket.path.clone(),
                qp_num: socket.queue_pair.qp_num(),
                healthy: socket.state.is_ok(),
                phase: StripePhase::Active,
            });
        }
        RdmaPathReport {
            devices,
            paths,
            pending_fill_peers,
            recovering_peers,
            completion_queues: self.pollers.report(),
        }
    }
}
