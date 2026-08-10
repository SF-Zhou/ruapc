use std::{
    sync::atomic::Ordering,
    sync::{Arc, Weak},
};

use super::{RdmaSocket, RdmaSocketPool};
use crate::{RdmaConnDirection, RdmaDeviceLoad, RdmaPathEntry, RdmaPathReport, StripePhase};

impl RdmaSocketPool {
    /// Snapshot of every live connection with its NIC pair and phase, plus
    /// per-device connection counts.
    pub(crate) async fn path_report(&self) -> RdmaPathReport {
        let devices = self
            .devices
            .rdma_devices()
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
        for peer in self.peers.iter() {
            let stripes = peer.value().stripes.read().unwrap();
            for (phase, entries) in [
                (StripePhase::Active, &stripes.active),
                (StripePhase::Draining, &stripes.draining),
            ] {
                for stripe in entries {
                    paths.push(RdmaPathEntry {
                        peer: Some(peer.addr),
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
        RdmaPathReport { devices, paths }
    }
}
