//! CQ shard selection: lazily started poll threads per device and admission.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use foldhash::fast::RandomState;

use super::{ConnReservation, DevicePoller, Dispatcher, PollerConfig, budget::Demand};
use crate::{
    Error, Result,
    rdma::{RdmaConnectionConfig, RdmaDevice, path::RdmaCqLoad},
};

/// Lazily-created CQ/poll-thread shards, keyed by device name. Connections
/// reserve completion credits on the least utilized shard that can admit
/// them, before QP creation or peer negotiation. All shards share one fixed
/// dispatcher worker pool; the configured thread count bounds CPU usage.
#[derive(Default)]
pub struct DevicePollers(Mutex<PollersInner>);

#[derive(Default)]
struct PollersInner {
    devices: std::collections::HashMap<String, DeviceShards, RandomState>,
    dispatcher: Option<Dispatcher>,
}

#[derive(Default)]
pub(super) struct DeviceShards {
    pub(super) shards: Vec<Arc<DevicePoller>>,
    next: usize,
}

impl DevicePollers {
    /// Select and reserve as one setup operation. Creating each configured
    /// shard lazily spreads even a small number of connections across cores.
    pub(crate) fn reserve(
        &self,
        device: &RdmaDevice,
        config: PollerConfig,
        shard_count: u32,
        connection: &RdmaConnectionConfig,
    ) -> Result<(Arc<DevicePoller>, ConnReservation)> {
        let name = device.info().name.clone();
        let demand = Demand::connection(connection);
        let mut inner = self.0.lock().unwrap();
        let dispatcher = inner
            .dispatcher
            .get_or_insert_with(|| {
                Dispatcher::start(
                    config.dispatch_workers,
                    Duration::from_micros(config.dispatch_spin_us),
                )
            })
            .clone();
        let entry = inner.devices.entry(name.clone()).or_default();
        if entry.shards.len() < shard_count as usize {
            let index = entry.shards.len();
            match DevicePoller::start(
                device.context(),
                &format!("{name}.{index}"),
                config,
                dispatcher,
            ) {
                Ok(poller) => {
                    tracing::info!(
                        device = name,
                        shard = index,
                        capacity = poller.cq_capacity,
                        "started RDMA CQ shard"
                    );
                    entry.shards.push(Arc::new(poller));
                }
                Err(error) if !entry.shards.is_empty() => {
                    // An optional new thread/CQ is not required if an existing
                    // shard has room (e.g. provider CQ resources exhausted).
                    return entry.reserve(demand).map_err(|admission| {
                        Error::new(
                            admission.kind,
                            format!(
                                "RDMA device {name}: {}; new shard failed: {error}",
                                admission.msg
                            ),
                        )
                    });
                }
                Err(error) => return Err(error),
            }
        }
        entry
            .reserve(demand)
            .map_err(|err| Error::new(err.kind, format!("RDMA device {name}: {}", err.msg)))
    }

    pub(crate) fn report(&self) -> Vec<RdmaCqLoad> {
        let inner = self.0.lock().unwrap();
        let mut result = Vec::new();
        for (device, entry) in &inner.devices {
            for (shard, poller) in entry.shards.iter().enumerate() {
                let (reserved, connections) = poller.shared.budget.snapshot();
                let registry = poller.cq.qp_registry_stats();
                result.push(RdmaCqLoad {
                    device: device.clone(),
                    shard,
                    capacity: poller.cq_capacity,
                    reserved,
                    connections,
                    registered_qps: registry.active,
                    next_sequence_floor: registry.next_sequence_floor,
                    sequence_bits: ruapc_rdma::WRID::SEQUENCE_BITS,
                });
            }
        }
        result.sort_by(|a, b| (&a.device, a.shard).cmp(&(&b.device, b.shard)));
        result
    }
}

impl DeviceShards {
    pub(super) fn reserve(
        &mut self,
        demand: Demand,
    ) -> Result<(Arc<DevicePoller>, ConnReservation)> {
        let count = self.shards.len();
        let start = self.next % count;
        self.next = self.next.wrapping_add(1);
        let mut candidates: Vec<_> = (0..count).map(|offset| (start + offset) % count).collect();
        // Stable sort rotates equally utilized shards. Recheck admission
        // under each budget's mutex; a reclamation can race these snapshots.
        let loads: Vec<_> = self
            .shards
            .iter()
            .map(|p| p.shared.budget.snapshot().0)
            .collect();
        candidates.sort_by(|&a, &b| {
            (u64::from(loads[a]) * u64::from(self.shards[b].cq_capacity))
                .cmp(&(u64::from(loads[b]) * u64::from(self.shards[a].cq_capacity)))
        });
        let mut error = None;
        for index in candidates {
            let poller = &self.shards[index];
            match poller.reserve(demand) {
                Ok(reservation) => return Ok((poller.clone(), reservation)),
                Err(err) => error = Some(err),
            }
        }
        let error = error.expect("at least one configured CQ shard");
        Err(Error::new(
            error.kind,
            format!(
                "all {count} RDMA CQ shards unavailable: {}; raise rdma.polling.device_cq_len or poll_threads_per_device",
                error.msg
            ),
        ))
    }
}

impl std::fmt::Debug for DevicePollers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevicePollers").finish()
    }
}
