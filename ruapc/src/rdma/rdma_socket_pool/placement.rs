//! Path/NIC selection and QP parameter negotiation: candidate enumeration,
//! scoring, GID matching and endpoint construction.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::Ordering;

use foldhash::fast::RandomState;
use ruapc_rdma::{DeviceInfo, Gid, GidType, LinkLayer, Port, QueuePair, ibv_mtu};

use super::super::path::{RdmaNicInfo, RdmaPathInfo, gid_ip};
use super::super::rdma_service::RdmaPortInfo;
use super::super::{DeviceSelection, Endpoint, RdmaConnectionConfig, RdmaDevice, RdmaInfo};
use super::{PeerState, RdmaSocketPool, Stripe, placement};
use crate::{Error, ErrorKind, RdmaQueuePairConfig, RdmaSubnetPolicy, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum PathClass {
    InfiniBand,
    RoceV2,
    RoceOther,
}

pub(super) struct Candidate<'a> {
    pub(super) index: usize,
    pub(super) local_index: usize,
    pub(super) remote: &'a str,
    pub(super) same_subnet: bool,
    pub(super) class: PathClass,
    pub(super) blacklisted: bool,
    pub(super) local_load: u64,
    pub(super) remote_load: u64,
}

pub(super) struct Selection<'a> {
    pub(super) required_remote: Option<&'a str>,
    pub(super) avoided_remotes: &'a HashSet<String>,
    pub(super) subnet_policy: RdmaSubnetPolicy,
}

pub(super) fn choose_path(
    candidates: &[Candidate<'_>],
    selection: Selection<'_>,
    draws: [u64; 2],
) -> Option<usize> {
    let eligible_indices = eligible_paths(candidates, &selection, true);
    let eligible: Vec<&Candidate<'_>> = candidates
        .iter()
        .filter(|candidate| eligible_indices.contains(&candidate.index))
        .collect();
    if eligible.is_empty() {
        return None;
    }

    let mut remote_loads = HashMap::<&str, u64>::new();
    let mut remotes = Vec::new();
    for candidate in &eligible {
        if !remote_loads.contains_key(candidate.remote) {
            remotes.push(candidate.remote);
            remote_loads.insert(candidate.remote, candidate.remote_load);
        }
    }
    let chosen_remote = if remotes.len() == 1 {
        remotes[0]
    } else {
        let a = draws[0] as usize % remotes.len();
        let mut b = draws[1] as usize % (remotes.len() - 1);
        if b >= a {
            b += 1;
        }
        if remote_loads[remotes[b]] < remote_loads[remotes[a]] {
            remotes[b]
        } else {
            remotes[a]
        }
    };

    eligible
        .into_iter()
        .filter(|candidate| candidate.remote == chosen_remote)
        .min_by_key(|candidate| (candidate.local_load, candidate.local_index))
        .map(|candidate| candidate.index)
}

pub(super) fn eligible_paths(
    candidates: &[Candidate<'_>],
    selection: &Selection<'_>,
    blacklist_fallback: bool,
) -> Vec<usize> {
    let mut eligible: Vec<&Candidate<'_>> = candidates
        .iter()
        .filter(|candidate| {
            selection
                .required_remote
                .is_none_or(|required| candidate.remote == required)
        })
        .collect();
    retain_if_any(&mut eligible, |candidate| {
        !selection.avoided_remotes.contains(candidate.remote)
    });
    if blacklist_fallback {
        retain_if_any(&mut eligible, |candidate| !candidate.blacklisted);
    } else {
        eligible.retain(|candidate| !candidate.blacklisted);
    }
    match selection.subnet_policy {
        RdmaSubnetPolicy::Prefer => retain_if_any(&mut eligible, |candidate| candidate.same_subnet),
        RdmaSubnetPolicy::Require => eligible.retain(|candidate| candidate.same_subnet),
    }
    if let Some(best_class) = eligible.iter().map(|candidate| candidate.class).min() {
        eligible.retain(|candidate| candidate.class == best_class);
    }
    eligible
        .into_iter()
        .map(|candidate| candidate.index)
        .collect()
}

fn addresses_share_subnet(
    local: Option<IpAddr>,
    remote: Option<IpAddr>,
    subnets: &[ipnet::IpNet],
) -> bool {
    local.zip(remote).is_some_and(|(local, remote)| {
        subnets
            .iter()
            .any(|subnet| subnet.contains(&local) && subnet.contains(&remote))
    })
}

fn retain_if_any<T>(items: &mut Vec<T>, predicate: impl Fn(&T) -> bool) {
    if items.iter().any(&predicate) {
        items.retain(predicate);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReconcileAction {
    ConnectCoverage(String),
    ConnectTarget,
}

pub(super) fn plan_connections(
    remote_names: &[String],
    healthy_remotes: &[String],
    coverage_blocked: &HashSet<String>,
    min_per_remote: usize,
    target: usize,
    max: usize,
) -> Vec<ReconcileAction> {
    let mut actions = Vec::new();
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for remote in healthy_remotes {
        *counts.entry(remote).or_default() += 1;
    }
    let mut total = healthy_remotes.len();
    while total < max {
        let mut progressed = false;
        for remote in remote_names {
            if total == max {
                break;
            }
            if coverage_blocked.contains(remote) {
                continue;
            }
            let count = counts.entry(remote).or_default();
            if *count < min_per_remote {
                *count += 1;
                total += 1;
                actions.push(ReconcileAction::ConnectCoverage(remote.clone()));
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }
    while total < target.min(max) {
        actions.push(ReconcileAction::ConnectTarget);
        total += 1;
    }
    actions
}

pub(super) struct ExistingStripe<'a> {
    pub(super) index: usize,
    pub(super) local: &'a str,
    pub(super) remote: &'a str,
    pub(super) local_load: u64,
    pub(super) remote_load: u64,
    pub(super) remote_healthy: usize,
    pub(super) remote_advertised: bool,
}

pub(super) struct Replacement<'a> {
    pub(super) index: usize,
    pub(super) local: &'a str,
    pub(super) remote: &'a str,
    pub(super) local_load: u64,
    pub(super) remote_load: u64,
}

pub(super) fn choose_rebalance(
    stripes: &[ExistingStripe<'_>],
    candidates: &[Replacement<'_>],
    min_remote_coverage: usize,
    threshold: u64,
    execute_gate: bool,
) -> Option<(usize, usize)> {
    if !execute_gate {
        return None;
    }
    let victim = stripes
        .iter()
        .filter(|stripe| !stripe.remote_advertised || stripe.remote_healthy > min_remote_coverage)
        .max_by_key(|stripe| {
            stripe.local_load.saturating_sub(1) + stripe.remote_load.saturating_sub(1)
        })?;
    let victim_score = victim.local_load.saturating_sub(1) + victim.remote_load.saturating_sub(1);
    let (replacement_score, replacement) = candidates
        .iter()
        .map(|candidate| {
            let local = candidate
                .local_load
                .saturating_sub(u64::from(candidate.local == victim.local));
            let remote = candidate
                .remote_load
                .saturating_sub(u64::from(candidate.remote == victim.remote));
            (local + remote, candidate)
        })
        .min_by_key(|(score, _)| *score)?;
    (replacement_score.saturating_add(threshold) <= victim_score)
        .then_some((victim.index, replacement.index))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soft_filters_fall_back_but_hard_constraint_does_not() {
        let avoided = HashSet::from(["a".to_owned(), "b".to_owned()]);
        let candidates = [candidate(0, "a", true), candidate(1, "b", true)];
        assert!(
            choose_path(
                &candidates,
                Selection {
                    required_remote: Some("missing"),
                    avoided_remotes: &avoided,
                    subnet_policy: RdmaSubnetPolicy::Prefer,
                },
                [0, 1],
            )
            .is_none()
        );
        assert!(
            choose_path(
                &candidates,
                Selection {
                    required_remote: None,
                    avoided_remotes: &avoided,
                    subnet_policy: RdmaSubnetPolicy::Prefer,
                },
                [0, 1],
            )
            .is_some()
        );
    }

    #[test]
    fn subnet_precedes_link_class() {
        let mut ib = candidate(0, "ib", false);
        ib.class = PathClass::InfiniBand;
        let mut roce = candidate(1, "roce", false);
        roce.same_subnet = true;
        assert_eq!(
            choose_path(
                &[ib, roce],
                Selection {
                    required_remote: None,
                    avoided_remotes: &HashSet::new(),
                    subnet_policy: RdmaSubnetPolicy::Prefer,
                },
                [0, 1],
            ),
            Some(1)
        );
    }

    #[test]
    fn required_subnet_does_not_fall_back() {
        let candidates = [candidate(0, "a", false), candidate(1, "b", false)];
        assert!(
            choose_path(
                &candidates,
                Selection {
                    required_remote: None,
                    avoided_remotes: &HashSet::new(),
                    subnet_policy: RdmaSubnetPolicy::Require,
                },
                [0, 1],
            )
            .is_none()
        );
    }

    #[test]
    fn reconcile_coverage_before_target() {
        assert_eq!(
            plan_connections(
                &["a".into(), "b".into()],
                &["a".into()],
                &HashSet::new(),
                1,
                3,
                4,
            ),
            [
                ReconcileAction::ConnectCoverage("b".into()),
                ReconcileAction::ConnectTarget,
            ]
        );
    }

    #[test]
    fn blocked_coverage_falls_back_to_target() {
        assert_eq!(
            plan_connections(
                &["a".into(), "b".into()],
                &["a".into()],
                &HashSet::from(["b".into()]),
                1,
                2,
                4,
            ),
            [ReconcileAction::ConnectTarget]
        );
    }

    #[test]
    fn rebalance_preserves_coverage_and_threshold() {
        let stripes = [ExistingStripe {
            index: 7,
            local: "l0",
            remote: "r0",
            local_load: 5,
            remote_load: 5,
            remote_healthy: 2,
            remote_advertised: true,
        }];
        let candidates = [Replacement {
            index: 3,
            local: "l1",
            remote: "r1",
            local_load: 0,
            remote_load: 0,
        }];
        assert_eq!(
            choose_rebalance(&stripes, &candidates, 1, 2, true),
            Some((7, 3))
        );
        assert_eq!(choose_rebalance(&stripes, &candidates, 2, 2, true), None);
        assert_eq!(choose_rebalance(&stripes, &candidates, 1, 20, true), None);
    }

    fn candidate(index: usize, remote: &str, blacklisted: bool) -> Candidate<'_> {
        Candidate {
            index,
            local_index: 0,
            remote,
            same_subnet: false,
            class: PathClass::RoceV2,
            blacklisted,
            local_load: 0,
            remote_load: index as u64,
        }
    }
}

impl RdmaSocketPool {
    pub(super) fn find_device_by_name(
        &self,
        selection: &DeviceSelection,
    ) -> Result<(usize, &RdmaDevice)> {
        self.devices
            .rdma_devices()
            .iter()
            .enumerate()
            .find(|(_, device)| device.info().name.as_str() == selection.device_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("RDMA device {} not found", selection.device_name),
                )
            })
    }

    /// Enumerates every compatible (local NIC, remote NIC) pair.
    ///
    /// One candidate is produced per compatible port pair (the GID within
    /// a port pair is chosen by the existing preference logic). Link class
    /// preference is applied after request constraints and reachability
    /// filters, keeping lower classes available as fallbacks.
    pub(super) fn enumerate_path_candidates(
        &self,
        remote_info: &RdmaInfo,
    ) -> Result<Vec<PathCandidate>> {
        let local_devices = self.devices.rdma_devices();
        if local_devices.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "no local RDMA device available".into(),
            ));
        }

        let mut matches = Vec::new();
        let mut remote_ports = 0usize;
        let mut local_usable_ports = 0usize;
        let mut link_layer_matches = 0usize;

        // Remote ports are pre-filtered: peers only advertise usable ports.
        for remote_device in &remote_info.devices {
            for remote_port in &remote_device.ports {
                remote_ports += 1;

                for (local_device_index, local_device) in local_devices.iter().enumerate() {
                    let local_info = local_device.info();
                    for local_port in &local_info.ports {
                        if !local_port.is_usable() {
                            continue;
                        }
                        local_usable_ports += 1;
                        if local_port.port_attr.link_layer != remote_port.link_layer {
                            continue;
                        }
                        link_layer_matches += 1;

                        let gid_pairs = Self::match_gid_pairs(local_port, remote_port);
                        let pair_limit = if self.config.subnets.is_empty() {
                            1
                        } else {
                            gid_pairs.len()
                        };
                        for (local_gid_index, remote_gid_index) in
                            gid_pairs.into_iter().take(pair_limit)
                        {
                            let local_ip = local_port
                                .find_gid(local_gid_index)
                                .and_then(|gid| gid_ip(&gid.gid));
                            let remote_ip = remote_port
                                .gids
                                .iter()
                                .find(|gid| gid.index == remote_gid_index)
                                .and_then(|gid| gid_ip(&gid.gid));
                            let same_subnet =
                                addresses_share_subnet(local_ip, remote_ip, &self.config.subnets);
                            let class = match local_port.port_attr.link_layer {
                                LinkLayer::InfiniBand => PathClass::InfiniBand,
                                LinkLayer::Ethernet
                                    if Self::gid_index_is_rocev2(local_port, local_gid_index) =>
                                {
                                    PathClass::RoceV2
                                }
                                LinkLayer::Ethernet => PathClass::RoceOther,
                                LinkLayer::Unspecified => continue,
                            };
                            matches.push(PathCandidate {
                                local_device_index,
                                remote: DeviceSelection {
                                    device_name: remote_device.name.clone(),
                                    port_num: remote_port.port_num,
                                    gid_index: remote_gid_index,
                                },
                                remote_limits: remote_device.connection,
                                class,
                                path: RdmaPathInfo {
                                    local: RdmaNicInfo {
                                        device: local_info.name.clone(),
                                        port_num: local_port.port_num,
                                        gid_index: local_gid_index,
                                        ip: local_ip,
                                    },
                                    remote: RdmaNicInfo {
                                        device: remote_device.name.clone(),
                                        port_num: remote_port.port_num,
                                        gid_index: remote_gid_index,
                                        ip: remote_ip,
                                    },
                                    same_subnet,
                                },
                            });
                        }
                    }
                }
            }
        }

        if matches.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "no compatible RDMA device/port/GID pair found: remote_devices={} local_devices={} remote_ports={} local_usable_ports={} link_layer_matches={}",
                    remote_info.devices.len(),
                    local_devices.len(),
                    remote_ports,
                    local_usable_ports,
                    link_layer_matches
                ),
            ));
        }
        Ok(matches)
    }

    /// Picks the path for a new stripe.
    ///
    /// Remote NIC: power-of-two-choices over the peer's advertised per-NIC
    /// connection counts (plus our own healthy stripes to this peer, which
    /// the possibly-stale advertisement may not include yet). P2C keeps
    /// clients from herding onto the same "least loaded" server NIC when
    /// they all act on the same cached snapshot.
    ///
    /// Local NIC: plain least-connections over the live per-device
    /// counters — they are exact, and equal counts self-balance because
    /// every placement increments the chosen device's counter.
    pub(super) fn select_candidate(
        &self,
        peer: &PeerState,
        candidates: &[PathCandidate],
        preference: PathPreference<'_>,
        remote_info: &RdmaInfo,
        peer_stripes: &[Stripe],
    ) -> Result<PathCandidate> {
        let views: Vec<placement::Candidate<'_>> = candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| {
                let advertised = remote_info
                    .devices
                    .iter()
                    .find(|device| device.name == candidate.path.remote.device)
                    .map_or(0, |device| u64::from(device.active_connections));
                let ours = peer_stripes
                    .iter()
                    .filter(|stripe| {
                        stripe.socket.state.is_ok()
                            && stripe.socket.path.remote.device == candidate.path.remote.device
                    })
                    .count() as u64;
                placement::Candidate {
                    index,
                    local_index: candidate.local_device_index,
                    remote: &candidate.path.remote.device,
                    same_subnet: candidate.path.same_subnet,
                    class: candidate.class,
                    blacklisted: self.is_blacklisted(peer, candidate),
                    local_load: self
                        .conn_counts
                        .get(candidate.local_device_index)
                        .map_or(0, |count| count.load(Ordering::Acquire) as u64),
                    remote_load: advertised + ours,
                }
            })
            .collect();
        let index = placement::choose_path(
            &views,
            placement::Selection {
                required_remote: preference.remote_device,
                avoided_remotes: preference.avoided_remote_nics,
                subnet_policy: self.config.subnet_policy,
            },
            [self.pseudo_random(), self.pseudo_random()],
        )
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "no compatible RDMA path matches remote device {:?}",
                    preference.remote_device
                ),
            )
        })?;
        Ok(candidates[index].clone())
    }

    fn gid_index_is_rocev2(port: &Port, gid_index: u8) -> bool {
        port.find_gid(gid_index)
            .is_some_and(|gid| gid.gid_type == GidType::RoCEv2)
    }

    /// Selects a (local, remote) GID index pair for the given port pair.
    ///
    /// Both GID tables only contain usable GIDs — unusable ones (RoCE v2
    /// loopback / link-local) are filtered out at collection time on each
    /// side (see `query_device_info`).
    fn match_gid_pairs(local_port: &Port, remote_port: &RdmaPortInfo) -> Vec<(u8, u8)> {
        let (local_gids, remote_gids) = (&local_port.gids[..], &remote_port.gids[..]);
        match local_port.port_attr.link_layer {
            LinkLayer::InfiniBand => vec![(
                Self::first_gid(local_gids, |_| true).unwrap_or(0),
                Self::first_gid(remote_gids, |_| true).unwrap_or(0),
            )],
            LinkLayer::Ethernet => Self::match_roce_gid_pairs(local_gids, remote_gids),
            LinkLayer::Unspecified => Vec::new(),
        }
    }

    fn match_roce_gid_pairs(local_gids: &[Gid], remote_gids: &[Gid]) -> Vec<(u8, u8)> {
        let mut pairs = Vec::new();
        // Prefer RoCE v2 pairs, then RoCE v1 pairs, while retaining every
        // compatible pair so subnet preference can select the right GID.
        for wanted in [GidType::RoCEv2, GidType::RoCEv1] {
            for local in local_gids.iter().filter(|gid| gid.gid_type == wanted) {
                for remote in remote_gids.iter().filter(|gid| gid.gid_type == wanted) {
                    pairs.push((local.index, remote.index));
                }
            }
        }

        // Then every other pair with matching GID types.
        for local in local_gids
            .iter()
            .filter(|gid| !matches!(gid.gid_type, GidType::RoCEv2 | GidType::RoCEv1))
        {
            for remote in remote_gids
                .iter()
                .filter(|remote| remote.gid_type == local.gid_type)
            {
                pairs.push((local.index, remote.index));
            }
        }

        // Preserve the previous best-effort fallback for unusual providers
        // whose two sides report different GID type names.
        if pairs.is_empty()
            && let (Some(local), Some(remote)) = (
                Self::first_gid(local_gids, |_| true),
                Self::first_gid(remote_gids, |_| true),
            )
        {
            pairs.push((local, remote));
        }
        pairs
    }

    /// Returns the index of the first GID matching `predicate`.
    fn first_gid(gids: &[Gid], mut predicate: impl FnMut(&Gid) -> bool) -> Option<u8> {
        gids.iter().find(|gid| predicate(gid)).map(|gid| gid.index)
    }

    pub(super) fn negotiate_connection_config(
        &self,
        local_device: &RdmaDevice,
        remote: &RdmaConnectionConfig,
    ) -> RdmaConnectionConfig {
        let local = self.local_connection_config(local_device);
        let remote = *remote;
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: local.qp.max_send_wr.min(remote.qp.max_recv_wr),
                max_recv_wr: local.qp.max_recv_wr.min(remote.qp.max_send_wr),
                // Scatter/gather lists are purely local WQE properties: a
                // gather-list SEND arrives as one contiguous message no
                // matter how many SGEs composed it, so neither side's SGE
                // capability constrains the other.
                max_send_sge: local.qp.max_send_sge,
                max_recv_sge: local.qp.max_recv_sge,
            },
            cq_len: local.cq_len.min(remote.cq_len),
            recv_queue_len: local.recv_queue_len.min(remote.recv_queue_len),
            max_msg_size: local.max_msg_size.min(remote.max_msg_size),
            // The connecting side dictates the traffic class; the remote
            // advertisement is irrelevant here.
            traffic_class: self.config.traffic_class,
        }
    }

    pub(super) fn clamp_connection_config(
        &self,
        device: &RdmaDevice,
        requested: RdmaConnectionConfig,
    ) -> RdmaConnectionConfig {
        let local = self.local_connection_config(device);
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: requested.qp.max_send_wr.min(local.qp.max_send_wr),
                max_recv_wr: requested.qp.max_recv_wr.min(local.qp.max_recv_wr),
                // SGE lists are local WQE properties (see
                // `negotiate_connection_config`): use our own capabilities
                // regardless of what the initiator requested for itself.
                max_send_sge: local.qp.max_send_sge,
                max_recv_sge: local.qp.max_recv_sge,
            },
            cq_len: requested.cq_len.min(local.cq_len),
            recv_queue_len: requested.recv_queue_len.min(local.recv_queue_len),
            max_msg_size: requested.max_msg_size.min(local.max_msg_size),
            // Client-chosen: applied verbatim so both directions of the
            // connection share one traffic class.
            traffic_class: requested.traffic_class,
        }
    }

    fn local_connection_config(&self, device: &RdmaDevice) -> RdmaConnectionConfig {
        let info = device.info();
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: self
                    .config
                    .qp
                    .max_send_wr
                    .min(info.device_attr.max_qp_wr as u32),
                max_recv_wr: self
                    .config
                    .qp
                    .max_recv_wr
                    .min(info.device_attr.max_qp_wr as u32),
                max_send_sge: self
                    .config
                    .qp
                    .max_send_sge
                    .min(info.device_attr.max_sge as u32),
                max_recv_sge: self
                    .config
                    .qp
                    .max_recv_sge
                    .min(info.device_attr.max_sge as u32),
            },
            cq_len: self.config.cq_len.min(info.device_attr.max_cqe as u32),
            recv_queue_len: self.config.recv_queue_len,
            // Enforce a small floor so a tiny misconfiguration cannot break
            // the RPC control plane.
            max_msg_size: self.config.max_msg_size.max(16 * 1024),
            traffic_class: self.config.traffic_class,
        }
    }

    /// Constructs an Endpoint from a QueuePair and selected local port/GID.
    pub(super) fn build_endpoint(
        &self,
        qp: &QueuePair,
        device: &RdmaDevice,
        port_num: u8,
        gid_index: u8,
    ) -> Result<Endpoint> {
        let info = device.info();
        let port = Self::find_port(&info, port_num)?;
        if !port.is_usable() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("RDMA port {}:{} is not active", info.name, port_num),
            ));
        }

        let gid = port.find_gid(gid_index).map(|gid| gid.gid);
        if port.port_attr.link_layer.is_ethernet() && gid.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA port {}:{} does not have GID index {}",
                    info.name, port_num, gid_index
                ),
            ));
        }

        Ok(Endpoint {
            connection_cookie: 0,
            qp_num: qp.qp_num(),
            port_num,
            gid_index,
            lid: port.port_attr.lid,
            gid: gid.unwrap_or_default(),
            link_layer: port.port_attr.link_layer,
            active_mtu: port.port_attr.active_mtu,
            psn: Self::random_psn(qp.qp_num()),
            rd_atomic_cap: Self::rd_atomic_cap(&info),
        })
    }

    /// The device cap on concurrent RDMA READs per QP, advertised to the
    /// peer via the endpoint exchange.
    ///
    /// The minimum of the initiator-side and responder-side device limits
    /// is used for both directions, clamped to a sane ceiling — beyond ~16
    /// the returns diminish while responder resources grow.
    fn rd_atomic_cap(info: &DeviceInfo) -> u8 {
        const RD_ATOMIC_CEILING: i32 = 16;
        let cap = info
            .device_attr
            .max_qp_rd_atom
            .min(info.device_attr.max_qp_init_rd_atom)
            .clamp(1, RD_ATOMIC_CEILING);
        u8::try_from(cap).unwrap_or(1)
    }

    /// Generates a pseudo-random 24-bit initial packet sequence number.
    ///
    /// Uniqueness across QP incarnations is what matters: drivers recycle
    /// qp numbers, and a fresh QP reusing the (qp_num, GID) pair of a
    /// recently destroyed one with a predictable PSN can silently blackhole
    /// against stale peer state.
    fn random_psn(qp_num: u32) -> u32 {
        use std::hash::BuildHasher as _;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        (RandomState::default().hash_one((qp_num, nanos)) as u32) & 0xFF_FFFF
    }

    pub(super) fn find_port(info: &DeviceInfo, port_num: u8) -> Result<&Port> {
        info.ports
            .iter()
            .find(|port| port.port_num == port_num)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("RDMA port {}:{} not found", info.name, port_num),
                )
            })
    }

    pub(super) fn bring_qp_to_rts(
        &self,
        qp: &QueuePair,
        local: &Endpoint,
        remote: &Endpoint,
        pkey_index: u16,
        traffic_class: u8,
    ) -> Result<()> {
        if local.link_layer != remote.link_layer {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA link layer mismatch: local {} remote {}",
                    local.link_layer, remote.link_layer
                ),
            ));
        }

        let path_mtu = Self::min_mtu(local.active_mtu, remote.active_mtu);
        // Both sides advertise their device cap and program the minimum
        // for both `max_rd_atomic` (outbound reads) and
        // `max_dest_rd_atomic` (inbound reads): the two ends compute the
        // same value, which keeps the RC requirement
        // `initiator.max_rd_atomic <= responder.max_dest_rd_atomic`
        // trivially satisfied.
        let rd_atomic = local.rd_atomic_cap.min(remote.rd_atomic_cap).max(1);
        qp.connect(
            local.port_num,
            local.gid_index,
            pkey_index,
            local.link_layer,
            path_mtu,
            remote.qp_num,
            remote.gid,
            remote.lid,
            local.psn,
            remote.psn,
            rd_atomic,
            rd_atomic,
            traffic_class,
        )
        .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
    }

    fn min_mtu(a: ibv_mtu, b: ibv_mtu) -> ibv_mtu {
        if (a as u32) <= (b as u32) { a } else { b }
    }
}

#[derive(Clone, Copy)]
pub(super) struct PathPreference<'a> {
    pub(super) remote_device: Option<&'a str>,
    pub(super) avoided_remote_nics: &'a HashSet<String>,
}

/// One compatible (local NIC, remote NIC) pair a new connection could use.
#[derive(Clone, Debug)]
pub(super) struct PathCandidate {
    /// Index of the local device in `devices.rdma_devices()`.
    pub(super) local_device_index: usize,
    /// Remote device/port/GID to request in the `connect` RPC.
    pub(super) remote: DeviceSelection,
    /// Remote per-connection resource limits advertised for that device.
    pub(super) remote_limits: RdmaConnectionConfig,
    /// Preferred transport class, considered after hard constraints and
    /// reachability filters so lower classes remain valid fallbacks.
    pub(super) class: PathClass,
    /// Full NIC-pair identity of this candidate.
    pub(super) path: RdmaPathInfo,
}

#[cfg(test)]
mod path_selection_tests {
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Weak};
    use std::time::{Duration, Instant};

    use super::super::accept::{
        AcceptLease, AcceptLeaseEvent, AcceptLeaseState, advance_accept_lease,
    };
    use super::super::maintenance::preconnect_backoff_delay;
    use super::super::{ConnCountGuard, RdmaSocketPool, next_connection_id};
    use super::*;
    use crate::RdmaSocketPoolConfig;
    use crate::rdma::ConnectionControl;
    use crate::rdma::rdma_service::RdmaDeviceInfo;

    fn make_pool() -> RdmaSocketPool {
        let devices = crate::rdma::test_utils::make_rdma_devices();
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        RdmaSocketPool::new(devices, buffer_pool, RdmaSocketPoolConfig::default()).unwrap()
    }

    fn connection_limits() -> RdmaConnectionConfig {
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig::default(),
            cq_len: 128,
            recv_queue_len: 8,
            max_msg_size: 64 * 1024,
            traffic_class: 0,
        }
    }

    fn candidate(local_index: usize, remote_dev: &str) -> PathCandidate {
        PathCandidate {
            local_device_index: local_index,
            remote: DeviceSelection {
                device_name: remote_dev.into(),
                port_num: 1,
                gid_index: 0,
            },
            remote_limits: connection_limits(),
            class: PathClass::InfiniBand,
            path: RdmaPathInfo {
                local: RdmaNicInfo {
                    device: format!("local{local_index}"),
                    port_num: 1,
                    gid_index: 0,
                    ip: None,
                },
                remote: RdmaNicInfo {
                    device: remote_dev.into(),
                    port_num: 1,
                    gid_index: 0,
                    ip: None,
                },
                same_subnet: false,
            },
        }
    }

    fn remote_info(devices: &[(&str, u32)]) -> RdmaInfo {
        RdmaInfo {
            devices: devices
                .iter()
                .map(|(name, load)| RdmaDeviceInfo {
                    name: (*name).to_string(),
                    active_connections: *load,
                    connection: connection_limits(),
                    ports: Vec::new(),
                })
                .collect(),
        }
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:9999".parse().unwrap()
    }

    fn peer() -> PeerState {
        PeerState::new(addr())
    }

    fn preference<'a>(
        remote_device: Option<&'a str>,
        avoided_remote_nics: &'a HashSet<String>,
    ) -> PathPreference<'a> {
        PathPreference {
            remote_device,
            avoided_remote_nics,
        }
    }

    #[tokio::test]
    async fn test_select_prefers_less_loaded_remote() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 5), ("remoteB", 0)]);
        // With exactly two distinct remote NICs, P2C compares both every
        // time, so the choice is deterministic.
        for _ in 0..8 {
            let chosen = pool
                .select_candidate(
                    &peer(),
                    &candidates,
                    preference(None, &HashSet::new()),
                    &info,
                    &[],
                )
                .unwrap();
            assert_eq!(chosen.path.remote.device, "remoteB");
        }
    }

    #[tokio::test]
    async fn test_select_avoids_remote_nic_before_fallback() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 5), ("remoteB", 0)]);

        let avoided = HashSet::from(["remoteB".to_owned()]);
        let selected = pool
            .select_candidate(&peer(), &candidates, preference(None, &avoided), &info, &[])
            .unwrap();
        assert_eq!(selected.path.remote.device, "remoteA");

        let all_avoided = HashSet::from(["remoteA".to_owned(), "remoteB".to_owned()]);
        let selected = pool
            .select_candidate(
                &peer(),
                &candidates,
                preference(None, &all_avoided),
                &info,
                &[],
            )
            .unwrap();
        assert_eq!(selected.path.remote.device, "remoteB");
    }

    #[tokio::test]
    async fn test_select_respects_internal_remote_constraint() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 5), ("remoteB", 0)]);
        let selected = pool
            .select_candidate(
                &peer(),
                &candidates,
                preference(Some("remoteA"), &HashSet::new()),
                &info,
                &[],
            )
            .unwrap();
        assert_eq!(selected.path.remote.device, "remoteA");
    }

    #[tokio::test]
    async fn test_rejects_too_short_connect_lease() {
        let devices = crate::rdma::test_utils::make_rdma_devices();
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let config = RdmaSocketPoolConfig {
            connect_lease_ms: 14_999,
            ..Default::default()
        };
        let err = RdmaSocketPool::new(devices, buffer_pool, config).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn test_accept_lease_transitions_are_state_checked() {
        let pool = make_pool();
        let connection_id = 42;
        let socket = Weak::new();
        pool.accept_leases.insert(
            connection_id,
            AcceptLease {
                socket: socket.clone(),
                server_connection_cookie: 7,
                state: AcceptLeaseState::Pending,
                expires_at: Instant::now() + Duration::from_secs(1),
            },
        );
        pool.observe_accept_receive(connection_id, &socket);
        assert!(pool.accept_leases.contains_key(&connection_id));
        assert_eq!(
            pool.accept_leases.get(&connection_id).unwrap().state,
            AcceptLeaseState::ReceiveObserved
        );

        pool.accept_leases.get_mut(&connection_id).unwrap().state = AcceptLeaseState::Confirmed;
        pool.observe_accept_receive(connection_id, &socket);
        assert_eq!(
            pool.accept_leases.get(&connection_id).unwrap().state,
            AcceptLeaseState::Active
        );
        pool.accept_leases.remove(&connection_id);

        pool.accept_leases.insert(
            connection_id,
            AcceptLease {
                socket: Weak::new(),
                server_connection_cookie: 7,
                state: AcceptLeaseState::Pending,
                expires_at: Instant::now() + Duration::from_secs(1),
            },
        );
        let mismatched = ConnectionControl {
            connection_id,
            server_connection_cookie: 8,
        };
        assert_eq!(
            pool.rdma_confirm(&mismatched).unwrap_err().kind,
            ErrorKind::InvalidArgument
        );
        pool.rdma_abort(&mismatched);
        assert!(pool.accept_leases.contains_key(&connection_id));
        pool.accept_leases.remove(&connection_id);

        pool.accept_leases.insert(
            connection_id,
            AcceptLease {
                socket: Weak::new(),
                server_connection_cookie: 7,
                state: AcceptLeaseState::Pending,
                expires_at: Instant::now() - Duration::from_millis(1),
            },
        );
        let control = ConnectionControl {
            connection_id,
            server_connection_cookie: 7,
        };
        let err = pool.rdma_confirm(&control).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
        assert!(!pool.accept_leases.contains_key(&connection_id));
    }

    #[test]
    fn test_accept_lease_events_commit_in_either_order() {
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Pending, AcceptLeaseEvent::Confirm),
            AcceptLeaseState::Confirmed
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Confirmed, AcceptLeaseEvent::Receive),
            AcceptLeaseState::Active
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Pending, AcceptLeaseEvent::Receive),
            AcceptLeaseState::ReceiveObserved
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::ReceiveObserved, AcceptLeaseEvent::Confirm),
            AcceptLeaseState::Active
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Confirmed, AcceptLeaseEvent::Confirm),
            AcceptLeaseState::Confirmed
        );
        assert_eq!(
            advance_accept_lease(AcceptLeaseState::Active, AcceptLeaseEvent::Confirm),
            AcceptLeaseState::Active
        );
    }

    #[test]
    fn test_addresses_share_client_subnet() {
        let subnets = ["10.11.0.0/16".parse().unwrap()];
        assert!(addresses_share_subnet(
            Some("10.11.1.2".parse().unwrap()),
            Some("10.11.200.3".parse().unwrap()),
            &subnets,
        ));
        assert!(!addresses_share_subnet(
            Some("10.11.1.2".parse().unwrap()),
            Some("10.12.1.2".parse().unwrap()),
            &subnets,
        ));
    }

    #[tokio::test]
    async fn test_same_subnet_is_preferred() {
        let pool = make_pool();
        let mut same_subnet = candidate(0, "remoteA");
        same_subnet.path.same_subnet = true;
        let other = candidate(0, "remoteB");
        let candidates = [same_subnet, other];
        let info = remote_info(&[("remoteA", 10), ("remoteB", 0)]);

        let selected = pool
            .select_candidate(
                &peer(),
                &candidates,
                preference(None, &HashSet::new()),
                &info,
                &[],
            )
            .unwrap();
        assert_eq!(selected.path.remote.device, "remoteA");
    }

    #[test]
    fn test_preconnect_backoff_is_bounded() {
        assert_eq!(preconnect_backoff_delay(1), Duration::from_millis(100));
        assert_eq!(preconnect_backoff_delay(4), Duration::from_millis(800));
        assert!(preconnect_backoff_delay(100) <= Duration::from_secs(30));
    }

    #[test]
    fn test_connection_ids_are_nonzero_and_unique() {
        let first = next_connection_id();
        let second = next_connection_id();
        assert_ne!(first, 0);
        assert_ne!(second, 0);
        assert_ne!(first, second);
    }

    /// The connecting client dictates the traffic class (its own config,
    /// not min'd with the remote advertisement); the accepting server
    /// applies the client's requested value verbatim.
    #[tokio::test]
    async fn test_traffic_class_client_decides_server_obeys() {
        let devices = crate::rdma::test_utils::make_rdma_devices();
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        let config = RdmaSocketPoolConfig {
            traffic_class: 96,
            ..Default::default()
        };
        let pool = RdmaSocketPool::new(devices, buffer_pool, config).unwrap();
        let rdma_devices = pool.devices.rdma_devices();
        let device = &rdma_devices[0];

        // Client path: local config wins over the remote advertisement.
        let mut advertised = connection_limits();
        advertised.traffic_class = 7;
        let negotiated = pool.negotiate_connection_config(device, &advertised);
        assert_eq!(negotiated.traffic_class, 96);

        // Server path: the requested (client-chosen) value passes through.
        let mut requested = connection_limits();
        requested.traffic_class = 42;
        let clamped = pool.clamp_connection_config(device, requested);
        assert_eq!(clamped.traffic_class, 42);
    }

    /// Old peers omit `traffic_class` from the handshake payload; it must
    /// deserialize to 0.
    #[test]
    fn test_connection_config_traffic_class_serde_default() {
        let encoded = rmp_serde::to_vec_named(&serde_json::json!({
            "qp": {
                "max_send_wr": 64,
                "max_recv_wr": 64,
                "max_send_sge": 16,
                "max_recv_sge": 1,
            },
            "cq_len": 128,
            "recv_queue_len": 8,
            "max_msg_size": 65536,
        }))
        .unwrap();
        let config: RdmaConnectionConfig = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(config.traffic_class, 0);
    }

    #[tokio::test]
    async fn test_select_local_least_connections() {
        let pool = make_pool();
        // Simulate load on local device 0; device index 1 may not exist in
        // this environment, but placement only reads its (zero) counter.
        pool.conn_counts[0].fetch_add(2, Ordering::AcqRel);
        let candidates = [candidate(0, "remoteA"), candidate(1, "remoteA")];
        let info = remote_info(&[("remoteA", 0)]);
        let chosen = pool
            .select_candidate(
                &peer(),
                &candidates,
                preference(None, &HashSet::new()),
                &info,
                &[],
            )
            .unwrap();
        assert_eq!(chosen.local_device_index, 1);
    }

    #[tokio::test]
    async fn test_select_avoids_blacklisted_pair() {
        let pool = make_pool();
        let candidates = [candidate(0, "remoteA"), candidate(0, "remoteB")];
        let info = remote_info(&[("remoteA", 0), ("remoteB", 0)]);

        let peer = peer();
        pool.blacklist_path(&peer, &candidates[0].path);
        for _ in 0..8 {
            let chosen = pool
                .select_candidate(
                    &peer,
                    &candidates,
                    preference(None, &HashSet::new()),
                    &info,
                    &[],
                )
                .unwrap();
            assert_eq!(chosen.path.remote.device, "remoteB");
        }
        // The blacklist is per peer: another address is unaffected.
        let other = PeerState::new("127.0.0.1:9998".parse().unwrap());
        assert!(!pool.is_blacklisted(&other, &candidates[0]));

        // Soft fallback: with every candidate blacklisted, selection still
        // returns one instead of failing.
        pool.blacklist_path(&peer, &candidates[1].path);
        let _ = pool
            .select_candidate(
                &peer,
                &candidates,
                preference(None, &HashSet::new()),
                &info,
                &[],
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_conn_count_guard_accounting() {
        let counts: Arc<Vec<AtomicUsize>> = Arc::new(vec![AtomicUsize::new(0)]);
        let a = ConnCountGuard::acquire(&counts, 0);
        let b = ConnCountGuard::acquire(&counts, 0);
        // Out-of-range indices are tolerated and count nothing.
        let c = ConnCountGuard::acquire(&counts, 7);
        assert_eq!(counts[0].load(Ordering::Acquire), 2);
        drop(a);
        assert_eq!(counts[0].load(Ordering::Acquire), 1);
        drop((b, c));
        assert_eq!(counts[0].load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_device_list_advertises_connection_counts() {
        let pool = make_pool();
        pool.conn_counts[0].fetch_add(3, Ordering::AcqRel);
        let info = pool.rdma_device_list().unwrap();
        assert_eq!(info.devices[0].active_connections, 3);
    }
}

#[cfg(test)]
mod gid_match_tests {
    use super::*;

    fn make_gid(addr: &str) -> ruapc_rdma::ibv_gid {
        let bits = addr.parse::<std::net::Ipv6Addr>().unwrap().to_bits();
        let mut gid = ruapc_rdma::ibv_gid::default();
        gid.global.subnet_prefix = ((bits >> 64) as u64).to_be();
        gid.global.interface_id = (bits as u64).to_be();
        gid
    }

    fn gid(index: u8, addr: &str, gid_type: GidType) -> Gid {
        Gid {
            index,
            gid: make_gid(addr),
            gid_type,
        }
    }

    fn local_port(gids: Vec<Gid>) -> Port {
        Port {
            port_num: 1,
            port_attr: ruapc_rdma::ibv_port_attr::default(),
            gids,
        }
    }

    #[test]
    fn test_prefers_rocev2_over_rocev1() {
        let local = [
            gid(0, "fe80::1", GidType::RoCEv1),
            gid(3, "::ffff:10.0.0.1", GidType::RoCEv2),
        ];
        let remote = [
            gid(0, "fe80::2", GidType::RoCEv1),
            gid(5, "::ffff:10.0.0.2", GidType::RoCEv2),
        ];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &remote)
                .first()
                .copied(),
            Some((3, 5))
        );
    }

    #[test]
    fn test_retains_all_compatible_gid_pairs_for_subnet_selection() {
        let local = [
            gid(1, "::ffff:10.0.0.1", GidType::RoCEv2),
            gid(2, "::ffff:10.1.0.1", GidType::RoCEv2),
        ];
        let remote = [
            gid(3, "::ffff:10.0.0.2", GidType::RoCEv2),
            gid(4, "::ffff:10.1.0.2", GidType::RoCEv2),
        ];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &remote),
            [(1, 3), (1, 4), (2, 3), (2, 4)]
        );
    }

    #[test]
    fn test_falls_back_to_rocev1_when_remote_lacks_rocev2() {
        let local = [
            gid(0, "fe80::1", GidType::RoCEv1),
            gid(3, "::ffff:10.0.0.1", GidType::RoCEv2),
        ];
        let remote = [gid(0, "fe80::2", GidType::RoCEv1)];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &remote)
                .first()
                .copied(),
            Some((0, 0))
        );
    }

    #[test]
    fn test_matches_same_gid_type_when_no_roce_pair() {
        let local = [gid(0, "fe80::1", GidType::Other("custom".into()))];
        let remote = [
            gid(0, "::ffff:10.0.0.2", GidType::RoCEv2),
            gid(2, "fe80::2", GidType::Other("custom".into())),
        ];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &remote)
                .first()
                .copied(),
            Some((0, 2))
        );
    }

    #[test]
    fn test_empty_remote_gid_table_returns_none() {
        let local = [gid(3, "::ffff:10.0.0.1", GidType::RoCEv2)];
        assert_eq!(
            RdmaSocketPool::match_roce_gid_pairs(&local, &[])
                .first()
                .copied(),
            None
        );
    }

    #[test]
    fn test_match_gid_pair_ethernet() {
        let mut local = local_port(vec![gid(0, "::ffff:10.0.0.1", GidType::RoCEv2)]);
        local.port_attr.link_layer = LinkLayer::Ethernet;
        let remote = RdmaPortInfo {
            port_num: 1,
            link_layer: LinkLayer::Ethernet,
            gids: vec![gid(2, "::ffff:10.0.0.2", GidType::RoCEv2)],
        };
        assert_eq!(
            RdmaSocketPool::match_gid_pairs(&local, &remote)
                .first()
                .copied(),
            Some((0, 2))
        );
    }

    #[test]
    fn test_gid_index_is_rocev2() {
        let port = local_port(vec![
            gid(0, "fe80::1", GidType::RoCEv1),
            gid(3, "::ffff:10.0.0.1", GidType::RoCEv2),
        ]);
        assert!(RdmaSocketPool::gid_index_is_rocev2(&port, 3));
        assert!(!RdmaSocketPool::gid_index_is_rocev2(&port, 0));
        assert!(!RdmaSocketPool::gid_index_is_rocev2(&port, 7));
    }
}
