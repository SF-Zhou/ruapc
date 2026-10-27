//! Public RDMA transport configuration.

use ruapc_wire::frame::MAX_MSG_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Error, ErrorKind, Result};

/// RDMA socket pool configuration grouped by runtime responsibility.
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaSocketPoolConfig {
    pub connection: RdmaConnectionTuningConfig,
    pub polling: RdmaPollingConfig,
    pub path: RdmaPathPolicyConfig,
    pub peers: RdmaPeerPoolConfig,
    pub maintenance: RdmaMaintenanceConfig,
    pub probe: RdmaProbeConfig,
    pub remote_memory: RdmaRemoteMemoryConfig,
}

impl RdmaSocketPoolConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        self.connection.validate()?;
        self.polling.validate()?;
        self.peers.validate()?;
        self.maintenance.validate()?;
        self.probe.validate()?;
        self.remote_memory.validate()
    }
}

/// Queue-pair, receive-ring and message transport settings.
#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaConnectionTuningConfig {
    pub qp: RdmaQueuePairConfig,
    /// Number of pre-posted receives. The send window is half this value.
    pub recv_queue_len: u32,
    pub pkey_index: u16,
    /// Selective signaling interval. Must be nonzero.
    pub send_signal_interval: u32,
    /// Maximum complete inline RPC frame size, including metadata and framing.
    /// Must be at least 16 KiB and less than 64 MiB.
    pub max_msg_size: u32,
    /// Maximum complete serialized message transferred with RDMA READ.
    /// Zero disables large messages. Otherwise this must cover `max_msg_size`
    /// and be less than 64 MiB, the shared exclusive frame-size limit.
    /// Registered storage may round up to the maximum 64 MiB pool allocation.
    pub max_large_msg_size: u32,
    /// Per-connection byte budget for large messages, applied independently
    /// to outgoing sources and incoming destinations. Must be nonzero and
    /// cover `max_large_msg_size`; this setting is not negotiated.
    pub max_inflight_large_bytes: u32,
    pub msg_aggregation: bool,
    /// GRH traffic class selected by the connection initiator.
    pub traffic_class: u8,
}

impl Default for RdmaConnectionTuningConfig {
    fn default() -> Self {
        Self {
            qp: RdmaQueuePairConfig::default(),
            recv_queue_len: 8,
            pkey_index: 0,
            send_signal_interval: 8,
            max_msg_size: 256 * 1024,
            max_large_msg_size: Self::MAX_LARGE_MSG_SIZE,
            max_inflight_large_bytes: 128 * 1024 * 1024,
            msg_aggregation: true,
            traffic_class: 0,
        }
    }
}

impl RdmaConnectionTuningConfig {
    pub const MIN_RECV_QUEUE_LEN: u32 = 2;
    pub const MIN_MAX_MSG_SIZE: u32 = 16 * 1024;
    /// Largest complete message accepted by the shared frame codec.
    pub const MAX_LARGE_MSG_SIZE: u32 = (MAX_MSG_SIZE - 1) as u32;

    fn validate(&self) -> Result<()> {
        if self.qp.max_send_wr < Self::MIN_RECV_QUEUE_LEN {
            return Err(invalid_config(format!(
                "rdma.connection.qp.max_send_wr must be at least {}",
                Self::MIN_RECV_QUEUE_LEN
            )));
        }
        if self.recv_queue_len < Self::MIN_RECV_QUEUE_LEN {
            return Err(invalid_config(format!(
                "rdma.connection.recv_queue_len must be at least {}",
                Self::MIN_RECV_QUEUE_LEN
            )));
        }
        if self.recv_queue_len > self.qp.max_recv_wr {
            return Err(invalid_config(
                "rdma.connection.recv_queue_len must not exceed qp.max_recv_wr",
            ));
        }
        if self.qp.max_send_sge == 0 || self.qp.max_recv_sge == 0 {
            return Err(invalid_config(
                "rdma.connection.qp scatter/gather limits must be nonzero",
            ));
        }
        if self.send_signal_interval == 0 {
            return Err(invalid_config(
                "rdma.connection.send_signal_interval must be nonzero",
            ));
        }
        if !(Self::MIN_MAX_MSG_SIZE..=Self::MAX_LARGE_MSG_SIZE).contains(&self.max_msg_size) {
            return Err(invalid_config(format!(
                "rdma.connection.max_msg_size must be at least {} and less than 64 MiB",
                Self::MIN_MAX_MSG_SIZE
            )));
        }
        if self.max_large_msg_size != 0
            && !(self.max_msg_size..=Self::MAX_LARGE_MSG_SIZE).contains(&self.max_large_msg_size)
        {
            return Err(invalid_config(
                "rdma.connection.max_large_msg_size must be zero or at least max_msg_size and less than 64 MiB",
            ));
        }
        if self.max_inflight_large_bytes == 0
            || self.max_inflight_large_bytes < self.max_large_msg_size
        {
            return Err(invalid_config(
                "rdma.connection.max_inflight_large_bytes must be nonzero and cover max_large_msg_size",
            ));
        }
        Ok(())
    }
}

/// Shared completion-queue, poll-thread and dispatch-worker settings.
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaPollingConfig {
    /// Requested entries per CQ shard, clamped to the device limit.
    /// Admission uses the actual capacity returned by the provider.
    pub device_cq_len: u32,
    /// Busy-poll duration after the latest completion. Zero disables spinning.
    pub poll_spin_us: u64,
    /// Maximum CQ shards (one poll thread each) per device. Shards start
    /// lazily and receive connections according to available CQ credits.
    pub poll_threads_per_device: u32,
    pub dispatch_workers: u32,
}

impl Default for RdmaPollingConfig {
    fn default() -> Self {
        Self {
            device_cq_len: 65_536,
            poll_spin_us: 50,
            poll_threads_per_device: 1,
            dispatch_workers: 32,
        }
    }
}

impl RdmaPollingConfig {
    fn validate(&self) -> Result<()> {
        if self.device_cq_len == 0 {
            return Err(invalid_config("rdma.polling.device_cq_len must be nonzero"));
        }
        if self.poll_threads_per_device == 0 {
            return Err(invalid_config(
                "rdma.polling.poll_threads_per_device must be nonzero",
            ));
        }
        if self.dispatch_workers == 0 {
            return Err(invalid_config(
                "rdma.polling.dispatch_workers must be nonzero",
            ));
        }
        Ok(())
    }
}

/// Static local-device and subnet selection policy.
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaPathPolicyConfig {
    /// If non-empty, only listed RDMA devices are used.
    pub device_filter: Vec<String>,
    /// Excluded devices. Exclusion takes precedence over `device_filter`.
    pub device_exclude: Vec<String>,
    /// Retain devices with a port currently in the DOWN state during discovery.
    /// The port remains unavailable for connections until it becomes active.
    /// Defaults to `false`.
    pub allow_down_ports: bool,
    /// Connectivity domains used to match local and remote NIC addresses.
    pub subnets: RdmaSubnetDomains,
    pub subnet_policy: RdmaSubnetPolicy,
}

/// Connection counts, admission limits and handshake lease settings.
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaPeerPoolConfig {
    /// Initial foreground stripe count and total healthy-connection target.
    pub connections_per_peer: u32,
    /// Background coverage target for each remote NIC with a compatible
    /// local path. Zero disables per-NIC coverage preconnection.
    pub min_connections_per_remote_nic: u32,
    /// Maximum active connections per peer. Coverage takes priority over
    /// extra stripes on an already covered NIC; replacements drain separately.
    pub preconnect_max_per_peer: u32,
    /// Budget for preparation and, after the first commit, data-plane
    /// activation. Once active, retain the lease for this long solely for
    /// idempotent commits. Duplicate commits do not extend any budget.
    /// Milliseconds; must be at least 15s.
    pub connect_lease_ms: u64,
}

impl Default for RdmaPeerPoolConfig {
    fn default() -> Self {
        Self {
            connections_per_peer: 1,
            min_connections_per_remote_nic: 1,
            preconnect_max_per_peer: 16,
            connect_lease_ms: 30_000,
        }
    }
}

impl RdmaPeerPoolConfig {
    fn validate(&self) -> Result<()> {
        if self.connections_per_peer == 0 {
            return Err(invalid_config(
                "rdma.peers.connections_per_peer must be nonzero",
            ));
        }
        if self.preconnect_max_per_peer < self.connections_per_peer {
            return Err(invalid_config(
                "rdma.peers.preconnect_max_per_peer must cover connections_per_peer",
            ));
        }
        if self.connect_lease_ms < 15_000 {
            return Err(invalid_config(
                "rdma.peers.connect_lease_ms must be at least 15000",
            ));
        }
        Ok(())
    }
}

/// Background connection maintenance and rebalancing settings.
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaMaintenanceConfig {
    /// Periodic maintenance interval in milliseconds. Zero disables periodic
    /// retries and rebalancing. First-use preconnection and independently
    /// configured health probes and recovery still run.
    pub interval_ms: u64,
    /// Minimum load improvement required before migrating a connection.
    pub rebalance_threshold: u32,
    /// Grace period for a migrated connection in milliseconds.
    pub drain_timeout_ms: u64,
}

impl Default for RdmaMaintenanceConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5_000,
            rebalance_threshold: 2,
            drain_timeout_ms: 10_000,
        }
    }
}

impl RdmaMaintenanceConfig {
    fn validate(&self) -> Result<()> {
        if self.rebalance_threshold == 0 {
            return Err(invalid_config(
                "rdma.maintenance.rebalance_threshold must be nonzero",
            ));
        }
        Ok(())
    }
}

/// Periodic connection health checks for both outbound and inbound RDMA connections.
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaProbeConfig {
    /// Base delay in milliseconds before the first health-check round and
    /// after each completed round, randomized to 0.5–1.5 times this value.
    /// Zero disables probing.
    pub interval_ms: u64,
    /// Total budget in milliseconds for one probe's RPC and remote READ checks.
    /// Must be nonzero when probing is enabled.
    pub timeout_ms: u64,
    /// Consecutive timed-out probes before closing and replacing a connection.
    /// Success or a non-timeout error resets the count. Must be nonzero when enabled.
    pub max_consecutive_timeouts: u32,
}

impl Default for RdmaProbeConfig {
    fn default() -> Self {
        Self {
            interval_ms: 60_000,
            timeout_ms: 1_000,
            max_consecutive_timeouts: 3,
        }
    }
}

impl RdmaProbeConfig {
    fn validate(&self) -> Result<()> {
        if self.interval_ms == 0 {
            return Ok(());
        }
        if self.timeout_ms == 0 {
            return Err(invalid_config(
                "rdma.probe.timeout_ms must be nonzero when probing is enabled",
            ));
        }
        if self.max_consecutive_timeouts == 0 {
            return Err(invalid_config(
                "rdma.probe.max_consecutive_timeouts must be nonzero when probing is enabled",
            ));
        }
        Ok(())
    }
}

/// RDMA READ timeout, concurrency and bandwidth settings.
#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaRemoteMemoryConfig {
    /// READ admission and completion budget in milliseconds. Zero disables
    /// this budget; the request deadline still applies.
    pub read_timeout_ms: u64,
    /// Shared in-flight RDMA READ work-request limit per local NIC.
    pub max_inflight_read_wrs: u32,
    /// Fraction of nominal port bandwidth available to remote I/O.
    /// Zero disables bandwidth limiting.
    pub bandwidth_limit_ratio: f64,
    pub bandwidth_limit_burst_ms: u64,
    /// Maximum bandwidth-budget wait. Zero rejects immediately.
    pub bandwidth_limit_max_wait_ms: u64,
}

impl Default for RdmaRemoteMemoryConfig {
    fn default() -> Self {
        Self {
            read_timeout_ms: 10_000,
            max_inflight_read_wrs: 32,
            bandwidth_limit_ratio: 0.95,
            bandwidth_limit_burst_ms: 0,
            bandwidth_limit_max_wait_ms: 1_000,
        }
    }
}

impl RdmaRemoteMemoryConfig {
    fn validate(&self) -> Result<()> {
        if self.max_inflight_read_wrs == 0 {
            return Err(invalid_config(
                "rdma.remote_memory.max_inflight_read_wrs must be nonzero",
            ));
        }
        if !self.bandwidth_limit_ratio.is_finite()
            || !(0.0..=1.0).contains(&self.bandwidth_limit_ratio)
        {
            return Err(invalid_config(
                "rdma.remote_memory.bandwidth_limit_ratio must be finite and between 0 and 1",
            ));
        }
        Ok(())
    }
}

/// Groups of CIDRs that describe RDMA connectivity domains.
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone)]
#[serde(transparent)]
pub struct RdmaSubnetDomains(Vec<Vec<ipnet::IpNet>>);

impl RdmaSubnetDomains {
    pub fn new(domains: Vec<Vec<ipnet::IpNet>>) -> Self {
        Self(domains)
    }

    pub fn domains(&self) -> &[Vec<ipnet::IpNet>] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn try_from_string(value: &str) -> Result<Self> {
        value.parse()
    }
}

impl std::fmt::Display for RdmaSubnetDomains {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (domain_index, domain) in self.0.iter().enumerate() {
            if domain_index > 0 {
                f.write_str(";")?;
            }
            for (subnet_index, subnet) in domain.iter().enumerate() {
                if subnet_index > 0 {
                    f.write_str(",")?;
                }
                subnet.fmt(f)?;
            }
        }
        Ok(())
    }
}

impl std::str::FromStr for RdmaSubnetDomains {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.trim().is_empty() {
            return Ok(Self::default());
        }
        let mut domains = Vec::new();
        for (domain_index, domain) in value.split(';').enumerate() {
            if domain.trim().is_empty() {
                return Err(invalid_config(format!(
                    "RDMA subnet domain {} is empty",
                    domain_index + 1
                )));
            }
            let mut subnets = Vec::new();
            for (subnet_index, subnet) in domain.split(',').enumerate() {
                let subnet = subnet.trim();
                if subnet.is_empty() {
                    return Err(invalid_config(format!(
                        "RDMA subnet {} in domain {} is empty",
                        subnet_index + 1,
                        domain_index + 1
                    )));
                }
                subnets.push(subnet.parse().map_err(|error| {
                    invalid_config(format!(
                        "invalid RDMA subnet {subnet:?} at domain {}, position {}: {error}",
                        domain_index + 1,
                        subnet_index + 1
                    ))
                })?);
            }
            domains.push(subnets);
        }
        Ok(Self(domains))
    }
}

impl From<Vec<Vec<ipnet::IpNet>>> for RdmaSubnetDomains {
    fn from(domains: Vec<Vec<ipnet::IpNet>>) -> Self {
        Self::new(domains)
    }
}

/// Policy for selecting paths based on configured RDMA subnets.
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum RdmaSubnetPolicy {
    #[default]
    Prefer,
    Require,
}

/// Queue Pair capabilities requested for an RDMA connection.
#[derive(Deserialize, Serialize, JsonSchema, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(default, deny_unknown_fields)]
pub struct RdmaQueuePairConfig {
    pub max_send_wr: u32,
    pub max_recv_wr: u32,
    pub max_send_sge: u32,
    pub max_recv_sge: u32,
}

impl Default for RdmaQueuePairConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 16,
            max_recv_sge: 1,
        }
    }
}

fn invalid_config(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidArgument, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorKind, SocketPoolConfig};

    #[test]
    fn partial_nested_config_uses_struct_defaults() {
        let config: SocketPoolConfig = serde_json::from_str(
            r#"{"rdma":{"connection":{"recv_queue_len":16},"path":{"device_exclude":["mlx5_1"],"allow_down_ports":true,"subnet_policy":"require"}}}"#,
        )
        .unwrap();
        let rdma = config.rdma.unwrap();
        assert_eq!(rdma.connection.recv_queue_len, 16);
        assert_eq!(rdma.connection.qp, RdmaQueuePairConfig::default());
        assert_eq!(rdma.polling, RdmaPollingConfig::default());
        assert_eq!(rdma.path.device_exclude, ["mlx5_1"]);
        assert!(rdma.path.allow_down_ports);
        assert_eq!(rdma.path.subnet_policy, RdmaSubnetPolicy::Require);
        assert_eq!(rdma.peers, RdmaPeerPoolConfig::default());
        assert_eq!(rdma.maintenance, RdmaMaintenanceConfig::default());
        assert_eq!(rdma.probe, RdmaProbeConfig::default());
        assert_eq!(rdma.remote_memory, RdmaRemoteMemoryConfig::default());
    }

    #[test]
    fn rust_and_serde_defaults_are_identical() {
        assert_eq!(
            serde_json::from_str::<RdmaSocketPoolConfig>("{}").unwrap(),
            RdmaSocketPoolConfig::default()
        );
        assert!(!RdmaPathPolicyConfig::default().allow_down_ports);
        assert_eq!(
            serde_json::from_str::<RdmaQueuePairConfig>(r#"{"max_send_wr":128}"#).unwrap(),
            RdmaQueuePairConfig {
                max_send_wr: 128,
                ..Default::default()
            }
        );
    }

    #[test]
    fn large_message_limits_and_byte_budgets_are_validated() {
        let defaults = RdmaConnectionTuningConfig::default();
        assert_eq!(defaults.max_large_msg_size, 64 * 1024 * 1024 - 1);
        assert_eq!(defaults.max_inflight_large_bytes, 128 * 1024 * 1024);
        defaults.validate().unwrap();

        for config in [
            RdmaConnectionTuningConfig {
                max_large_msg_size: defaults.max_msg_size - 1,
                ..defaults.clone()
            },
            RdmaConnectionTuningConfig {
                max_large_msg_size: RdmaConnectionTuningConfig::MAX_LARGE_MSG_SIZE + 1,
                ..defaults.clone()
            },
            RdmaConnectionTuningConfig {
                max_inflight_large_bytes: 0,
                ..defaults.clone()
            },
            RdmaConnectionTuningConfig {
                max_inflight_large_bytes: defaults.max_large_msg_size - 1,
                ..defaults.clone()
            },
        ] {
            assert!(config.validate().is_err());
        }

        RdmaConnectionTuningConfig {
            max_large_msg_size: 0,
            max_inflight_large_bytes: 1,
            ..defaults.clone()
        }
        .validate()
        .unwrap();
        RdmaConnectionTuningConfig {
            max_inflight_large_bytes: defaults.max_large_msg_size,
            ..defaults
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn rejects_invalid_effective_limits() {
        let mut config = RdmaSocketPoolConfig::default();
        config.connection.recv_queue_len = 0;
        assert!(config.validate().is_err());
        config.connection = RdmaConnectionTuningConfig::default();
        config.connection.qp.max_send_wr = 1;
        assert!(config.validate().is_err());
        config.connection = RdmaConnectionTuningConfig::default();
        config.connection.qp.max_send_sge = 0;
        assert!(config.validate().is_err());
        config.connection = RdmaConnectionTuningConfig::default();
        config.polling.dispatch_workers = 0;
        assert!(config.validate().is_err());
        config.polling = RdmaPollingConfig::default();
        config.remote_memory.max_inflight_read_wrs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn inline_limits_share_the_frame_boundary_when_large_messages_are_disabled() {
        let mut config = RdmaConnectionTuningConfig {
            max_large_msg_size: 0,
            ..Default::default()
        };
        for limit in [
            RdmaConnectionTuningConfig::MIN_MAX_MSG_SIZE,
            RdmaConnectionTuningConfig::MAX_LARGE_MSG_SIZE,
        ] {
            config.max_msg_size = limit;
            config.validate().unwrap();
        }
        for limit in [
            RdmaConnectionTuningConfig::MIN_MAX_MSG_SIZE - 1,
            MAX_MSG_SIZE as u32,
            u32::MAX,
        ] {
            config.max_msg_size = limit;
            assert_eq!(
                config.validate().unwrap_err().kind,
                ErrorKind::InvalidArgument
            );
        }
    }

    #[test]
    fn probe_defaults_and_nested_roundtrip() {
        let defaults = RdmaProbeConfig::default();
        assert_eq!(defaults.interval_ms, 60_000);
        assert_eq!(defaults.timeout_ms, 1_000);
        assert_eq!(defaults.max_consecutive_timeouts, 3);
        defaults.validate().unwrap();
        assert_eq!(
            serde_json::from_str::<RdmaProbeConfig>("{}").unwrap(),
            defaults
        );

        let config: SocketPoolConfig = serde_json::from_value(serde_json::json!({
            "rdma": { "probe": { "interval_ms": 250, "timeout_ms": 50 } }
        }))
        .unwrap();
        assert_eq!(
            config.rdma.as_ref().unwrap().probe,
            RdmaProbeConfig {
                interval_ms: 250,
                timeout_ms: 50,
                ..defaults
            }
        );
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(
            encoded["rdma"]["probe"],
            serde_json::json!({
                "interval_ms": 250,
                "timeout_ms": 50,
                "max_consecutive_timeouts": 3
            })
        );
        assert_eq!(
            serde_json::from_value::<SocketPoolConfig>(encoded).unwrap(),
            config
        );
        assert!(
            serde_json::from_value::<SocketPoolConfig>(serde_json::json!({
                "rdma": { "probe": { "timeout": 50 } }
            }))
            .is_err()
        );
    }

    #[test]
    fn probe_validation_requires_nonzero_budget_and_threshold_when_enabled() {
        for probe in [
            RdmaProbeConfig {
                timeout_ms: 0,
                ..Default::default()
            },
            RdmaProbeConfig {
                max_consecutive_timeouts: 0,
                ..Default::default()
            },
        ] {
            let config = RdmaSocketPoolConfig {
                probe,
                ..Default::default()
            };
            let error = config.validate().unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidArgument);
        }

        RdmaSocketPoolConfig {
            probe: RdmaProbeConfig {
                interval_ms: 0,
                timeout_ms: 0,
                max_consecutive_timeouts: 0,
            },
            ..Default::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn rdma_bandwidth_limit_validation() {
        for ratio in [0.0, 0.95, 1.0] {
            let mut config = RdmaSocketPoolConfig::default();
            config.remote_memory.bandwidth_limit_ratio = ratio;
            assert!(config.validate().is_ok());
        }
        for ratio in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
            let mut config = RdmaSocketPoolConfig::default();
            config.remote_memory.bandwidth_limit_ratio = ratio;
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn rdma_subnet_domains_string_roundtrip() {
        let domains =
            RdmaSubnetDomains::try_from_string(" 10.11.0.0/16, 10.12.0.0/16 ; 2001:db8::/32 ")
                .unwrap();
        assert_eq!(
            domains.to_string(),
            "10.11.0.0/16,10.12.0.0/16;2001:db8::/32"
        );
        assert_eq!(
            serde_json::to_value(&domains).unwrap(),
            serde_json::json!([["10.11.0.0/16", "10.12.0.0/16"], ["2001:db8::/32"]])
        );
        assert_eq!(
            domains.to_string().parse::<RdmaSubnetDomains>().unwrap(),
            domains
        );
        assert!(RdmaSubnetDomains::try_from_string("").unwrap().is_empty());
    }

    #[test]
    fn rdma_subnet_domains_reject_invalid_strings() {
        for value in [
            ";",
            "10.0.0.0/8;",
            ";10.0.0.0/8",
            "10.0.0.0/8,,10.1.0.0/16",
            "invalid",
        ] {
            let error = RdmaSubnetDomains::try_from_string(value).unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidArgument, "{value}");
        }
    }
}
