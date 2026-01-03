use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Global Identifier (GID) type for InfiniBand/RoCE networks.
///
/// Different GID types represent different network layer protocols:
/// - IB: Native InfiniBand
/// - RoCEv1: RDMA over Converged Ethernet version 1
/// - RoCEv2: RDMA over Converged Ethernet version 2
/// - Other: Custom or unrecognized GID type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum GidType {
    /// Native InfiniBand.
    IB,
    /// RDMA over Converged Ethernet version 1.
    RoCEv1,
    /// RDMA over Converged Ethernet version 2.
    RoCEv2,
    /// Custom or unrecognized GID type.
    Other(String),
}

/// Configuration for RDMA device selection and filtering.
///
/// The `Config` allows filtering which RDMA devices and ports
/// to use based on various criteria.
#[derive(Debug, Default)]
pub struct Config {
    /// Device-specific configuration.
    pub device: DeviceConfig,
}

/// Device-level configuration for RDMA device filtering.
///
/// Controls which devices, ports, and GID types are selected
/// for RDMA operations.
#[derive(Debug, Default)]
pub struct DeviceConfig {
    /// Set of device names to include. Empty means all devices.
    pub device_filter: HashSet<String>,
    /// Set of GID types to include. Empty means all types.
    pub gid_type_filter: HashSet<GidType>,
    /// Whether to skip inactive ports during device enumeration.
    pub skip_inactive_port: bool,
    /// For RoCE v2, whether to skip link-local addresses.
    pub roce_v2_skip_link_local_addr: bool,
}
