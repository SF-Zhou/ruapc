//! RDMA path awareness: NIC identities, path selectors and path reports.
//!
//! A *path* is the pair of NICs an RDMA connection runs on. Peers are still
//! identified by their bootstrap TCP address; the path is a property of
//! each individual connection (stripe), giving full NIC visibility and
//! control without changing the peer identity.

use std::net::{IpAddr, SocketAddr};

use ruapc_rdma::ibv_gid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Best-effort IP address carried by a GID.
///
/// RoCE v2 GIDs embed the IP address of the associated net device
/// (IPv4-mapped for RoCE v2 over IPv4). IB and RoCE v1 GIDs are link-local
/// EUI-64 values that do not correspond to an IP, so they yield `None`.
pub(crate) fn gid_ip(gid: &ibv_gid) -> Option<IpAddr> {
    if gid.is_null() {
        return None;
    }
    let v6 = gid.as_ipv6();
    if v6.is_unicast_link_local() {
        return None;
    }
    Some(v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4))
}

/// Identity of one NIC endpoint (device + port + GID) of an RDMA path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RdmaNicInfo {
    /// RDMA device name (e.g. `mlx5_0`).
    pub device: String,
    /// Port number on the device (1-based).
    pub port_num: u8,
    /// GID index used on that port.
    pub gid_index: u8,
    /// IP address carried by the GID (RoCE v2 only; `None` for IB/RoCE v1).
    pub ip: Option<IpAddr>,
}

/// The pair of NICs an RDMA connection runs on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RdmaPathInfo {
    /// NIC on this side of the connection.
    pub local: RdmaNicInfo,
    /// NIC on the peer side of the connection.
    pub remote: RdmaNicInfo,
}

/// Selects a NIC either by device name or by the IP its GID is derived
/// from (RoCE v2). IP selectors never match IB / RoCE v1 NICs, whose GIDs
/// carry no IP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum NicSelector {
    /// Match by RDMA device name (e.g. `mlx5_0`).
    Device(String),
    /// Match by the IP address embedded in the NIC's RoCE v2 GID.
    Ip(IpAddr),
}

impl NicSelector {
    /// Whether `nic` satisfies this selector.
    #[must_use]
    pub fn matches(&self, nic: &RdmaNicInfo) -> bool {
        match self {
            NicSelector::Device(name) => nic.device == *name,
            NicSelector::Ip(ip) => nic.ip == Some(*ip),
        }
    }
}

/// Constrains which (local NIC, remote NIC) pair an RDMA connection may
/// use. `None` on a side leaves that side unconstrained.
///
/// Attached to a [`Context`](crate::Context) via
/// [`with_rdma_path`](crate::Context::with_rdma_path): the request then
/// only uses (or establishes) connections whose path matches.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RdmaPathSelector {
    /// Constraint on the local NIC.
    pub local: Option<NicSelector>,
    /// Constraint on the remote NIC.
    pub remote: Option<NicSelector>,
}

impl RdmaPathSelector {
    /// Selects a specific local NIC by device name.
    #[must_use]
    pub fn local_device(name: impl Into<String>) -> Self {
        Self {
            local: Some(NicSelector::Device(name.into())),
            remote: None,
        }
    }

    /// Selects a specific remote NIC by device name.
    #[must_use]
    pub fn remote_device(name: impl Into<String>) -> Self {
        Self {
            local: None,
            remote: Some(NicSelector::Device(name.into())),
        }
    }

    /// Selects a specific local NIC by GID-embedded IP (RoCE v2).
    #[must_use]
    pub fn local_ip(ip: IpAddr) -> Self {
        Self {
            local: Some(NicSelector::Ip(ip)),
            remote: None,
        }
    }

    /// Selects a specific remote NIC by GID-embedded IP (RoCE v2).
    #[must_use]
    pub fn remote_ip(ip: IpAddr) -> Self {
        Self {
            local: None,
            remote: Some(NicSelector::Ip(ip)),
        }
    }

    /// Whether `path` satisfies both sides of this selector.
    #[must_use]
    pub fn matches(&self, path: &RdmaPathInfo) -> bool {
        self.local.as_ref().is_none_or(|s| s.matches(&path.local))
            && self.remote.as_ref().is_none_or(|s| s.matches(&path.remote))
    }
}

/// Direction of an RDMA connection relative to this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum RdmaConnDirection {
    /// Established by this side (client role).
    Outbound,
    /// Accepted from a peer (server role).
    Inbound,
}

/// One RDMA connection and the path it runs on.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RdmaPathEntry {
    /// Bootstrap TCP address of the peer (outbound connections only).
    pub peer: Option<SocketAddr>,
    /// Whether this side established or accepted the connection.
    pub direction: RdmaConnDirection,
    /// The NIC pair the connection runs on.
    pub path: RdmaPathInfo,
    /// Local queue pair number.
    pub qp_num: u32,
    /// Whether the connection is usable (not in the error state).
    pub healthy: bool,
    /// Whether the connection was created for an explicit path selector;
    /// pinned connections are exempt from rebalancing.
    pub pinned: bool,
}

/// Live connection count of one local RDMA device.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RdmaDeviceLoad {
    /// RDMA device name.
    pub device: String,
    /// Live connections (outbound + inbound) on this device.
    pub connections: usize,
}

/// Snapshot of all live RDMA connections and per-device load.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RdmaPathReport {
    /// Per-device live connection counts.
    pub devices: Vec<RdmaDeviceLoad>,
    /// Every live connection with its path.
    pub paths: Vec<RdmaPathEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_gid(addr: &str) -> ibv_gid {
        let bits = addr.parse::<std::net::Ipv6Addr>().unwrap().to_bits();
        let mut gid = ibv_gid::default();
        gid.global.subnet_prefix = ((bits >> 64) as u64).to_be();
        gid.global.interface_id = (bits as u64).to_be();
        gid
    }

    fn nic(device: &str, ip: Option<&str>) -> RdmaNicInfo {
        RdmaNicInfo {
            device: device.into(),
            port_num: 1,
            gid_index: 0,
            ip: ip.map(|s| s.parse().unwrap()),
        }
    }

    #[test]
    fn test_gid_ip_v4_mapped() {
        assert_eq!(
            gid_ip(&make_gid("::ffff:10.1.2.3")),
            Some("10.1.2.3".parse().unwrap())
        );
    }

    #[test]
    fn test_gid_ip_v6_global() {
        assert_eq!(
            gid_ip(&make_gid("2001:db8::1")),
            Some("2001:db8::1".parse().unwrap())
        );
    }

    #[test]
    fn test_gid_ip_link_local_and_null() {
        // IB / RoCE v1 style link-local GIDs carry no IP.
        assert_eq!(gid_ip(&make_gid("fe80::a288:c2ff:fe32:1a74")), None);
        assert_eq!(gid_ip(&ibv_gid::default()), None);
    }

    #[test]
    fn test_nic_selector_matches() {
        let n = nic("mlx5_0", Some("10.0.0.1"));
        assert!(NicSelector::Device("mlx5_0".into()).matches(&n));
        assert!(!NicSelector::Device("mlx5_1".into()).matches(&n));
        assert!(NicSelector::Ip("10.0.0.1".parse().unwrap()).matches(&n));
        assert!(!NicSelector::Ip("10.0.0.2".parse().unwrap()).matches(&n));
        // IP selectors never match NICs without a GID-embedded IP.
        assert!(!NicSelector::Ip("10.0.0.1".parse().unwrap()).matches(&nic("mlx5_0", None)));
    }

    #[test]
    fn test_path_selector_matches() {
        let path = RdmaPathInfo {
            local: nic("mlx5_0", Some("10.0.0.1")),
            remote: nic("mlx5_1", Some("10.0.0.2")),
        };
        assert!(RdmaPathSelector::default().matches(&path));
        assert!(RdmaPathSelector::local_device("mlx5_0").matches(&path));
        assert!(!RdmaPathSelector::local_device("mlx5_1").matches(&path));
        assert!(RdmaPathSelector::remote_device("mlx5_1").matches(&path));
        assert!(RdmaPathSelector::remote_ip("10.0.0.2".parse().unwrap()).matches(&path));
        assert!(!RdmaPathSelector::remote_ip("10.0.0.9".parse().unwrap()).matches(&path));
        let both = RdmaPathSelector {
            local: Some(NicSelector::Device("mlx5_0".into())),
            remote: Some(NicSelector::Ip("10.0.0.9".parse().unwrap())),
        };
        assert!(!both.matches(&path));
    }

    #[test]
    fn test_selector_serde_roundtrip() {
        let selector = RdmaPathSelector {
            local: Some(NicSelector::Device("mlx5_0".into())),
            remote: Some(NicSelector::Ip("10.0.0.2".parse().unwrap())),
        };
        let json = serde_json::to_string(&selector).unwrap();
        let recovered: RdmaPathSelector = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, selector);
    }
}
