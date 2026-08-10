//! RDMA path awareness: NIC identities and path reports.
//!
//! A *path* is the pair of NICs an RDMA connection runs on. Peers are still
//! identified by their bootstrap TCP address; the path is a property of
//! each individual connection (stripe), giving full NIC visibility without
//! changing the peer identity.

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
    /// Virtual zone names assigned from the selected GID's netdev address.
    pub zones: Vec<String>,
}

/// The pair of NICs an RDMA connection runs on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RdmaPathInfo {
    /// NIC on this side of the connection.
    pub local: RdmaNicInfo,
    /// NIC on the peer side of the connection.
    pub remote: RdmaNicInfo,
}

/// Direction of an RDMA connection relative to this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum RdmaConnDirection {
    /// Established by this side (client role).
    Outbound,
    /// Accepted from a peer (server role).
    Inbound,
}

/// Lifecycle phase of an outbound RDMA stripe.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum StripePhase {
    #[default]
    Active,
    Draining,
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
    /// Active connections are selectable; draining connections only finish
    /// requests that were already in flight.
    #[serde(default)]
    pub phase: StripePhase,
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
}
