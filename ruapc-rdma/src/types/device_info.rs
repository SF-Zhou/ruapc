//! # Device data types
//!
//! This module contains serializable data structures representing RDMA device
//! information, ports, and GIDs.
//!
//! ## Types
//!
//! - [`DeviceInfo`]: Complete device metadata including name, GUID, attributes, and ports
//! - [`Port`]: Port information with attributes and GID list
//! - [`Gid`]: Global Identifier entry with type classification
//!
//! All types derive `Serialize`, `Deserialize`, and `JsonSchema` for use in
//! configuration and API responses.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::{Guid, LinkLayer, ibv_device_attr, ibv_gid, ibv_port_attr, ibv_transport_type};

/// Information about an RDMA device.
///
/// Contains device metadata including name, GUID, attributes,
/// and available ports with their GIDs.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, Clone)]
pub struct DeviceInfo {
    /// Device name (e.g., "mlx5_0").
    pub name: String,
    /// Globally unique identifier for the device.
    pub guid: Guid,
    /// Transport type of the device.
    pub transport_type: ibv_transport_type,
    /// Path to the device in sysfs.
    pub ibdev_path: PathBuf,
    /// Device attributes including capabilities.
    pub device_attr: ibv_device_attr,
    /// Available ports on this device.
    pub ports: Vec<Port>,
}

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

/// Global Identifier (GID) information for a port.
///
/// A GID uniquely identifies a port on an RDMA network and
/// includes the GID type (IB, RoCEv1, RoCEv2).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Gid {
    /// GID index on the port.
    pub index: u8,
    /// The GID value.
    pub gid: ibv_gid,
    /// The type of this GID.
    pub gid_type: GidType,
}

impl Gid {
    /// Builds a `Gid` entry, returning `None` if the GID cannot be used to
    /// communicate with remote peers.
    ///
    /// RoCE v2 GIDs are derived from the IP addresses assigned to the
    /// associated net device. GIDs derived from loopback or IPv6 link-local
    /// (`fe80::/10`) addresses are not routable and cannot be used to reach
    /// remote hosts, so they are rejected.
    ///
    /// IB and RoCE v1 GIDs always use the link-local `fe80::/64` subnet
    /// prefix by design (IB default subnet prefix / MAC-derived EUI-64), so
    /// the link-local filter must not be applied to them.
    pub fn usable(index: u8, gid: ibv_gid, gid_type: GidType) -> Option<Self> {
        let usable = match gid_type {
            GidType::RoCEv2 => {
                let addr = gid.as_ipv6();
                !addr.is_loopback() && !addr.is_unicast_link_local()
            }
            GidType::IB | GidType::RoCEv1 | GidType::Other(_) => true,
        };
        usable.then_some(Self {
            index,
            gid,
            gid_type,
        })
    }
}

/// RDMA device port information.
///
/// Contains port attributes and the list of available GIDs
/// for that port.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Port {
    /// Port number (1-based).
    pub port_num: u8,
    /// The attributes of the port.
    pub port_attr: ibv_port_attr,
    /// The GID (Global Identifier) list of the port.
    pub gids: Vec<Gid>,
}

impl Port {
    /// Looks up a GID entry by its index in the port GID table.
    pub fn find_gid(&self, gid_index: u8) -> Option<&Gid> {
        self.gids.iter().find(|gid| gid.index == gid_index)
    }

    /// Returns `true` if the port can carry RDMA traffic: it is active and
    /// uses a supported link layer (InfiniBand or Ethernet).
    pub fn is_usable(&self) -> bool {
        matches!(
            self.port_attr.state,
            crate::ibv_port_state::IBV_PORT_ACTIVE | crate::ibv_port_state::IBV_PORT_ACTIVE_DEFER
        ) && matches!(
            self.port_attr.link_layer,
            LinkLayer::InfiniBand | LinkLayer::Ethernet
        )
    }
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

    fn usable(addr: &str, gid_type: GidType) -> bool {
        Gid::usable(0, make_gid(addr), gid_type).is_some()
    }

    #[test]
    fn test_rocev2_link_local_gid_not_usable() {
        assert!(!usable("fe80::a288:c2ff:fe32:1a74", GidType::RoCEv2));
    }

    #[test]
    fn test_rocev2_loopback_gid_not_usable() {
        assert!(!usable("::1", GidType::RoCEv2));
    }

    #[test]
    fn test_rocev2_routable_gids_usable() {
        // IPv4-mapped GID (RoCE v2 over IPv4).
        assert!(usable("::ffff:10.0.0.1", GidType::RoCEv2));

        // Global unicast IPv6 GID.
        assert!(usable("2001:db8::1", GidType::RoCEv2));
    }

    #[test]
    fn test_link_local_prefix_usable_for_ib_and_rocev1() {
        // IB and RoCE v1 GIDs always live in fe80::/64 by design; the
        // link-local filter must not remove them.
        assert!(usable("fe80::a288:c2ff:fe32:1a74", GidType::IB));
        assert!(usable("fe80::a288:c2ff:fe32:1a74", GidType::RoCEv1));
        assert!(usable(
            "fe80::a288:c2ff:fe32:1a74",
            GidType::Other("custom".into())
        ));
    }

    #[test]
    fn test_usable_keeps_index_and_type() {
        let gid = Gid::usable(3, make_gid("::ffff:192.168.1.10"), GidType::RoCEv2).unwrap();
        assert_eq!(gid.index, 3);
        assert_eq!(gid.gid_type, GidType::RoCEv2);
    }

    #[test]
    fn test_port_is_usable() {
        let mut port = Port {
            port_num: 1,
            port_attr: ibv_port_attr::default(),
            gids: Vec::new(),
        };
        // Zeroed attrs: NOP state, unspecified link layer.
        assert!(!port.is_usable());

        port.port_attr.state = crate::ibv_port_state::IBV_PORT_ACTIVE;
        port.port_attr.link_layer = LinkLayer::Ethernet;
        assert!(port.is_usable());

        port.port_attr.state = crate::ibv_port_state::IBV_PORT_DOWN;
        assert!(!port.is_usable());

        port.port_attr.state = crate::ibv_port_state::IBV_PORT_ACTIVE;
        port.port_attr.link_layer = LinkLayer::Unspecified;
        assert!(!port.is_usable());
    }

    #[test]
    fn test_port_find_gid() {
        let gid = Gid::usable(5, make_gid("::ffff:10.0.0.1"), GidType::RoCEv2).unwrap();
        let port = Port {
            port_num: 1,
            port_attr: ibv_port_attr::default(),
            gids: vec![gid],
        };
        assert_eq!(port.find_gid(5).map(|g| g.index), Some(5));
        assert!(port.find_gid(0).is_none());
    }
}
