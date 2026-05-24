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

use crate::{Guid, ibv_device_attr, ibv_gid, ibv_port_attr, ibv_transport_type};

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
    pub index: u16,
    /// The GID value.
    pub gid: ibv_gid,
    /// The type of this GID.
    pub gid_type: GidType,
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
