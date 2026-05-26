use ruapc_rdma::{LinkLayer, ibv_gid, ibv_mtu};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::rdma_service::{RdmaQpCaps, RdmaQpParams};

/// RDMA connection endpoint information.
///
/// Contains the QP and address metadata needed to move a queue pair to RTR/RTS.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct Endpoint {
    /// Queue pair number.
    pub qp_num: u32,
    /// Local port number used by this QP.
    pub port_num: u8,
    /// Local GID index used by this QP.
    pub gid_index: u8,
    /// Local Identifier for InfiniBand routing.
    pub lid: u16,
    /// Global Identifier for RoCE routing.
    pub gid: ibv_gid,
    /// Link layer for this endpoint.
    pub link_layer: LinkLayer,
    /// Active MTU for the selected port.
    pub active_mtu: ibv_mtu,
}

/// Server-side RDMA device/port/GID selected by the client.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct DeviceSelection {
    /// RDMA device name, such as mlx5_0.
    pub device_name: String,
    /// Target port number on the device.
    pub port_num: u8,
    /// Target GID index on the port.
    pub gid_index: u8,
}

/// RDMA connection request sent after the client has selected a server port.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct ConnectRequest {
    /// Client endpoint to connect with.
    pub endpoint: Endpoint,
    /// Server device/port/GID that should accept this connection.
    pub target: DeviceSelection,
    /// Negotiated QP capabilities the client chose (bounded by server's advertised caps).
    pub qp_caps: RdmaQpCaps,
    /// Negotiated QP state transition parameters.
    pub qp_params: RdmaQpParams,
}
