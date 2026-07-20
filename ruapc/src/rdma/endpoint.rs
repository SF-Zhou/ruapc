use ruapc_rdma::{LinkLayer, ibv_gid, ibv_mtu};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::RdmaQueuePairConfig;

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
    /// Initial packet sequence number this endpoint will use on its send
    /// queue (the peer programs it as `rq_psn`).
    ///
    /// Randomized per QP: qp numbers are recycled by the driver, and a new
    /// QP reusing the (qp_num, GID) pair of a recently destroyed one with a
    /// predictable PSN can silently blackhole against stale peer state.
    pub psn: u32,
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

/// Queue Pair and completion queue settings for this RDMA connection.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct RdmaConnectionConfig {
    /// Negotiated Queue Pair capabilities.
    pub qp: RdmaQueuePairConfig,
    /// Completion Queue length requested for this connection.
    pub cq_len: u32,
    /// Number of receive buffers pre-posted by this endpoint.
    pub recv_queue_len: u32,
    /// Maximum serialized message size accepted by this endpoint; the
    /// receive buffers are sized accordingly. Negotiated as the minimum of
    /// both sides.
    pub max_msg_size: u32,
}

/// RDMA connection request sent after the client has selected a server port.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct ConnectRequest {
    /// Client endpoint to connect with.
    pub endpoint: Endpoint,
    /// Server device/port/GID that should accept this connection.
    pub target: DeviceSelection,
    /// Queue Pair settings negotiated by the client for this connection.
    pub config: RdmaConnectionConfig,
}
