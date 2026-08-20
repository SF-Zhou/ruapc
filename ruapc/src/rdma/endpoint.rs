use ruapc_rdma::{LinkLayer, ibv_gid, ibv_mtu};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;

use crate::RdmaQueuePairConfig;

/// RDMA connection endpoint information.
///
/// Contains the QP and address metadata needed to move a queue pair to RTR/RTS.
#[serde_inline_default]
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct Endpoint {
    /// Process-unique identity of the accepted connection. Set by the
    /// accepting peer and used only for lifecycle control.
    #[serde_inline_default(0u64)]
    pub connection_cookie: u64,
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
    /// Device cap on concurrent RDMA READs per QP, advertised so both
    /// sides can program `max_rd_atomic` / `max_dest_rd_atomic` as the
    /// minimum of the two caps (they compute identical values, which the
    /// RC protocol requires). Higher values let batched `remote_read`
    /// work requests proceed in parallel inside the NIC. Peers that do
    /// not advertise a cap default to the conservative 1.
    #[serde_inline_default(1u8)]
    pub rd_atomic_cap: u8,
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
    /// GRH traffic class (RoCE: DSCP/ECN byte) programmed into both sides'
    /// address handles. Chosen by the connecting client; the server applies
    /// the client's value verbatim. Peers that do not send it use 0.
    /// Ignored on InfiniBand link layers (no GRH).
    #[serde(default)]
    pub traffic_class: u8,
}

/// RDMA connection request sent after the client has selected a server port.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct ConnectRequest {
    /// Random initiator token used to confirm or expire this accept. It is a
    /// lifecycle correlation ID, not an authentication credential.
    pub connection_id: u64,
    /// Client endpoint to connect with.
    pub endpoint: Endpoint,
    /// Name of the client-side RDMA device this connection originates
    /// from; gives the server full path (NIC pair) visibility.
    pub source_device: String,
    /// Whether the client matched both NIC addresses to one configured subnet.
    pub same_subnet: bool,
    /// Server device/port/GID that should accept this connection.
    pub target: DeviceSelection,
    /// Queue Pair settings negotiated by the client for this connection.
    pub config: RdmaConnectionConfig,
}

/// Identifies one accepted connection for lifecycle control RPCs.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct ConnectionControl {
    pub connection_id: u64,
    pub server_connection_cookie: u64,
}
