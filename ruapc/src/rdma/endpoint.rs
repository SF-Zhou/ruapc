use ruapc_rdma::ibv_gid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// RDMA connection endpoint information.
///
/// Contains the necessary information to establish an RDMA connection
/// between two queue pairs, including queue pair number, local ID, and GID.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct Endpoint {
    /// Queue pair number.
    pub qp_num: u32,
    /// Local Identifier for InfiniBand routing.
    pub lid: u16,
    /// Global Identifier for RoCE routing.
    pub gid: ibv_gid,
}
