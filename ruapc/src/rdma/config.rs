use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;

/// Configuration for RDMA Queue Pair (QP) creation and state transitions.
///
/// All fields have sensible defaults so that users can create an RDMA connection
/// without specifying any configuration.
///
/// # Examples
///
/// ```rust
/// use ruapc::RdmaConfig;
///
/// // Use all defaults.
/// let config = RdmaConfig::default();
///
/// // Customize specific fields.
/// let config = RdmaConfig {
///     max_send_wr: 128,
///     max_recv_wr: 128,
///     ..Default::default()
/// };
/// ```
#[serde_inline_default]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RdmaConfig {
    // --- QP creation caps ---
    /// Maximum number of outstanding send work requests in the send queue.
    #[serde_inline_default(64)]
    pub max_send_wr: u32,

    /// Maximum number of outstanding receive work requests in the receive queue.
    #[serde_inline_default(64)]
    pub max_recv_wr: u32,

    /// Maximum number of scatter/gather elements per send work request.
    #[serde_inline_default(1)]
    pub max_send_sge: u32,

    /// Maximum number of scatter/gather elements per receive work request.
    #[serde_inline_default(1)]
    pub max_recv_sge: u32,

    /// Maximum inline data size for send operations (0 to disable).
    #[serde_inline_default(0)]
    pub max_inline_data: u32,

    // --- Completion queue ---
    /// Completion queue depth.
    #[serde_inline_default(128)]
    pub cq_size: u32,

    // --- QP INIT state ---
    /// Partition key index used during QP INIT transition.
    #[serde_inline_default(0)]
    pub pkey_index: u16,

    // --- QP RTR state ---
    /// Maximum number of outstanding RDMA read/atomic operations as destination.
    #[serde_inline_default(1)]
    pub max_dest_rd_atomic: u8,

    /// Minimum RNR NAK timer field value (delay before retrying after RNR NAK).
    #[serde_inline_default(0x12)]
    pub min_rnr_timer: u8,

    // --- QP RTS state ---
    /// Local ACK timeout (= 4.096 μs × 2^timeout). 0x12 ≈ 1 second.
    #[serde_inline_default(0x12)]
    pub timeout: u8,

    /// Number of retries for transport-level errors before reporting failure.
    #[serde_inline_default(6)]
    pub retry_cnt: u8,

    /// Number of RNR retries (7 = infinite).
    #[serde_inline_default(6)]
    pub rnr_retry: u8,

    /// Maximum number of outstanding RDMA read/atomic operations as initiator.
    #[serde_inline_default(1)]
    pub max_rd_atomic: u8,

    // --- Event loop ---
    /// Number of receive buffers to pre-post during connection establishment.
    #[serde_inline_default(64)]
    pub recv_buffer_count: u32,
}

impl Default for RdmaConfig {
    fn default() -> Self {
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdma_config_default_values() {
        let config = RdmaConfig::default();
        assert_eq!(config.max_send_wr, 64);
        assert_eq!(config.max_recv_wr, 64);
        assert_eq!(config.max_send_sge, 1);
        assert_eq!(config.max_recv_sge, 1);
        assert_eq!(config.max_inline_data, 0);
        assert_eq!(config.cq_size, 128);
        assert_eq!(config.pkey_index, 0);
        assert_eq!(config.max_dest_rd_atomic, 1);
        assert_eq!(config.min_rnr_timer, 0x12);
        assert_eq!(config.timeout, 0x12);
        assert_eq!(config.retry_cnt, 6);
        assert_eq!(config.rnr_retry, 6);
        assert_eq!(config.max_rd_atomic, 1);
        assert_eq!(config.recv_buffer_count, 64);
    }

    #[test]
    fn test_rdma_config_serde_roundtrip() {
        let config = RdmaConfig {
            max_send_wr: 128,
            max_recv_wr: 256,
            pkey_index: 1,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let recovered: RdmaConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, config);
    }

    #[test]
    fn test_rdma_config_partial_deserialization() {
        // Only specifying a subset of fields should use defaults for the rest.
        let json = r#"{"max_send_wr": 256}"#;
        let config: RdmaConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_send_wr, 256);
        assert_eq!(config.max_recv_wr, 64); // default
        assert_eq!(config.pkey_index, 0); // default
    }
}
