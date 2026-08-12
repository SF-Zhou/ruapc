//! Socket pool configuration structures. All defaults live in inline
//! serde attributes, so `Default` and "deserialize an empty object" are
//! one source of truth.

#[cfg(feature = "rdma")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;

use crate::ListenMode;

/// Socket pool configuration.
///
/// Configures listener behavior and transport resources.
///
/// # Examples
///
/// ```rust
/// use ruapc::{ListenMode, SocketPoolConfig};
///
/// let config = SocketPoolConfig {
///     listen_mode: ListenMode::TCP,
///     ..Default::default()
/// };
/// ```
#[serde_inline_default]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct SocketPoolConfig {
    /// How accepted TCP streams are interpreted. Outbound transport is part
    /// of each [`Endpoint`](crate::Endpoint), not this configuration.
    #[serde_inline_default(ListenMode::TCP)]
    pub listen_mode: ListenMode,
    /// Maximum memory of the shared buffer pool in bytes (0 = library
    /// default, currently 256 MiB). Size it for the workload: every RDMA
    /// connection pre-posts `recv_queue_len x max_msg_size` receive
    /// buffers, and in-flight sends allocate from the same pool.
    #[serde_inline_default(0usize)]
    pub buffer_pool_memory: usize,
    /// Maximum number of server-side requests processed concurrently;
    /// excess requests are rejected immediately with an `Overloaded` error
    /// response (load shedding). `0` disables the cap.
    #[serde_inline_default(0usize)]
    pub max_inflight_requests: usize,
    /// RDMA-specific settings. `None` disables RDMA device discovery,
    /// memory registration, and connection resources.
    #[cfg(feature = "rdma")]
    #[serde(default)]
    pub rdma: Option<RdmaSocketPoolConfig>,
}

impl Default for SocketPoolConfig {
    fn default() -> Self {
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap()
    }
}

/// RDMA socket pool configuration.
#[cfg(feature = "rdma")]
#[serde_inline_default]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct RdmaSocketPoolConfig {
    /// Requested Queue Pair capabilities for newly created RDMA connections.
    #[serde(default)]
    pub qp: RdmaQueuePairConfig,
    /// Completion Queue length requested for each RDMA connection.
    #[serde_inline_default(128u32)]
    pub cq_len: u32,
    /// Number of receive buffers to pre-post for each RDMA connection
    /// (negotiated to the minimum of both sides). The send window is half
    /// of it (the other half absorbs standalone ACKs).
    ///
    /// Deliberately small by default: a tight window makes high message
    /// rates overflow into the pending path, where the poll thread packs
    /// the backlog into few aggregated WRs — measured ~2x QPS on 1 KiB
    /// echo at high concurrency versus a deep window posting one WR per
    /// message, with no ping-pong latency cost (the window only binds
    /// beyond `recv_queue_len / 2` in-flight sends). Raise it for
    /// pipelines of large unaggregatable messages (up to `max_msg_size`
    /// each), where more in-flight WRs are needed to fill the wire.
    #[serde_inline_default(8u32)]
    pub recv_queue_len: u32,
    /// P_Key table index used when moving the Queue Pair to INIT.
    #[serde_inline_default(0u16)]
    pub pkey_index: u16,
    /// Selective signaling interval for data sends (local behavior, not
    /// negotiated). With interval `N > 1` only every Nth data send requests
    /// a completion; buffers of unsignaled sends are reclaimed when a later
    /// signaled completion arrives (RC SQs complete in order). `1` signals
    /// every send. Clamped to `max_send_wr / 2`.
    #[serde_inline_default(8u32)]
    pub send_signal_interval: u32,
    /// Capacity (entries) of the per-device shared completion queue used by
    /// the dedicated poll thread. Bounds the number of concurrent
    /// connections per device: the sum of every connection's queue depths
    /// must fit.
    #[serde_inline_default(65536u32)]
    pub device_cq_len: u32,
    /// Busy-poll window of the per-device poll thread, in microseconds:
    /// after the last completion the thread keeps polling for this long
    /// before arming the CQ interrupt and sleeping. `0` disables spinning
    /// (pure event-driven mode).
    #[serde_inline_default(50u64)]
    pub poll_spin_us: u64,
    /// Maximum serialized message size for RDMA sends; also the size of
    /// each pre-posted receive buffer (negotiated to the minimum of both
    /// sides). Larger payloads must use the remote read/write paths.
    #[serde_inline_default(256 * 1024u32)]
    pub max_msg_size: u32,
    /// Whether to aggregate sends: under backlog, multiple small
    /// window-blocked sends are packed into a single RDMA send, which
    /// consumes a single send-window credit and a single receive buffer
    /// on the peer. Send-side toggle only; every RDMA send is a sequence
    /// of length-prefixed frames, so receivers walk the same parse loop
    /// either way.
    #[serde_inline_default(true)]
    pub msg_aggregation: bool,
    /// Number of (shared CQ + poll thread) shards per RDMA device.
    /// Connections are assigned round-robin, spreading completion
    /// processing across cores. Each shard burns up to one core while
    /// spinning.
    #[serde_inline_default(1u32)]
    pub poll_threads_per_device: u32,
    /// Number of long-lived dispatch worker tasks shared by all RDMA poll
    /// threads of this pool, each owning one SPSC queue. Received buffers
    /// are batched per CQ drain and routed to a per-poll-thread home
    /// worker (spilling to further workers only under backlog pressure);
    /// the workers walk and parse the contained message frames and hand
    /// each to the router (requests) or waiter (responses). When every
    /// worker is saturated the poll thread falls back to spawning a
    /// one-shot task per batch, so it never blocks.
    #[serde_inline_default(32u32)]
    pub dispatch_workers: u32,
    /// Number of RDMA connections (QPs) to establish per peer. Requests
    /// are striped round-robin across them; combined with poll thread
    /// shards this scales single-peer throughput across cores. RPC
    /// messages carry no cross-message ordering guarantees.
    #[serde_inline_default(1u32)]
    pub connections_per_peer: u32,
    /// Minimum healthy connections maintained to every discovered remote
    /// RDMA device. Coverage is automatic and does not affect request APIs.
    #[serde_inline_default(1u32)]
    pub min_connections_per_remote_nic: u32,
    /// Hard cap on healthy outbound connections maintained for one peer.
    #[serde_inline_default(16u32)]
    pub preconnect_max_per_peer: u32,
    /// Maximum time an accepted RDMA connection may wait for confirmation
    /// or data-plane activation. Expired leases reclaim their QP and receive
    /// ring. This must be at least 15s; default is 30s.
    #[serde_inline_default(30_000u64)]
    pub connect_lease_ms: u64,
    /// If non-empty, only RDMA devices whose name is listed are used.
    /// Useful when a host has NICs without connectivity to the target
    /// fabric (device matching cannot verify reachability).
    #[serde(default)]
    pub device_filter: Vec<String>,
    /// Virtual zones defined by stable names and IP subnets. Ports whose
    /// addresses match the same zone name are preferred during path selection.
    #[serde(default)]
    pub zones: Vec<RdmaZoneConfig>,
    /// Interval (milliseconds) of the background maintenance task, which
    /// fails connections on downed local ports and replaces dead stripes.
    /// The interval is jittered by ±50% per process. `0` disables
    /// maintenance entirely.
    #[serde_inline_default(5000u64)]
    pub maintenance_interval_ms: u64,
    /// Minimum load improvement required before migrating a connection.
    #[serde_inline_default(2u32)]
    pub rebalance_threshold: u32,
    /// Grace period for a migrated connection to finish in-flight responses.
    #[serde_inline_default(10_000u64)]
    pub drain_timeout_ms: u64,
    /// Software timeout (milliseconds) for RDMA READ completions,
    /// enforced by the poll thread's periodic sweep (no per-operation
    /// timers). A read exceeding it fails its caller and moves the
    /// connection to the error state, so the NIC flushes the outstanding
    /// work requests and their buffers are reclaimed safely. `0`
    /// disables the timeout. Default is 10s.
    #[serde_inline_default(10_000u64)]
    pub read_timeout_ms: u64,
    /// Maximum in-flight RDMA READ work requests per *local NIC*
    /// (device), shared by every connection on it — the primary
    /// congestion control for read-heavy traffic. Both server-side
    /// `remote_read` and client-side `pull` (the read half of
    /// `remote_write`) draw from the same per-device budget; excess
    /// reads queue in software (FIFO). Default is 32.
    ///
    /// Independently, each connection caps its own in-flight reads at
    /// `qp.max_send_wr / 2` (not configurable): the send queue is shared
    /// with regular sends, and a device-wide budget landing on a single
    /// QP must not overflow it.
    #[serde_inline_default(32u32)]
    pub max_inflight_read_wrs: u32,
    /// GRH traffic class (RoCE: the DSCP/ECN byte of outgoing RDMA
    /// packets) for connections *initiated by this pool*. The client
    /// decides: the value travels in the connect request and the server
    /// programs the same value into its own address handle, so both
    /// directions of a connection share one traffic class. Inbound
    /// connections ignore the local setting. No effect on InfiniBand
    /// link layers (no GRH). Default is 0.
    #[serde_inline_default(0u8)]
    pub traffic_class: u8,
}

#[cfg(feature = "rdma")]
impl Default for RdmaSocketPoolConfig {
    fn default() -> Self {
        // Every field carries an inline serde default, so the canonical
        // default is "deserialize an empty object" — one source of truth.
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap()
    }
}

/// A virtual RDMA zone assigned from the IP addresses of an RDMA netdev.
#[cfg(feature = "rdma")]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct RdmaZoneConfig {
    /// Stable name exchanged with peers, for example `storage-a`.
    pub name: String,
    /// IP networks whose local interface addresses belong to this subnet.
    pub cidrs: Vec<ipnet::IpNet>,
}

/// Queue Pair capabilities requested or negotiated for an RDMA connection.
#[cfg(feature = "rdma")]
#[derive(Deserialize, Serialize, JsonSchema, Debug, PartialEq, Eq, Clone, Copy)]
pub struct RdmaQueuePairConfig {
    pub max_send_wr: u32,
    pub max_recv_wr: u32,
    pub max_send_sge: u32,
    pub max_recv_sge: u32,
}

#[cfg(feature = "rdma")]
impl Default for RdmaQueuePairConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 64,
            max_recv_wr: 64,
            // Gather-list capacity for zero-copy send aggregation: the
            // poll thread packs window-blocked messages into one WR whose
            // SGEs point at the original framed buffers. Clamped to the
            // device's `max_sge` at connection setup.
            max_send_sge: 16,
            max_recv_sge: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_to_tcp_without_rdma() {
        let config = SocketPoolConfig::default();
        assert_eq!(config.listen_mode, ListenMode::TCP);
        #[cfg(feature = "rdma")]
        assert!(config.rdma.is_none());
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = SocketPoolConfig {
            listen_mode: ListenMode::UNIFIED,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let recovered: SocketPoolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, config);
        assert!(serde_json::from_str::<SocketPoolConfig>(r#"{"socket_type":"UNIFIED"}"#).is_err());
    }

    /// Every RDMA config field carries an inline serde default, so a
    /// partial `rdma` object deserializes with the remaining fields at
    /// their documented defaults (also the source of `Default`).
    #[cfg(feature = "rdma")]
    #[test]
    fn rdma_config_partial_object_fills_defaults() {
        let config: SocketPoolConfig =
            serde_json::from_str(r#"{"rdma":{"recv_queue_len":16}}"#).unwrap();
        let rdma = config.rdma.unwrap();
        assert_eq!(rdma.recv_queue_len, 16);
        assert_eq!(rdma.qp, RdmaQueuePairConfig::default());
        assert_eq!(rdma.cq_len, 128);
        assert_eq!(rdma.pkey_index, 0);
        assert_eq!(rdma.max_msg_size, 256 * 1024);
        assert_eq!(rdma.min_connections_per_remote_nic, 1);
        assert_eq!(rdma.preconnect_max_per_peer, 16);
        assert_eq!(rdma.connect_lease_ms, 30_000);
        assert_eq!(rdma.rebalance_threshold, 2);
        assert_eq!(rdma.drain_timeout_ms, 10_000);
        assert_eq!(rdma.dispatch_workers, 32);
        assert_eq!(rdma.read_timeout_ms, 10_000);
        assert_eq!(rdma.max_inflight_read_wrs, 32);
        assert_eq!(rdma.traffic_class, 0);
        // `Default` is exactly the all-defaults deserialization.
        let default: RdmaSocketPoolConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(default, RdmaSocketPoolConfig::default());
        assert!(serde_json::from_str::<RdmaSocketPoolConfig>(r#"{"path_modes":[]}"#).is_err());
        assert!(
            serde_json::from_str::<RdmaSocketPoolConfig>(r#"{"remote_device_filter":[]}"#).is_err()
        );
    }
}
