use ruapc_rdma::{LinkLayer, ibv_gid, ibv_mtu};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{RdmaConnectionTuningConfig, RdmaQueuePairConfig};
use crate::{Error, ErrorKind, Result};

/// Queue-pair and address metadata exchanged during RDMA bootstrap.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct RdmaQpEndpoint {
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
    /// Device cap on concurrent RDMA READs per QP. Both sides program the
    /// minimum advertised value as `max_rd_atomic` / `max_dest_rd_atomic`.
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

/// Directional connection limits exchanged during RDMA bootstrap.
///
/// Send and receive limits are expressed from the owner's perspective. A
/// local send queue is therefore bounded by the peer's receive limit, and
/// vice versa. Scatter/gather limits are deliberately absent because they
/// are properties of the local work requests only.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
pub struct RdmaConnectionLimits {
    pub max_send_wr: u32,
    pub max_recv_wr: u32,
    /// Number of receive buffers the endpoint can pre-post.
    pub recv_queue_len: u32,
    /// Maximum serialized message size accepted by the endpoint.
    pub max_msg_size: u32,
}

impl RdmaConnectionLimits {
    /// Resolves connection limits for `self` against a peer's directional
    /// capabilities, preserving this endpoint's point of view.
    pub(crate) fn negotiate(self, peer: Self) -> Result<Self> {
        let max_send_wr = self.max_send_wr.min(peer.max_recv_wr);
        let max_recv_wr = self.max_recv_wr.min(peer.max_send_wr);
        let negotiated = Self {
            max_send_wr,
            max_recv_wr,
            // Both peers use this value for their receive ring and derive
            // their send-credit window from it. Keep it symmetric and
            // within both negotiated QP directions.
            recv_queue_len: self
                .recv_queue_len
                .min(peer.recv_queue_len)
                .min(max_send_wr)
                .min(max_recv_wr),
            max_msg_size: self.max_msg_size.min(peer.max_msg_size),
        };
        negotiated.validate()?;
        Ok(negotiated)
    }

    fn validate(self) -> Result<()> {
        if self.recv_queue_len < RdmaConnectionTuningConfig::MIN_RECV_QUEUE_LEN {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "negotiated RDMA recv_queue_len must be at least {}",
                    RdmaConnectionTuningConfig::MIN_RECV_QUEUE_LEN
                ),
            ));
        }
        if self.recv_queue_len > self.max_send_wr || self.recv_queue_len > self.max_recv_wr {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "negotiated RDMA recv_queue_len exceeds a queue-pair work-request limit".into(),
            ));
        }
        if self.max_msg_size < RdmaConnectionTuningConfig::MIN_MAX_MSG_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "negotiated RDMA max_msg_size must be at least {}",
                    RdmaConnectionTuningConfig::MIN_MAX_MSG_SIZE
                ),
            ));
        }
        Ok(())
    }
}

/// Fully resolved settings used only by the local RDMA runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RdmaConnectionConfig {
    pub qp: RdmaQueuePairConfig,
    pub recv_queue_len: u32,
    pub max_msg_size: u32,
    /// GRH traffic class selected by the connection initiator.
    pub traffic_class: u8,
}

impl From<RdmaConnectionConfig> for RdmaConnectionLimits {
    fn from(config: RdmaConnectionConfig) -> Self {
        Self {
            max_send_wr: config.qp.max_send_wr,
            max_recv_wr: config.qp.max_recv_wr,
            recv_queue_len: config.recv_queue_len,
            max_msg_size: config.max_msg_size,
        }
    }
}

/// RDMA bootstrap request sent after the initiator selects an acceptor port.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct PrepareConnectionRequest {
    /// Random initiator token used to correlate this bootstrap attempt.
    /// It is not an authentication credential.
    pub attempt_id: u64,
    /// Initiator queue-pair endpoint.
    pub endpoint: RdmaQpEndpoint,
    /// Name of the initiator-side RDMA device.
    pub source_device: String,
    /// Whether both NIC addresses matched one configured connectivity domain.
    pub same_connectivity_domain: bool,
    /// Acceptor device/port/GID selected by the initiator.
    pub target: DeviceSelection,
    /// Initiator limits, expressed from the initiator's perspective.
    pub limits: RdmaConnectionLimits,
    /// GRH traffic class chosen by the initiator.
    pub traffic_class: u8,
}

/// Identifies one accepted connection for lifecycle control RPCs.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct ConnectionLease {
    pub attempt_id: u64,
    pub accepted_connection_id: u64,
}

/// Acceptor result for a prepared RDMA connection.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct PrepareConnectionResponse {
    pub endpoint: RdmaQpEndpoint,
    pub lease: ConnectionLease,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asymmetric_limits_are_mirrored_between_peers() {
        let initiator = RdmaConnectionLimits {
            max_send_wr: 96,
            max_recv_wr: 24,
            recv_queue_len: 20,
            max_msg_size: 256 * 1024,
        };
        let acceptor = RdmaConnectionLimits {
            max_send_wr: 4,
            max_recv_wr: 80,
            recv_queue_len: 8,
            max_msg_size: 64 * 1024,
        };

        let initiator_resolved = initiator.negotiate(acceptor).unwrap();
        let acceptor_resolved = acceptor.negotiate(initiator_resolved).unwrap();

        assert_eq!(initiator_resolved.max_send_wr, 80);
        assert_eq!(initiator_resolved.max_recv_wr, 4);
        assert_eq!(acceptor_resolved.max_send_wr, 4);
        assert_eq!(acceptor_resolved.max_recv_wr, 80);
        assert_eq!(initiator_resolved.recv_queue_len, 4);
        assert_eq!(acceptor_resolved.recv_queue_len, 4);
        assert_eq!(initiator_resolved.max_msg_size, 64 * 1024);
        assert_eq!(acceptor_resolved.max_msg_size, 64 * 1024);
    }

    #[test]
    fn receive_queue_negotiation_stays_symmetric_when_send_is_smaller() {
        let initiator = RdmaConnectionLimits {
            max_send_wr: 4,
            max_recv_wr: 96,
            recv_queue_len: 20,
            max_msg_size: 256 * 1024,
        };
        let acceptor = RdmaConnectionLimits {
            max_send_wr: 80,
            max_recv_wr: 24,
            recv_queue_len: 20,
            max_msg_size: 64 * 1024,
        };

        let initiator_resolved = initiator.negotiate(acceptor).unwrap();
        let acceptor_resolved = acceptor.negotiate(initiator_resolved).unwrap();

        assert_eq!(initiator_resolved.recv_queue_len, 4);
        assert_eq!(acceptor_resolved.recv_queue_len, 4);
        assert_eq!(
            initiator_resolved.recv_queue_len,
            acceptor_resolved.recv_queue_len
        );
    }

    #[test]
    fn rejects_negotiated_limits_too_small_for_the_runtime() {
        let local = RdmaConnectionLimits {
            max_send_wr: 64,
            max_recv_wr: 64,
            recv_queue_len: 8,
            max_msg_size: 256 * 1024,
        };

        for peer in [
            RdmaConnectionLimits {
                max_send_wr: 1,
                ..local
            },
            RdmaConnectionLimits {
                max_recv_wr: 1,
                ..local
            },
            RdmaConnectionLimits {
                recv_queue_len: 1,
                ..local
            },
            RdmaConnectionLimits {
                max_msg_size: RdmaConnectionTuningConfig::MIN_MAX_MSG_SIZE - 1,
                ..local
            },
        ] {
            let err = local.negotiate(peer).unwrap_err();
            assert_eq!(err.kind, ErrorKind::InvalidArgument);
        }
    }
}
