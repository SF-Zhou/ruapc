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

impl RdmaQpEndpoint {
    /// Checks the wire endpoint before its values reach the verbs interface.
    pub(crate) fn validate(&self) -> Result<()> {
        const MAX_QPN_PSN: u32 = 0xFF_FFFF;
        if self.qp_num == 0 || self.qp_num > MAX_QPN_PSN {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA endpoint qp_num {} must be in 1..={MAX_QPN_PSN}",
                    self.qp_num
                ),
            ));
        }
        if self.psn > MAX_QPN_PSN {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA endpoint psn {} exceeds the 24-bit limit {MAX_QPN_PSN}",
                    self.psn
                ),
            ));
        }
        if self.port_num == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA endpoint port_num 0 is invalid; ports are numbered from 1".into(),
            ));
        }
        if !(1..=16).contains(&self.rd_atomic_cap) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA endpoint rd_atomic_cap {} must be in 1..=16",
                    self.rd_atomic_cap
                ),
            ));
        }
        // `ibv_mtu` is a Rust enum: deserialization already restricts the
        // active MTU to the five verbs-supported sizes (256 through 4096).
        match self.link_layer {
            LinkLayer::Unspecified => {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "RDMA endpoint link_layer is Unspecified; expected InfiniBand or Ethernet"
                        .into(),
                ));
            }
            // Native IB uses LID routing in our QP setup, so a GID is optional.
            LinkLayer::InfiniBand if !(1..=0xBFFF).contains(&self.lid) => {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "RDMA InfiniBand endpoint lid {} must be a unicast LID in 1..=49151",
                        self.lid
                    ),
                ));
            }
            LinkLayer::Ethernet => {
                let gid = self.gid.as_ipv6();
                if gid.is_unspecified() || gid.is_multicast() {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!("RDMA Ethernet endpoint gid {gid} must be a nonzero unicast GID"),
                    ));
                }
            }
            LinkLayer::InfiniBand => {}
        }
        Ok(())
    }
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

    pub(crate) fn validate(self) -> Result<()> {
        if self.recv_queue_len < RdmaConnectionTuningConfig::MIN_RECV_QUEUE_LEN {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA recv_queue_len {} must be at least {}",
                    self.recv_queue_len,
                    RdmaConnectionTuningConfig::MIN_RECV_QUEUE_LEN
                ),
            ));
        }
        if self.recv_queue_len > self.max_send_wr || self.recv_queue_len > self.max_recv_wr {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA recv_queue_len {} exceeds max_send_wr {} or max_recv_wr {}",
                    self.recv_queue_len, self.max_send_wr, self.max_recv_wr
                ),
            ));
        }
        if self.max_msg_size < RdmaConnectionTuningConfig::MIN_MAX_MSG_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA max_msg_size {} must be at least {}",
                    self.max_msg_size,
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
    /// Actual limits configured by the acceptor, from its own perspective.
    pub limits: RdmaConnectionLimits,
}

impl PrepareConnectionResponse {
    /// Confirms that the acceptor prepared the exact path and limits requested.
    /// A stale discovery result must fail bootstrap instead of producing peers
    /// with different receive rings, send windows, or message-size limits.
    pub(crate) fn validate_for(&self, request: &PrepareConnectionRequest) -> Result<()> {
        if self.lease.attempt_id == 0
            || self.lease.attempt_id != request.attempt_id
            || self.lease.accepted_connection_id == 0
        {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA prepare response lease {:?} does not identify request attempt_id {} with a nonzero accepted_connection_id",
                    self.lease, request.attempt_id
                ),
            ));
        }
        self.endpoint.validate()?;
        if self.endpoint.port_num != request.target.port_num
            || self.endpoint.gid_index != request.target.gid_index
        {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA prepare response port {} GID index {} differs from requested target {}:{} GID index {}",
                    self.endpoint.port_num,
                    self.endpoint.gid_index,
                    request.target.device_name,
                    request.target.port_num,
                    request.target.gid_index
                ),
            ));
        }
        if self.endpoint.link_layer != request.endpoint.link_layer {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA prepare response link layer {} differs from initiator {}",
                    self.endpoint.link_layer, request.endpoint.link_layer
                ),
            ));
        }
        self.limits.validate()?;
        let expected = RdmaConnectionLimits {
            max_send_wr: request.limits.max_recv_wr,
            max_recv_wr: request.limits.max_send_wr,
            recv_queue_len: request.limits.recv_queue_len,
            max_msg_size: request.limits.max_msg_size,
        };
        if self.limits != expected {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA prepare response limits {:?} differ from expected acceptor limits {expected:?}; peer capabilities may have changed since discovery",
                    self.limits
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_endpoint() -> RdmaQpEndpoint {
        RdmaQpEndpoint {
            qp_num: 42,
            port_num: 1,
            gid_index: 0,
            lid: 1,
            gid: ibv_gid::default(),
            link_layer: LinkLayer::InfiniBand,
            active_mtu: ibv_mtu::IBV_MTU_1024,
            psn: 0,
            rd_atomic_cap: 16,
        }
    }

    fn prepared_connection() -> (PrepareConnectionRequest, PrepareConnectionResponse) {
        let request = PrepareConnectionRequest {
            attempt_id: 11,
            endpoint: valid_endpoint(),
            source_device: "initiator_0".into(),
            same_connectivity_domain: true,
            target: DeviceSelection {
                device_name: "acceptor_0".into(),
                port_num: 2,
                gid_index: 3,
            },
            limits: RdmaConnectionLimits {
                max_send_wr: 80,
                max_recv_wr: 4,
                recv_queue_len: 4,
                max_msg_size: 64 * 1024,
            },
            traffic_class: 0,
        };
        let response = PrepareConnectionResponse {
            endpoint: RdmaQpEndpoint {
                qp_num: 43,
                port_num: request.target.port_num,
                gid_index: request.target.gid_index,
                ..valid_endpoint()
            },
            lease: ConnectionLease {
                attempt_id: request.attempt_id,
                accepted_connection_id: 12,
            },
            limits: RdmaConnectionLimits {
                max_send_wr: 4,
                max_recv_wr: 80,
                ..request.limits
            },
        };
        (request, response)
    }

    #[test]
    fn endpoint_accepts_native_ib_without_gid_and_unicast_roce_addresses() {
        let endpoint = valid_endpoint();
        endpoint.validate().unwrap();
        for address in ["fe80::1", "::ffff:192.0.2.1", "2001:db8:1::"] {
            RdmaQpEndpoint {
                link_layer: LinkLayer::Ethernet,
                lid: 0,
                gid: serde_json::from_value(serde_json::json!(address)).unwrap(),
                ..endpoint
            }
            .validate()
            .unwrap();
        }
        RdmaQpEndpoint {
            qp_num: 0xFF_FFFF,
            psn: 0xFF_FFFF,
            rd_atomic_cap: 1,
            lid: 0xBFFF,
            ..endpoint
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn endpoint_rejects_invalid_wire_values_before_qp_setup() {
        let endpoint = valid_endpoint();
        for (invalid, field) in [
            (
                RdmaQpEndpoint {
                    qp_num: 0,
                    ..endpoint
                },
                "qp_num",
            ),
            (
                RdmaQpEndpoint {
                    qp_num: 0x100_0000,
                    ..endpoint
                },
                "qp_num",
            ),
            (
                RdmaQpEndpoint {
                    psn: 0x100_0000,
                    ..endpoint
                },
                "psn",
            ),
            (
                RdmaQpEndpoint {
                    port_num: 0,
                    ..endpoint
                },
                "port_num",
            ),
            (
                RdmaQpEndpoint {
                    rd_atomic_cap: 0,
                    ..endpoint
                },
                "rd_atomic_cap",
            ),
            (
                RdmaQpEndpoint {
                    rd_atomic_cap: 17,
                    ..endpoint
                },
                "rd_atomic_cap",
            ),
            (
                RdmaQpEndpoint {
                    link_layer: LinkLayer::Unspecified,
                    ..endpoint
                },
                "link_layer",
            ),
            (RdmaQpEndpoint { lid: 0, ..endpoint }, "lid"),
            (
                RdmaQpEndpoint {
                    lid: 0xC000,
                    ..endpoint
                },
                "lid",
            ),
            (
                RdmaQpEndpoint {
                    link_layer: LinkLayer::Ethernet,
                    ..endpoint
                },
                "gid",
            ),
            (
                RdmaQpEndpoint {
                    link_layer: LinkLayer::Ethernet,
                    gid: serde_json::from_value(serde_json::json!("ff02::1")).unwrap(),
                    ..endpoint
                },
                "gid",
            ),
        ] {
            let err = invalid.validate().unwrap_err();
            assert_eq!(err.kind, ErrorKind::InvalidArgument);
            assert!(err.msg.contains(field), "{err}");
        }
    }

    #[test]
    fn endpoint_wire_format_rejects_unknown_mtu() {
        let mut endpoint = serde_json::to_value(valid_endpoint()).unwrap();
        endpoint["active_mtu"] = serde_json::json!("IBV_MTU_8192");
        assert!(serde_json::from_value::<RdmaQpEndpoint>(endpoint).is_err());
    }

    #[test]
    fn prepare_response_requires_correlated_nonzero_lease() {
        let (request, response) = prepared_connection();
        response.validate_for(&request).unwrap();
        for lease in [
            ConnectionLease {
                attempt_id: 0,
                ..response.lease
            },
            ConnectionLease {
                attempt_id: request.attempt_id + 1,
                ..response.lease
            },
            ConnectionLease {
                accepted_connection_id: 0,
                ..response.lease
            },
        ] {
            let err = PrepareConnectionResponse { lease, ..response }
                .validate_for(&request)
                .unwrap_err();
            assert!(err.msg.contains("lease"), "{err}");
        }
    }

    #[test]
    fn prepare_response_requires_valid_endpoint_on_selected_port_and_link() {
        let (request, response) = prepared_connection();
        for endpoint in [
            RdmaQpEndpoint {
                port_num: 1,
                ..response.endpoint
            },
            RdmaQpEndpoint {
                gid_index: 0,
                ..response.endpoint
            },
            RdmaQpEndpoint {
                psn: 0x100_0000,
                ..response.endpoint
            },
            RdmaQpEndpoint {
                link_layer: LinkLayer::Ethernet,
                gid: serde_json::from_value(serde_json::json!("fe80::1")).unwrap(),
                ..response.endpoint
            },
        ] {
            assert!(
                PrepareConnectionResponse {
                    endpoint,
                    ..response
                }
                .validate_for(&request)
                .is_err()
            );
        }
    }

    #[test]
    fn prepare_response_rejects_changed_or_unmirrored_limits() {
        let (request, response) = prepared_connection();
        for limits in [
            request.limits,
            RdmaConnectionLimits {
                max_send_wr: 5,
                ..response.limits
            },
            RdmaConnectionLimits {
                max_recv_wr: 79,
                ..response.limits
            },
            RdmaConnectionLimits {
                recv_queue_len: 3,
                ..response.limits
            },
            RdmaConnectionLimits {
                max_msg_size: 32 * 1024,
                ..response.limits
            },
        ] {
            let err = PrepareConnectionResponse { limits, ..response }
                .validate_for(&request)
                .unwrap_err();
            assert!(err.msg.contains("limits"), "{err}");
            assert!(err.msg.contains("discovery"), "{err}");
        }
    }

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
