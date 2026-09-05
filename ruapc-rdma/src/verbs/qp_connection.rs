use crate::{
    Error, ErrorKind, LinkLayer, Result, ibv_gid, ibv_mtu, ibv_qp_attr, ibv_qp_attr_mask,
    ibv_qp_state,
};

/// Negotiated parameters for connecting a reliable-connected queue pair.
///
/// The caller selects a path and negotiates MTU and READ concurrency before
/// passing this configuration to [`QueuePair::connect`](super::QueuePair::connect).
/// PSNs and QP numbers are checked rather than silently truncated. Invalid
/// configurations are rejected before any QP state transition.
#[derive(Debug, Clone, Copy)]
pub struct QpConnectionConfig {
    pub local_port_num: u8,
    pub local_gid_index: u8,
    pub pkey_index: u16,
    pub link_layer: LinkLayer,
    pub path_mtu: ibv_mtu,
    pub remote_qp_num: u32,
    pub remote_gid: ibv_gid,
    pub remote_lid: u16,
    pub local_psn: u32,
    pub remote_psn: u32,
    /// Maximum outbound READ concurrency, bounded by both devices' capabilities.
    pub max_rd_atomic: u8,
    /// Maximum inbound READ concurrency, bounded by both devices' capabilities.
    pub max_dest_rd_atomic: u8,
    /// RoCE GRH traffic class. Unused for native InfiniBand paths.
    pub traffic_class: u8,
}

impl QpConnectionConfig {
    /// Checks protocol-level bounds without accessing an RDMA device.
    ///
    /// Device-specific limits (port count, GID/P_Key table indices, MTU and
    /// READ concurrency) remain the caller's responsibility and are also
    /// checked by the provider when the QP is modified.
    pub fn validate(&self) -> Result<()> {
        let invalid = |message: String| Error::new(ErrorKind::InvalidQueuePairConfig, message);
        if self.local_port_num == 0 {
            return Err(invalid("local_port_num must be nonzero".into()));
        }
        if !(1..=0xFF_FFFF).contains(&self.remote_qp_num) {
            return Err(invalid(format!(
                "remote_qp_num must be a nonzero 24-bit number, got {}",
                self.remote_qp_num
            )));
        }
        for (name, psn) in [
            ("local_psn", self.local_psn),
            ("remote_psn", self.remote_psn),
        ] {
            if psn > 0xFF_FFFF {
                return Err(invalid(format!("{name} must fit in 24 bits, got {psn}")));
            }
        }
        for (name, value) in [
            ("max_rd_atomic", self.max_rd_atomic),
            ("max_dest_rd_atomic", self.max_dest_rd_atomic),
        ] {
            if value == 0 {
                return Err(invalid(format!(
                    "{name} must be nonzero for RDMA READ support"
                )));
            }
        }
        match self.link_layer {
            LinkLayer::InfiniBand if self.remote_lid == 0 || self.remote_lid >= 0xC000 => {
                return Err(invalid(format!(
                    "remote_lid must be a unicast InfiniBand LID (1..=49151), got {}",
                    self.remote_lid
                )));
            }
            LinkLayer::Ethernet
                if self.remote_gid.as_bits() == 0 || self.remote_gid.as_ipv6().is_multicast() =>
            {
                return Err(invalid(format!(
                    "remote_gid must be a nonzero unicast RoCE GID, got {:?}",
                    self.remote_gid
                )));
            }
            LinkLayer::Unspecified => {
                return Err(invalid("RDMA link layer is unspecified".into()));
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn init_attributes(&self) -> (ibv_qp_attr, ibv_qp_attr_mask) {
        // Relaxed ordering is a memory-registration property, not a QP access
        // permission. Only access permissions belong in qp_access_flags.
        let access_flags = crate::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | crate::ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | crate::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
        let attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_INIT,
            pkey_index: self.pkey_index,
            port_num: self.local_port_num,
            qp_access_flags: access_flags.0,
            ..Default::default()
        };
        let mask = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        (attr, mask)
    }

    pub(super) fn receive_attributes(&self) -> (ibv_qp_attr, ibv_qp_attr_mask) {
        let mut ah_attr = crate::ibv_ah_attr {
            port_num: self.local_port_num,
            ..Default::default()
        };
        match self.link_layer {
            LinkLayer::InfiniBand => {
                ah_attr.dlid = self.remote_lid;
            }
            LinkLayer::Ethernet => {
                ah_attr.grh = crate::ibv_global_route {
                    dgid: self.remote_gid,
                    flow_label: 0,
                    sgid_index: self.local_gid_index,
                    hop_limit: 0xff,
                    traffic_class: self.traffic_class,
                };
                ah_attr.is_global = 1;
            }
            LinkLayer::Unspecified => unreachable!("connection configuration must be validated"),
        }
        let attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_RTR,
            path_mtu: self.path_mtu,
            dest_qp_num: self.remote_qp_num,
            rq_psn: self.remote_psn,
            max_dest_rd_atomic: self.max_dest_rd_atomic,
            // 10 microseconds: short backoff for transient receive repost lag.
            min_rnr_timer: 0x01,
            ah_attr,
            ..Default::default()
        };
        let mask = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_AV
            | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN
            | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
            | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        (attr, mask)
    }

    pub(super) fn send_attributes(&self) -> (ibv_qp_attr, ibv_qp_attr_mask) {
        let attr = ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_RTS,
            timeout: 0x12,
            retry_cnt: 6,
            rnr_retry: 6,
            sq_psn: self.local_psn,
            max_rd_atomic: self.max_rd_atomic,
            ..Default::default()
        };
        let mask = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        (attr, mask)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> QpConnectionConfig {
        QpConnectionConfig {
            local_port_num: 1,
            local_gid_index: 3,
            pkey_index: 0,
            link_layer: LinkLayer::Ethernet,
            path_mtu: ibv_mtu::IBV_MTU_1024,
            remote_qp_num: 0x123456,
            remote_gid: ibv_gid {
                raw: std::net::Ipv6Addr::LOCALHOST.octets(),
            },
            remote_lid: 0,
            local_psn: 0,
            remote_psn: 0xFF_FFFF,
            max_rd_atomic: 8,
            max_dest_rd_atomic: 4,
            traffic_class: 104,
        }
    }

    #[test]
    fn rejects_invalid_connection_parameters() {
        for (field, invalid) in [
            (
                "local_port_num",
                QpConnectionConfig {
                    local_port_num: 0,
                    ..config()
                },
            ),
            (
                "remote_qp_num",
                QpConnectionConfig {
                    remote_qp_num: 0,
                    ..config()
                },
            ),
            (
                "remote_qp_num",
                QpConnectionConfig {
                    remote_qp_num: 0x100_0000,
                    ..config()
                },
            ),
            (
                "local_psn",
                QpConnectionConfig {
                    local_psn: 0x100_0000,
                    ..config()
                },
            ),
            (
                "remote_psn",
                QpConnectionConfig {
                    remote_psn: 0x100_0000,
                    ..config()
                },
            ),
            (
                "max_rd_atomic",
                QpConnectionConfig {
                    max_rd_atomic: 0,
                    ..config()
                },
            ),
            (
                "max_dest_rd_atomic",
                QpConnectionConfig {
                    max_dest_rd_atomic: 0,
                    ..config()
                },
            ),
            (
                "remote_gid",
                QpConnectionConfig {
                    remote_gid: ibv_gid::default(),
                    ..config()
                },
            ),
            (
                "link layer",
                QpConnectionConfig {
                    link_layer: LinkLayer::Unspecified,
                    ..config()
                },
            ),
            (
                "remote_lid",
                QpConnectionConfig {
                    link_layer: LinkLayer::InfiniBand,
                    remote_lid: 0,
                    ..config()
                },
            ),
            (
                "remote_lid",
                QpConnectionConfig {
                    link_layer: LinkLayer::InfiniBand,
                    remote_lid: 0xC000,
                    ..config()
                },
            ),
        ] {
            let error = invalid.validate().unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidQueuePairConfig);
            assert!(error.msg.contains(field), "{error}");
        }
    }

    #[test]
    fn roce_uses_grh_and_preserves_negotiated_psns_and_read_limits() {
        let config = config();
        config.validate().unwrap();
        let (recv, _) = config.receive_attributes();
        let (send, _) = config.send_attributes();
        assert_eq!(recv.ah_attr.is_global, 1);
        assert_eq!(recv.ah_attr.grh.dgid, config.remote_gid);
        assert_eq!(recv.ah_attr.grh.sgid_index, config.local_gid_index);
        assert_eq!(recv.ah_attr.grh.traffic_class, config.traffic_class);
        assert_eq!(recv.rq_psn, 0xFF_FFFF);
        assert_eq!(send.sq_psn, 0);
        assert_eq!(recv.max_dest_rd_atomic, 4);
        assert_eq!(send.max_rd_atomic, 8);
    }

    #[test]
    fn infiniband_uses_unicast_lid_without_grh() {
        let config = QpConnectionConfig {
            link_layer: LinkLayer::InfiniBand,
            remote_lid: 0xBFFF,
            remote_gid: ibv_gid::default(),
            ..config()
        };
        config.validate().unwrap();
        let (recv, _) = config.receive_attributes();
        assert_eq!(recv.ah_attr.is_global, 0);
        assert_eq!(recv.ah_attr.dlid, 0xBFFF);
    }
}
