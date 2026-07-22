use ruapc_rdma::{Gid, LinkLayer};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, RdmaQueuePairConfig, Result, rdma, service};

/// Port information advertised for RDMA connection negotiation.
///
/// Deliberately kept independent of the `ibv_*` structures generated from
/// the ibverbs headers: it carries exactly the fields the peer needs to
/// select a port/GID pair, nothing else. Only usable ports (active, with an
/// InfiniBand or Ethernet link layer) are advertised, so no port state
/// travels on the wire.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaPortInfo {
    /// Port number (1-based).
    pub port_num: u8,
    /// Link layer type (InfiniBand or Ethernet).
    pub link_layer: LinkLayer,
    /// Usable GIDs of this port (filtered at collection time).
    pub gids: Vec<Gid>,
}

/// RDMA device information for connection negotiation.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaDeviceInfo {
    /// Device name (e.g., "mlx5_0").
    pub name: String,
    /// Live RDMA connections (outbound + inbound) currently on this device.
    /// Advertised so that clients can prefer less-loaded server NICs.
    pub active_connections: u32,
    /// Server-advertised per-connection RDMA resource limits for this device.
    pub connection: rdma::RdmaConnectionConfig,
    /// Usable ports on this device.
    pub ports: Vec<RdmaPortInfo>,
}

/// Information about available RDMA devices in the system.
///
/// This structure is exchanged between client and server during
/// RDMA connection negotiation. It contains only the minimal
/// information needed to select a compatible device/port/GID pair.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaInfo {
    /// List of available RDMA devices with connection-relevant info
    pub devices: Vec<RdmaDeviceInfo>,
}

impl RdmaInfo {
    /// Build `RdmaInfo` from the full device info list.
    ///
    /// Only usable ports are advertised. GIDs unusable for communication
    /// (RoCE v2 GIDs derived from loopback or IPv6 link-local addresses)
    /// are already filtered out at collection time.
    pub(crate) fn from_devices(
        devices: &[super::RdmaDevice],
        config: &crate::RdmaSocketPoolConfig,
        conn_counts: &[std::sync::atomic::AtomicUsize],
    ) -> Self {
        RdmaInfo {
            devices: devices
                .iter()
                .enumerate()
                .map(|(index, d)| {
                    let info = d.info();
                    RdmaDeviceInfo {
                        name: info.name.clone(),
                        active_connections: conn_counts
                            .get(index)
                            .map(|c| c.load(std::sync::atomic::Ordering::Acquire))
                            .unwrap_or(0)
                            .try_into()
                            .unwrap_or(u32::MAX),
                        connection: rdma::RdmaConnectionConfig {
                            qp: RdmaQueuePairConfig {
                                max_send_wr: config
                                    .qp
                                    .max_send_wr
                                    .min(info.device_attr.max_qp_wr as u32),
                                max_recv_wr: config
                                    .qp
                                    .max_recv_wr
                                    .min(info.device_attr.max_qp_wr as u32),
                                max_send_sge: config
                                    .qp
                                    .max_send_sge
                                    .min(info.device_attr.max_sge as u32),
                                max_recv_sge: config
                                    .qp
                                    .max_recv_sge
                                    .min(info.device_attr.max_sge as u32),
                                max_inline_data: config.qp.max_inline_data,
                            },
                            cq_len: config.cq_len.min(info.device_attr.max_cqe as u32),
                            recv_queue_len: config.recv_queue_len,
                            max_msg_size: config.max_msg_size.max(16 * 1024),
                        },
                        ports: info
                            .ports
                            .iter()
                            .filter(|port| port.is_usable())
                            .map(|port| RdmaPortInfo {
                                port_num: port.port_num,
                                link_layer: port.port_attr.link_layer,
                                gids: port.gids.clone(),
                            })
                            .collect(),
                    }
                })
                .collect(),
        }
    }
}

/// Service interface for RDMA operations.
///
/// This trait defines the core operations for managing RDMA connections
/// and querying RDMA device information. It is designed to work with
/// the service macro for RPC functionality.
#[service]
pub trait RdmaService {
    /// Retrieves information about available RDMA devices.
    async fn info(&self, ctx: &Context, _: &()) -> Result<rdma::RdmaInfo>;

    /// Establishes an RDMA connection with the selected server endpoint.
    async fn connect(
        &self,
        ctx: &Context,
        request: &rdma::ConnectRequest,
    ) -> Result<rdma::Endpoint>;
}

/// Default implementation of `RdmaService` for the unit type.
///
/// This implementation delegates all operations to the socket pool
/// stored in the context's state.
impl RdmaService for () {
    /// Retrieves RDMA device information from the socket pool.
    async fn info(&self, ctx: &Context, (): &()) -> Result<rdma::RdmaInfo> {
        ctx.state.socket_pool.rdma_device_list()
    }

    /// Establishes an RDMA connection using the socket pool.
    async fn connect(
        &self,
        ctx: &Context,
        request: &rdma::ConnectRequest,
    ) -> Result<rdma::Endpoint> {
        ctx.state.socket_pool.rdma_accept(request, &ctx.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SocketPoolConfig, SocketType};

    #[tokio::test]
    async fn test_rdma_service_info_returns_devices() {
        let config = SocketPoolConfig {
            socket_type: SocketType::RDMA,
            ..Default::default()
        };
        let ctx = Context::create(&config).expect("failed to create RDMA context");
        let result = ().info(&ctx, &()).await;
        assert!(result.is_ok());
        let info = result.unwrap();
        assert!(!info.devices.is_empty());
    }

    #[tokio::test]
    async fn test_rdma_service_connect_non_rdma_returns_err() {
        // With a TCP pool, `connect` should propagate the error from rdma_accept.
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let endpoint = rdma::Endpoint {
            qp_num: 0,
            port_num: 1,
            gid_index: 0,
            gid: ruapc_rdma::ibv_gid::default(),
            lid: 0,
            link_layer: ruapc_rdma::LinkLayer::Ethernet,
            active_mtu: ruapc_rdma::ibv_mtu::IBV_MTU_512,
            psn: 0,
        };
        let request = rdma::ConnectRequest {
            endpoint,
            source_device: "test".into(),
            target: rdma::DeviceSelection {
                device_name: "missing".into(),
                port_num: 1,
                gid_index: 0,
            },
            config: rdma::RdmaConnectionConfig {
                qp: RdmaQueuePairConfig::default(),
                cq_len: 128,
                recv_queue_len: 64,
                max_msg_size: 1024 * 1024,
            },
        };
        let result = ().connect(&ctx, &request).await;
        assert!(result.is_err());
    }
}
