use ruapc_rdma::{DeviceInfo, Gid, LinkLayer, Port};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, Result, rdma, service};

/// Version of the internal RDMA bootstrap protocol.
pub(crate) const RDMA_BOOTSTRAP_PROTOCOL_VERSION: u32 = 2;

fn port_is_connectable(port: &Port) -> bool {
    port.is_usable() && (!port.port_attr.link_layer.is_ethernet() || !port.gids.is_empty())
}

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
    pub limits: rdma::RdmaConnectionLimits,
    /// Usable ports on this device.
    pub ports: Vec<RdmaPortInfo>,
}

impl RdmaDeviceInfo {
    fn from_device_info(
        info: &DeviceInfo,
        config: &crate::rdma::RdmaSocketPoolConfig,
        active_connections: u32,
    ) -> Option<Self> {
        let ports: Vec<_> = info
            .ports
            .iter()
            .filter(|port| port_is_connectable(port))
            .map(|port| RdmaPortInfo {
                port_num: port.port_num,
                link_layer: port.port_attr.link_layer,
                gids: port.gids.clone(),
            })
            .collect();
        if ports.is_empty() {
            return None;
        }
        let max_send_wr = config
            .connection
            .qp
            .max_send_wr
            .min(info.device_attr.max_qp_wr as u32);
        let max_recv_wr = config
            .connection
            .qp
            .max_recv_wr
            .min(info.device_attr.max_qp_wr as u32);
        Some(Self {
            name: info.name.clone(),
            active_connections,
            limits: rdma::RdmaConnectionLimits {
                max_send_wr,
                max_recv_wr,
                recv_queue_len: config.connection.recv_queue_len.min(max_recv_wr),
                max_msg_size: config.connection.max_msg_size,
            },
            ports,
        })
    }
}

/// Information about available RDMA devices in the system.
///
/// This structure is exchanged between client and server during
/// RDMA connection negotiation. It contains only the minimal
/// information needed to select a compatible device/port/GID pair.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaPeerAdvertisement {
    /// Bootstrap wire protocol version understood by this peer.
    pub protocol_version: u32,
    /// List of available RDMA devices with connection-relevant info
    pub devices: Vec<RdmaDeviceInfo>,
}

impl RdmaPeerAdvertisement {
    /// Build an advertisement from the full device info list.
    ///
    /// Only usable ports are advertised. GIDs unusable for communication
    /// (RoCE v2 GIDs derived from loopback or IPv6 link-local addresses)
    /// are already filtered out at collection time.
    pub(crate) fn from_devices(
        devices: &[super::RdmaDevice],
        config: &crate::rdma::RdmaSocketPoolConfig,
        conn_counts: &[std::sync::atomic::AtomicUsize],
    ) -> Self {
        RdmaPeerAdvertisement {
            protocol_version: RDMA_BOOTSTRAP_PROTOCOL_VERSION,
            devices: devices
                .iter()
                .enumerate()
                .filter_map(|(index, d)| {
                    let info = d.info();
                    let active_connections = conn_counts
                        .get(index)
                        .map(|c| c.load(std::sync::atomic::Ordering::Acquire))
                        .unwrap_or(0)
                        .try_into()
                        .unwrap_or(u32::MAX);
                    RdmaDeviceInfo::from_device_info(&info, config, active_connections)
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
#[service(name = "_ruapc.rdma", internal)]
pub(crate) trait RdmaBootstrapService {
    /// Discovers the peer's connectable RDMA devices and limits.
    async fn discover(&self, ctx: &Context, _: &()) -> Result<rdma::RdmaPeerAdvertisement>;

    /// Prepares an RDMA connection with the selected acceptor endpoint.
    async fn prepare_connection(
        &self,
        ctx: &Context,
        request: &rdma::PrepareConnectionRequest,
    ) -> Result<rdma::PrepareConnectionResponse>;

    /// Confirms that the initiator received the endpoint and retained its
    /// local QP. The first confirmation starts the data-plane activation
    /// budget; repeated confirmations do not extend it. The connection
    /// remains leased until a successful receive also proves activation.
    /// The bootstrap service assumes a trusted control plane; the token
    /// correlates lifecycle state but does not authenticate the caller.
    async fn commit_connection(&self, ctx: &Context, lease: &rdma::ConnectionLease) -> Result<()>;

    /// Best-effort idempotent cleanup when the initiator cannot complete the
    /// confirmation exchange.
    async fn cancel_connection(&self, ctx: &Context, lease: &rdma::ConnectionLease) -> Result<()>;
}

/// Default implementation of `RdmaBootstrapService` for the unit type.
///
/// This implementation delegates all operations to the socket pool
/// stored in the context's state.
impl RdmaBootstrapService for () {
    /// Retrieves RDMA device information from the socket pool.
    async fn discover(&self, ctx: &Context, (): &()) -> Result<rdma::RdmaPeerAdvertisement> {
        ctx.state.socket_pool.rdma_peer_advertisement()
    }

    /// Prepares an RDMA connection using the socket pool.
    async fn prepare_connection(
        &self,
        ctx: &Context,
        request: &rdma::PrepareConnectionRequest,
    ) -> Result<rdma::PrepareConnectionResponse> {
        ctx.state
            .socket_pool
            .rdma_prepare_connection(request, &ctx.state)
    }

    async fn commit_connection(&self, ctx: &Context, lease: &rdma::ConnectionLease) -> Result<()> {
        ctx.state.socket_pool.rdma_commit_connection(lease)
    }

    async fn cancel_connection(&self, ctx: &Context, lease: &rdma::ConnectionLease) -> Result<()> {
        ctx.state.socket_pool.rdma_cancel_connection(lease)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SocketPoolConfig;

    #[test]
    fn ethernet_advertisement_requires_a_gid() {
        let mut ethernet = Port {
            port_num: 1,
            port_attr: ruapc_rdma::ibv_port_attr {
                state: ruapc_rdma::ibv_port_state::IBV_PORT_ACTIVE,
                link_layer: LinkLayer::Ethernet,
                ..Default::default()
            },
            gids: Vec::new(),
        };
        assert!(!port_is_connectable(&ethernet));
        let config = crate::rdma::RdmaSocketPoolConfig::default();
        let ethernet_only = DeviceInfo {
            name: "ethernet-only".into(),
            ports: vec![ethernet.clone()],
            ..Default::default()
        };
        assert!(RdmaDeviceInfo::from_device_info(&ethernet_only, &config, 0).is_none());

        ethernet.gids.push(Gid {
            index: 0,
            gid: ruapc_rdma::ibv_gid::default(),
            gid_type: ruapc_rdma::GidType::RoCEv2,
        });
        assert!(port_is_connectable(&ethernet));
        let ethernet_with_gid = DeviceInfo {
            name: "ethernet-with-gid".into(),
            ports: vec![ethernet],
            ..Default::default()
        };
        assert!(RdmaDeviceInfo::from_device_info(&ethernet_with_gid, &config, 0).is_some());

        let infiniband = Port {
            port_num: 1,
            port_attr: ruapc_rdma::ibv_port_attr {
                state: ruapc_rdma::ibv_port_state::IBV_PORT_ACTIVE,
                link_layer: LinkLayer::InfiniBand,
                ..Default::default()
            },
            gids: Vec::new(),
        };
        assert!(port_is_connectable(&infiniband));
    }

    #[tokio::test]
    async fn test_rdma_bootstrap_discover_returns_connectable_devices() {
        let config = SocketPoolConfig::default();
        let ctx = Context::create(&config).expect("failed to create RDMA context");
        let result = ().discover(&ctx, &()).await;
        assert!(result.is_ok());
        let advertisement = result.unwrap();
        assert_eq!(
            advertisement.protocol_version,
            RDMA_BOOTSTRAP_PROTOCOL_VERSION
        );
        assert!(!advertisement.devices.is_empty());
        assert!(
            advertisement
                .devices
                .iter()
                .all(|device| !device.ports.is_empty())
        );
        let lease = rdma::ConnectionLease {
            attempt_id: u64::MAX,
            accepted_connection_id: u64::MAX,
        };
        let err = ().commit_connection(&ctx, &lease).await.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::InvalidArgument);
        ().cancel_connection(&ctx, &lease).await.unwrap();
    }

    #[tokio::test]
    async fn test_rdma_bootstrap_prepare_non_rdma_returns_err() {
        // With a TCP pool, preparation propagates the socket-pool error.
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let endpoint = rdma::RdmaQpEndpoint {
            qp_num: 0,
            port_num: 1,
            gid_index: 0,
            gid: ruapc_rdma::ibv_gid::default(),
            lid: 0,
            link_layer: ruapc_rdma::LinkLayer::Ethernet,
            active_mtu: ruapc_rdma::ibv_mtu::IBV_MTU_512,
            psn: 0,
            rd_atomic_cap: 1,
        };
        let request = rdma::PrepareConnectionRequest {
            attempt_id: 1,
            endpoint,
            source_device: "test".into(),
            same_connectivity_domain: false,
            target: rdma::DeviceSelection {
                device_name: "missing".into(),
                port_num: 1,
                gid_index: 0,
            },
            limits: rdma::RdmaConnectionLimits {
                max_send_wr: 64,
                max_recv_wr: 64,
                recv_queue_len: 64,
                max_msg_size: 1024 * 1024,
            },
            traffic_class: 0,
        };
        let result = ().prepare_connection(&ctx, &request).await;
        assert!(result.is_err());
    }
}
