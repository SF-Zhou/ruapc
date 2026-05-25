use ruapc_rdma::{GidType, LinkLayer, ibv_gid, ibv_mtu, ibv_port_state};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, Result, rdma, service};

/// Minimal port information for RDMA connection negotiation.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaPortInfo {
    /// Port number (1-based).
    pub port_num: u8,
    /// Port state (active, down, etc.).
    pub state: ibv_port_state,
    /// Link layer type (InfiniBand or Ethernet).
    pub link_layer: LinkLayer,
    /// Active MTU for the port.
    pub active_mtu: ibv_mtu,
    /// Local Identifier for InfiniBand routing.
    pub lid: u16,
    /// Available GIDs for this port.
    pub gids: Vec<RdmaGidInfo>,
}

/// Minimal GID information for RDMA connection negotiation.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaGidInfo {
    /// GID index on the port.
    pub index: u16,
    /// The GID value.
    pub gid: ibv_gid,
    /// The type of this GID.
    pub gid_type: GidType,
}

/// Minimal RDMA device information for connection negotiation.
///
/// Contains only the fields needed to establish an RDMA connection,
/// without heavy metadata like device_attr or ibdev_path.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaDeviceInfo {
    /// Device name (e.g., "mlx5_0").
    pub name: String,
    /// Available ports on this device.
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
    /// Build `RdmaInfo` from the full device info list, filtering out
    /// loopback GIDs for RoCE v2 since they cannot be used for communication.
    pub(crate) fn from_devices(devices: &[super::RdmaDevice]) -> Self {
        RdmaInfo {
            devices: devices
                .iter()
                .map(|d| {
                    let info = d.info();
                    RdmaDeviceInfo {
                        name: info.name.clone(),
                        ports: info
                            .ports
                            .iter()
                            .map(|port| RdmaPortInfo {
                                port_num: port.port_num,
                                state: port.port_attr.state,
                                link_layer: port.port_attr.link_layer,
                                active_mtu: port.port_attr.active_mtu,
                                lid: port.port_attr.lid,
                                gids: port
                                    .gids
                                    .iter()
                                    .filter(|gid| {
                                        // Filter out loopback addresses for RoCE v2
                                        !(matches!(gid.gid_type, GidType::RoCEv2)
                                            && gid.gid.as_ipv6().is_loopback())
                                    })
                                    .map(|gid| RdmaGidInfo {
                                        index: gid.index,
                                        gid: gid.gid,
                                        gid_type: gid.gid_type.clone(),
                                    })
                                    .collect(),
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

    fn make_rdma_context() -> Context {
        let active_devices =
            ruapc_rdma::ActiveDevice::available().expect("RDMA devices should be available");
        let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
        let mut devices = crate::Devices::default();
        for dev in active_devices {
            if prefer_rxe && !dev.info().name.starts_with("rxe") {
                continue;
            }
            devices.add_rdma_device(dev);
        }
        assert!(!devices.rdma_devices().is_empty(), "no RDMA device found");
        let config = SocketPoolConfig {
            socket_type: SocketType::RDMA,
        };
        Context::create(&config).expect("failed to create RDMA context")
    }

    #[tokio::test]
    async fn test_rdma_service_info_returns_devices() {
        let ctx = make_rdma_context();
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
        };
        let request = rdma::ConnectRequest {
            endpoint,
            target: rdma::DeviceSelection {
                device_name: "missing".into(),
                port_num: 1,
                gid_index: 0,
            },
        };
        let result = ().connect(&ctx, &request).await;
        assert!(result.is_err());
    }
}
