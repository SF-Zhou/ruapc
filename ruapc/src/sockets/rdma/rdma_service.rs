use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, Result, rdma, service};

/// Information about available RDMA devices in the system.
///
/// This structure is used to exchange RDMA device capabilities and
/// configuration between services.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RdmaInfo {
    /// List of available RDMA devices and their capabilities
    pub devices: Vec<ruapc_rdma_sys::DeviceInfo>,
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

    /// Establishes an RDMA connection with the specified endpoint.
    async fn connect(&self, ctx: &Context, endpoint: &rdma::Endpoint) -> Result<rdma::Endpoint>;
}

/// Default implementation of `RdmaService` for the unit type.
///
/// This implementation delegates all operations to the socket pool
/// stored in the context's state.
impl RdmaService for () {
    /// Retrieves RDMA device information from the socket pool.
    async fn info(&self, ctx: &Context, (): &()) -> Result<rdma::RdmaInfo> {
        ctx.state.socket_pool.rdma_info()
    }

    /// Establishes an RDMA connection using the socket pool.
    async fn connect(&self, ctx: &Context, endpoint: &rdma::Endpoint) -> Result<rdma::Endpoint> {
        ctx.state.socket_pool.rdma_connect(endpoint, &ctx.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SocketPoolConfig, SocketType};

    fn make_rdma_context() -> Option<Context> {
        let active_devices = ruapc_rdma_sys::ActiveDevice::available().ok()?;
        let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
        let mut devices = crate::Devices::new();
        for dev in active_devices {
            if prefer_rxe && !dev.info().name.starts_with("rxe") {
                continue;
            }
            devices.add_rdma_device(dev);
        }
        if devices.rdma_devices().is_empty() {
            return None;
        }
        let config = SocketPoolConfig {
            socket_type: SocketType::RDMA,
        };
        Context::create(&config).ok()
    }

    #[tokio::test]
    async fn test_rdma_service_info_returns_devices() {
        let ctx = match make_rdma_context() {
            Some(c) => c,
            None => return,
        };
        let result = ().info(&ctx, &()).await;
        assert!(result.is_ok());
        let info = result.unwrap();
        assert!(!info.devices.is_empty());
    }

    #[tokio::test]
    async fn test_rdma_service_connect_non_rdma_returns_err() {
        // With a TCP pool, `connect` should propagate the error from rdma_connect.
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let endpoint = rdma::Endpoint {
            qp_num: 0,
            gid: ruapc_rdma_sys::ibv_gid::default(),
            lid: 0,
        };
        let result = ().connect(&ctx, &endpoint).await;
        assert!(result.is_err());
    }
}
