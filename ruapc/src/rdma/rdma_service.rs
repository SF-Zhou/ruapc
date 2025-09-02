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
    pub devices: Vec<ruapc_rdma::DeviceInfo>,
}

/// Service interface for RDMA operations.
///
/// This trait defines the core operations for managing RDMA connections
/// and querying RDMA device information. It is designed to work with
/// the service macro for RPC functionality.
#[service]
pub trait RdmaService {
    /// Retrieves information about available RDMA devices.
    ///
    /// # Arguments
    /// * `ctx` - The context containing state and configuration
    /// * `_` - Unused parameter required by the service framework
    ///
    /// # Returns
    /// * `Ok(RdmaInfo)` - Information about available RDMA devices
    /// * `Err(Error)` - If device information cannot be retrieved
    async fn info(&self, ctx: &Context, _: &()) -> Result<rdma::RdmaInfo>;

    /// Establishes an RDMA connection with the specified endpoint.
    ///
    /// # Arguments
    /// * `ctx` - The context containing state and configuration
    /// * `endpoint` - The remote endpoint to connect to
    ///
    /// # Returns
    /// * `Ok(Endpoint)` - The local endpoint information if connection succeeds
    /// * `Err(Error)` - If connection establishment fails
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
