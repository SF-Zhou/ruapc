use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, Result, rdma, service};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RdmaInfo {
    pub devices: Vec<ruapc_rdma::DeviceInfo>,
}

#[service]
pub trait RdmaService {
    async fn info(&self, ctx: &Context, _: &()) -> Result<rdma::RdmaInfo>;

    async fn connect(&self, ctx: &Context, endpoint: &rdma::Endpoint) -> Result<rdma::Endpoint>;
}

impl RdmaService for () {
    async fn info(&self, ctx: &Context, (): &()) -> Result<rdma::RdmaInfo> {
        ctx.state.socket_pool.rdma_info()
    }

    async fn connect(&self, ctx: &Context, endpoint: &rdma::Endpoint) -> Result<rdma::Endpoint> {
        ctx.state.socket_pool.rdma_connect(endpoint, &ctx.state)
    }
}
