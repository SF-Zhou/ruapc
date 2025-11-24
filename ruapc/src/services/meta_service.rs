use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, Result, router::MethodInfo};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Metadata {
    methods: HashMap<String, MethodInfo>,
}

#[ruapc_macro::service]
pub trait MetaService {
    async fn openapi(&self, ctx: &Context, req: &()) -> Result<serde_json::Value>;
    async fn list_methods(&self, ctx: &Context, req: &()) -> Result<Vec<String>>;

    /// Verifies if a given UUID is currently being waited on.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The RPC context
    /// * `uuid` - The UUID to verify
    ///
    /// # Returns
    ///
    /// Returns true if the UUID is being waited on, false otherwise
    async fn verify_uuid(&self, ctx: &Context, uuid: &u64) -> Result<bool>;
}

impl MetaService for () {
    async fn openapi(&self, ctx: &Context, (): &()) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(&ctx.state.router.openapi).unwrap())
    }

    async fn list_methods(&self, ctx: &Context, (): &()) -> Result<Vec<String>> {
        Ok(ctx.state.router.methods.keys().cloned().collect())
    }

    async fn verify_uuid(&self, ctx: &Context, uuid: &u64) -> Result<bool> {
        Ok(ctx.state.waiter.contains_uuid(*uuid))
    }
}
