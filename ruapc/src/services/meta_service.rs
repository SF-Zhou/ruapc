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
}

impl MetaService for () {
    async fn openapi(&self, ctx: &Context, (): &()) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(&ctx.state.router.openapi).unwrap())
    }

    async fn list_methods(&self, ctx: &Context, (): &()) -> Result<Vec<String>> {
        Ok(ctx.state.router.methods.keys().cloned().collect())
    }
}
