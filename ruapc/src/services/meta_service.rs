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
    async fn get_metadata(&self, ctx: &Context, req: &()) -> Result<Metadata>;
    async fn list_methods(&self, ctx: &Context, req: &()) -> Result<Vec<String>>;
}

impl MetaService for () {
    async fn get_metadata(&self, ctx: &Context, (): &()) -> Result<Metadata> {
        let methods = ctx
            .state
            .router
            .methods
            .iter()
            .map(|(name, method)| (name.clone(), method.info.clone()))
            .collect();
        Ok(Metadata { methods })
    }

    async fn list_methods(&self, ctx: &Context, (): &()) -> Result<Vec<String>> {
        Ok(ctx.state.router.methods.keys().cloned().collect())
    }
}
