use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Context, Result, router::MethodInfo};

/// Service metadata information.
///
/// Contains information about all registered methods in a service.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Metadata {
    /// Map of method names to their schema information.
    methods: HashMap<String, MethodInfo>,
}

/// Built-in metadata and introspection service.
///
/// The MetaService provides endpoints for discovering available methods
/// and obtaining OpenAPI specifications. It's automatically registered
/// with every RuaPC server.
///
/// # Methods
///
/// - `openapi`: Returns the complete OpenAPI 3.0 specification
/// - `list_methods`: Lists all registered RPC methods
/// - `is_message_waiting`: Checks if a message ID is awaiting a response
///
/// # Examples
///
/// ```rust,no_run
/// # use ruapc::*;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let ctx = Context::create(&SocketPoolConfig::default())?;
/// # let client = Client::default();
/// // List all available methods
/// let methods = client.list_methods(&ctx, &()).await?;
/// println!("Available methods: {:?}", methods);
///
/// // Get OpenAPI specification
/// let spec = client.openapi(&ctx, &()).await?;
/// # Ok(())
/// # }
/// ```
#[ruapc_macro::service]
pub trait MetaService {
    /// Returns the OpenAPI 3.0 specification for all registered services.
    ///
    /// # Arguments
    ///
    /// * `ctx` - RPC context
    /// * `req` - Empty request (unit type)
    ///
    /// # Returns
    ///
    /// Returns a JSON value containing the complete OpenAPI specification.
    async fn openapi(&self, ctx: &Context, req: &()) -> Result<serde_json::Value>;

    /// Lists all registered RPC method names.
    ///
    /// # Arguments
    ///
    /// * `ctx` - RPC context
    /// * `req` - Empty request (unit type)
    ///
    /// # Returns
    ///
    /// Returns a vector of method names in "ServiceName/method_name" format.
    async fn list_methods(&self, ctx: &Context, req: &()) -> Result<Vec<String>>;

    /// Verifies if a given message ID is currently being waited on.
    ///
    /// Useful for debugging and monitoring pending RPC calls.
    ///
    /// # Arguments
    ///
    /// * `ctx` - RPC context
    /// * `msgid` - The message ID to check
    ///
    /// # Returns
    ///
    /// Returns true if a response is being awaited for this message ID.
    async fn is_message_waiting(&self, ctx: &Context, msgid: &u64) -> Result<bool>;
}

impl MetaService for () {
    async fn openapi(&self, ctx: &Context, (): &()) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(&ctx.state.router.openapi).unwrap())
    }

    async fn list_methods(&self, ctx: &Context, (): &()) -> Result<Vec<String>> {
        Ok(ctx.state.router.methods.keys().cloned().collect())
    }

    async fn is_message_waiting(&self, ctx: &Context, msgid: &u64) -> Result<bool> {
        Ok(ctx.state.waiter.contains_message_id(*msgid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, SocketPoolConfig};

    #[tokio::test]
    async fn test_openapi_returns_json_object() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let result = ().openapi(&ctx, &()).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_object());
    }

    #[tokio::test]
    async fn test_is_message_waiting_false_when_no_waiter() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let result = ().is_message_waiting(&ctx, &9999u64).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_is_message_waiting_true_when_allocated() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let (msgid, _rx) = ctx.state.waiter.alloc();
        let result = ().is_message_waiting(&ctx, &msgid).await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }
}
