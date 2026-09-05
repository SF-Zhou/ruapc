use std::collections::BTreeMap;

use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize};

use crate::{Context, Result};

/// Version of RuaPC's built-in reflection contract.
pub const REFLECTION_PROTOCOL_VERSION: u32 = 1;

/// Selects the public services returned by [`ReflectionService::describe`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct DescribeRequest {
    /// Exact wire service name to return. `None` returns every public service.
    pub service: Option<String>,
}

/// Public RPC surface and implementation version reported by a RuaPC peer.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ServerDescription {
    /// Version of the reflection contract used by this response.
    pub protocol_version: u32,
    /// Version of RuaPC serving the request.
    pub ruapc_version: String,
    /// Public services, sorted by wire name.
    pub services: Vec<ServiceDescription>,
    /// OpenAPI components referenced by the method schemas.
    ///
    /// Keeping these at the response root makes references such as
    /// `#/components/schemas/Request` directly resolvable.
    pub components: serde_json::Value,
}

/// One publicly discoverable service.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ServiceDescription {
    /// Service name used on the wire.
    pub name: String,
    /// Public methods, sorted by their service-local name.
    pub methods: Vec<MethodDescription>,
}

/// Name and JSON schemas of one publicly discoverable method.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MethodDescription {
    /// Method name within its service.
    pub name: String,
    /// JSON schema of the request body.
    #[schemars(with = "serde_json::Value")]
    pub request_schema: Schema,
    /// JSON schema of the complete wire response, including RuaPC's `Result`
    /// envelope.
    #[schemars(with = "serde_json::Value")]
    pub response_schema: Schema,
}

/// Built-in public reflection service.
///
/// It exposes a compact, structured method directory as well as the full
/// OpenAPI document. Internal bootstrap and remote-memory methods are omitted
/// from both responses.
#[ruapc_macro::service(name = "_ruapc.meta")]
pub trait ReflectionService {
    /// Describes public RPC services and their request/response schemas.
    async fn describe(&self, ctx: &Context, req: &DescribeRequest) -> Result<ServerDescription>;

    /// Returns the OpenAPI 3.0 specification for public RPC methods.
    async fn openapi(&self, ctx: &Context, req: &()) -> Result<serde_json::Value>;
}

impl ReflectionService for () {
    async fn describe(&self, ctx: &Context, req: &DescribeRequest) -> Result<ServerDescription> {
        let mut services = BTreeMap::<String, Vec<MethodDescription>>::new();
        for (qualified_name, schema) in ctx.state.router.method_schemas() {
            let (service_name, method_name) = qualified_name
                .split_once('/')
                .expect("router validates wire method names during registration");
            if req
                .service
                .as_deref()
                .is_some_and(|requested| requested != service_name)
            {
                continue;
            }
            services
                .entry(service_name.to_owned())
                .or_default()
                .push(MethodDescription {
                    name: method_name.to_owned(),
                    request_schema: schema.request_schema.clone(),
                    response_schema: schema.response_schema.clone(),
                });
        }

        let services = services
            .into_iter()
            .map(|(name, mut methods)| {
                methods.sort_by(|left, right| left.name.cmp(&right.name));
                ServiceDescription { name, methods }
            })
            .collect();

        Ok(ServerDescription {
            protocol_version: REFLECTION_PROTOCOL_VERSION,
            ruapc_version: env!("CARGO_PKG_VERSION").to_owned(),
            services,
            components: ctx
                .state
                .router
                .openapi
                .components
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?
                .unwrap_or_else(|| serde_json::json!({})),
        })
    }

    async fn openapi(&self, ctx: &Context, (): &()) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(&ctx.state.router.openapi)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, SocketPoolConfig};

    #[tokio::test]
    async fn test_describe_is_sorted_and_hides_internal_services() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let description = ().describe(&ctx, &DescribeRequest::default()).await.unwrap();

        assert_eq!(description.protocol_version, REFLECTION_PROTOCOL_VERSION);
        assert_eq!(description.ruapc_version, env!("CARGO_PKG_VERSION"));
        assert!(
            description
                .services
                .windows(2)
                .all(|pair| pair[0].name < pair[1].name)
        );
        assert!(description.services.iter().all(|service| {
            service
                .methods
                .windows(2)
                .all(|pair| pair[0].name < pair[1].name)
        }));
        assert!(
            description
                .services
                .iter()
                .any(|service| service.name == "_ruapc.meta")
        );
        assert!(
            description
                .services
                .iter()
                .all(|service| service.name != "_ruapc.memory")
        );
        assert!(
            description
                .services
                .iter()
                .all(|service| service.name != "_ruapc.rdma")
        );
    }

    #[tokio::test]
    async fn test_describe_filters_by_exact_service_name() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let description = ()
            .describe(
                &ctx,
                &DescribeRequest {
                    service: Some("_ruapc.meta".to_owned()),
                },
            )
            .await
            .unwrap();
        assert_eq!(description.services.len(), 1);
        assert_eq!(description.services[0].name, "_ruapc.meta");

        let missing = ()
            .describe(
                &ctx,
                &DescribeRequest {
                    service: Some("missing".to_owned()),
                },
            )
            .await
            .unwrap();
        assert!(missing.services.is_empty());
    }

    #[tokio::test]
    async fn test_describe_method_references_resolve_in_response() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let description = ().describe(&ctx, &DescribeRequest::default()).await.unwrap();
        let method = description
            .services
            .iter()
            .find(|service| service.name == "_ruapc.meta")
            .unwrap()
            .methods
            .iter()
            .find(|method| method.name == "describe")
            .unwrap();
        let request_schema = serde_json::to_value(&method.request_schema).unwrap();
        let reference = request_schema
            .get("$ref")
            .and_then(serde_json::Value::as_str)
            .expect("describe request schema should reference an OpenAPI component");

        let document = serde_json::to_value(&description).unwrap();
        let pointer = reference
            .strip_prefix('#')
            .expect("local OpenAPI schema reference");
        assert!(
            document.pointer(pointer).is_some(),
            "unresolved method schema reference: {reference}"
        );
    }

    #[tokio::test]
    async fn test_openapi_returns_json_object() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let result = ().openapi(&ctx, &()).await.unwrap();
        assert!(result.is_object());
    }
}
