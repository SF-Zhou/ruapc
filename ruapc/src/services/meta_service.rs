use super::{DescribeRequest, ServerDescription};
use crate::{Context, Result};

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
        Ok(ServerDescription::from_methods(
            env!("CARGO_PKG_VERSION"),
            req,
            ctx.state.router.method_schemas(),
            ctx.state.router.openapi.components.as_ref(),
        )?)
    }

    async fn openapi(&self, ctx: &Context, (): &()) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(&ctx.state.router.openapi)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, SocketPoolConfig, services::REFLECTION_PROTOCOL_VERSION};

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
