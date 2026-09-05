//! Conditions must guard all generated items, including registration code.

use std::sync::Arc;

type ContextAlias = ruapc::Context;
type RequestAlias = String;
type ResponseAlias = ruapc::Result<String>;

// Generated code must not accidentally resolve these names in the caller.
#[allow(dead_code)]
struct Send;
#[allow(dead_code)]
struct Box;

#[ruapc::service]
#[cfg(any())]
trait DisabledService {
    async fn missing(&self, ctx: &MissingContext, req: &MissingRequest) -> MissingResponse;
}

#[ruapc::service]
#[cfg_attr(all(), cfg_attr(all(), cfg(any()), doc = "disabled service"))]
trait NestedDisabledService {
    async fn missing(&self, ctx: &MissingContext, req: &MissingRequest) -> MissingResponse;
}

#[ruapc::service]
/// Trait attributes and supertraits survive expansion.
trait ConditionalService: ::core::marker::Send + Sync {
    #[cfg(any())]
    async fn missing(&self, ctx: &MissingContext, req: &MissingRequest) -> MissingResponse;

    #[cfg_attr(all(), cfg_attr(all(), cfg(any()), doc = "disabled method"))]
    async fn nested_missing(&self, ctx: &MissingContext, req: &MissingRequest) -> MissingResponse;

    #[cfg_attr(all(), doc = "enabled method")]
    async fn r#type(&self, _: &ContextAlias, _: &RequestAlias) -> ResponseAlias;
}

struct Handler;

impl ConditionalService for Handler {
    async fn r#type(&self, _: &ContextAlias, request: &RequestAlias) -> ResponseAlias {
        Ok(request.clone())
    }
}

fn main() {
    let mut router = ruapc::Router::default();
    Arc::new(Handler).ruapc_export(&mut router);
    assert_eq!(
        router
            .method_names()
            .filter(|name| name.starts_with("ConditionalService/"))
            .collect::<Vec<_>>(),
        ["ConditionalService/type"]
    );
}
