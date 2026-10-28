use std::{collections::HashMap, sync::Arc};

use foldhash::fast::RandomState;
use openapiv3::OpenAPI;
use schemars::JsonSchema;

pub use ruapc_schema::MethodSchema;
use ruapc_schema::SchemaRegistry;

#[cfg(feature = "rdma")]
use crate::rdma::RdmaService;
use crate::{
    Context, Payload,
    error::{Error, ErrorKind, Result},
    services::MemoryService,
};

/// Type alias for service method handler functions.
type Func = Box<dyn Fn(Context, Payload) -> Result<()> + Send + Sync>;

/// Internal method representation containing metadata and handler.
struct Method {
    /// Method schema information.
    schema: MethodSchema,
    /// The actual handler function for this method.
    func: Func,
}

/// Service registry, request dispatch and method schemas.
///
/// Define services with [`#[service]`](crate::service) and call their generated
/// `ruapc_export` before passing the router to [`Server::create`](crate::Server::create)
/// or [`Context::create_with_router`]. The default router includes
/// remote-memory services, plus RDMA bootstrap, payload transfers and
/// connection probes when that feature is enabled.
///
/// All registered methods appear in [`method_names`](Self::method_names),
/// [`method_schemas`](Self::method_schemas) and OpenAPI, and can be called through
/// unary HTTP subject to their handler's transport and context requirements.
/// Registration rejects duplicate wire names.
/// Incoming dispatch is internal; applications invoke services through clients.
///
/// ```compile_fail
/// use ruapc::{Context, Payload, Router};
/// fn dispatch(router: &Router, context: Context, payload: Payload) {
///     router.dispatch(context, payload);
/// }
/// ```
pub struct Router {
    schemas: SchemaRegistry,
    /// Registered methods mapped by name (e.g., "ServiceName/method_name").
    methods: HashMap<String, Method, RandomState>,
    /// OpenAPI specification populated by [`build_open_api`](Self::build_open_api),
    /// which server and context creation call automatically.
    pub openapi: OpenAPI,
}

impl Default for Router {
    fn default() -> Self {
        let mut this = Self {
            schemas: SchemaRegistry::default(),
            methods: HashMap::default(),
            openapi: OpenAPI::default(),
        };
        let dummy = Arc::new(());
        MemoryService::ruapc_export(dummy.clone(), &mut this);
        #[cfg(feature = "rdma")]
        RdmaService::ruapc_export(dummy.clone(), &mut this);
        this
    }
}

impl Router {
    /// Interposes on an existing handler without changing its schemas.
    #[cfg(all(test, feature = "rdma"))]
    pub(crate) fn wrap_method(&mut self, name: &str, wrap: impl FnOnce(Func) -> Func) {
        let method = self.methods.get_mut(name).expect("test method must exist");
        let original = std::mem::replace(&mut method.func, Box::new(|_, _| Ok(())));
        method.func = wrap(original);
    }

    /// Registers a method and its request/response schemas.
    ///
    /// Normally called by [`#[service]`](crate::service). `name` is the wire name
    /// `Service/method`; `func` receives the context and undecoded payload.
    /// Custom callbacks are responsible for scheduling and response delivery.
    ///
    /// # Panics
    ///
    /// Panics if the wire name is already registered or is not `Service/method`
    /// with two nonempty components and no surrounding whitespace.
    pub fn add_method<Req, Rsp>(&mut self, name: &str, func: Func)
    where
        Req: JsonSchema,
        Rsp: JsonSchema,
    {
        let valid_name = name.split_once('/').is_some_and(|(service, method)| {
            !service.is_empty()
                && !method.is_empty()
                && !method.contains('/')
                && service.trim() == service
                && method.trim() == method
        });
        assert!(
            valid_name,
            "RPC method `{name}` must have the form `Service/method`"
        );
        assert!(
            !self.methods.contains_key(name),
            "RPC method `{name}` is already registered"
        );

        self.methods.insert(
            name.to_string(),
            Method {
                schema: self.schemas.register::<Req, Rsp>(),
                func,
            },
        );
    }

    /// Rebuilds [`openapi`](Self::openapi) from all methods as OpenAPI 3.0
    /// POST endpoints with request, response and referenced component schemas.
    /// Server and context creation call this automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if generated schemas cannot be converted to OpenAPI types.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::Router;
    /// # let mut router = Router::default();
    /// router.build_open_api().unwrap();
    /// let openapi_json = serde_json::to_string_pretty(&router.openapi).unwrap();
    /// ```
    pub fn build_open_api(&mut self) -> Result<()> {
        self.openapi = self.schemas.build(self.method_schemas())?;
        Ok(())
    }

    /// Returns an iterator over all registered method names.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::Router;
    /// # let router = Router::default();
    /// for method_name in router.method_names() {
    ///     println!("Registered method: {}", method_name);
    /// }
    /// ```
    pub fn method_names(&self) -> impl Iterator<Item = &str> {
        self.methods.keys().map(String::as_str)
    }

    /// Returns whether `name` identifies a registered method.
    pub fn has_method(&self, name: &str) -> bool {
        self.methods.contains_key(name)
    }

    /// Returns the schemas of all registered methods.
    pub fn method_schemas(&self) -> impl Iterator<Item = (&str, &MethodSchema)> {
        self.methods
            .iter()
            .map(|(name, method)| (name.as_str(), &method.schema))
    }

    /// Calls the registered callback for `ctx.msg_meta.method` synchronously.
    ///
    /// Generated callbacks schedule handler execution with admission control,
    /// deadline checks and panic containment. Custom callbacks supply their own
    /// policies; their returned errors are discarded here. An unknown method
    /// spawns a task to send an [`ErrorKind::InvalidArgument`] response.
    pub(crate) fn dispatch(&self, mut ctx: Context, payload: Payload) {
        if let Some(method) = self.methods.get(&ctx.msg_meta.method) {
            let _ = (method.func)(ctx, payload);
        } else {
            tokio::spawn(async move {
                let m = format!("method not found: {}", ctx.msg_meta.method);
                tracing::error!("{}", m);
                ctx.send_err_rsp(Error::new(ErrorKind::InvalidArgument, m))
                    .await;
            });
        }
    }
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("methods", &self.method_names().collect::<Vec<_>>())
            .field("generator", &())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[cfg(feature = "rdma")]
    use crate::{ListenMode, Server, SocketPoolConfig};

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct DummyReq {
        value: String,
    }

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct DummyRsp {
        result: u64,
    }

    fn noop_func(_ctx: Context, _payload: Payload) -> Result<()> {
        Ok(())
    }

    #[test]
    fn test_router_default_has_exact_builtin_methods() {
        let mut router = Router::default();
        router.build_open_api().unwrap();
        let mut all_names: Vec<_> = router.method_names().collect();
        all_names.sort_unstable();
        let mut expected = vec![
            "MemoryService/read_inline",
            "MemoryService/read_into_target",
            "MemoryService/request_is_pending",
            "MemoryService/write_inline",
        ];
        #[cfg(feature = "rdma")]
        expected.extend([
            "RdmaService/cancel_connection",
            "RdmaService/commit_connection",
            "RdmaService/discover",
            "RdmaService/prepare_connection",
            "RdmaService/finish",
            "RdmaService/offer",
            "RdmaService/probe",
        ]);
        expected.sort_unstable();
        assert_eq!(all_names, expected);
        let mut schemas: Vec<_> = router.method_schemas().map(|(name, _)| name).collect();
        schemas.sort_unstable();
        assert_eq!(schemas, expected);
        assert_eq!(router.openapi.paths.paths.len(), expected.len());
        for name in expected {
            assert!(router.has_method(name));
            assert!(router.openapi.paths.paths.contains_key(&format!("/{name}")));
        }

        let document = serde_json::to_value(&router.openapi).unwrap();
        assert_resolved_references(&document, &document);
    }

    fn assert_resolved_references(value: &serde_json::Value, document: &serde_json::Value) {
        match value {
            serde_json::Value::Object(object) => {
                if let Some(reference) = object.get("$ref").and_then(serde_json::Value::as_str) {
                    let pointer = reference.strip_prefix('#').expect("local schema reference");
                    assert!(
                        document.pointer(pointer).is_some(),
                        "unresolved {reference}"
                    );
                }
                for value in object.values() {
                    assert_resolved_references(value, document);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    assert_resolved_references(value, document);
                }
            }
            _ => {}
        }
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_builtin_rdma_probe_over_http_requires_rdma_connection() {
        let config = SocketPoolConfig {
            listen_mode: ListenMode::UNIFIED,
            rdma: None,
            ..Default::default()
        };
        let server = Server::create(Router::default(), &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        assert!(
            server
                .state()
                .router
                .openapi
                .paths
                .paths
                .contains_key("/RdmaService/probe")
        );
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{address}/RdmaService/probe"))
            .json(&7u64)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let error = response
            .json::<Result<serde_json::Value>>()
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidArgument);
        assert!(
            error
                .to_string()
                .contains("requires an established RDMA connection")
        );
        server.stop();
        server.join().await;
    }

    #[test]
    fn test_add_method_and_method_names() {
        let mut router = Router::default();
        let initial_count = router.method_names().count();

        router.add_method::<DummyReq, DummyRsp>("TestSvc/do_thing", Box::new(noop_func));

        let new_count = router.method_names().count();
        assert_eq!(new_count, initial_count + 1);
        assert!(router.method_names().any(|n| n == "TestSvc/do_thing"));
        assert!(router.has_method("TestSvc/do_thing"));
        assert!(!router.has_method("TestSvc/missing"));
    }

    #[test]
    fn test_build_open_api_populates_paths() {
        let mut router = Router::default();
        router.add_method::<DummyReq, DummyRsp>("MySvc/my_op", Box::new(noop_func));
        router.build_open_api().unwrap();

        let openapi = &router.openapi;
        assert_eq!(openapi.openapi, "3.0.0");
        // Paths should include the registered method.
        let path_keys: Vec<_> = openapi.paths.paths.keys().collect();
        assert!(
            path_keys.iter().any(|k| k.contains("MySvc/my_op")),
            "expected MySvc/my_op in OpenAPI paths, got: {:?}",
            path_keys
        );

        let first = serde_json::to_value(&router.openapi).unwrap();
        router.build_open_api().unwrap();
        let second = serde_json::to_value(&router.openapi).unwrap();
        assert_eq!(second, first, "OpenAPI generation should be idempotent");
    }

    #[test]
    fn test_build_open_api_error_preserves_previous_document() {
        struct Unsupported;

        impl JsonSchema for Unsupported {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                "Unsupported".into()
            }

            fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({"properties": 42})
            }
        }

        let mut router = Router::default();
        router.build_open_api().unwrap();
        let previous = serde_json::to_value(&router.openapi).unwrap();
        router.add_method::<Unsupported, ()>("Unsupported/run", Box::new(noop_func));

        let error = router.build_open_api().unwrap_err();
        assert_eq!(error.kind, ErrorKind::SerdeJsonError);
        assert_eq!(serde_json::to_value(&router.openapi).unwrap(), previous);
    }

    #[test]
    #[should_panic(expected = "RPC method `Duplicate/run` is already registered")]
    fn test_duplicate_wire_name_is_rejected() {
        let mut router = Router::default();
        router.add_method::<DummyReq, DummyRsp>("Duplicate/run", Box::new(noop_func));
        router.add_method::<DummyReq, DummyRsp>("Duplicate/run", Box::new(noop_func));
    }

    #[test]
    #[should_panic(expected = "must have the form `Service/method`")]
    fn test_invalid_wire_method_name_is_rejected() {
        let mut router = Router::default();
        router.add_method::<DummyReq, DummyRsp>("missing_separator", Box::new(noop_func));
    }

    #[test]
    fn test_router_debug_format() {
        let router = Router::default();
        let debug = format!("{:?}", router);
        assert!(debug.contains("Router"));
    }

    #[tokio::test]
    async fn test_dispatch_found_method_calls_func() {
        use crate::{Endpoint, SocketPoolConfig};
        let mut router = Router::default();
        router.add_method::<DummyReq, DummyRsp>("TestSvc/noop", Box::new(noop_func));

        // Build a context so dispatch has something to work with.
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let mut ctx =
            ctx.with_endpoint(Endpoint::tcp(["127.0.0.1:9999".parse().unwrap()]).unwrap());
        ctx.msg_meta.method = "TestSvc/noop".to_string();

        // dispatch() calls the func synchronously when the method is found.
        router.dispatch(ctx, Payload::Empty);
    }

    #[tokio::test]
    async fn test_dispatch_method_not_found_spawns_error_response() {
        use crate::{Endpoint, SocketPoolConfig};
        let router = Router::default();

        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let mut ctx =
            ctx.with_endpoint(Endpoint::tcp(["127.0.0.1:9999".parse().unwrap()]).unwrap());
        ctx.msg_meta.method = "UnknownSvc/missing".to_string();

        // dispatch() spawns a tokio task that tries to send an error response for unknown
        // methods. The task will fail silently since there is no connected socket endpoint.
        // We just verify dispatch() can be called without panicking.
        router.dispatch(ctx, Payload::Empty);
        // Yield to give the runtime a chance to poll the spawned task.
        tokio::task::yield_now().await;
    }
}
