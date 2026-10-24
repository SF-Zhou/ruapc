use std::{collections::HashMap, sync::Arc};

use foldhash::fast::RandomState;
use openapiv3::OpenAPI;
use schemars::JsonSchema;

mod schema;
pub use schema::MethodSchema;

#[cfg(feature = "rdma")]
use crate::rdma::{RdmaBootstrapService, RdmaProbeService};
use crate::{
    Context, Payload,
    error::{Error, ErrorKind, Result},
    services::{MemoryService, ReflectionService},
};

/// Type alias for service method handler functions.
type Func = Box<dyn Fn(Context, Payload) -> Result<()> + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MethodVisibility {
    Public,
    Internal,
}

impl MethodVisibility {
    fn is_public(self) -> bool {
        self == Self::Public
    }
}

/// Internal method representation containing metadata and handler.
struct Method {
    /// Method schema information.
    schema: MethodSchema,
    /// Whether this method is exposed through public reflection APIs.
    visibility: MethodVisibility,
    /// The actual handler function for this method.
    func: Func,
}

/// Service registry, request dispatch and public method schemas.
///
/// Define services with [`#[service]`](crate::service) and call their generated
/// `ruapc_export` before passing the router to [`Server::create`](crate::Server::create)
/// or [`Context::create_with_router`]. The default router includes public
/// reflection and internal remote-memory services, plus RDMA bootstrap and
/// connection probes when that feature is enabled.
///
/// Internal methods remain dispatchable but are excluded from
/// [`method_names`](Self::method_names), [`method_schemas`](Self::method_schemas)
/// and OpenAPI. Registration rejects duplicate wire names across both visibilities.
pub struct Router {
    schemas: schema::SchemaRegistry,
    /// Registered methods mapped by name (e.g., "ServiceName/method_name").
    methods: HashMap<String, Method, RandomState>,
    /// OpenAPI specification populated by [`build_open_api`](Self::build_open_api),
    /// which server and context creation call automatically.
    pub openapi: OpenAPI,
}

impl Default for Router {
    fn default() -> Self {
        let mut this = Self {
            schemas: schema::SchemaRegistry::default(),
            methods: HashMap::default(),
            openapi: OpenAPI::default(),
        };
        let dummy = Arc::new(());
        ReflectionService::ruapc_export(dummy.clone(), &mut this);
        MemoryService::ruapc_export(dummy.clone(), &mut this);
        #[cfg(feature = "rdma")]
        RdmaBootstrapService::ruapc_export(dummy.clone(), &mut this);
        #[cfg(feature = "rdma")]
        RdmaProbeService::ruapc_export(dummy.clone(), &mut this);
        this
    }
}

impl Router {
    /// Interposes on an existing handler without changing its schemas or visibility.
    #[cfg(all(test, feature = "rdma"))]
    pub(crate) fn wrap_method(&mut self, name: &str, wrap: impl FnOnce(Func) -> Func) {
        let method = self.methods.get_mut(name).expect("test method must exist");
        let original = std::mem::replace(&mut method.func, Box::new(|_, _| Ok(())));
        method.func = wrap(original);
    }

    /// Registers a public method and its request/response schemas.
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
        self.add_method_with_visibility::<Req, Rsp>(name, func, MethodVisibility::Public);
    }

    /// Registers a method hidden from unary HTTP, public reflection and OpenAPI.
    ///
    /// This is public only because `#[ruapc::service(internal)]` expands in
    /// downstream crates and must be able to call it. Name validation and
    /// duplicate-registration panics match [`add_method`](Self::add_method).
    #[doc(hidden)]
    pub fn add_internal_method<Req, Rsp>(&mut self, name: &str, func: Func)
    where
        Req: JsonSchema,
        Rsp: JsonSchema,
    {
        self.add_method_with_visibility::<Req, Rsp>(name, func, MethodVisibility::Internal);
    }

    fn add_method_with_visibility<Req, Rsp>(
        &mut self,
        name: &str,
        func: Func,
        visibility: MethodVisibility,
    ) where
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
                schema: self.schemas.register::<Req, Rsp>(visibility.is_public()),
                visibility,
                func,
            },
        );
    }

    /// Rebuilds [`openapi`](Self::openapi) from public methods as OpenAPI 3.0
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

    /// Returns an iterator over publicly discoverable method names.
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
        self.methods
            .iter()
            .filter(|(_, method)| method.visibility.is_public())
            .map(|(name, _)| name.as_str())
    }

    /// Returns whether `name` identifies a publicly exposed method.
    pub fn is_public_method(&self, name: &str) -> bool {
        self.methods
            .get(name)
            .is_some_and(|method| method.visibility.is_public())
    }

    pub(crate) fn all_method_names(&self) -> impl Iterator<Item = &str> {
        self.methods.keys().map(String::as_str)
    }

    /// Returns the schemas of all publicly discoverable methods.
    pub fn method_schemas(&self) -> impl Iterator<Item = (&str, &MethodSchema)> {
        self.methods.iter().filter_map(|(name, method)| {
            method
                .visibility
                .is_public()
                .then_some((name.as_str(), &method.schema))
        })
    }

    /// Calls the registered callback for `ctx.msg_meta.method` synchronously.
    ///
    /// Generated callbacks schedule handler execution with admission control,
    /// deadline checks and panic containment. Custom callbacks supply their own
    /// policies; their returned errors are discarded here. An unknown method
    /// spawns a task to send an [`ErrorKind::InvalidArgument`] response.
    ///
    /// This dispatch path includes internal methods. Unary HTTP checks public
    /// visibility before calling it.
    pub fn dispatch(&self, mut ctx: Context, payload: Payload) {
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
            .field("methods", &self.all_method_names().collect::<Vec<_>>())
            .field("generator", &())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct DummyReq {
        value: String,
    }

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct DummyRsp {
        result: u64,
    }

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct InternalReq {
        secret: String,
    }

    fn noop_func(_ctx: Context, _payload: Payload) -> Result<()> {
        Ok(())
    }

    #[test]
    fn test_router_default_has_exact_builtin_methods() {
        let router = Router::default();
        let mut public_names: Vec<_> = router.method_names().collect();
        public_names.sort_unstable();
        assert_eq!(
            public_names,
            ["_ruapc.meta/describe", "_ruapc.meta/openapi"]
        );

        let mut all_names: Vec<_> = router.all_method_names().collect();
        all_names.sort_unstable();
        let mut expected = vec![
            "_ruapc.memory/read_inline",
            "_ruapc.memory/read_into_target",
            "_ruapc.memory/request_is_pending",
            "_ruapc.memory/write_inline",
            "_ruapc.meta/describe",
            "_ruapc.meta/openapi",
        ];
        #[cfg(feature = "rdma")]
        expected.extend([
            "_ruapc.rdma/cancel_connection",
            "_ruapc.rdma/commit_connection",
            "_ruapc.rdma/discover",
            "_ruapc.rdma/prepare_connection",
            "_ruapc.rdma/probe",
        ]);
        expected.sort_unstable();
        assert_eq!(all_names, expected);
        #[cfg(feature = "rdma")]
        {
            assert!(!router.is_public_method("_ruapc.rdma/probe"));
            assert!(
                router
                    .method_schemas()
                    .all(|(name, _)| name != "_ruapc.rdma/probe")
            );
        }
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn test_builtin_rdma_probe_is_not_a_unary_http_endpoint() {
        let config = crate::SocketPoolConfig {
            listen_mode: crate::ListenMode::UNIFIED,
            rdma: None,
            ..Default::default()
        };
        let server = crate::Server::create(Router::default(), &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        assert!(
            !server
                .state()
                .router
                .openapi
                .paths
                .paths
                .contains_key("/_ruapc.rdma/probe")
        );
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{address}/_ruapc.rdma/probe"))
            .json(&7u64)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404);
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
        assert!(router.is_public_method("TestSvc/do_thing"));
    }

    #[test]
    fn test_internal_method_is_dispatchable_but_not_publicly_discoverable() {
        let mut router = Router::default();
        let initial_public_count = router.method_names().count();
        let initial_total_count = router.all_method_names().count();

        router.add_internal_method::<InternalReq, DummyRsp>("Control/secret", Box::new(noop_func));

        assert_eq!(router.method_names().count(), initial_public_count);
        assert_eq!(router.all_method_names().count(), initial_total_count + 1);
        assert!(!router.is_public_method("Control/secret"));
        assert!(
            router
                .all_method_names()
                .any(|name| name == "Control/secret")
        );
        assert!(
            router
                .method_schemas()
                .all(|(name, _)| name != "Control/secret")
        );
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
    fn test_build_open_api_excludes_internal_methods_and_schemas() {
        let mut router = Router::default();
        router.add_internal_method::<InternalReq, DummyRsp>("Control/secret", Box::new(noop_func));
        router.build_open_api().unwrap();

        assert!(!router.openapi.paths.paths.contains_key("/Control/secret"));
        let components = router.openapi.components.as_ref().unwrap();
        assert!(!components.schemas.contains_key("InternalReq"));
    }

    #[test]
    #[should_panic(expected = "RPC method `Duplicate/run` is already registered")]
    fn test_duplicate_wire_name_is_rejected_across_visibilities() {
        let mut router = Router::default();
        router.add_method::<DummyReq, DummyRsp>("Duplicate/run", Box::new(noop_func));
        router.add_internal_method::<DummyReq, DummyRsp>("Duplicate/run", Box::new(noop_func));
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
        let ctx = crate::Context::create(&SocketPoolConfig::default()).unwrap();
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

        let ctx = crate::Context::create(&SocketPoolConfig::default()).unwrap();
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
