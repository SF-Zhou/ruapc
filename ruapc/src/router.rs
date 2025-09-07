use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use foldhash::fast::RandomState;
use indexmap::IndexMap;
use openapiv3::{
    Components, MediaType, OpenAPI, Operation, PathItem, Paths, ReferenceOr, RequestBody, Response,
    Responses, StatusCode,
};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

#[cfg(feature = "rdma")]
use crate::rdma::RdmaService;
use crate::{
    Context, Message,
    error::{Error, ErrorKind, Result},
    services::MetaService,
};

type Func = Box<dyn Fn(Context, Message) -> Result<()> + Send + Sync>;

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct MethodInfo {
    pub req_schema: Schema,
    pub rsp_schema: Schema,
}

pub struct Method {
    pub info: MethodInfo,
    func: Func,
}

pub struct Router {
    pub generator: Mutex<SchemaGenerator>,
    pub methods: HashMap<String, Method, RandomState>,
    pub openapi: OpenAPI,
}

impl Default for Router {
    fn default() -> Self {
        let settings = schemars::generate::SchemaSettings::openapi3();
        let mut this = Self {
            generator: Mutex::new(SchemaGenerator::new(settings)),
            methods: HashMap::default(),
            openapi: OpenAPI::default(),
        };
        MetaService::ruapc_export(Arc::new(()), &mut this);
        #[cfg(feature = "rdma")]
        RdmaService::ruapc_export(Arc::new(()), &mut this);
        this
    }
}

impl Router {
    pub fn add_method<Req, Rsp>(&mut self, name: &str, func: Func)
    where
        Req: JsonSchema,
        Rsp: JsonSchema,
    {
        let (req_schema, rsp_schema) = {
            let mut generator = self.generator.lock().unwrap();
            (
                generator.subschema_for::<Req>(),
                generator.subschema_for::<Rsp>(),
            )
        };

        self.methods.insert(
            name.to_string(),
            Method {
                info: MethodInfo {
                    req_schema,
                    rsp_schema,
                },
                func,
            },
        );
    }

    pub fn build_open_api(&mut self) -> Result<()> {
        let mut paths = IndexMap::<String, ReferenceOr<PathItem>>::new();
        for (name, method) in &self.methods {
            let request_schema = serde_json::to_value(&method.info.req_schema)?;
            let response_schema = serde_json::to_value(&method.info.rsp_schema)?;

            let request_body = RequestBody {
                content: {
                    IndexMap::from([(
                        "application/json".to_string(),
                        MediaType {
                            schema: Some(serde_json::from_value(request_schema)?),
                            ..Default::default()
                        },
                    )])
                },
                required: true,
                ..Default::default()
            };

            let response = Response {
                content: {
                    IndexMap::from([(
                        "application/json".to_string(),
                        MediaType {
                            schema: Some(serde_json::from_value(response_schema)?),
                            ..Default::default()
                        },
                    )])
                },
                ..Default::default()
            };

            let operation = Operation {
                operation_id: Some(name.clone()),
                request_body: Some(ReferenceOr::Item(request_body)),
                responses: Responses {
                    responses: IndexMap::from([(
                        StatusCode::Code(200),
                        ReferenceOr::Item(response),
                    )]),
                    ..Default::default()
                },
                ..Default::default()
            };

            let path_item = openapiv3::PathItem {
                post: Some(operation),
                ..Default::default()
            };

            paths.insert(name.clone(), ReferenceOr::Item(path_item));
        }

        let mut generator = self.generator.lock().unwrap();
        let definitions = generator.take_definitions(true);
        let schemas = definitions
            .into_iter()
            .map(|(name, schema)| (name, serde_json::from_value(schema).unwrap()))
            .collect::<IndexMap<_, _>>();

        // Create base OpenAPI specification with natural builder pattern
        self.openapi = OpenAPI {
            openapi: "3.0.0".to_string(),
            components: Some(Components {
                schemas,
                ..Default::default()
            }),
            paths: Paths {
                paths,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(())
    }

    pub fn method_names(&self) -> impl Iterator<Item = &String> {
        self.methods.keys()
    }

    pub fn dispatch(&self, mut ctx: Context, msg: Message) {
        if let Some(method) = self.methods.get(&msg.meta.method) {
            let _ = (method.func)(ctx, msg);
        } else {
            tokio::spawn(async move {
                let m = format!("method not found: {}", msg.meta.method);
                tracing::error!("{}", m);
                ctx.send_err_rsp(msg.meta, Error::new(ErrorKind::InvalidArgument, m))
                    .await;
            });
        }
    }
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("methods", &self.methods.keys())
            .field("generator", &())
            .finish()
    }
}
