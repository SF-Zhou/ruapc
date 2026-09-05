//! Schema generation and OpenAPI projection, separate from runtime dispatch.

use crate::Result;
use indexmap::IndexMap;
use openapiv3::{
    Components, MediaType, OpenAPI, Operation, Paths, ReferenceOr, RequestBody, Response,
    Responses, StatusCode,
};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Mutex};

/// JSON schema information for a service method.
///
/// Contains the request and response schemas exposed through reflection and
/// OpenAPI generation.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct MethodSchema {
    /// JSON schema for the request type.
    pub request_schema: Schema,
    /// JSON schema for the response type.
    pub response_schema: Schema,
}

pub(super) struct SchemaRegistry {
    // Generators contain Send-only transform callbacks. The mutex makes a
    // registered Router shareable; registration itself has exclusive access.
    public: Mutex<SchemaGenerator>,
    internal: Mutex<SchemaGenerator>,
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        let generator = || {
            Mutex::new(SchemaGenerator::new(
                schemars::generate::SchemaSettings::openapi3(),
            ))
        };
        Self {
            public: generator(),
            internal: generator(),
        }
    }
}

impl SchemaRegistry {
    pub(super) fn register<Req: JsonSchema, Rsp: JsonSchema>(
        &mut self,
        public: bool,
    ) -> MethodSchema {
        let generator = if public {
            &mut self.public
        } else {
            &mut self.internal
        };
        let generator = generator.get_mut().unwrap();
        MethodSchema {
            request_schema: generator.subschema_for::<Req>(),
            response_schema: generator.subschema_for::<Rsp>(),
        }
    }

    pub(super) fn build<'a>(
        &self,
        methods: impl Iterator<Item = (&'a str, &'a MethodSchema)>,
    ) -> Result<OpenAPI> {
        let mut paths = BTreeMap::new();
        for (name, schema) in methods {
            let operation = Operation {
                operation_id: Some(format!("/{name}")),
                request_body: Some(ReferenceOr::Item(RequestBody {
                    content: json_content(&schema.request_schema)?,
                    required: true,
                    ..Default::default()
                })),
                responses: Responses {
                    responses: IndexMap::from([(
                        StatusCode::Code(200),
                        ReferenceOr::Item(Response {
                            content: json_content(&schema.response_schema)?,
                            ..Default::default()
                        }),
                    )]),
                    ..Default::default()
                },
                ..Default::default()
            };

            let path_item = openapiv3::PathItem {
                post: Some(operation),
                ..Default::default()
            };

            paths.insert(format!("/{name}"), ReferenceOr::Item(path_item));
        }

        let definitions = {
            let generator = self.public.lock().unwrap();
            let mut snapshot = generator.clone();
            snapshot.take_definitions(true)
        };
        let schemas = definitions
            .into_iter()
            .map(|(name, schema)| Ok((name, serde_json::from_value(schema)?)))
            .collect::<Result<IndexMap<_, _>>>()?;

        Ok(OpenAPI {
            openapi: "3.0.0".to_string(),
            components: Some(Components {
                schemas,
                ..Default::default()
            }),
            paths: Paths {
                paths: paths.into_iter().collect(),
                ..Default::default()
            },
            ..Default::default()
        })
    }
}

fn json_content(schema: &Schema) -> Result<IndexMap<String, MediaType>> {
    Ok(IndexMap::from([(
        "application/json".into(),
        MediaType {
            schema: Some(serde_json::from_value(serde_json::to_value(schema)?)?),
            ..Default::default()
        },
    )]))
}
