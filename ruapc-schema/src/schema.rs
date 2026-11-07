//! Schema generation and OpenAPI projection, separate from runtime dispatch.

use indexmap::IndexMap;
use openapiv3::{
    Components, MediaType, OpenAPI, Operation, Paths, ReferenceOr, RequestBody, Response,
    Responses, StatusCode,
};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeMap, sync::Mutex};

/// Encoding of a method's request or response payload.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PayloadFormat {
    /// The schema describes the complete serialized payload.
    #[default]
    Serialized,
    /// The schema describes control data followed by an unencoded byte tail.
    /// The wire payload carries a big-endian control length before the control.
    Raw,
}

impl PayloadFormat {
    fn is_serialized(&self) -> bool {
        *self == Self::Serialized
    }
}

/// Connection required to execute a method successfully.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MethodTransport {
    /// No transport restriction, including unary HTTP.
    #[default]
    Any,
    /// An established TCP, WebSocket or HTTP/2 RPC stream, including raw payloads.
    Stream,
    /// An established RDMA connection.
    Rdma,
}

impl MethodTransport {
    fn is_any(&self) -> bool {
        *self == Self::Any
    }
}

/// Peer state required by a method, independently of payload encoding.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MethodContext {
    /// The call does not depend on an earlier request at this peer.
    #[default]
    Standalone,
    /// The call addresses request or transfer state at the original peer.
    /// A request-status probe may legitimately report that state is gone.
    OriginalPeer,
}

impl MethodContext {
    fn is_standalone(&self) -> bool {
        *self == Self::Standalone
    }
}

/// Payload formats and execution prerequisites for a method.
///
/// Defaults describe ordinary serialized calls. These properties document
/// requirements; dispatch and validation remain the runtime's responsibility.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
pub struct MethodProtocol {
    #[serde(default, skip_serializing_if = "PayloadFormat::is_serialized")]
    pub request_format: PayloadFormat,
    #[serde(default, skip_serializing_if = "PayloadFormat::is_serialized")]
    pub response_format: PayloadFormat,
    #[serde(default, skip_serializing_if = "MethodTransport::is_any")]
    pub transport: MethodTransport,
    #[serde(default, skip_serializing_if = "MethodContext::is_standalone")]
    pub context: MethodContext,
}

impl MethodProtocol {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// Schema and protocol information for a service method.
///
/// Contains the request and response schemas exposed through the method
/// directory and OpenAPI generation.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct MethodSchema {
    /// Request schema, or the control schema when the request format is raw.
    pub request_schema: Schema,
    /// Response schema, or the control schema when the response format is raw.
    pub response_schema: Schema,
    /// Payload formats and execution prerequisites.
    #[serde(default, skip_serializing_if = "MethodProtocol::is_default")]
    pub protocol: MethodProtocol,
}

/// Generates method schemas and their referenced definitions.
///
/// Register schemas while constructing a method directory, then pass its
/// entries to [`build`](Self::build). The registry owns the referenced
/// definitions; callers own method names.
pub struct SchemaRegistry {
    // Generators contain Send-only transform callbacks. The mutex makes a
    // registered directory shareable; registration itself has exclusive access.
    generator: Mutex<SchemaGenerator>,
    framework_error_schema: Option<Schema>,
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self {
            generator: Mutex::new(SchemaGenerator::new(
                schemars::generate::SchemaSettings::openapi3(),
            )),
            framework_error_schema: None,
        }
    }
}

impl SchemaRegistry {
    /// Generates schemas for a method's request and complete serialized response.
    /// Set the returned [`MethodSchema::protocol`] for raw control values or
    /// methods that require an established connection or original peer state.
    pub fn register<Req: JsonSchema, Rsp: JsonSchema>(&mut self) -> MethodSchema {
        let generator = self.generator.get_mut().unwrap();
        MethodSchema {
            request_schema: generator.subschema_for::<Req>(),
            response_schema: generator.subschema_for::<Rsp>(),
            protocol: MethodProtocol::default(),
        }
    }

    /// Registers the common serialized error envelope used when
    /// `MsgFlags::IsFrameworkError` (flag value 16) is set, independently of a method's
    /// declared response format. The runtime supplies its `Result<(), Error>`
    /// type so this crate remains independent of RPC error types.
    pub fn register_framework_error<Envelope: JsonSchema>(&mut self) {
        self.framework_error_schema = Some(
            self.generator
                .get_mut()
                .unwrap()
                .subschema_for::<Envelope>(),
        );
    }

    /// Builds an OpenAPI 3.0 document for the supplied methods.
    ///
    /// Each name is a wire name such as `Service/method`. Callers must supply
    /// only methods whose schemas were registered with this registry.
    /// Building snapshots the definitions without consuming them, so
    /// repeated builds preserve references and produce the same document.
    ///
    /// Returns an error if a generated schema cannot be converted to an OpenAPI
    /// type.
    pub fn build<'a>(
        &self,
        methods: impl Iterator<Item = (&'a str, &'a MethodSchema)>,
    ) -> serde_json::Result<OpenAPI> {
        let mut paths = BTreeMap::new();
        for (name, schema) in methods {
            let mut extensions = IndexMap::new();
            let mut requirements = Vec::new();
            if schema.protocol.transport != MethodTransport::Any {
                extensions.insert(
                    "x-ruapc-transport".into(),
                    serde_json::to_value(schema.protocol.transport)?,
                );
                requirements.push(match schema.protocol.transport {
                    MethodTransport::Stream => "an established TCP, WebSocket or HTTP/2 RPC stream",
                    MethodTransport::Rdma => "an established RDMA connection",
                    MethodTransport::Any => unreachable!(),
                });
            }
            if schema.protocol.context != MethodContext::Standalone {
                extensions.insert(
                    "x-ruapc-context".into(),
                    serde_json::to_value(schema.protocol.context)?,
                );
                requirements.push("the peer that owns the referenced request or transfer state");
            }
            let mut description = (!requirements.is_empty())
                .then(|| format!("Requires {}.", requirements.join(" and ")));
            if schema.protocol.transport != MethodTransport::Any
                || schema.protocol.request_format == PayloadFormat::Raw
                || schema.protocol.response_format == PayloadFormat::Raw
            {
                let description = description.get_or_insert_with(String::new);
                if !description.is_empty() {
                    description.push(' ');
                }
                description
                    .push_str("This method cannot be used as an independent unary JSON call.");
            }
            let operation = Operation {
                operation_id: Some(format!("/{name}")),
                description,
                request_body: Some(ReferenceOr::Item(RequestBody {
                    content: payload_content(
                        &schema.request_schema,
                        schema.protocol.request_format,
                    )?,
                    required: true,
                    ..Default::default()
                })),
                responses: Responses {
                    responses: IndexMap::from([(
                        StatusCode::Code(200),
                        ReferenceOr::Item(Response {
                            content: payload_content(
                                &schema.response_schema,
                                schema.protocol.response_format,
                            )?,
                            ..Default::default()
                        }),
                    )]),
                    ..Default::default()
                },
                extensions,
                ..Default::default()
            };

            let path_item = openapiv3::PathItem {
                post: Some(operation),
                ..Default::default()
            };

            paths.insert(format!("/{name}"), ReferenceOr::Item(path_item));
        }

        let definitions = {
            let generator = self.generator.lock().unwrap();
            let mut snapshot = generator.clone();
            snapshot.take_definitions(true)
        };
        let schemas = definitions
            .into_iter()
            .map(|(name, schema)| Ok((name, serde_json::from_value(schema)?)))
            .collect::<serde_json::Result<IndexMap<_, _>>>()?;

        let mut extensions = IndexMap::new();
        if let Some(schema) = &self.framework_error_schema {
            extensions.insert(
                "x-ruapc-framework-error".into(),
                json!({
                    "flag": 16,
                    "payload_format": "serialized",
                    "schema": schema,
                    "unary_header": { "name": "x-ruapc-framework-error", "value": "true" }
                }),
            );
        }

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
            extensions,
            ..Default::default()
        })
    }
}

fn payload_content(
    schema: &Schema,
    format: PayloadFormat,
) -> serde_json::Result<IndexMap<String, MediaType>> {
    let (content_type, schema, extensions) = match format {
        PayloadFormat::Serialized => (
            "application/json",
            serde_json::to_value(schema)?,
            IndexMap::new(),
        ),
        PayloadFormat::Raw => (
            "application/octet-stream",
            json!({
                "type": "string",
                "format": "binary",
                "description": "RuaPC raw payload: a 4-byte big-endian control length, encoded control, then raw bytes. The control codec follows UseMessagePack."
            }),
            IndexMap::from([(
                "x-ruapc-control-schema".into(),
                serde_json::to_value(schema)?,
            )]),
        ),
    };
    Ok(IndexMap::from([(
        content_type.into(),
        MediaType {
            schema: Some(serde_json::from_value(schema)?),
            extensions,
            ..Default::default()
        },
    )]))
}
