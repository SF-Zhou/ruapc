use std::collections::BTreeMap;

use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize};

use crate::MethodSchema;

/// Version of RuaPC's built-in reflection contract.
pub const REFLECTION_PROTOCOL_VERSION: u32 = 1;

/// Selects the public services returned by RuaPC's reflection service.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
// Preserve the description emitted before this type moved out of `ruapc`.
#[schemars(
    description = "Selects the public services returned by [`ReflectionService::describe`]."
)]
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

impl ServerDescription {
    /// Builds a sorted, optionally filtered public method directory.
    ///
    /// `version` is the serving RuaPC implementation's version, not this
    /// crate's version. Callers supply only public methods with validated
    /// `Service/method` wire names and the components from the corresponding
    /// OpenAPI document. Filtering retains those components so schema references
    /// remain resolvable.
    ///
    /// Returns an error if the components cannot be serialized.
    ///
    /// # Panics
    ///
    /// Panics if a supplied method name has no `/` separator.
    pub fn from_methods<'a>(
        version: impl Into<String>,
        req: &DescribeRequest,
        methods: impl Iterator<Item = (&'a str, &'a MethodSchema)>,
        components: Option<&openapiv3::Components>,
    ) -> serde_json::Result<Self> {
        let mut services = BTreeMap::<String, Vec<MethodDescription>>::new();
        for (qualified_name, schema) in methods {
            let (service_name, method_name) = qualified_name
                .split_once('/')
                .expect("method names must have the form Service/method");
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

        Ok(Self {
            protocol_version: REFLECTION_PROTOCOL_VERSION,
            ruapc_version: version.into(),
            services,
            components: components
                .map(serde_json::to_value)
                .transpose()?
                .unwrap_or_else(|| serde_json::json!({})),
        })
    }
}
