use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize};

/// JSON schema information for a service method.
///
/// Contains the request and response schemas used for OpenAPI generation
/// and runtime validation.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct MethodInfo {
    /// JSON schema for the request type.
    pub req_schema: Schema,
    /// JSON schema for the response type.
    pub rsp_schema: Schema,
}
