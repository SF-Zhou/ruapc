//! Service schemas, OpenAPI projection and reflection data for RuaPC.
//!
//! [`SchemaRegistry`] generates request and response schemas during method
//! registration, keeping public and internal definitions separate. Its OpenAPI
//! projection and [`ServerDescription::from_methods`] consume the public method
//! directory supplied by the caller. RPC dispatch, visibility enforcement and
//! the reflection service itself remain in `ruapc`.
//!
//! This crate does not depend on an async runtime, transport or buffer pool.

#![forbid(unsafe_code)]

mod reflection;
mod schema;

pub use reflection::{
    DescribeRequest, MethodDescription, REFLECTION_PROTOCOL_VERSION, ServerDescription,
    ServiceDescription,
};
pub use schema::{MethodSchema, SchemaRegistry};
