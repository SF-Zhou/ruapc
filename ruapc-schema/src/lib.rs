//! Service schemas and OpenAPI projection for RuaPC.
//!
//! [`SchemaRegistry`] generates request and response schemas during method
//! registration. Its OpenAPI projection consumes the method directory supplied
//! by the caller. RPC registration and dispatch remain in `ruapc`.
//!
//! This crate does not depend on an async runtime, transport or buffer pool.

#![forbid(unsafe_code)]

mod schema;

pub use schema::{MethodSchema, SchemaRegistry};
