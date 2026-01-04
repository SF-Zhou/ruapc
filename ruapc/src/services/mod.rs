//! Built-in RPC services.
//!
//! This module provides built-in services that are automatically
//! registered with every RuaPC server, including:
//! - MetaService: Introspection and metadata service

mod meta_service;
pub use meta_service::{MetaService, Metadata};
