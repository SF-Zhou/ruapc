//! # ruapc-http
//!
//! HTTP transport implementation for the RuaPC RPC library.
//!
//! This crate provides:
//! - [`HttpSocket`] - HTTP socket implementing [`RuapcSocket`](ruapc_transport::RuapcSocket)
//! - [`HttpSocketPool`] - HTTP socket pool implementing [`RuapcSocketPool`](ruapc_transport::RuapcSocketPool)
//!
//! ## Features
//!
//! - HTTP/1.1 client and server support
//! - WebSocket upgrade support (delegates to ruapc-ws)
//! - Static file serving for API documentation
//!
//! ## Example
//!
//! ```rust,ignore
//! use ruapc_http::HttpSocketPool;
//! use ruapc_transport::{RuapcSocketPool, MessageHandler};
//! use std::sync::Arc;
//!
//! let pool = HttpSocketPool::new();
//! ```

#![forbid(unsafe_code)]

pub use ruapc_transport::{
    BoxFuture, Error, ErrorKind, Message, MessageHandler, MsgFlags, MsgMeta, Payload,
    RawStream, Result, RuapcSocket, RuapcSocketPool, SendMsg, SocketPoolConfig, SocketType,
    TaskSupervisor, TaskSupervisorGuard,
};

mod http_socket;
pub use http_socket::{Connections, HttpSocket};

mod http_socket_pool;
pub use http_socket_pool::{HttpRequestHandler, HttpSocketPool};

/// RapiDoc static files for API documentation.
pub mod rapidoc {
    /// The RapiDoc JavaScript file.
    pub const RAPIDOC_MIN_JS: &str = include_str!("rapidoc/rapidoc-min.js");
    /// The RapiDoc HTML template.
    pub const INDEX_HTML: &str = include_str!("rapidoc/index.html");
}
