//! # RuaPC - A High-Performance Rust RPC Library
//!
//! RuaPC is a versatile RPC (Remote Procedure Call) library that supports multiple transport protocols
//! including TCP, WebSocket, HTTP, and optionally RDMA (Remote Direct Memory Access).
//!
//! ## Features
//!
//! - **Multiple Transport Protocols**: TCP, WebSocket, HTTP, RDMA (optional), and a unified protocol
//! - **Multiple Serialization Formats**: JSON (default) and MessagePack support
//! - **OpenAPI Integration**: Automatic OpenAPI 3.0 specification generation
//! - **Type Safety**: Strongly typed service definitions with compile-time validation
//! - **Async/Await**: Built on tokio for efficient asynchronous I/O
//!
//! ## Quick Start
//!
//! ### Define a Service
//!
//! ```rust,ignore
//! use ruapc::{Context, Result};
//! use schemars::JsonSchema;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
//! pub struct Request(pub String);
//!
//! #[ruapc::service]
//! pub trait EchoService {
//!     async fn echo(&self, c: &Context, r: &Request) -> Result<String>;
//! }
//! ```
//!
//! ### Implement and Start a Server
//!
//! ```rust,ignore
//! use std::{net::SocketAddr, str::FromStr, sync::Arc};
//!
//! struct DemoImpl;
//!
//! impl EchoService for DemoImpl {
//!     async fn echo(&self, _c: &Context, r: &Request) -> Result<String> {
//!         Ok(r.0.clone())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let demo = Arc::new(DemoImpl);
//!     let mut router = Router::default();
//!     EchoService::ruapc_export(demo.clone(), &mut router);
//!     let server = Server::create(router, &SocketPoolConfig::default()).unwrap();
//!
//!     let server = Arc::new(server);
//!     let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
//!     let addr = server.listen(addr).await.unwrap();
//!     println!("Serving on {addr}...");
//!     server.join().await
//! }
//! ```
//!
//! ### Create a Client
//!
//! ```rust,ignore
//! use std::{net::SocketAddr, str::FromStr};
//!
//! #[tokio::main]
//! async fn main() {
//!     let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
//!     let ctx = Context::create(&SocketPoolConfig::default()).unwrap().with_addr(addr);
//!     let client = Client::default();
//!
//!     let rsp = client.echo(&ctx, &Request("Rua!".into())).await;
//!     println!("echo rsp: {:?}", rsp);
//! }
//! ```

#![forbid(unsafe_code)]
#![feature(return_type_notation)]

/// Procedural macro for defining RPC services.
///
/// This macro generates the necessary boilerplate code for both server-side service
/// implementation and client-side method invocation.
///
/// # Example
///
/// ```rust,ignore
/// # use ruapc::{Context, Result};
/// # use schemars::JsonSchema;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, JsonSchema)]
/// # struct Request;
/// #[ruapc::service]
/// pub trait MyService {
///     async fn my_method(&self, ctx: &Context, req: &Request) -> Result<String>;
/// }
/// ```
pub use ruapc_macro::service;

/// Error types and error handling utilities.
mod error;
pub use error::{Error, ErrorKind, Result};

/// Message payload representation supporting different backends.
mod payload;
pub use payload::Payload;

/// Message types and serialization/deserialization.
mod msg;
pub use msg::{Message, MsgFlags, MsgMeta};

/// Request routing and method dispatch.
mod router;
pub use router::Router;

/// Response waiting mechanism for asynchronous RPC calls.
mod waiter;
pub use waiter::{Waiter, WaiterCleaner};

/// Task lifecycle management.
mod task_supervisor;
pub use task_supervisor::TaskSupervisor;

/// Internal message receiver.
mod receiver;
use receiver::Receiver;

/// Socket abstraction layer.
mod socket;
pub use socket::Socket;

/// Socket pool management and configuration.
mod socket_pool;
pub use socket_pool::{RawStream, SocketPool, SocketPoolConfig, SocketType};

/// HTTP transport implementation.
mod http;
/// RDMA (Remote Direct Memory Access) transport implementation.
#[cfg(feature = "rdma")]
mod rdma;
/// TCP transport implementation.
mod tcp;
/// Unified transport that supports multiple protocols simultaneously.
mod unified;
/// WebSocket transport implementation.
mod ws;

/// Shared state management.
mod state;
pub use state::State;

/// Built-in services (MetaService, RdmaService).
pub mod services;

/// RPC context carrying request metadata and connection information.
mod context;
pub use context::{Context, SocketEndpoint};

/// Network listener for accepting incoming connections.
mod listener;
pub use listener::Listener;

/// RPC client for making requests.
mod client;
pub use client::Client;

/// RPC server for handling incoming requests.
mod server;
pub use server::Server;
