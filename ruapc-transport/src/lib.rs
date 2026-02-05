//! # ruapc-transport
//!
//! Transport layer traits and common types for the RuaPC RPC library.
//!
//! This crate provides:
//! - [`RuapcSocket`] - Trait for socket implementations
//! - [`RuapcSocketPool`] - Trait for socket pool implementations
//! - [`MessageHandler`] - Trait for handling received messages
//! - [`SocketType`], [`SocketPoolConfig`], [`RawStream`] - Common types
//!
//! ## Architecture
//!
//! The transport layer uses traits to enable different transport implementations
//! (TCP, WebSocket, HTTP, RDMA) to be used interchangeably. This allows:
//! - Independent versioning of transport implementations
//! - Cleaner dependency management (only depend on transports you need)
//! - Easier testing with mock implementations
//!
//! ## Example
//!
//! ```rust,ignore
//! use ruapc_transport::{RuapcSocket, RuapcSocketPool, SocketPoolConfig, SocketType};
//!
//! // Create a socket pool configuration
//! let config = SocketPoolConfig {
//!     socket_type: SocketType::TCP,
//! };
//! ```

#![forbid(unsafe_code)]

// Re-export core types for convenience
pub use ruapc_async::{TaskSupervisor, TaskSupervisorGuard};
pub use ruapc_core::{Error, ErrorKind, Message, MsgFlags, MsgMeta, Payload, Result, SendMsg};

mod socket_type;
pub use socket_type::{RawStream, SocketPoolConfig, SocketType};

mod traits;
pub use traits::{BoxFuture, MessageHandler, RuapcSocket, RuapcSocketPool};
