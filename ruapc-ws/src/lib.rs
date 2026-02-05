//! # ruapc-ws
//!
//! WebSocket transport implementation for the RuaPC RPC library.
//!
//! This crate provides:
//! - [`WebSocket`] - WebSocket implementing [`RuapcSocket`](ruapc_transport::RuapcSocket)
//! - [`WebSocketPool`] - WebSocket pool implementing [`RuapcSocketPool`](ruapc_transport::RuapcSocketPool)
//!
//! ## Example
//!
//! ```rust,ignore
//! use ruapc_ws::WebSocketPool;
//! use ruapc_transport::{RuapcSocketPool, MessageHandler};
//! use std::sync::Arc;
//!
//! let pool = WebSocketPool::new();
//! ```

#![forbid(unsafe_code)]

pub use ruapc_transport::{
    BoxFuture, Error, ErrorKind, Message, MessageHandler, MsgFlags, MsgMeta, Payload, RawStream,
    Result, RuapcSocket, RuapcSocketPool, SendMsg, SocketPoolConfig, SocketType, TaskSupervisor,
    TaskSupervisorGuard,
};

mod web_socket;
pub use web_socket::WebSocket;

mod web_socket_pool;
pub use web_socket_pool::WebSocketPool;
