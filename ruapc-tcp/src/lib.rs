//! # ruapc-tcp
//!
//! TCP transport implementation for the RuaPC RPC library.
//!
//! This crate provides:
//! - [`TcpSocket`] - TCP socket implementing [`RuapcSocket`](ruapc_transport::RuapcSocket)
//! - [`TcpSocketPool`] - TCP socket pool implementing [`RuapcSocketPool`](ruapc_transport::RuapcSocketPool)
//!
//! ## Protocol
//!
//! The TCP transport uses a simple framing protocol:
//! - 4-byte magic number: `RUA!` (0x52554121)
//! - 4-byte length: Total message length (big-endian)
//! - Variable-length message body
//!
//! ## Example
//!
//! ```rust,ignore
//! use ruapc_tcp::TcpSocketPool;
//! use ruapc_transport::{RuapcSocketPool, MessageHandler};
//! use std::sync::Arc;
//!
//! let pool = TcpSocketPool::new();
//! ```

#![forbid(unsafe_code)]

pub use ruapc_transport::{
    BoxFuture, Error, ErrorKind, Message, MessageHandler, MsgFlags, MsgMeta, Payload,
    RawStream, Result, RuapcSocket, RuapcSocketPool, SendMsg, SocketPoolConfig, SocketType,
    TaskSupervisor, TaskSupervisorGuard,
};

/// Magic number for TCP protocol framing.
pub const MAGIC_NUM: u32 = u32::from_be_bytes(*b"RUA!");

/// Maximum message size (64 MB).
pub const MAX_MSG_SIZE: usize = 64 << 20;

mod tcp_socket;
pub use tcp_socket::TcpSocket;

mod tcp_socket_pool;
pub use tcp_socket_pool::TcpSocketPool;
