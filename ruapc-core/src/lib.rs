//! # ruapc-core
//!
//! Core types and traits for the RuaPC RPC library.
//!
//! This crate provides fundamental types that are used across the RuaPC ecosystem:
//! - Error types ([`Error`], [`ErrorKind`], [`Result`])
//! - Message types ([`Message`], [`MsgMeta`], [`MsgFlags`])
//! - Payload abstraction ([`Payload`])
//! - Message serialization trait ([`SendMsg`])
//!
//! ## Features
//!
//! - `rdma` - Enables RDMA buffer support in Payload
//!
//! ## Example
//!
//! ```rust
//! use ruapc_core::{Error, ErrorKind, Result, MsgMeta, MsgFlags};
//!
//! fn example() -> Result<()> {
//!     let meta = MsgMeta {
//!         method: "MyService/my_method".to_string(),
//!         flags: MsgFlags::IsReq,
//!         msgid: 1,
//!     };
//!     assert!(meta.is_req());
//!     Ok(())
//! }
//! ```

#![forbid(unsafe_code)]

/// Error types and error handling utilities.
mod error;
pub use error::{Error, ErrorKind, Result};

/// Message payload representation supporting different backends.
mod payload;
pub use payload::Payload;

/// Message types and serialization/deserialization.
mod msg;
pub use msg::{Message, MsgFlags, MsgMeta, SendMsg};
