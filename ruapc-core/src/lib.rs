//! # RuaPC Core
//!
//! Core types and utilities for the RuaPC RPC library.
//!
//! This crate provides the foundational types used throughout the RuaPC ecosystem,
//! including error handling, message serialization, async primitives, and configuration.
//! It is designed to be transport-agnostic, allowing different transport implementations
//! to share common types without circular dependencies.
//!
//! ## Modules
//!
//! - **error**: Error types and error handling utilities
//! - **payload**: Message payload representation
//! - **msg**: Message types and serialization
//! - **waiter**: Response waiting mechanism for async RPC calls
//! - **task_supervisor**: Task lifecycle management
//! - **config**: Transport configuration types
//! - **client**: Client configuration
//! - **method_info**: Service method schema information

#![forbid(unsafe_code)]
#![feature(return_type_notation)]

/// Error types and error handling utilities.
mod error;
pub use error::{Error, ErrorKind, Result};

/// Message payload representation.
mod payload;
pub use payload::Payload;

/// Message types and serialization/deserialization.
mod msg;
pub use msg::{Message, MsgFlags, MsgMeta, SendMsg};

/// Response waiting mechanism for asynchronous RPC calls.
mod waiter;
pub use waiter::{Waiter, WaiterCleaner};

/// Internal message receiver.
mod receiver;
pub use receiver::Receiver;

/// Task lifecycle management.
mod task_supervisor;
pub use task_supervisor::{TaskSupervisor, TaskSupervisorGuard};

/// Transport configuration types.
mod config;
pub use config::{SocketPoolConfig, SocketType};

/// Service method schema information.
mod method_info;
pub use method_info::MethodInfo;
