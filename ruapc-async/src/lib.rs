//! # ruapc-async
//!
//! Async utilities for the RuaPC RPC library.
//!
//! This crate provides reusable async components:
//! - [`TaskSupervisor`] - Task lifecycle management for graceful shutdown
//!
//! ## Example
//!
//! ```rust,no_run
//! # use ruapc_async::TaskSupervisor;
//! # #[tokio::main]
//! # async fn main() {
//! let supervisor = TaskSupervisor::create();
//! let guard = supervisor.start_async_task();
//! tokio::spawn(async move {
//!     // Task work here
//!     drop(guard); // Automatically decrements running count
//! });
//! supervisor.stop();
//! supervisor.all_stopped().await;
//! # }
//! ```

#![forbid(unsafe_code)]

mod task_supervisor;
pub use task_supervisor::{TaskSupervisor, TaskSupervisorGuard};
