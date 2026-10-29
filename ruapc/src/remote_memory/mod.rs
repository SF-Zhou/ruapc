//! Request-bound remote-memory execution and buffer recovery.
//!
//! Pure layouts and copy batches live in `ruapc-memory`. `transfer` binds them
//! to one request before dispatch; `rpc_transfer` owns the reverse protocol.
//! `write_target` controls destination ownership; `reply` exposes completion
//! and local buffer availability as separate facts.

mod context;
mod reply;
pub(crate) mod rpc_transfer;
pub(crate) mod transfer;
mod write_target;

pub use reply::{BufferAvailability, BufferReply, ResultBufferReply, WriteReceipt};
pub(crate) use ruapc_memory::ReadSource;
pub use ruapc_memory::{CopyOp, MAX_COPY_OPS, MAX_REGIONS, RemoteSpace};
pub(crate) use write_target::WriteTarget;

#[cfg(all(test, feature = "rdma"))]
mod timeout_tests;
