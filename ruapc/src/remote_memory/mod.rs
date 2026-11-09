//! Request-bound remote-memory execution and buffer recovery.
//!
//! Pure layouts and copy batches live in `ruapc-memory`. `transfer` binds them
//! to one request before dispatch; `rpc_transfer` owns the reverse protocol.
//! `write_target` retains local READ destinations through completion, including
//! cancellation. `read_source` delivers bounded commands to the request future
//! that borrows application sources; only that future reads them. Safe sources
//! confirm validation there too; unsafe source capabilities allow direct checks.
//! Buffer availability and successful transfer remain separate facts.

mod context;
pub use context::RemoteBuffers;
mod read_source;
mod rpc_transfer;
pub(crate) mod transfer;
mod write_target;

pub(crate) use read_source::{SourceRequests, SourceSender};
pub(crate) use rpc_transfer::MemoryService;
#[cfg(all(test, feature = "rdma"))]
pub(crate) use rpc_transfer::ReadIntoTargetRequest;
#[cfg(feature = "rdma")]
pub(crate) use rpc_transfer::RequestStatusRequest;
pub use ruapc_memory::{CopyOp, MAX_COPY_OPS, MAX_REGIONS, RemoteSpace};
pub(crate) use write_target::Recovery;
pub(crate) use write_target::WriteTarget;
