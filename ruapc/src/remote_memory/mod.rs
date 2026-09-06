//! Vectored remote-memory transfers and their ownership contracts.
//!
//! `scatter` validates logical spaces and plans contiguous fragments.
//! `context` validates application calls before transport execution.
//! `inline` implements the reverse-RPC fallback. `write_target` pins
//! destinations through timeouts; `with_buffers` exposes completion witnesses.

pub(crate) mod inline;

mod context;
pub use context::RemoteSpace;

pub(crate) mod scatter;
pub use scatter::{CopyOp, MAX_COPY_OPS, MAX_REGIONS};

mod write_target;
pub(crate) use write_target::WriteTarget;

mod read_source;
pub(crate) use read_source::ReadSource;

mod with_buffers;
pub use with_buffers::{ResultWithBuffers, SentBuffers, WithBuffers};
