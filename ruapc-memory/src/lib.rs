#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod buffer_view;
mod copy;
mod error;
mod layout;
mod plan;
mod read_source;
mod remote_space;

pub use buffer_view::{BufferView, BufferViewMut};
pub use copy::{BufferSet, BufferSetError, CopyBatch, CopyOp, MAX_COPY_OPS};
pub use error::{MemoryError, MemoryErrorKind, MemoryResult};
pub use layout::{MAX_REGIONS, SpaceLayout};
pub use plan::{DstSlice, SrcChunk};
pub use read_source::{BorrowedSource, ReadSource};
pub use remote_space::RemoteSpace;
