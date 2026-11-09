//! Registered buffer pool with slab and buddy allocation.
//!
//! Allocations use 16 KiB, 64 KiB, and 256 KiB slab chunks or 1 MiB, 4 MiB,
//! 16 MiB, and 64 MiB buddy nodes. Small allocations use reclaimable thread
//! caches by default. [`Buffer`] returns its allocation to the pool on drop.
//!
//! New blocks are zero-initialized and registered with the configured devices;
//! reused buffers retain their contents. [`BufferPool::allocate`] fails when
//! capacity is unavailable, while [`BufferPool::async_allocate`] can wait for
//! a returned buffer. Growth and device registration run on the calling thread.
//!
//! ## Example
//!
//! ```rust
//! use ruapc_bufpool::BufferPoolBuilder;
//!
//! # fn main() -> std::io::Result<()> {
//! // Create a buffer pool with 256MiB max memory
//! let pool = BufferPoolBuilder::new(Vec::new())
//!     .max_memory(256 * 1024 * 1024)
//!     .build();
//!
//! // Allocate a 1MiB buffer synchronously
//! let buffer = pool.allocate(1024 * 1024)?;
//! assert!(buffer.len() >= 1024 * 1024);
//!
//! // Buffer is automatically returned to the pool when dropped
//! drop(buffer);
//! # Ok(())
//! # }
//! ```

mod aligned;
mod buddy;
mod buffer;
#[doc(hidden)]
pub mod completion;
mod intrusive_list;
mod pool;
mod slab;
mod thread_cache;

pub use aligned::AlignedMemory;
pub use buffer::{Buffer, ReadSources, WriteBuffer};
pub use pool::{BufferPool, BufferPoolBuilder, DEFAULT_BUFFER_POOL_MEMORY};

mod key;
pub use key::{MemoryKey, RemoteBufferInfo};

mod registration;
pub use registration::{MemoryRegistrar, Registration};
