//! # ruapc-bufpool
//!
//! A high-performance memory pool using buddy memory allocation algorithm for efficient
//! fixed-size buffer management. This crate is part of the [ruapc](https://github.com/SF-Zhou/ruapc) project.
//!
//! ## Features
//!
//! - **Buddy Memory Allocation**: Supports allocation of 1MiB, 4MiB, 16MiB, and 64MiB buffers
//! - **Both Sync and Async APIs**: Designed for tokio environments with async-first design
//! - **Automatic Memory Reclamation**: Buffers are automatically returned to the pool on drop
//! - **Memory Limits**: Configurable maximum memory usage with async waiting when limits are reached
//! - **Custom Allocators**: Pluggable allocator trait for memory allocation backend
//! - **O(1) Buddy Merging**: Intrusive doubly-linked list with O(1) free/merge operations
//! - **Device Registration**: Support for registering memory with devices (RDMA, TCP)
//!
//! ## Example
//!
//! ```rust
//! use std::sync::Arc;
//! use ruapc_bufpool::{BufferPoolBuilder, EmptyDevices};
//!
//! # fn main() -> std::io::Result<()> {
//! // Create a buffer pool with 256MiB max memory
//! let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
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
mod intrusive_list;
mod pool;
mod slab;
mod thread_cache;

pub use aligned::AlignedMemory;
pub use buffer::Buffer;
pub use pool::{BufferPool, BufferPoolBuilder};

mod key;
pub use key::{MemoryKey, RemoteBufferInfo};

mod device;
pub use device::{AsDeviceIndex, Device, DeviceIndex, Registration};

mod tcp_device;
pub use tcp_device::{TcpDevice, TcpMemoryRegistration};

mod devices;
pub use devices::{Devices, EmptyDevices};
