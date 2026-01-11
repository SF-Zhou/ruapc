//! Buffer management for RDMA operations.
//!
//! This module provides different buffer types optimized for RDMA:
//! - [`AlignedBuffer`]: Page-aligned memory buffer
//! - [`RegisteredBuffer`]: RDMA-registered memory buffer
//! - [`BufferPool`]: Pool of reusable RDMA buffers with buddy memory allocation
//! - [`Buffer`]: Smart pointer to pooled buffers
//!
//! ## Buffer Pool Features
//!
//! The buffer pool implements a sophisticated memory management system:
//! - **64 MiB RDMA blocks**: Memory is allocated in 64 MiB chunks for RDMA registration
//! - **Buddy memory allocation**: For sizes 1 MiB, 4 MiB, 16 MiB, 64 MiB
//! - **Small buffer optimization**: Buffers smaller than `small_buffer_size` use a separate
//!   allocation mechanism for efficiency
//! - **Automatic recycling**: Buffers are returned to the pool when dropped
//! - **Memory limits**: Configurable maximum memory usage

mod aligned_buffer;
pub use aligned_buffer::AlignedBuffer;

mod rdma_buffer;
pub use rdma_buffer::RegisteredBuffer;

mod buffer_pool;
pub use buffer_pool::{Buffer, BufferPool, BufferPoolConfig, RDMA_BLOCK_SIZE, DEFAULT_SMALL_BUFFER_SIZE};
