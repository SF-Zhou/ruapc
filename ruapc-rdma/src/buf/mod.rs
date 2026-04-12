//! Buffer management for RDMA operations.
//!
//! This module provides different buffer types optimized for RDMA:
//! - [`AlignedBuffer`]: Page-aligned memory buffer
//! - [`RegisteredBuffer`]: RDMA-registered memory buffer

mod aligned_buffer;
pub use aligned_buffer::AlignedBuffer;

mod rdma_buffer;
pub use rdma_buffer::{RawMemoryRegion, RegisteredBuffer};
