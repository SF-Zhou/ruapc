//! `ruapc-bufpool` — A generic, high-performance buffer pool with device registration.
//!
//! This crate provides the core memory management primitives for RPC systems
//! that need registered memory (e.g. for RDMA or TCP remote read/write):
//!
//! - [`AlignedMemory`] — 2 MiB-aligned memory allocation
//! - [`Device`] / [`Registration`] traits — generic device registration interface
//! - [`Devices`] — a collection of devices
//! - [`RegisteredMemory`] — aligned memory registered on a set of devices
//! - [`Buffer`] — a fixed-size buffer from a pool, returned on drop
//! - [`BufferPool`] — manages large registered chunks, hands out `Buffer`s

#[allow(unsafe_code)]
mod aligned_memory;
pub use aligned_memory::AlignedMemory;

mod device;
pub use device::{Device, Devices, Registration};

mod memory;
pub use memory::RegisteredMemory;

#[allow(unsafe_code)]
mod buffer;
pub use buffer::Buffer;

mod buffer_pool;
pub use buffer_pool::BufferPool;
