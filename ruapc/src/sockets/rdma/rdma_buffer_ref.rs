//! [`RdmaBufferRef`]: A wrapper around [`Buffer`] that implements [`RdmaBuffer`]
//! for a specific RDMA device.
//!
//! Since a [`Buffer`] can be registered on multiple RDMA devices (each with
//! different lkey/rkey), this wrapper binds a buffer to a specific device's
//! memory registration, providing the correct lkey/rkey for that device.

use std::sync::Arc;

use ruapc_rdma_sys::RdmaBuffer;

use crate::Device;
use crate::memory::{Buffer, BufferExt, MemoryKey};

/// A buffer reference bound to a specific RDMA device's memory registration.
///
/// Implements [`RdmaBuffer`] by extracting the lkey/rkey from the buffer's
/// memory registration for the device associated with the given `Device`.
#[derive(Debug)]
pub struct RdmaBufferRef {
    buffer: Arc<Buffer>,
    lkey: u32,
    rkey: u32,
}

impl RdmaBufferRef {
    /// Creates a new `RdmaBufferRef` by looking up the buffer's RDMA registration
    /// for the given device.
    ///
    /// Returns `None` if the buffer is not registered on the specified device
    /// or if the registration is not an RDMA registration.
    pub fn new(buffer: Arc<Buffer>, device: &Arc<Device>) -> Option<Self> {
        let key = buffer.memory_key(device).ok()?;
        match key {
            MemoryKey::Rdma { lkey, rkey } => Some(Self { buffer, lkey, rkey }),
            _ => None,
        }
    }

    /// Creates a new `RdmaBufferRef` with explicitly provided lkey/rkey.
    pub fn with_keys(buffer: Arc<Buffer>, lkey: u32, rkey: u32) -> Self {
        Self { buffer, lkey, rkey }
    }

    /// Returns a reference to the underlying buffer.
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// Consumes this wrapper and returns the underlying buffer.
    pub fn into_buffer(self) -> Arc<Buffer> {
        self.buffer
    }
}

impl RdmaBuffer for RdmaBufferRef {
    fn addr(&self) -> *mut std::ffi::c_void {
        self.buffer.as_ptr() as *mut std::ffi::c_void
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn lkey(&self) -> u32 {
        self.lkey
    }

    fn rkey(&self) -> u32 {
        self.rkey
    }
}

/// A lightweight buffer descriptor for one-sided RDMA operations.
///
/// Unlike [`RdmaBufferRef`], this does not own a pool-allocated `Buffer`.
/// It is used when the caller has a raw memory region (e.g. temporarily
/// registered for a single RDMA verb) and needs to pass it through the
/// safe `QueuePair::read_untracked()` / `QueuePair::write_untracked()` API.
pub struct MrBufferRef {
    addr: *mut std::ffi::c_void,
    len: usize,
    lkey: u32,
    rkey: u32,
}

// SAFETY: The MrBufferRef is only used while the corresponding MemoryRegion
// (and the underlying memory) is alive. The caller guarantees this by holding
// the MR until the RDMA operation completes.
#[allow(unsafe_code)]
unsafe impl Send for MrBufferRef {}

impl MrBufferRef {
    /// Creates a new `MrBufferRef` from a registered memory region with a
    /// specified length (which may be less than the full MR length).
    pub fn new(mr: &ruapc_rdma_sys::MemoryRegion, len: usize) -> Self {
        Self {
            addr: mr.addr(),
            len,
            lkey: mr.lkey(),
            rkey: mr.rkey(),
        }
    }
}

impl RdmaBuffer for MrBufferRef {
    fn addr(&self) -> *mut std::ffi::c_void {
        self.addr
    }

    fn len(&self) -> usize {
        self.len
    }

    fn lkey(&self) -> u32 {
        self.lkey
    }

    fn rkey(&self) -> u32 {
        self.rkey
    }
}
