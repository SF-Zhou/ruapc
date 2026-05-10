//! [`RdmaBufferRef`]: A wrapper around [`Buffer`] that implements [`RdmaBuffer`]
//! for a specific RDMA device.
//!
//! Since a [`Buffer`] can be registered on multiple RDMA devices (each with
//! different lkey/rkey), this wrapper binds a buffer to a specific device's
//! memory registration, providing the correct lkey/rkey for that device.

use std::sync::Arc;

use ruapc_rdma_sys::RdmaBuffer;

use crate::Device;
use crate::memory::{Buffer, MemoryKey};

/// A buffer bound to a specific RDMA device's memory registration.
///
/// Owns the [`Buffer`] directly (no `Arc` indirection) and implements
/// [`RdmaBuffer`] by extracting the lkey/rkey from the buffer's memory
/// registration for the associated device.
///
/// When `len_override` is `Some(n)`, the [`RdmaBuffer::len`] implementation
/// returns `n` instead of the buffer's logical length. This is used for
/// one-sided RDMA operations where the transfer size may differ from the
/// buffer's current logical length.
#[derive(Debug)]
pub struct RdmaBufferRef {
    buffer: Buffer,
    lkey: u32,
    rkey: u32,
    /// Optional length override for one-sided RDMA operations.
    len_override: Option<usize>,
}

impl RdmaBufferRef {
    /// Creates a new `RdmaBufferRef` by looking up the buffer's RDMA registration
    /// for the given device.
    ///
    /// Returns `None` if the buffer is not registered on the specified device
    /// or if the registration is not an RDMA registration.
    pub fn new(buffer: Buffer, device: &Arc<Device>) -> Option<Self> {
        let key = device.memory_key(&buffer).ok()?;
        match key {
            MemoryKey::Rdma { lkey, rkey } => Some(Self {
                buffer,
                lkey,
                rkey,
                len_override: None,
            }),
            _ => None,
        }
    }

    /// Creates a new `RdmaBufferRef` with explicitly provided lkey/rkey.
    pub fn with_keys(buffer: Buffer, lkey: u32, rkey: u32) -> Self {
        Self {
            buffer,
            lkey,
            rkey,
            len_override: None,
        }
    }

    /// Sets the length override for one-sided RDMA operations.
    ///
    /// When set, [`RdmaBuffer::len`] will return this value instead of the
    /// buffer's logical length. This is useful for `remote_read`/`remote_write`
    /// where the transfer size is determined by the remote buffer info.
    pub fn with_len(mut self, len: usize) -> Self {
        self.len_override = Some(len);
        self
    }

    /// Returns a reference to the underlying buffer.
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// Consumes this wrapper and returns the underlying buffer.
    pub fn into_buffer(self) -> Buffer {
        self.buffer
    }
}

impl RdmaBuffer for RdmaBufferRef {
    fn addr(&self) -> *mut std::ffi::c_void {
        self.buffer.as_ptr() as *mut std::ffi::c_void
    }

    fn len(&self) -> usize {
        self.len_override.unwrap_or_else(|| self.buffer.len())
    }

    fn lkey(&self) -> u32 {
        self.lkey
    }

    fn rkey(&self) -> u32 {
        self.rkey
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SocketPoolConfig;

    #[tokio::test]
    async fn test_with_len_override() {
        // Create a buffer pool with default config (TCP-only, no RDMA required).
        let config = SocketPoolConfig::default();
        let ctx = crate::Context::create(&config).unwrap();
        let mut buf = ctx.state.buffer_pool.allocate().unwrap();

        // Fill with data and verify initial len
        let data = b"test data for len override";
        buf[..data.len()].copy_from_slice(data);
        let original_len = buf.len();
        assert!(original_len > data.len());

        // Create RdmaBufferRef with explicit keys (bypasses actual device lookup).
        let buf_ref = RdmaBufferRef::with_keys(buf, 42, 99);

        // Without override, len() returns the buffer's logical length.
        assert_eq!(buf_ref.len(), original_len);
        assert_eq!(buf_ref.lkey(), 42);
        assert_eq!(buf_ref.rkey(), 99);

        // With override, len() returns the custom value.
        let buf_ref = buf_ref.with_len(data.len());
        assert_eq!(buf_ref.len(), data.len());

        // Verify we can recover the buffer.
        let recovered_buf = buf_ref.into_buffer();
        assert_eq!(&recovered_buf[..data.len()], data);
    }

    #[tokio::test]
    async fn test_ownership_round_trip() {
        // Verify that wrapping a buffer in RdmaBufferRef and unwrapping
        // it returns the same buffer directly (no Arc overhead).
        let config = SocketPoolConfig::default();
        let ctx = crate::Context::create(&config).unwrap();
        let buf = ctx.state.buffer_pool.allocate().unwrap();
        let ptr = buf.as_ptr();

        let buf_ref = RdmaBufferRef::with_keys(buf, 0, 0);
        let recovered = buf_ref.into_buffer();
        assert_eq!(recovered.as_ptr(), ptr);
    }

    #[tokio::test]
    async fn test_len_override_does_not_affect_addr() {
        let config = SocketPoolConfig::default();
        let ctx = crate::Context::create(&config).unwrap();
        let buf = ctx.state.buffer_pool.allocate().unwrap();
        let expected_addr = buf.as_ptr() as *mut std::ffi::c_void;

        let buf_ref = RdmaBufferRef::with_keys(buf, 1, 2).with_len(16);

        // addr() should still point to the original buffer start
        assert_eq!(buf_ref.addr(), expected_addr);
        // len() returns the override
        assert_eq!(buf_ref.len(), 16);
    }
}
