use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::Result;
use crate::device::Device;

use super::buffer_pool::BufferPool;
use super::remote_buffer_info::RemoteBufferInfo;

/// A fixed-size memory buffer allocated from a [`BufferPool`].
///
/// Holds an `Arc<BufferPool>` to keep the pool (and the underlying
/// registered `Memory`) alive. On drop, the buffer is returned to
/// the pool's free list for reuse.
///
/// Implements `Deref<Target=[u8]>` and `DerefMut` for convenient
/// byte-slice access.
pub struct Buffer {
    pool: Arc<BufferPool>,
    ptr: NonNull<u8>,
    len: usize,
    memory_index: usize,
    block_index: usize,
}

// SAFETY: The pointer is valid for the buffer's lifetime (guaranteed
// by the Arc<BufferPool> preventing Memory from being freed), and
// each Buffer is the sole owner of its block until returned.
#[allow(unsafe_code)]
unsafe impl Send for Buffer {}
#[allow(unsafe_code)]
unsafe impl Sync for Buffer {}

#[allow(unsafe_code)]
impl Buffer {
    /// Creates a new buffer. Called internally by `BufferPool::allocate`.
    pub(crate) fn new(
        pool: Arc<BufferPool>,
        ptr: NonNull<u8>,
        len: usize,
        memory_index: usize,
        block_index: usize,
    ) -> Self {
        Self {
            pool,
            ptr,
            len,
            memory_index,
            block_index,
        }
    }

    /// Returns the size of this buffer in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Buffers always have non-zero size.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Returns a raw pointer to the buffer's memory.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to the buffer's memory.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Builds the [`RemoteBufferInfo`] for this buffer on the given device.
    ///
    /// This information can be sent to a remote peer via RPC so it can
    /// perform Remote Read/Write operations on this buffer.
    pub fn remote_buffer_info(&self, device: &Device) -> Result<RemoteBufferInfo> {
        let key = self.pool.get_memory_key(self.memory_index, device)?;
        Ok(RemoteBufferInfo {
            key,
            addr: self.ptr.as_ptr() as u64,
            len: self.len as u64,
        })
    }
}

#[allow(unsafe_code)]
impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid for `len` bytes.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

#[allow(unsafe_code)]
impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for `len` bytes and we have exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for Buffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.pool.return_buffer(self.memory_index, self.block_index);
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .field("memory_index", &self.memory_index)
            .field("block_index", &self.block_index)
            .finish()
    }
}
