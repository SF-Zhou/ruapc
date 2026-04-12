use std::io::{Error, ErrorKind, Result};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::buffer_pool::BufferPool;
use crate::device::Device;

/// A memory buffer allocated from a [`BufferPool`].
///
/// Holds an `Arc<BufferPool>` to keep the pool (and the underlying
/// registered `Memory`) alive. On drop, the buffer is returned to
/// the pool's free list for reuse.
///
/// The buffer has a fixed `capacity` (the block size) and a variable
/// `len` that tracks how much of the capacity is in use. By default
/// `len == capacity` after allocation, but callers can use
/// [`set_len`](Self::set_len) and [`extend_from_slice`](Self::extend_from_slice)
/// for incremental filling (e.g. in RDMA send paths).
///
/// Implements `Deref<Target=[u8]>` and `DerefMut` for convenient
/// byte-slice access to the `[0..len]` region.
pub struct Buffer<D: Device> {
    pool: Arc<BufferPool<D>>,
    ptr: NonNull<u8>,
    capacity: usize,
    len: usize,
    memory_index: usize,
    block_index: usize,
}

// SAFETY: The pointer is valid for the buffer's lifetime (guaranteed
// by the Arc<BufferPool> preventing Memory from being freed), and
// each Buffer is the sole owner of its block until returned.
#[allow(unsafe_code)]
unsafe impl<D: Device> Send for Buffer<D> {}
#[allow(unsafe_code)]
unsafe impl<D: Device> Sync for Buffer<D> {}

#[allow(unsafe_code)]
impl<D: Device> Buffer<D> {
    /// Creates a new buffer. Called internally by `BufferPool::allocate`.
    pub(crate) fn new(
        pool: Arc<BufferPool<D>>,
        ptr: NonNull<u8>,
        capacity: usize,
        memory_index: usize,
        block_index: usize,
    ) -> Self {
        Self {
            pool,
            ptr,
            capacity,
            len: capacity,
            memory_index,
            block_index,
        }
    }

    /// Returns the number of bytes currently in use.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the total capacity of this buffer (block size).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns a reference to the owning buffer pool.
    pub fn pool(&self) -> &Arc<BufferPool<D>> {
        &self.pool
    }

    /// Returns `true` if `len` is zero.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the memory index within the pool.
    pub fn memory_index(&self) -> usize {
        self.memory_index
    }

    /// Sets the length of the used region.
    ///
    /// # Panics
    ///
    /// Panics if `len > capacity`.
    pub fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.capacity,
            "len {len} > capacity {}",
            self.capacity
        );
        self.len = len;
    }

    /// Appends `data` to the used region, growing `len` accordingly.
    ///
    /// Returns an error if appending would exceed capacity.
    pub fn extend_from_slice(&mut self, data: &[u8]) -> Result<()> {
        let new_len = self.len + data.len();
        if new_len > self.capacity {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "extend_from_slice: offset {} + length {} > capacity {}",
                    self.len,
                    data.len(),
                    self.capacity
                ),
            ));
        }
        // SAFETY: ptr + self.len is within the allocated block.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.ptr.as_ptr().add(self.len),
                data.len(),
            );
        }
        self.len = new_len;
        Ok(())
    }

    /// Returns a raw pointer to the buffer's memory.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to the buffer's memory.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

#[allow(unsafe_code)]
impl<D: Device> Deref for Buffer<D> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid for `len` bytes.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

#[allow(unsafe_code)]
impl<D: Device> DerefMut for Buffer<D> {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for `len` bytes and we have exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl<D: Device> AsRef<[u8]> for Buffer<D> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl<D: Device> AsMut<[u8]> for Buffer<D> {
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

impl<D: Device> Drop for Buffer<D> {
    fn drop(&mut self) {
        self.pool.return_buffer(self.memory_index, self.block_index);
    }
}

impl<D: Device> std::fmt::Debug for Buffer<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("ptr", &self.ptr)
            .field("capacity", &self.capacity)
            .field("len", &self.len)
            .field("memory_index", &self.memory_index)
            .field("block_index", &self.block_index)
            .finish()
    }
}
