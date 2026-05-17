//! Buffer type that automatically returns to the pool on drop.

use std::io::Result;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::buddy::{BuddyBlock, FreeNode};
use crate::{AsDeviceIndex, BufferPool, MemoryKey, RemoteBufferInfo};

/// A buffer allocated from the pool.
///
/// When dropped, the buffer is automatically returned to the pool for reuse.
/// The buffer holds an `Arc<BufferPool>` reference to ensure the pool remains
/// valid for the lifetime of the buffer.
///
/// The buffer supports a logical length that can be less than or equal to its
/// capacity. Use `set_len` and `extend_from_slice` to manage the logical length.
pub struct Buffer {
    /// Pointer to the allocated memory.
    ptr: NonNull<u8>,

    /// Reference to the pool for returning the buffer.
    pool: Arc<BufferPool>,

    /// Pointer to the buddy block this buffer belongs to.
    block: NonNull<BuddyBlock>,

    /// Index of the block within the pool's block list (for registration lookup).
    block_index: usize,

    /// Logical length of data in the buffer (may be less than capacity).
    len: usize,

    /// The allocation level (0-3).
    level: u8,

    /// Index within the level in the buddy block.
    index: u8,
}

// SAFETY: Buffer can be sent between threads as it only contains
// raw pointers that are owned by the pool
unsafe impl Send for Buffer {}

// SAFETY: Buffer can be shared between threads as it provides
// exclusive access to its memory region
unsafe impl Sync for Buffer {}

impl Buffer {
    /// Creates a new buffer.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `ptr` points to a valid memory region of the appropriate size for `level`
    /// - `block` points to a valid `BuddyBlock`
    /// - `level` is in range 0-3
    /// - `index` is valid for the given level
    pub(crate) unsafe fn new(
        ptr: NonNull<u8>,
        level: usize,
        index: usize,
        block: NonNull<BuddyBlock>,
        block_index: usize,
        pool: Arc<BufferPool>,
    ) -> Self {
        debug_assert!(level < crate::buddy::NUM_LEVELS, "level must be 0-3");
        debug_assert!(
            index < crate::buddy::NODES_PER_LEVEL[0],
            "index must be less than 64"
        );
        let capacity = crate::buddy::LEVEL_SIZES[level];
        Self {
            ptr,
            pool,
            block,
            block_index,
            len: capacity,
            #[allow(clippy::cast_possible_truncation)]
            level: level as u8,
            #[allow(clippy::cast_possible_truncation)]
            index: index as u8,
        }
    }

    /// Returns the logical length of the buffer in bytes.
    ///
    /// Initially set to the full capacity. Use `set_len` to adjust.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns the capacity of the buffer in bytes (determined by allocation level).
    ///
    /// - Level 0: 1 MiB
    /// - Level 1: 4 MiB
    /// - Level 2: 16 MiB
    /// - Level 3: 64 MiB
    #[inline]
    #[must_use]
    pub const fn capacity(&self) -> usize {
        crate::buddy::LEVEL_SIZES[self.level as usize]
    }

    /// Returns `true` if the buffer's logical length is 0.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Sets the logical length of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the buffer's capacity.
    #[inline]
    pub fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.capacity(),
            "len {len} > capacity {}",
            self.capacity()
        );
        self.len = len;
    }

    /// Appends data to the buffer at the current logical length.
    ///
    /// # Errors
    ///
    /// Returns an error if the new length would exceed the capacity.
    pub fn extend_from_slice(&mut self, data: &[u8]) -> Result<()> {
        let new_len = self.len + data.len();
        if new_len > self.capacity() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "extend_from_slice: offset {} + length {} > capacity {}",
                    self.len,
                    data.len(),
                    self.capacity()
                ),
            ));
        }
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
    #[inline]
    #[must_use]
    pub const fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to the buffer's memory.
    #[inline]
    #[must_use]
    pub const fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns the buffer as a byte slice (up to the logical length).
    #[inline]
    #[must_use]
    pub const fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns the buffer as a mutable byte slice (up to the logical length).
    #[inline]
    #[must_use]
    pub const fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Returns the allocation level of this buffer (0-3).
    #[inline]
    #[allow(dead_code)]
    pub(crate) const fn level(&self) -> usize {
        self.level as usize
    }

    /// Returns the index within the level (0-63).
    #[inline]
    #[allow(dead_code)]
    pub(crate) const fn index(&self) -> usize {
        self.index as usize
    }

    /// Returns the buddy block pointer.
    #[inline]
    #[allow(dead_code)]
    pub(crate) const fn block(&self) -> NonNull<BuddyBlock> {
        self.block
    }

    /// Returns a pointer to the free node for this buffer.
    #[allow(dead_code)]
    pub(crate) unsafe fn free_node(&self) -> NonNull<FreeNode> {
        unsafe {
            (*self.block.as_ptr()).get_free_node_mut(self.level as usize, self.index as usize)
        }
    }

    /// Returns the memory key for this buffer's underlying block on the given device.
    ///
    /// # Errors
    ///
    /// Returns an error if the device index is not registered.
    pub fn memory_key(&self, device_index: &impl AsDeviceIndex) -> Result<MemoryKey> {
        self.pool.memory_key(self.block_index, device_index)
    }

    /// Returns the remote buffer info for RDMA-style operations.
    ///
    /// Note: uses the buffer's capacity (not logical length) for the remote info.
    ///
    /// # Errors
    ///
    /// Returns an error if the device index is not registered.
    pub fn remote_buffer_info(
        &self,
        device_index: &impl AsDeviceIndex,
    ) -> Result<RemoteBufferInfo> {
        let key = self.memory_key(device_index)?;
        Ok(RemoteBufferInfo {
            key,
            addr: self.as_ptr() as u64,
            len: self.capacity() as u64,
        })
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.pool
            .return_buffer(self.level as usize, self.index as usize, self.block);
    }
}

impl Deref for Buffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for Buffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl AsRef<[u8]> for Buffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for Buffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("ptr", &self.ptr)
            .field("capacity", &self.capacity())
            .field("len", &self.len)
            .field("level", &self.level)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{BufferPoolBuilder, EmptyDevices};

    fn test_pool() -> Arc<crate::BufferPool> {
        BufferPoolBuilder::new(Arc::new(EmptyDevices)).build()
    }

    #[test]
    fn test_buffer_basic_operations() {
        let pool = test_pool();
        let mut buffer = pool.allocate(1024).unwrap();

        assert!(buffer.len() >= 1024);
        assert!(!buffer.is_empty());

        buffer[0] = 0xAB;
        buffer[1] = 0xCD;
        assert_eq!(buffer[0], 0xAB);
        assert_eq!(buffer[1], 0xCD);

        let slice = buffer.as_slice();
        assert_eq!(slice[0], 0xAB);

        buffer.as_mut_slice()[2] = 0xEF;
        assert_eq!(buffer[2], 0xEF);
    }

    #[test]
    fn test_buffer_deref() {
        let pool = test_pool();
        let mut buffer = pool.allocate(1024).unwrap();

        for (i, byte) in buffer.iter_mut().take(100).enumerate() {
            *byte = i as u8;
        }

        for i in 0..100 {
            assert_eq!(buffer[i], i as u8);
        }
    }

    #[test]
    fn test_buffer_debug() {
        let pool = test_pool();
        let buffer = pool.allocate(1024).unwrap();

        let debug_str = format!("{buffer:?}");
        assert!(debug_str.contains("Buffer"));
        assert!(debug_str.contains("len"));
    }
}
