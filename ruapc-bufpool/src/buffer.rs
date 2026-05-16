use std::io::{Error, ErrorKind, Result};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::{AsDeviceIndex, BufferPool, MemoryKey, RegisteredMemory, RemoteBufferInfo};

pub struct Buffer {
    pool: Arc<BufferPool>,
    ptr: NonNull<u8>,
    capacity: usize,
    len: usize,
    memory: NonNull<RegisteredMemory>,
    memory_index: usize,
    block_index: usize,
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Buffer {
    pub(crate) fn new(
        pool: Arc<BufferPool>,
        ptr: NonNull<u8>,
        capacity: usize,
        memory: NonNull<RegisteredMemory>,
        memory_index: usize,
        block_index: usize,
    ) -> Self {
        Self {
            pool,
            ptr,
            capacity,
            len: capacity,
            memory,
            memory_index,
            block_index,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn memory(&self) -> &RegisteredMemory {
        unsafe { self.memory.as_ref() }
    }

    pub fn memory_key(&self, device_index: &impl AsDeviceIndex) -> Result<MemoryKey> {
        let reg = self.memory().registration(device_index)?;
        Ok(reg.memory_key())
    }

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

    pub fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.capacity,
            "len {len} > capacity {}",
            self.capacity
        );
        self.len = len;
    }

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

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
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
            .field("capacity", &self.capacity)
            .field("len", &self.len)
            .field("block_index", &self.block_index)
            .finish()
    }
}
