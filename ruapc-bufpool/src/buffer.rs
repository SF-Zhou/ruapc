use std::io::{Error, ErrorKind, Result};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::buffer_pool::BufferPool;
use crate::device::Devices;
use crate::memory::{Reg, RegisteredMemory};

pub struct Buffer<DS: Devices> {
    pool: Arc<BufferPool<DS>>,
    ptr: NonNull<u8>,
    capacity: usize,
    len: usize,
    memory: NonNull<RegisteredMemory<Reg<DS>>>,
    memory_index: usize,
    block_index: usize,
}

unsafe impl<DS: Devices> Send for Buffer<DS> {}
unsafe impl<DS: Devices> Sync for Buffer<DS> {}

impl<DS: Devices> Buffer<DS> {
    pub(crate) fn new(
        pool: Arc<BufferPool<DS>>,
        ptr: NonNull<u8>,
        capacity: usize,
        memory: NonNull<RegisteredMemory<Reg<DS>>>,
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

    pub fn pool(&self) -> &Arc<BufferPool<DS>> {
        &self.pool
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn memory(&self) -> &RegisteredMemory<Reg<DS>> {
        unsafe { self.memory.as_ref() }
    }

    pub fn registration(&self, device: &DS::Device) -> std::io::Result<&Reg<DS>> {
        self.memory().registration(device)
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

impl<DS: Devices> Deref for Buffer<DS> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<DS: Devices> DerefMut for Buffer<DS> {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl<DS: Devices> AsRef<[u8]> for Buffer<DS> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl<DS: Devices> AsMut<[u8]> for Buffer<DS> {
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

impl<DS: Devices> Drop for Buffer<DS> {
    fn drop(&mut self) {
        self.pool.return_buffer(self.memory_index, self.block_index);
    }
}

impl<DS: Devices> std::fmt::Debug for Buffer<DS> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("ptr", &self.ptr)
            .field("capacity", &self.capacity)
            .field("len", &self.len)
            .field("block_index", &self.block_index)
            .finish()
    }
}
