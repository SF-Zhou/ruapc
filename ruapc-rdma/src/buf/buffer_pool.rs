use crate::*;
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

/// A pool of buffers that can be allocated and deallocated.
/// This pool is designed to manage a fixed-size buffer that can be divided into smaller blocks.
pub struct BufferPool {
    buffer: RegisteredBuffer,
    block_size: usize,
    free_list: Mutex<Vec<usize>>,
}

pub struct Buffer {
    pool: Arc<BufferPool>,
    idx: usize,
    length: usize,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.pool.deallocate(self.idx);
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let slice = self.as_slice();
        unsafe { std::slice::from_raw_parts_mut(slice.as_ptr() as *mut u8, slice.len()) }
    }
}

impl Buffer {
    pub fn lkey(&self, device: &Device) -> u32 {
        self.pool.buffer.lkey(device.index())
    }

    pub fn rkey(&self, device: &Device) -> u32 {
        self.pool.buffer.rkey(device.index())
    }

    pub fn capacity(&self) -> usize {
        self.pool.block_size
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        let offset = self.idx * self.capacity();
        &self.pool.buffer[offset..offset + self.length]
    }

    pub fn set_len(&mut self, len: usize) {
        self.length = len;
    }

    pub fn extend_from_slice(&mut self, buf: &[u8]) -> Result<()> {
        let cap = self.capacity();
        if buf.len() + self.len() <= cap {
            let offset = self.length;
            self.length += buf.len();
            self.deref_mut()[offset..].copy_from_slice(buf);
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InsufficientBuffer,
                format!(
                    "offset {} + length {} > capacity {}",
                    self.length,
                    buf.len(),
                    cap
                ),
            ))
        }
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slice = self.as_slice();
        bytes::Bytes::from_static(unsafe {
            std::slice::from_raw_parts_mut(slice.as_ptr() as *mut _, slice.len())
        })
        .fmt(f)
    }
}

impl BufferPool {
    pub fn create(block_size: usize, block_count: usize, devices: &Devices) -> Result<Arc<Self>> {
        let buffer_size = block_size * block_count;
        let buffer = RegisteredBuffer::create(devices, buffer_size)?;
        let free_list = Mutex::new((0..block_count).collect());
        Ok(Arc::new(Self {
            buffer,
            block_size,
            free_list,
        }))
    }

    pub fn allocate(self: &Arc<Self>) -> Result<Buffer> {
        let mut free_list = self.free_list.lock().unwrap();
        match free_list.pop() {
            Some(idx) => Ok(Buffer {
                pool: self.clone(),
                idx,
                length: 0,
            }),
            None => Err(ErrorKind::AllocMemoryFailed.into()),
        }
    }

    fn deallocate(&self, idx: usize) {
        let mut free_list = self.free_list.lock().unwrap();
        free_list.push(idx);
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("block_size", &self.block_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer() {
        const LEN: usize = 1 << 20;
        let devices = Devices::availables().unwrap();
        let buffer_pool = BufferPool::create(LEN, 32, &devices).unwrap();

        let mut buf = buffer_pool.allocate().unwrap();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.capacity(), LEN);

        const SLICE: [u8; 233] = [1u8; 233];
        buf.extend_from_slice(&SLICE).unwrap();
        assert_eq!(buf.len(), 233);
        assert_eq!(buf.capacity(), LEN);
        assert_eq!(buf.deref(), SLICE);

        buf.deref_mut().fill(2);
        assert_eq!(buf.len(), 233);
        assert_eq!(buf.capacity(), LEN);
        assert_eq!(buf.deref(), SLICE.map(|v| v * 2));
    }
}
