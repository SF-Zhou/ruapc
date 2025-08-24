use crate::{ErrorKind, Result};
use std::alloc::Layout;

pub const ALIGN_SIZE: usize = 4096;

/// A buffer that is aligned to a specific size, typically used for RDMA operations.
pub struct AlignedBuffer(&'static mut [u8]);

impl AlignedBuffer {
    pub fn new(size: usize) -> Result<Self> {
        assert_ne!(size, 0, "the buffer length cannot be zero!");
        let size = size.next_multiple_of(ALIGN_SIZE);
        unsafe {
            let layout = Layout::from_size_align_unchecked(size, ALIGN_SIZE);
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                Err(ErrorKind::AllocMemoryFailed.into())
            } else {
                Ok(Self(std::slice::from_raw_parts_mut(ptr, size)))
            }
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.0.len(), ALIGN_SIZE);
            std::alloc::dealloc(self.0.as_mut_ptr(), layout);
        }
    }
}

impl std::ops::Deref for AlignedBuffer {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl std::ops::DerefMut for AlignedBuffer {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}
