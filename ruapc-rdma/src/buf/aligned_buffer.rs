use crate::{ErrorKind, Result};
use std::alloc::Layout;

/// Page alignment size for RDMA buffers (4 KB).
pub const ALIGN_SIZE: usize = 4096;

/// Page-aligned memory buffer for RDMA operations.
///
/// Allocates memory aligned to 4KB boundaries as required by RDMA hardware.
/// The buffer size is automatically rounded up to the nearest multiple of
/// [`ALIGN_SIZE`].
///
/// # Examples
///
/// ```rust,no_run
/// # use ruapc_rdma::AlignedBuffer;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let buffer = AlignedBuffer::new(8192)?;
/// assert_eq!(buffer.len(), 8192);
/// # Ok(())
/// # }
/// ```
pub struct AlignedBuffer(&'static mut [u8]);

impl AlignedBuffer {
    /// Creates a new page-aligned buffer.
    ///
    /// The actual buffer size will be rounded up to the nearest multiple
    /// of [`ALIGN_SIZE`] (4096 bytes).
    ///
    /// # Arguments
    ///
    /// * `size` - Desired buffer size in bytes (must be non-zero)
    ///
    /// # Errors
    ///
    /// Returns an error if memory allocation fails.
    ///
    /// # Panics
    ///
    /// Panics if `size` is zero.
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
