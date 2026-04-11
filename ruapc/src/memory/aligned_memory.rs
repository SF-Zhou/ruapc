use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;

/// Alignment size: 2 MiB for huge page compatibility on 64-bit platforms.
#[cfg(target_pointer_width = "64")]
const ALIGN: usize = 2 * 1024 * 1024;

#[cfg(not(target_pointer_width = "64"))]
const ALIGN: usize = 4096;

/// An owned, aligned memory block. This is the system's smallest unit
/// of memory allocation and deallocation.
///
/// The memory is aligned to 2 MiB (on 64-bit) for huge page support.
/// Automatically freed on drop via `std::alloc::dealloc`.
pub struct AlignedMemory {
    ptr: NonNull<u8>,
    size: usize,
}

// SAFETY: The memory is exclusively owned and not aliased.
unsafe impl Send for AlignedMemory {}
// SAFETY: Shared access (&self) only provides immutable views.
unsafe impl Sync for AlignedMemory {}

#[allow(unsafe_code)]
impl AlignedMemory {
    /// Allocates a new aligned memory block of the given size.
    ///
    /// The size is rounded up to a multiple of the alignment.
    /// Returns an error if size is zero or allocation fails.
    pub fn new(size: usize) -> crate::Result<Self> {
        if size == 0 {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "cannot allocate zero-sized memory".into(),
            ));
        }

        // Round up to alignment boundary.
        let size = (size + ALIGN - 1) & !(ALIGN - 1);
        let layout = Layout::from_size_align(size, ALIGN).map_err(|e| {
            crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                format!("bad layout: {e}"),
            )
        })?;

        // SAFETY: layout has non-zero size (checked above).
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::Unknown(format!(
                    "failed to allocate {size} bytes with {ALIGN} alignment"
                )),
                String::new(),
            )
        })?;

        Ok(Self { ptr, size })
    }

    /// Returns the size of the allocation in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns a raw pointer to the memory.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to the memory.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns the memory as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for `size` bytes and properly aligned.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    /// Returns the memory as a mutable byte slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for `size` bytes and we have exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }
}

#[allow(unsafe_code)]
impl Drop for AlignedMemory {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with the same layout.
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.size, ALIGN);
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl std::fmt::Debug for AlignedMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedMemory")
            .field("ptr", &self.ptr)
            .field("size", &self.size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_memory_basic() {
        let mem = AlignedMemory::new(4096).unwrap();
        assert!(mem.size() >= 4096);
        assert_eq!(mem.as_ptr() as usize % ALIGN, 0);
    }

    #[test]
    fn test_aligned_memory_rounds_up() {
        let mem = AlignedMemory::new(1).unwrap();
        assert_eq!(mem.size(), ALIGN);
    }

    #[test]
    fn test_aligned_memory_zero_size() {
        assert!(AlignedMemory::new(0).is_err());
    }

    #[test]
    fn test_aligned_memory_read_write() {
        let mut mem = AlignedMemory::new(ALIGN).unwrap();
        let slice = mem.as_mut_slice();
        slice[0] = 0x42;
        slice[1] = 0x43;
        assert_eq!(mem.as_slice()[0], 0x42);
        assert_eq!(mem.as_slice()[1], 0x43);
    }
}
