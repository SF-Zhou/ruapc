//! Aligned memory block with automatic allocation and deallocation.

use std::alloc::Layout;
#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
use std::alloc::{alloc_zeroed, dealloc};
use std::io::{Error, ErrorKind, Result};
use std::ptr::NonNull;

/// Alignment size: 2 MiB for huge page compatibility on 64-bit platforms.
#[cfg(target_pointer_width = "64")]
const ALIGN: usize = 2 * 1024 * 1024;

#[cfg(not(target_pointer_width = "64"))]
const ALIGN: usize = 4096;

/// An owned, zero-initialized, aligned memory block.
///
/// Linux 64-bit builds use an anonymous mapping: pages are initialized lazily by
/// the OS, so creating a block does not write through the entire allocation.
/// Other targets use the system's zeroed allocator. Pool reuse does not clear
/// previously written bytes.
///
/// The memory is aligned to 2 MiB (on 64-bit) for huge page support.
pub struct AlignedMemory {
    ptr: NonNull<u8>,
    size: usize,
}

// SAFETY: The memory is exclusively owned and not aliased.
unsafe impl Send for AlignedMemory {}
// SAFETY: shared access only provides immutable slices/raw pointers; writing
// through a raw pointer requires the caller to enforce exclusive access.
unsafe impl Sync for AlignedMemory {}

impl AlignedMemory {
    /// Allocates a new aligned memory block of the given size.
    ///
    /// The size is rounded up to a multiple of the alignment.
    /// Returns an error if size is zero or allocation fails.
    pub fn new(size: usize) -> Result<Self> {
        if size == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "cannot allocate zero-sized memory",
            ));
        }

        let size = size
            .checked_add(ALIGN - 1)
            .map(|size| size & !(ALIGN - 1))
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "aligned size overflow"))?;
        // Validate the slice/layout limit on every backend, including mmap.
        let layout = Layout::from_size_align(size, ALIGN)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, format!("bad layout: {e}")))?;
        let ptr = allocate_zeroed(layout)?;

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
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns the memory as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for `size` bytes and properly aligned.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    /// Returns the memory as a mutable byte slice.
    ///
    /// The mutable borrow prevents safe callers from creating overlapping
    /// mutable views. Shared registrations use raw pointers and must enforce
    /// their own access and lifetime rules.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for `size` bytes and properly aligned.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }
}

/// Reserve enough address space to select an aligned subrange, then discard
/// the unused page-aligned prefix and suffix. No committed pages are copied.
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
fn allocate_zeroed(layout: Layout) -> Result<NonNull<u8>> {
    let mapped_len = layout
        .size()
        .checked_add(ALIGN)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "mapping size overflow"))?;
    // Large anonymous mappings are often naturally aligned. Keep that mapping
    // directly when possible and pay for overmapping/trimming only otherwise.
    let mapping = map_anonymous(layout.size())?;
    if mapping as usize & (ALIGN - 1) == 0 && !mapping.is_null() {
        return Ok(NonNull::new(mapping.cast()).unwrap());
    }
    // SAFETY: no reference or pointer escaped this unsuccessful alignment probe.
    if unsafe { libc::munmap(mapping, layout.size()) } != 0 {
        return Err(Error::last_os_error());
    }
    let mapping = map_anonymous(mapped_len)?;
    let base = mapping.cast::<u8>();
    let prefix = (ALIGN - (base as usize & (ALIGN - 1))) & (ALIGN - 1);
    let suffix = ALIGN - prefix;
    // SAFETY: prefix is within the mapping and retained.size == layout.size.
    // Linux page sizes divide ALIGN, so both trims start/end on page boundaries.
    let aligned = unsafe { base.add(prefix) };
    if prefix != 0 && unsafe { libc::munmap(mapping, prefix) } != 0 {
        let error = Error::last_os_error();
        // SAFETY: no trim succeeded; the original mapping remains owned here.
        unsafe {
            libc::munmap(mapping, mapped_len);
        }
        return Err(error);
    }
    if unsafe { libc::munmap(aligned.add(layout.size()).cast(), suffix) } != 0 {
        let error = Error::last_os_error();
        // SAFETY: the prefix was removed, but the retained range/suffix are live.
        unsafe {
            libc::munmap(aligned.cast(), layout.size() + suffix);
        }
        return Err(error);
    }
    NonNull::new(aligned).ok_or_else(|| {
        // A null address cannot back Rust slices, even if an OS permitted it.
        // SAFETY: this is the successfully retained mapping.
        unsafe {
            libc::munmap(aligned.cast(), layout.size());
        }
        Error::new(ErrorKind::OutOfMemory, "mapping returned a null address")
    })
}

#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
fn map_anonymous(len: usize) -> Result<*mut libc::c_void> {
    // SAFETY: anonymous private mapping, no file descriptor or existing address.
    let mapping = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if mapping == libc::MAP_FAILED {
        Err(Error::last_os_error())
    } else {
        Ok(mapping)
    }
}

#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
fn allocate_zeroed(layout: Layout) -> Result<NonNull<u8>> {
    // SAFETY: layout was validated and has nonzero size.
    NonNull::new(unsafe { alloc_zeroed(layout) })
        .ok_or_else(|| Error::new(ErrorKind::OutOfMemory, "aligned allocation failed"))
}

impl Drop for AlignedMemory {
    fn drop(&mut self) {
        #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
        // SAFETY: this is exactly the mapping retained by allocate_zeroed.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.size);
        }
        #[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
        // SAFETY: allocation and deallocation use the same validated layout.
        unsafe {
            dealloc(
                self.ptr.as_ptr(),
                Layout::from_size_align_unchecked(self.size, ALIGN),
            );
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

    #[test]
    fn test_aligned_memory_debug() {
        let mem = AlignedMemory::new(ALIGN).unwrap();
        let debug = format!("{mem:?}");
        assert!(debug.contains("AlignedMemory"));
        assert!(debug.contains("size"));
    }

    #[test]
    fn test_aligned_memory_mut_ptr() {
        let mem = AlignedMemory::new(ALIGN).unwrap();
        let ptr = mem.as_ptr();
        let mut_ptr = mem.as_mut_ptr();
        assert_eq!(ptr, mut_ptr as *const u8);
    }

    #[test]
    fn initial_bytes_are_zero_across_the_mapping() {
        let memory = AlignedMemory::new(2 * ALIGN).unwrap();
        assert!(memory.as_slice().iter().all(|&byte| byte == 0));
    }

    #[test]
    fn impossible_sizes_are_rejected_before_allocation() {
        for size in [usize::MAX, usize::MAX - ALIGN + 1, isize::MAX as usize] {
            assert_eq!(
                AlignedMemory::new(size).unwrap_err().kind(),
                ErrorKind::InvalidInput
            );
        }
    }
}
