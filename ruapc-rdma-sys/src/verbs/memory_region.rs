//! [`MemoryRegion`]: RAII wrapper for `ibv_mr`.

use std::{os::raw::c_int, sync::Arc};

use super::protection_domain::ProtectionDomain;
use crate::{ErrorKind, Result};

/// A registered memory region (MR).
///
/// Pins a contiguous block of memory for RDMA access. Holds a shared
/// reference to the [`ProtectionDomain`] to ensure it outlives this MR.
pub struct MemoryRegion {
    ptr: *mut crate::ibv_mr,
    /// Prevents the PD (and transitively the context) from being freed.
    _pd: Arc<ProtectionDomain>,
}

impl MemoryRegion {
    /// Registers a memory region for RDMA access.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `addr` points to a valid buffer of at least `length` bytes
    /// - The buffer remains valid and is not deallocated while the MR exists
    pub unsafe fn register(
        pd: &Arc<ProtectionDomain>,
        addr: *mut std::ffi::c_void,
        length: usize,
        access: c_int,
    ) -> Result<Self> {
        let ptr = unsafe { crate::ibv_reg_mr(pd.as_ptr(), addr, length, access) };
        if ptr.is_null() {
            return Err(ErrorKind::IBRegMemoryRegionFail.with_errno());
        }
        Ok(Self {
            ptr,
            _pd: Arc::clone(pd),
        })
    }

    /// Returns the raw MR pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_mr {
        self.ptr
    }

    /// Returns the local key for this memory region.
    pub fn lkey(&self) -> u32 {
        unsafe { (*self.ptr).lkey }
    }

    /// Returns the remote key for this memory region.
    pub fn rkey(&self) -> u32 {
        unsafe { (*self.ptr).rkey }
    }

    /// Returns the registered address.
    pub fn addr(&self) -> *mut std::ffi::c_void {
        unsafe { (*self.ptr).addr }
    }

    /// Returns the registered length in bytes.
    pub fn length(&self) -> usize {
        unsafe { (*self.ptr).length }
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        let _ = unsafe { crate::ibv_dereg_mr(self.ptr) };
    }
}

impl std::fmt::Debug for MemoryRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryRegion")
            .field("ptr", &self.ptr)
            .field("lkey", &self.lkey())
            .field("rkey", &self.rkey())
            .field("length", &self.length())
            .finish()
    }
}

unsafe impl Send for MemoryRegion {}
unsafe impl Sync for MemoryRegion {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_utils::open_device;
    use crate::*;

    #[test]
    fn test_memory_region_register() {
        let dev = open_device();
        let pd = Arc::clone(dev.pd());
        let mut buf = vec![0u8; 4096];
        let mr = unsafe {
            MemoryRegion::register(
                &pd,
                buf.as_mut_ptr() as *mut _,
                buf.len(),
                ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 as _,
            )
            .unwrap()
        };
        assert_ne!(mr.lkey(), 0);
        assert_eq!(mr.length(), 4096);
        assert!(!mr.addr().is_null());
    }

    #[test]
    fn test_memory_region_debug() {
        let dev = open_device();
        let pd = Arc::clone(dev.pd());
        let mut buf = vec![0u8; 64];
        let mr = unsafe {
            MemoryRegion::register(
                &pd,
                buf.as_mut_ptr() as *mut _,
                buf.len(),
                ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 as _,
            )
            .unwrap()
        };
        let debug = format!("{:?}", mr);
        assert!(debug.contains("MemoryRegion"));
        assert!(debug.contains("lkey"));
    }
}
