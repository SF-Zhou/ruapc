//! [`MemoryRegion`]: RAII wrapper for `ibv_mr`.

use std::{os::raw::c_int, sync::Arc};

use ruapc_bufpool::AlignedMemory;

use super::protection_domain::ProtectionDomain;
use crate::{ErrorKind, Result};

/// A registered memory region (MR).
///
/// Pins a contiguous block of memory for RDMA access. Holds a shared
/// reference to the [`ProtectionDomain`] and the underlying
/// [`AlignedMemory`] to keep them alive for the MR's lifetime.
pub struct MemoryRegion {
    ptr: *mut crate::ibv_mr,
    _pd: Arc<ProtectionDomain>,
    _memory: Arc<AlignedMemory>,
}

impl MemoryRegion {
    /// Registers an `Arc<AlignedMemory>` for RDMA access.
    ///
    /// The returned `MemoryRegion` holds a clone of the `Arc`, ensuring the
    /// memory stays alive until the MR is dropped and deregistered.
    pub fn register(
        pd: &Arc<ProtectionDomain>,
        memory: &Arc<AlignedMemory>,
        access: c_int,
    ) -> Result<Self> {
        let ptr = unsafe {
            crate::ibv_reg_mr(pd.as_ptr(), memory.as_mut_ptr() as _, memory.size(), access)
        };
        if ptr.is_null() {
            return Err(ErrorKind::IBRegMemoryRegionFail.with_errno());
        }
        Ok(Self {
            ptr,
            _pd: Arc::clone(pd),
            _memory: Arc::clone(memory),
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

impl ruapc_bufpool::Registration for MemoryRegion {
    fn memory_key(&self) -> ruapc_bufpool::MemoryKey {
        ruapc_bufpool::MemoryKey {
            lkey: self.lkey(),
            rkey: self.rkey(),
        }
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

    use ruapc_bufpool::AlignedMemory;

    use crate::test_utils::open_device;
    use crate::*;

    #[test]
    fn test_memory_region_register() {
        let dev = open_device();
        let pd = Arc::clone(dev.pd());
        let mem = Arc::new(AlignedMemory::new(4096).unwrap());
        let mr = MemoryRegion::register(&pd, &mem, ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 as _)
            .unwrap();
        assert_ne!(mr.lkey(), 0);
        assert_eq!(mr.length(), mem.size());
        assert!(!mr.addr().is_null());
    }

    #[test]
    fn test_memory_region_memory_keeps_alive() {
        let dev = open_device();
        let pd = Arc::clone(dev.pd());
        let mem = Arc::new(AlignedMemory::new(4096).unwrap());
        let mr = MemoryRegion::register(&pd, &mem, ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 as _)
            .unwrap();
        assert_ne!(mr.lkey(), 0);
        assert_eq!(mr.length(), mem.size());
        assert_eq!(mr.addr(), mem.as_ptr() as *mut _);
    }

    #[test]
    fn test_memory_region_debug() {
        let dev = open_device();
        let pd = Arc::clone(dev.pd());
        let mem = Arc::new(AlignedMemory::new(64).unwrap());
        let mr = MemoryRegion::register(&pd, &mem, ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 as _)
            .unwrap();
        let debug = format!("{:?}", mr);
        assert!(debug.contains("MemoryRegion"));
        assert!(debug.contains("lkey"));
    }
}
