//! [`ProtectionDomain`]: RAII wrapper for `ibv_pd`.

use std::sync::Arc;

use super::context::Context;
use crate::{ErrorKind, Result};

/// A protection domain (PD).
///
/// Isolates memory regions and queue pairs from each other. Holds a
/// shared reference to the [`Context`] to guarantee the device context
/// remains valid.
pub struct ProtectionDomain {
    ptr: *mut crate::ibv_pd,
    /// Prevents the context from being closed while this PD exists.
    _context: Arc<Context>,
}

impl ProtectionDomain {
    /// Allocates a new protection domain on the given context.
    pub fn alloc(context: &Arc<Context>) -> Result<Arc<Self>> {
        let ptr = unsafe { crate::ruapc_ibv_alloc_pd(context.as_ptr()) };
        if ptr.is_null() {
            return Err(ErrorKind::IBAllocPDFail.with_errno());
        }
        Ok(Arc::new(Self {
            ptr,
            _context: Arc::clone(context),
        }))
    }

    /// Returns the raw protection domain pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_pd {
        self.ptr
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        let _ = unsafe { crate::ruapc_ibv_dealloc_pd(self.ptr) };
    }
}

impl std::fmt::Debug for ProtectionDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtectionDomain")
            .field("ptr", &self.ptr)
            .finish()
    }
}

unsafe impl Send for ProtectionDomain {}
unsafe impl Sync for ProtectionDomain {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_utils::open_device;
    use crate::*;

    #[test]
    fn test_protection_domain_alloc() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let pd = ProtectionDomain::alloc(&ctx).unwrap();
        assert!(!pd.as_ptr().is_null());
    }

    #[test]
    fn test_protection_domain_debug() {
        let dev = open_device();
        let pd = Arc::clone(dev.pd());
        let debug = format!("{:?}", pd);
        assert!(debug.contains("ProtectionDomain"));
    }
}
