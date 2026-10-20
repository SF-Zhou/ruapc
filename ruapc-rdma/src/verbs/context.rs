//! [`Context`]: RAII wrapper for `ibv_context`.

use std::sync::Arc;

use crate::{ErrorKind, Result};

mod gid;

/// An opened RDMA device context.
///
/// Wraps `ibv_context` and ensures proper cleanup via `ibv_close_device`
/// when dropped.
///
/// libibverbs internally reference-counts `ibv_device` structs, so the
/// device list can be safely freed after opening a device.
pub struct Context {
    ptr: *mut crate::ibv_context,
    gid_query: gid::GidQuery,
}

impl Context {
    /// Opens a device and returns a new `Context`.
    ///
    /// # Safety
    ///
    /// `device` must be a valid pointer obtained from `ibv_get_device_list`.
    /// The device list may be freed after this call returns successfully.
    pub(crate) fn open(device: *mut crate::ibv_device) -> Result<Arc<Self>> {
        let ptr = unsafe { crate::ruapc_ibv_open_device(device) };
        if ptr.is_null() {
            return Err(ErrorKind::IBOpenDeviceFail.with_errno());
        }
        Ok(Arc::new(Self {
            ptr,
            gid_query: gid::GidQuery::default(),
        }))
    }

    /// Returns the raw context pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_context {
        self.ptr
    }

    /// Queries device attributes.
    pub fn query_device(&self) -> Result<crate::ibv_device_attr> {
        let mut attr = crate::ibv_device_attr::default();
        let ret = unsafe { crate::ruapc_ibv_query_device(self.ptr, &mut attr) };
        if ret != 0 {
            return Err(ErrorKind::IBQueryDeviceFail.with_errno());
        }
        Ok(attr)
    }

    /// Queries port attributes.
    pub fn query_port(&self, port_num: u8) -> Result<crate::ibv_port_attr> {
        // Keep the out buffer zeroed without constructing a Rust value first:
        // zero is not a valid discriminant for fields such as `ibv_mtu`.
        let mut attr = std::mem::MaybeUninit::<crate::ibv_port_attr>::uninit();
        let ret = unsafe { crate::ruapc_ibv_query_port(self.ptr, port_num, attr.as_mut_ptr()) };
        if ret != 0 {
            return Err(ErrorKind::IBQueryPortFail.with_errno());
        }
        Ok(unsafe { attr.assume_init() })
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let _ = unsafe { crate::ruapc_ibv_close_device(self.ptr) };
    }
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context").field("ptr", &self.ptr).finish()
    }
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_utils::open_device;

    fn ctx() -> Arc<crate::Context> {
        Arc::clone(open_device().context())
    }

    #[test]
    fn test_context_query_device() {
        let ctx = ctx();
        let attr = ctx.query_device().unwrap();
        assert!(attr.phys_port_cnt >= 1);
    }

    #[test]
    fn test_context_query_port() {
        let ctx = ctx();
        let port_attr = ctx.query_port(1).unwrap();
        assert!(port_attr.gid_tbl_len > 0);
    }

    #[test]
    fn test_context_query_gid() {
        let ctx = ctx();
        let gid = ctx.query_gid(1, 0).unwrap();
        assert!(!gid.is_null());
    }

    #[test]
    fn test_context_debug() {
        let ctx = ctx();
        let debug = format!("{:?}", ctx);
        assert!(debug.contains("Context"));
    }
}
