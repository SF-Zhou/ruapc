//! [`Context`]: RAII wrapper for `ibv_context`.

use std::{path::Path, sync::Arc};

use crate::{Error, ErrorKind, GidType, LinkLayer, Result};

/// GID type string values from sysfs.
const GID_TYPE_IB_ROCE_V1: &str = "IB/RoCE v1\n";
const GID_TYPE_ROCE_V2: &str = "RoCE v2\n";

/// An opened RDMA device context.
///
/// Wraps `ibv_context` and ensures proper cleanup via `ibv_close_device`
/// when dropped.
///
/// libibverbs internally reference-counts `ibv_device` structs, so the
/// device list can be safely freed after opening a device.
pub struct Context {
    ptr: *mut crate::ibv_context,
}

impl Context {
    /// Opens a device and returns a new `Context`.
    ///
    /// # Safety
    ///
    /// `device` must be a valid pointer obtained from `ibv_get_device_list`.
    /// The device list may be freed after this call returns successfully.
    pub(crate) fn open(device: *mut crate::ibv_device) -> Result<Arc<Self>> {
        let ptr = unsafe { crate::ibv_open_device(device) };
        if ptr.is_null() {
            return Err(ErrorKind::IBOpenDeviceFail.with_errno());
        }
        Ok(Arc::new(Self { ptr }))
    }

    /// Returns the raw context pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_context {
        self.ptr
    }

    /// Queries device attributes.
    pub fn query_device(&self) -> Result<crate::ibv_device_attr> {
        let mut attr = crate::ibv_device_attr::default();
        let ret = unsafe { crate::ibv_query_device(self.ptr, &mut attr) };
        if ret != 0 {
            return Err(ErrorKind::IBQueryDeviceFail.with_errno());
        }
        Ok(attr)
    }

    /// Queries port attributes.
    pub fn query_port(&self, port_num: u8) -> Result<crate::ibv_port_attr> {
        let mut attr = std::mem::MaybeUninit::<crate::ibv_port_attr>::uninit();
        let ret = unsafe { crate::ibv_query_port(self.ptr, port_num, attr.as_mut_ptr() as _) };
        if ret != 0 {
            return Err(ErrorKind::IBQueryPortFail.with_errno());
        }
        Ok(unsafe { attr.assume_init() })
    }

    /// Queries a GID for the specified port and index.
    pub fn query_gid(&self, port_num: u8, gid_index: u16) -> Result<crate::ibv_gid> {
        let mut gid = crate::ibv_gid::default();
        let ret =
            unsafe { crate::ibv_query_gid(self.ptr, port_num as _, gid_index as _, &mut gid) };
        if ret == 0 && !gid.is_null() {
            Ok(gid)
        } else {
            Err(ErrorKind::IBQueryGidFail.with_errno())
        }
    }

    /// Queries the GID type from sysfs.
    pub fn query_gid_type(
        &self,
        port_num: u8,
        gid_index: u16,
        ibdev_path: &Path,
        port_attr: &crate::ibv_port_attr,
    ) -> Result<GidType> {
        let path = ibdev_path.join(format!("ports/{port_num}/gid_attrs/types/{gid_index}"));
        match std::fs::read_to_string(path) {
            Ok(content) => {
                if content == GID_TYPE_IB_ROCE_V1 {
                    match port_attr.link_layer {
                        LinkLayer::InfiniBand => Ok(GidType::IB),
                        LinkLayer::Ethernet => Ok(GidType::RoCEv1),
                        _ => Ok(GidType::Other(content.trim().to_string())),
                    }
                } else if content == GID_TYPE_ROCE_V2 {
                    Ok(GidType::RoCEv2)
                } else {
                    Ok(GidType::Other(content.trim().to_string()))
                }
            }
            Err(err) => Err(Error::new(ErrorKind::IBQueryGidTypeFail, err.to_string())),
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let _ = unsafe { crate::ibv_close_device(self.ptr) };
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
