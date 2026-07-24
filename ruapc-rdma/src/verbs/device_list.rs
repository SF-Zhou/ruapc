//! Device list wrapper.

use std::ops::Deref;

use super::device::Device;
use crate::{ErrorKind, Result};

/// Device list returned by `ibv_get_device_list`.
///
/// Iterates as `&[Device]` for type-safe device discovery and opening.
pub struct DeviceList {
    devices: Vec<Device>,
    raw_ptr: *mut *mut crate::ibv_device,
}

impl DeviceList {
    pub fn available() -> Result<Self> {
        let mut num_devices: libc::c_int = 0;
        let ptr = unsafe { crate::ruapc_ibv_get_device_list(&mut num_devices) };
        if ptr.is_null() {
            return Err(ErrorKind::IBGetDeviceListFail.with_errno());
        }
        if num_devices == 0 {
            return Err(ErrorKind::IBDeviceNotFound.into());
        }
        let len = num_devices as usize;
        let mut devices = Vec::with_capacity(len);
        for i in 0..len {
            let device_ptr = unsafe { *ptr.add(i) };
            devices.push(Device::from_ptr(device_ptr));
        }
        Ok(Self {
            devices,
            raw_ptr: ptr,
        })
    }
}

impl Drop for DeviceList {
    fn drop(&mut self) {
        unsafe { crate::ruapc_ibv_free_device_list(self.raw_ptr) };
    }
}

impl Deref for DeviceList {
    type Target = [Device];

    fn deref(&self) -> &Self::Target {
        &self.devices
    }
}

impl std::fmt::Debug for DeviceList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceList")
            .field("num_devices", &self.devices.len())
            .finish()
    }
}

unsafe impl Send for DeviceList {}
unsafe impl Sync for DeviceList {}
