//! # RDMA device handle
//!
//! [`Device`] wraps an `ibv_device` pointer with cached metadata.
//! [`ActiveDevice`] represents an opened device with context and protection domain.

use std::{ffi::CStr, os::unix::ffi::OsStrExt, path::Path, sync::Arc};

use super::{
    context::Context, device_list::DeviceList, memory_region::MemoryRegion,
    protection_domain::ProtectionDomain,
};
use crate::{DeviceInfo, Gid, Guid, Port, Result};

/// Lightweight RDMA device handle.
///
/// Wraps an `ibv_device` pointer with cached name, GUID, and sysfs path.
/// Call [`open`](Self::open) to create an [`ActiveDevice`].
///
/// The internal raw pointer is valid only while the originating
/// [`DeviceList`] exists.
pub struct Device {
    ptr: *mut crate::ibv_device,
    name: String,
    guid: Guid,
    transport_type: crate::ibv_transport_type,
    ibdev_path: std::path::PathBuf,
}

impl Device {
    pub(crate) fn from_ptr(ptr: *mut crate::ibv_device) -> Self {
        let name = unsafe {
            CStr::from_ptr((*ptr).name.as_ptr())
                .to_string_lossy()
                .to_string()
        };
        let guid = Guid::from_be(unsafe { crate::ibv_get_device_guid(ptr) });
        let transport_type = unsafe { (*ptr).transport_type };
        let ibdev_path = unsafe {
            Path::new(std::ffi::OsStr::from_bytes(
                CStr::from_ptr((*ptr).ibdev_path.as_ptr()).to_bytes(),
            ))
        }
        .to_path_buf();

        Self {
            ptr,
            name,
            guid,
            transport_type,
            ibdev_path,
        }
    }

    /// Device name (e.g., "mlx5_0").
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Globally unique identifier.
    pub fn guid(&self) -> Guid {
        self.guid
    }

    /// Path to the device in sysfs.
    pub fn ibdev_path(&self) -> &Path {
        &self.ibdev_path
    }

    /// Opens this device, creating an [`ActiveDevice`] with context and PD.
    ///
    /// The originating [`DeviceList`] must still be alive when calling
    /// this method.
    pub fn open(&self) -> Result<ActiveDevice> {
        let context = Context::open(self.ptr)?;
        let pd = ProtectionDomain::alloc(&context)?;

        let mut active = ActiveDevice {
            pd,
            context,
            info: DeviceInfo {
                name: self.name.clone(),
                guid: self.guid,
                transport_type: self.transport_type,
                ibdev_path: self.ibdev_path.clone(),
                ..Default::default()
            },
        };
        active.update_attr()?;

        Ok(active)
    }
}

impl std::fmt::Debug for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Device")
            .field("name", &self.name)
            .field("guid", &self.guid)
            .field("transport_type", &self.transport_type)
            .finish()
    }
}

/// An opened RDMA device with context, protection domain, and metadata.
///
/// Created via [`Device::open`] or the convenience method
/// [`available`](Self::available).
///
/// # Examples
///
/// ```rust,no_run
/// let devices = ruapc_rdma::ActiveDevice::available()?;
/// for dev in &devices {
///     println!("{}: {}", dev.info().name, dev.info().guid);
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ActiveDevice {
    pd: Arc<ProtectionDomain>,
    context: Arc<Context>,
    info: DeviceInfo,
}

impl ActiveDevice {
    /// Discovers and opens all available RDMA devices.
    pub fn available() -> Result<Vec<Self>> {
        let device_list = DeviceList::available()?;
        let mut devices = Vec::with_capacity(device_list.len());
        for device in device_list.iter() {
            devices.push(device.open()?);
        }
        Ok(devices)
    }

    /// Returns a shared reference to the device context.
    pub fn context(&self) -> &Arc<Context> {
        &self.context
    }

    /// Returns a shared reference to the protection domain.
    pub fn pd(&self) -> &Arc<ProtectionDomain> {
        &self.pd
    }

    /// Returns device information.
    pub fn info(&self) -> &DeviceInfo {
        &self.info
    }

    /// Registers an `AlignedMemory` for RDMA access with full remote
    /// read/write permissions.
    ///
    /// The returned [`MemoryRegion`] holds a clone of the `Arc`, so the
    /// memory stays alive until the MR is dropped and deregistered.
    pub fn register(&self, memory: &Arc<ruapc_bufpool::AlignedMemory>) -> Result<MemoryRegion> {
        let access = crate::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
            | crate::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
            | crate::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
            | crate::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING.0;
        MemoryRegion::register(&self.pd, memory, access as _)
    }

    /// Queries the hardware and returns a fresh snapshot of the device
    /// information (attributes, ports, and usable GIDs).
    ///
    /// Unusable GIDs (e.g. RoCE v2 GIDs derived from loopback or link-local
    /// addresses, see [`crate::is_gid_usable`]) are filtered out at
    /// collection time and never appear in the returned snapshot.
    pub fn query_device_info(&self) -> Result<DeviceInfo> {
        let device_attr = self.context.query_device()?;

        let mut ports = Vec::with_capacity(device_attr.phys_port_cnt as usize);
        for port_num in 1..=device_attr.phys_port_cnt {
            let port_attr = self.context.query_port(port_num)?;

            let gids = self.collect_port_gids(port_num, &port_attr);
            ports.push(Port {
                port_num,
                port_attr,
                gids,
            });
        }

        Ok(DeviceInfo {
            device_attr,
            ports,
            ..self.info.clone()
        })
    }

    /// Updates the cached device attributes by querying the hardware.
    pub fn update_attr(&mut self) -> Result<()> {
        self.info = self.query_device_info()?;
        Ok(())
    }

    fn collect_port_gids(&self, port_num: u8, port_attr: &crate::ibv_port_attr) -> Vec<Gid> {
        // GID indices are exchanged as `u8` during connection negotiation,
        // so entries beyond index 255 are unusable and not collected.
        let gid_tbl_len = port_attr.gid_tbl_len.clamp(0, 1 + u8::MAX as i32);
        let mut gids = Vec::with_capacity(gid_tbl_len as usize);
        for gid_index in 0..gid_tbl_len {
            let gid_index = gid_index as u8;
            let Ok(gid) = self.context.query_gid(port_num, gid_index) else {
                continue;
            };
            if let Ok(gid_type) =
                self.context
                    .query_gid_type(port_num, gid_index, &self.info.ibdev_path, port_attr)
                && let Some(gid) = Gid::usable(gid_index, gid, gid_type)
            {
                gids.push(gid);
            }
        }
        gids
    }
}

impl std::fmt::Debug for ActiveDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.info, f)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_device_available() {
        let devices = ActiveDevice::available().expect("no RDMA devices");
        assert!(!devices.is_empty());
    }

    #[test]
    fn test_device_info() {
        let devices = ActiveDevice::available().unwrap();
        let dev = devices.first().unwrap();
        let info = dev.info();
        assert!(!info.name.is_empty());
        assert!(!info.ports.is_empty());
    }

    #[test]
    fn test_device_debug() {
        let devices = ActiveDevice::available().unwrap();
        let debug = format!("{:?}", devices.first().unwrap());
        assert!(!debug.is_empty());
    }
}
