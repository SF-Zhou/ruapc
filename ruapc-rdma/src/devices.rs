use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{DeviceConfig, Error, ErrorKind, GidType, Result, verbs};
use std::{
    ffi::{CStr, OsStr, c_int},
    ops::Deref,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    sync::Arc,
};

struct RawDeviceList {
    ptr: *mut *mut verbs::ibv_device,
    num_devices: usize,
}

impl RawDeviceList {
    fn available() -> Result<Self> {
        let mut num_devices: c_int = 0;
        let ptr = unsafe { verbs::ibv_get_device_list(&mut num_devices) };
        if ptr.is_null() {
            return Err(ErrorKind::IBGetDeviceListFail.with_errno());
        }
        if num_devices == 0 {
            return Err(ErrorKind::IBDeviceNotFound.into());
        }
        Ok(Self {
            ptr,
            num_devices: num_devices as usize,
        })
    }
}

impl Drop for RawDeviceList {
    fn drop(&mut self) {
        unsafe { verbs::ibv_free_device_list(self.ptr) };
    }
}

impl Deref for RawDeviceList {
    type Target = [*mut verbs::ibv_device];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.num_devices) }
    }
}

unsafe impl Send for RawDeviceList {}
unsafe impl Sync for RawDeviceList {}

struct RawContext(*mut verbs::ibv_context);
impl Drop for RawContext {
    fn drop(&mut self) {
        let _ = unsafe { verbs::ibv_close_device(self.0) };
    }
}
impl RawContext {
    fn query_device(&self) -> Result<verbs::ibv_device_attr> {
        let mut device_attr = verbs::ibv_device_attr::default();
        let ret = unsafe { verbs::ibv_query_device(self.0, &mut device_attr) };
        if ret != 0 {
            Err(ErrorKind::IBQueryDeviceFail.with_errno())
        } else {
            Ok(device_attr)
        }
    }

    fn query_port(&self, port_num: u8) -> Result<verbs::ibv_port_attr> {
        let mut port_attr = std::mem::MaybeUninit::<verbs::ibv_port_attr>::uninit();
        let ret = unsafe { verbs::ibv_query_port(self.0, port_num, port_attr.as_mut_ptr() as _) };
        if ret == 0 {
            Ok(unsafe { port_attr.assume_init() })
        } else {
            Err(ErrorKind::IBQueryPortFail.with_errno())
        }
    }

    fn query_gid(&self, port_num: u8, gid_index: u16) -> Result<verbs::ibv_gid> {
        let mut gid = verbs::ibv_gid::default();
        let ret = unsafe { verbs::ibv_query_gid(self.0, port_num as _, gid_index as _, &mut gid) };
        if ret == 0 && !gid.is_null() {
            Ok(gid)
        } else {
            Err(ErrorKind::IBQueryGidFail.with_errno())
        }
    }

    fn query_gid_type(
        &self,
        port_num: u8,
        gid_index: u16,
        ibdev_path: &Path,
        port_attr: &verbs::ibv_port_attr,
    ) -> Result<GidType> {
        let path = ibdev_path.join(format!("ports/{port_num}/gid_attrs/types/{gid_index}"));
        match std::fs::read_to_string(path) {
            Ok(content) => {
                if content == "IB/RoCE v1\n" {
                    if port_attr.link_layer == verbs::IBV_LINK_LAYER::INFINIBAND as u8 {
                        Ok(GidType::IB)
                    } else {
                        Ok(GidType::RoCEv1)
                    }
                } else if content == "RoCE v2\n" {
                    Ok(GidType::RoCEv2)
                } else {
                    Ok(GidType::Other(content.trim().to_string()))
                }
            }
            Err(err) => Err(Error::new(ErrorKind::IBQueryGidTypeFail, err.to_string())),
        }
    }
}
unsafe impl Send for RawContext {}
unsafe impl Sync for RawContext {}

pub struct RawProtectionDomain(*mut verbs::ibv_pd);
impl Drop for RawProtectionDomain {
    fn drop(&mut self) {
        let _ = unsafe { verbs::ibv_dealloc_pd(self.0) };
    }
}
unsafe impl Send for RawProtectionDomain {}
unsafe impl Sync for RawProtectionDomain {}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, Clone)]
#[allow(unused)]
pub struct DeviceInfo {
    pub index: usize,
    pub name: String,
    pub guid: u64,
    pub ibdev_path: PathBuf,
    pub device_attr: verbs::ibv_device_attr,
    pub ports: Vec<Port>,
}

/// Represents an RDMA device.
#[allow(unused)]
pub struct Device {
    protection_domain: RawProtectionDomain,
    context: RawContext,
    device: *mut verbs::ibv_device,
    info: DeviceInfo,
}

unsafe impl Send for Device {}
unsafe impl Sync for Device {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Gid {
    pub index: u16,
    pub gid: verbs::ibv_gid,
    pub gid_type: GidType,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Port {
    pub port_num: u8,
    /// The attributes of the port.
    pub port_attr: verbs::ibv_port_attr,
    /// The GID (Global Identifier) list of the port.
    pub gids: Vec<Gid>,
}

#[allow(unused)]
impl Device {
    fn open(device: *mut verbs::ibv_device, index: usize, config: &DeviceConfig) -> Result<Self> {
        let name = unsafe { CStr::from_ptr((*device).name.as_ptr()) }
            .to_string_lossy()
            .to_string();
        let guid = u64::from_be(unsafe { verbs::ibv_get_device_guid(device) });
        let str = unsafe { CStr::from_ptr((*device).ibdev_path.as_ptr()) };
        let ibdev_path = PathBuf::from(OsStr::from_bytes(str.to_bytes()));

        let context = RawContext(unsafe {
            let context = verbs::ibv_open_device(device);
            if context.is_null() {
                return Err(ErrorKind::IBOpenDeviceFail.with_errno());
            }
            context
        });

        let protection_domain = RawProtectionDomain(unsafe {
            let protection_domain = verbs::ibv_alloc_pd(context.0);
            if protection_domain.is_null() {
                return Err(ErrorKind::IBAllocPDFail.with_errno());
            }
            protection_domain
        });

        let mut device = Self {
            protection_domain,
            context,
            device,
            info: DeviceInfo {
                index,
                name,
                guid,
                ibdev_path,
                ..Default::default()
            },
        };
        device.update_attr(config)?;

        Ok(device)
    }

    fn update_attr(&mut self, config: &DeviceConfig) -> Result<()> {
        // 1. query device attr.
        let device_attr = self.context.query_device()?;

        let mut ports = vec![];
        for port_num in 1..=device_attr.phys_port_cnt {
            let port_attr = self.context.query_port(port_num)?;
            if port_attr.state != verbs::ibv_port_state::IBV_PORT_ACTIVE
                && config.skip_inactive_port
            {
                continue;
            }

            let mut gids = vec![];
            for gid_index in 0..port_attr.gid_tbl_len as u16 {
                if let Ok(gid) = self.context.query_gid(port_num, gid_index) {
                    let gid_type = self.context.query_gid_type(
                        port_num,
                        gid_index,
                        &self.info.ibdev_path,
                        &port_attr,
                    )?;
                    if !config.gid_type_filter.is_empty()
                        && !config.gid_type_filter.contains(&gid_type)
                    {
                        continue;
                    }

                    if config.roce_v2_skip_link_local_addr && gid_type == GidType::RoCEv2 {
                        let ip = gid.as_ipv6();
                        if ip.is_unicast_link_local() {
                            continue;
                        }
                    }

                    gids.push(Gid {
                        index: gid_index,
                        gid,
                        gid_type,
                    })
                }
            }

            ports.push(Port {
                port_num,
                port_attr,
                gids,
            });
        }

        self.info.device_attr = device_attr;
        self.info.ports = ports;

        Ok(())
    }

    pub(crate) fn device_ptr(&self) -> *mut verbs::ibv_device {
        self.device
    }

    pub(crate) fn context_ptr(&self) -> *mut verbs::ibv_context {
        self.context.0
    }

    pub(crate) fn pd_ptr(&self) -> *mut verbs::ibv_pd {
        self.protection_domain.0
    }

    pub fn index(&self) -> usize {
        self.info.index
    }

    pub fn info(&self) -> &DeviceInfo {
        &self.info
    }
}

impl std::fmt::Debug for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.info, f)
    }
}

/// A collection of RDMA devices available on the system.
#[derive(Clone)]
pub struct Devices(Vec<Arc<Device>>);

impl Devices {
    /// Returns a list of available RDMA devices.
    pub fn availables() -> Result<Devices> {
        Self::open(&Default::default())
    }

    /// Opens RDMA devices based on the provided configuration.
    pub fn open(config: &DeviceConfig) -> Result<Devices> {
        let list = RawDeviceList::available()?;
        let mut devices = Vec::with_capacity(list.len());
        for &device in list.iter() {
            let index = devices.len();
            let device = Device::open(device, index, config)?;
            if !config.device_filter.is_empty() && !config.device_filter.contains(&device.info.name)
            {
                continue;
            }

            devices.push(Arc::new(device));
        }
        if devices.is_empty() {
            Err(ErrorKind::IBDeviceNotFound.into())
        } else {
            Ok(Devices(devices))
        }
    }
}

impl Deref for Devices {
    type Target = [Arc<Device>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for &'a Devices {
    type Item = &'a Arc<Device>;
    type IntoIter = std::slice::Iter<'a, Arc<Device>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_devices() {
        let devices = Devices::availables().unwrap();
        assert!(!devices.is_empty());
        for device in &devices {
            println!("{:#?}", device);
        }
    }
}
