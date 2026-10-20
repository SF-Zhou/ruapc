//! GID queries with a direct uverbs ioctl and a legacy sysfs fallback.

use std::{
    io,
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use super::Context;
use crate::{Error, ErrorKind, Gid, GidType, LinkLayer, Result, ibv_gid, ibv_port_attr};

/// Cache discovery and unsupported-method results per opened context.
#[derive(Default)]
pub(super) struct GidQuery {
    driver: Mutex<DriverDiscovery>,
    unsupported: AtomicBool,
}

#[derive(Default)]
enum DriverDiscovery {
    #[default]
    Unknown,
    Available(u32),
    Unavailable,
    RetryAfter(Instant),
}

impl GidQuery {
    fn query<T>(
        &self,
        discover: impl FnOnce() -> io::Result<u32>,
        ioctl: impl FnOnce(u32) -> io::Result<T>,
    ) -> io::Result<Option<T>> {
        if self.unsupported.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let Some(driver_id) = self.driver_id(discover) else {
            return Ok(None);
        };
        match ioctl(driver_id) {
            Ok(entry) => Ok(Some(entry)),
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(libc::ENOTTY | libc::EPROTONOSUPPORT)
                ) =>
            {
                // Match rdma-core's execute_ioctl_fallback. In particular,
                // ENODATA and EINVAL are query failures, not missing support.
                self.unsupported.store(true, Ordering::Relaxed);
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    fn driver_id(&self, discover: impl FnOnce() -> io::Result<u32>) -> Option<u32> {
        let mut driver = self.driver.lock().unwrap_or_else(|err| err.into_inner());
        match *driver {
            DriverDiscovery::Available(id) => return Some(id),
            DriverDiscovery::Unavailable => return None,
            DriverDiscovery::RetryAfter(deadline) if Instant::now() < deadline => return None,
            _ => {}
        }
        match discover() {
            Ok(id) => {
                *driver = DriverDiscovery::Available(id);
                Some(id)
            }
            Err(err) => {
                // Without a safely discovered ID, use the legacy path.
                // Cache genuinely missing support, but let later refreshes
                // recover from interrupted dumps, timeouts or other failures.
                // The cooldown avoids repeating discovery for every GID slot.
                *driver = if matches!(
                    err.raw_os_error(),
                    Some(
                        libc::ENOSYS
                            | libc::ENOTTY
                            | libc::EOPNOTSUPP
                            | libc::EPROTONOSUPPORT
                            | libc::EAFNOSUPPORT
                    )
                ) {
                    DriverDiscovery::Unavailable
                } else {
                    DriverDiscovery::RetryAfter(Instant::now() + Duration::from_secs(1))
                };
                None
            }
        }
    }
}

impl Context {
    fn query_gid_ioctl(&self, port_num: u8, gid_index: u8) -> io::Result<Option<Gid>> {
        self.gid_query.query(
            || {
                let mut driver_id = 0;
                // SAFETY: the context remains open for this synchronous call;
                // the output points to a live u32.
                let ret = unsafe { crate::ruapc_ibv_query_gid_driver_id(self.ptr, &mut driver_id) };
                if ret == 0 {
                    Ok(driver_id)
                } else {
                    Err(io::Error::from_raw_os_error(ret))
                }
            },
            |driver_id| {
                let mut gid = ibv_gid::default();
                let mut gid_type = 0;
                // SAFETY: the context and both output buffers remain live
                // throughout the synchronous ioctl. The shim owns its UAPI
                // response buffer and only copies it out on success.
                let ret = unsafe {
                    crate::ruapc_ibv_query_gid_entry(
                        self.ptr,
                        driver_id,
                        port_num,
                        u32::from(gid_index),
                        &mut gid,
                        &mut gid_type,
                    )
                };
                if ret != 0 {
                    return Err(io::Error::from_raw_os_error(ret));
                }
                Ok(Gid {
                    index: gid_index,
                    gid,
                    gid_type: gid_type_from_uapi(gid_type),
                })
            },
        )
    }

    /// Queries a nonempty GID, trying the kernel ioctl before `ibv_query_gid`.
    ///
    /// Falls back when driver discovery or the ioctl method is unavailable.
    /// Other ioctl errors are returned without consulting sysfs.
    pub fn query_gid(&self, port_num: u8, gid_index: u8) -> Result<ibv_gid> {
        match self
            .query_gid_ioctl(port_num, gid_index)
            .map_err(|err| query_error(ErrorKind::IBQueryGidFail, err))?
        {
            Some(entry) => nonempty_gid(entry.gid),
            None => self.query_gid_legacy(port_num, gid_index),
        }
    }

    fn query_gid_legacy(&self, port_num: u8, gid_index: u8) -> Result<ibv_gid> {
        let mut gid = ibv_gid::default();
        let ret = unsafe {
            crate::ruapc_ibv_query_gid(self.ptr, port_num, i32::from(gid_index), &mut gid)
        };
        if ret != 0 {
            let err = if ret > 0 {
                io::Error::from_raw_os_error(ret)
            } else {
                io::Error::last_os_error()
            };
            return Err(query_error(ErrorKind::IBQueryGidFail, err));
        }
        nonempty_gid(gid)
    }

    /// Queries a GID type, trying the kernel ioctl before sysfs.
    ///
    /// `ibdev_path` and `port_attr` are used only by the legacy fallback.
    /// Native InfiniBand ports do not need a sysfs GID type file.
    pub fn query_gid_type(
        &self,
        port_num: u8,
        gid_index: u8,
        ibdev_path: &Path,
        port_attr: &ibv_port_attr,
    ) -> Result<GidType> {
        match self
            .query_gid_ioctl(port_num, gid_index)
            .map_err(|err| query_error(ErrorKind::IBQueryGidTypeFail, err))?
        {
            Some(entry) => Ok(entry.gid_type),
            None => query_gid_type_sysfs(port_num, gid_index, ibdev_path, port_attr.link_layer),
        }
    }

    /// Queries a nonempty GID and its type together.
    ///
    /// On supported kernels both come from one ioctl, without reading sysfs.
    /// If driver discovery or the ioctl method is unavailable, uses
    /// `ibv_query_gid` and the legacy sysfs type query instead. Other ioctl
    /// errors (including empty entries) do not trigger a fallback.
    ///
    /// This does not apply [`Gid::usable`] filtering.
    pub fn query_gid_entry(
        &self,
        port_num: u8,
        gid_index: u8,
        ibdev_path: &Path,
        port_attr: &ibv_port_attr,
    ) -> Result<Gid> {
        match self
            .query_gid_ioctl(port_num, gid_index)
            .map_err(|err| query_error(ErrorKind::IBQueryGidFail, err))?
        {
            Some(entry) => {
                nonempty_gid(entry.gid)?;
                Ok(entry)
            }
            None => Ok(Gid {
                index: gid_index,
                gid: self.query_gid_legacy(port_num, gid_index)?,
                gid_type: query_gid_type_sysfs(
                    port_num,
                    gid_index,
                    ibdev_path,
                    port_attr.link_layer,
                )?,
            }),
        }
    }
}

fn query_error(kind: ErrorKind, err: io::Error) -> Error {
    Error::new(kind, err.to_string())
}

fn nonempty_gid(gid: ibv_gid) -> Result<ibv_gid> {
    if gid.is_null() {
        Err(query_error(
            ErrorKind::IBQueryGidFail,
            io::Error::from_raw_os_error(libc::ENODATA),
        ))
    } else {
        Ok(gid)
    }
}

fn gid_type_from_uapi(gid_type: u32) -> GidType {
    // ib_uverbs_gid_type values, independent of the installed verbs.h.
    match gid_type {
        0 => GidType::IB,
        1 => GidType::RoCEv1,
        2 => GidType::RoCEv2,
        other => GidType::Other(other.to_string()),
    }
}

fn query_gid_type_sysfs(
    port_num: u8,
    gid_index: u8,
    ibdev_path: &Path,
    link_layer: LinkLayer,
) -> Result<GidType> {
    if link_layer == LinkLayer::InfiniBand {
        return Ok(GidType::IB);
    }
    let path = ibdev_path.join(format!("ports/{port_num}/gid_attrs/types/{gid_index}"));
    let content = std::fs::read_to_string(path)
        .map_err(|err| query_error(ErrorKind::IBQueryGidTypeFail, err))?;
    Ok(gid_type_from_sysfs(&content, link_layer))
}

fn gid_type_from_sysfs(content: &str, link_layer: LinkLayer) -> GidType {
    match content.trim() {
        "IB/RoCE v1" if link_layer == LinkLayer::InfiniBand => GidType::IB,
        "IB/RoCE v1" if link_layer == LinkLayer::Ethernet => GidType::RoCEv1,
        "RoCE v2" => GidType::RoCEv2,
        other => GidType::Other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ioctl_uses_discovered_driver_and_caches_it() {
        let query = GidQuery::default();
        assert_eq!(query.query(|| Ok(42), Ok).unwrap(), Some(42));
        assert_eq!(
            query.query(|| panic!("discovery repeated"), Ok).unwrap(),
            Some(42)
        );
    }

    #[test]
    fn unavailable_discovery_skips_ioctl() {
        let query = GidQuery::default();
        let result = query.query::<()>(
            || Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP)),
            |_| panic!("ioctl without driver ID"),
        );
        assert_eq!(result.unwrap(), None);
        assert_eq!(
            query
                .query::<()>(|| panic!("discovery repeated"), |_| panic!("ioctl"))
                .unwrap(),
            None
        );
    }

    #[test]
    fn transient_discovery_failure_can_recover_after_cooldown() {
        for errno in [libc::EINTR, libc::EAGAIN, libc::ETIMEDOUT, libc::ENOMEM] {
            let query = GidQuery::default();
            assert_eq!(
                query
                    .query::<()>(
                        || Err(io::Error::from_raw_os_error(errno)),
                        |_| panic!("ioctl without driver ID")
                    )
                    .unwrap(),
                None
            );
            assert_eq!(
                query
                    .query::<()>(|| panic!("discovery during cooldown"), |_| panic!("ioctl"))
                    .unwrap(),
                None
            );
            // Advance the retry deadline instead of sleeping in this test.
            let mut driver = query.driver.lock().unwrap();
            let DriverDiscovery::RetryAfter(deadline) = &mut *driver else {
                panic!("transient failure permanently cached");
            };
            *deadline = Instant::now();
            drop(driver);
            assert_eq!(query.query(|| Ok(42), Ok).unwrap(), Some(42));
        }
    }

    #[test]
    fn unsupported_ioctl_enables_and_caches_legacy_fallback() {
        for errno in [libc::ENOTTY, libc::EPROTONOSUPPORT] {
            let query = GidQuery::default();
            assert_eq!(
                query
                    .query::<()>(|| Ok(1), |_| Err(io::Error::from_raw_os_error(errno)))
                    .unwrap(),
                None
            );
            assert_eq!(
                query
                    .query::<()>(
                        || panic!("discovery"),
                        |_| panic!("unsupported ioctl repeated")
                    )
                    .unwrap(),
                None
            );
        }
    }

    #[test]
    fn query_errors_do_not_fall_back_or_disable_ioctl() {
        for errno in [
            libc::ENODATA,
            libc::EINVAL,
            libc::EIO,
            libc::EPERM,
            libc::EOPNOTSUPP,
            libc::EINTR,
        ] {
            let query = GidQuery::default();
            let err = query
                .query::<()>(|| Ok(1), |_| Err(io::Error::from_raw_os_error(errno)))
                .unwrap_err();
            assert_eq!(err.raw_os_error(), Some(errno));
            assert_eq!(query.query(|| panic!("discovery"), Ok).unwrap(), Some(1));
        }
    }

    #[test]
    fn kernel_and_sysfs_gid_types() {
        for (raw, text, link, expected) in [
            (0, "IB/RoCE v1\n", LinkLayer::InfiniBand, GidType::IB),
            (1, "IB/RoCE v1\n", LinkLayer::Ethernet, GidType::RoCEv1),
            (2, "RoCE v2\n", LinkLayer::Ethernet, GidType::RoCEv2),
        ] {
            assert_eq!(gid_type_from_uapi(raw), expected);
            assert_eq!(gid_type_from_sysfs(text, link), expected);
        }
        assert_eq!(gid_type_from_uapi(123), GidType::Other("123".into()));
        assert_eq!(
            gid_type_from_sysfs("new type\n", LinkLayer::Ethernet),
            GidType::Other("new type".into())
        );
        // A native IB port does not need gid_attrs, including on old kernels.
        assert_eq!(
            query_gid_type_sysfs(1, 0, Path::new("/dev/null"), LinkLayer::InfiniBand).unwrap(),
            GidType::IB
        );
        assert!(query_gid_type_sysfs(1, 0, Path::new("/dev/null"), LinkLayer::Ethernet).is_err());
    }

    #[test]
    fn ioctl_gid_entry_does_not_need_sysfs() {
        let dev = crate::test_utils::open_device();
        let ctx = dev.context();
        let mut checked = false;
        for port in &dev.info().ports {
            for gid in &port.gids {
                let Some(entry) = ctx.query_gid_ioctl(port.port_num, gid.index).unwrap() else {
                    // Hardware tests must also run on kernels without the method.
                    return;
                };
                assert_eq!(
                    entry.gid,
                    ctx.query_gid_legacy(port.port_num, gid.index).unwrap()
                );
                let combined = ctx
                    .query_gid_entry(
                        port.port_num,
                        gid.index,
                        Path::new("/dev/null"),
                        &port.port_attr,
                    )
                    .unwrap();
                assert_eq!(combined.gid, entry.gid);
                assert_eq!(combined.gid_type, entry.gid_type);
                assert_eq!(
                    ctx.query_gid_type(
                        port.port_num,
                        gid.index,
                        Path::new("/dev/null"),
                        &port.port_attr
                    )
                    .unwrap(),
                    entry.gid_type
                );
                assert_eq!(entry.gid_type, gid.gid_type);
                checked = true;
            }
        }
        assert!(checked, "no usable GID");
    }
}
