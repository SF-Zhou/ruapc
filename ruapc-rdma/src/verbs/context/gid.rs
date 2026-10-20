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

/// Size of one `QUERY_GID_TABLE` response entry in the kernel UAPI.
const GID_ENTRY_SIZE: usize = 32;

/// `ib_uverbs_attr::len` is a `u16`, so one table response holds at most
/// `u16::MAX / GID_ENTRY_SIZE` entries.
const MAX_GID_TABLE_ENTRIES: usize = u16::MAX as usize / GID_ENTRY_SIZE;

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
    /// The kernel or device does not expose a driver ID.
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

    /// Whether falling back to the legacy queries is expected here.
    ///
    /// Tests distinguish a supported kernel that silently stopped using the
    /// ioctl from an old kernel or a driver without a driver ID.
    #[cfg(test)]
    fn fallback_is_expected(&self) -> bool {
        self.unsupported.load(Ordering::Relaxed)
            || matches!(
                *self.driver.lock().unwrap_or_else(|err| err.into_inner()),
                DriverDiscovery::Unavailable
            )
    }
}

/// A single entry of a `QUERY_GID_TABLE` response, in kernel UAPI layout.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct GidTableEntryRaw {
    gid: [u8; 16],
    gid_index: u32,
    port_num: u32,
    gid_type: u32,
    /// Present in the ABI but unused here.
    _netdev_ifindex: u32,
}

const _: () = assert!(std::mem::size_of::<GidTableEntryRaw>() == GID_ENTRY_SIZE);

impl Context {
    fn gid_driver_id(&self) -> io::Result<u32> {
        let mut driver_id = 0;
        // SAFETY: the context remains open for this synchronous call; the
        // output points to a live u32.
        let ret = unsafe { crate::ruapc_ibv_query_gid_driver_id(self.ptr, &mut driver_id) };
        if ret == 0 {
            Ok(driver_id)
        } else {
            Err(io::Error::from_raw_os_error(ret))
        }
    }

    /// Runs one GID ioctl with the cached driver ID.
    ///
    /// `Ok(None)` means the direct methods are unavailable.
    fn gid_ioctl<T>(&self, ioctl: impl FnOnce(u32) -> io::Result<T>) -> io::Result<Option<T>> {
        self.gid_query.query(|| self.gid_driver_id(), ioctl)
    }

    fn query_gid_ioctl(&self, port_num: u8, gid_index: u8) -> io::Result<Option<Gid>> {
        self.gid_ioctl(|driver_id| {
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
        })
    }

    /// Queries every nonempty GID of the device with one ioctl.
    ///
    /// Returns `(port_num, gid)` pairs covering all ports. `capacity` must
    /// bound them, otherwise the kernel rejects the request. `None` means the
    /// method is unavailable and the caller should fall back to per-index
    /// queries.
    ///
    /// This does not apply [`Gid::usable`] filtering.
    pub(crate) fn query_gid_table(&self, capacity: usize) -> Result<Option<Vec<(u8, Gid)>>> {
        self.query_gid_table_ioctl(capacity)
            .map_err(|err| query_error(ErrorKind::IBQueryGidFail, err))
    }

    fn query_gid_table_ioctl(&self, capacity: usize) -> io::Result<Option<Vec<(u8, Gid)>>> {
        let capacity = capacity.min(MAX_GID_TABLE_ENTRIES);
        if capacity == 0 {
            return Ok(Some(Vec::new()));
        }
        self.gid_ioctl(|driver_id| {
            let mut raw = vec![GidTableEntryRaw::default(); capacity];
            let mut num_entries = 0;
            // SAFETY: the context stays open for this synchronous call,
            // `raw` holds `capacity` writable entries, and the shim only
            // publishes `num_entries` on success.
            let ret = unsafe {
                crate::ruapc_ibv_query_gid_table(
                    self.ptr,
                    driver_id,
                    raw.as_mut_ptr().cast(),
                    capacity as u32,
                    &mut num_entries,
                )
            };
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
            let entries = raw
                .get(..num_entries as usize)
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EPROTO))?;
            Ok(entries.iter().filter_map(gid_table_entry).collect())
        })
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

fn gid_table_entry(raw: &GidTableEntryRaw) -> Option<(u8, Gid)> {
    let port_num = u8::try_from(raw.port_num).ok()?;
    let index = u8::try_from(raw.gid_index).ok()?;
    let gid = ibv_gid { raw: raw.gid };
    if gid.is_null() {
        return None;
    }
    Some((
        port_num,
        Gid {
            index,
            gid,
            gid_type: gid_type_from_uapi(raw.gid_type),
        },
    ))
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

    /// The raw table entry must decode to the same GID as the per-index query,
    /// including for entries the snapshot filters out.
    #[test]
    fn raw_table_entries_decode() {
        let mut raw = GidTableEntryRaw {
            gid_index: 3,
            port_num: 1,
            gid_type: 2,
            ..Default::default()
        };
        assert!(gid_table_entry(&raw).is_none(), "zero GID must be skipped");

        raw.gid = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        let (port_num, gid) = gid_table_entry(&raw).expect("nonempty entry");
        assert_eq!(port_num, 1);
        assert_eq!(gid.index, 3);
        assert_eq!(gid.gid_type, GidType::RoCEv2);
        assert_eq!(gid.gid.as_bits(), u128::from_be_bytes(raw.gid));

        raw.gid_index = 256;
        assert!(gid_table_entry(&raw).is_none(), "index must fit in u8");
        raw.gid_index = 3;
        raw.port_num = 256;
        assert!(gid_table_entry(&raw).is_none(), "port must fit in u8");
    }

    #[test]
    fn ioctl_gid_entry_does_not_need_sysfs() {
        let dev = crate::test_utils::open_device();
        let ctx = dev.context();
        let mut checked = false;
        for port in &dev.info().ports {
            for gid in &port.gids {
                let Some(entry) = ctx.query_gid_ioctl(port.port_num, gid.index).unwrap() else {
                    // Hardware tests must also run on kernels without the
                    // method, but on a supported kernel a silent downgrade to
                    // sysfs is a discovery or ioctl regression.
                    assert!(
                        ctx.gid_query.fallback_is_expected(),
                        "GID ioctl path silently fell back to the legacy query"
                    );
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

    /// The device-wide table must return the same usable entries as the
    /// per-index queries and the cached snapshot.
    #[test]
    fn gid_table_matches_per_index_queries() {
        let dev = crate::test_utils::open_device();
        let ctx = dev.context();
        let info = dev.info();
        let capacity = info
            .ports
            .iter()
            .map(|port| port.port_attr.gid_tbl_len.max(0) as usize)
            .fold(0usize, usize::saturating_add);

        let table = match ctx.query_gid_table(capacity) {
            Ok(Some(entries)) => entries,
            Ok(None) => {
                assert!(
                    ctx.gid_query.fallback_is_expected(),
                    "GID table ioctl path silently fell back to the legacy query"
                );
                return;
            }
            Err(err) => panic!("GID table query failed: {err}"),
        };

        let mut expected = Vec::new();
        for port in &info.ports {
            let len = port.port_attr.gid_tbl_len.clamp(0, 1 + u8::MAX as i32);
            for index in 0..len {
                let index = index as u8;
                let Ok(entry) =
                    ctx.query_gid_entry(port.port_num, index, &info.ibdev_path, &port.port_attr)
                else {
                    continue;
                };
                if let Some(gid) = Gid::usable(index, entry.gid, entry.gid_type) {
                    expected.push((port.port_num, gid.index, gid.gid.as_bits(), gid.gid_type));
                }
            }
        }

        let mut actual = table
            .iter()
            .filter_map(|(port_num, gid)| {
                let gid = Gid::usable(gid.index, gid.gid, gid.gid_type.clone())?;
                Some((*port_num, gid.index, gid.gid.as_bits(), gid.gid_type))
            })
            .collect::<Vec<_>>();

        actual.sort_by_key(|(port, index, ..)| (*port, *index));
        expected.sort_by_key(|(port, index, ..)| (*port, *index));
        assert_eq!(actual, expected);
    }
}
