//! GID query facade. Backend selection happens only at this module boundary.

use std::{io, path::Path};

use super::Context;
use crate::{Error, ErrorKind, Gid, GidType, Result, ibv_gid, ibv_port_attr};

#[cfg_attr(feature = "ibv-query-gid-ex", path = "gid/native.rs")]
#[cfg_attr(not(feature = "ibv-query-gid-ex"), path = "gid/compat.rs")]
mod backend;

pub(super) use backend::GidQuery;

impl Context {
    /// Queries a nonempty GID.
    ///
    /// By default, tries the kernel ioctl before `ibv_query_gid`. Driver
    /// discovery failure or an unavailable ioctl enables the legacy fallback;
    /// other query errors are returned. With `ibv-query-gid-ex`, calls
    /// libibverbs' `ibv_query_gid_ex` directly through the C shim.
    pub fn query_gid(&self, port_num: u8, gid_index: u8) -> Result<ibv_gid> {
        self.gid_query.query_gid(self, port_num, gid_index)
    }

    /// Queries a GID type.
    ///
    /// By default, tries the kernel ioctl before sysfs. `ibdev_path` and
    /// `port_attr` are used only by that legacy fallback; native InfiniBand
    /// ports need no sysfs type file. With `ibv-query-gid-ex`, the type comes
    /// from libibverbs and both fallback arguments are unused.
    pub fn query_gid_type(
        &self,
        port_num: u8,
        gid_index: u8,
        ibdev_path: &Path,
        port_attr: &ibv_port_attr,
    ) -> Result<GidType> {
        self.gid_query
            .query_gid_type(self, port_num, gid_index, ibdev_path, port_attr)
    }

    /// Queries a nonempty GID and its type together.
    ///
    /// By default, both come from one ioctl. If driver discovery or the ioctl
    /// method is unavailable, uses `ibv_query_gid` and sysfs instead. Other
    /// ioctl errors (including empty entries) do not trigger a fallback.
    /// With `ibv-query-gid-ex`, both come from libibverbs and the fallback
    /// arguments are unused.
    ///
    /// This does not apply [`Gid::usable`] filtering.
    pub fn query_gid_entry(
        &self,
        port_num: u8,
        gid_index: u8,
        ibdev_path: &Path,
        port_attr: &ibv_port_attr,
    ) -> Result<Gid> {
        self.gid_query
            .query_gid_entry(self, port_num, gid_index, ibdev_path, port_attr)
    }

    /// Queries every nonempty GID of the device in one batch.
    ///
    /// Returns `(port_num, gid)` pairs covering all ports. `capacity` must
    /// bound them, otherwise the query fails. `None` means the backend cannot
    /// query the table; the caller should fall back to per-index queries.
    /// Uses a direct ioctl by default, or `ibv_query_gid_table` with the
    /// `ibv-query-gid-ex` feature. This does not apply [`Gid::usable`] filtering.
    pub(crate) fn query_gid_table(&self, capacity: usize) -> Result<Option<Vec<(u8, Gid)>>> {
        self.gid_query.query_gid_table(self, capacity)
    }
}

fn gid_table_entry(
    gid: ibv_gid,
    gid_index: u32,
    port_num: u32,
    gid_type: u32,
) -> Option<(u8, Gid)> {
    let port_num = u8::try_from(port_num).ok()?;
    let index = u8::try_from(gid_index).ok()?;
    if gid.is_null() {
        return None;
    }
    Some((
        port_num,
        Gid {
            index,
            gid,
            gid_type: gid_type_from_raw(gid_type),
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

fn gid_type_from_raw(gid_type: u32) -> GidType {
    // ib_uverbs_gid_type and ibv_gid_type share these values.
    match gid_type {
        0 => GidType::IB,
        1 => GidType::RoCEv1,
        2 => GidType::RoCEv2,
        other => GidType::Other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_entries_decode() {
        let gid = ibv_gid {
            raw: [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                0x77, 0x88,
            ],
        };
        assert!(gid_table_entry(ibv_gid::default(), 3, 1, 2).is_none());
        let (port_num, entry) = gid_table_entry(gid, 3, 1, 2).unwrap();
        assert_eq!(port_num, 1);
        assert_eq!(entry.index, 3);
        assert_eq!(entry.gid_type, GidType::RoCEv2);
        assert_eq!(entry.gid, gid);
        assert!(gid_table_entry(gid, 256, 1, 2).is_none());
        assert!(gid_table_entry(gid, 3, 256, 2).is_none());
        assert_eq!(
            gid_table_entry(gid, 3, 1, 123).unwrap().1.gid_type,
            GidType::Other("123".into())
        );
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
                    "GID table query unexpectedly unavailable"
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
