//! GID queries delegated to the installed libibverbs implementation.

use std::{io, path::Path};

use super::{Context, gid_table_entry, gid_type_from_raw, nonempty_gid, query_error};
use crate::{
    ErrorKind, Gid, GidType, Result, ibv_gid, ibv_gid_entry, ibv_port_attr, ruapc_ibv_query_gid_ex,
    ruapc_ibv_query_gid_table_native,
};

/// libibverbs owns discovery and compatibility handling for this backend.
#[derive(Default)]
pub(in super::super) struct GidQuery {}

impl GidQuery {
    fn query_entry(&self, ctx: &Context, port_num: u8, gid_index: u8) -> io::Result<Gid> {
        let mut entry = ibv_gid_entry::default();
        // SAFETY: the context stays open and the output remains writable for
        // this synchronous call. The C shim honors the installed header's ABI.
        let ret = unsafe {
            ruapc_ibv_query_gid_ex(
                ctx.as_ptr(),
                u32::from(port_num),
                u32::from(gid_index),
                &mut entry,
            )
        };
        // Unlike the table API, ibv_query_gid_ex returns a positive errno.
        if ret != 0 {
            return Err(io::Error::from_raw_os_error(ret));
        }
        Ok(Gid {
            index: gid_index,
            gid: entry.gid,
            gid_type: gid_type_from_raw(entry.gid_type),
        })
    }

    pub(super) fn query_gid(&self, ctx: &Context, port_num: u8, gid_index: u8) -> Result<ibv_gid> {
        let entry = self
            .query_entry(ctx, port_num, gid_index)
            .map_err(|err| query_error(ErrorKind::IBQueryGidFail, err))?;
        nonempty_gid(entry.gid)
    }

    pub(super) fn query_gid_type(
        &self,
        ctx: &Context,
        port_num: u8,
        gid_index: u8,
        _ibdev_path: &Path,
        _port_attr: &ibv_port_attr,
    ) -> Result<GidType> {
        self.query_entry(ctx, port_num, gid_index)
            .map(|entry| entry.gid_type)
            .map_err(|err| query_error(ErrorKind::IBQueryGidTypeFail, err))
    }

    pub(super) fn query_gid_entry(
        &self,
        ctx: &Context,
        port_num: u8,
        gid_index: u8,
        _ibdev_path: &Path,
        _port_attr: &ibv_port_attr,
    ) -> Result<Gid> {
        let entry = self
            .query_entry(ctx, port_num, gid_index)
            .map_err(|err| query_error(ErrorKind::IBQueryGidFail, err))?;
        nonempty_gid(entry.gid)?;
        Ok(entry)
    }

    pub(super) fn query_gid_table(
        &self,
        ctx: &Context,
        capacity: usize,
    ) -> Result<Option<Vec<(u8, Gid)>>> {
        if capacity == 0 {
            return Ok(Some(Vec::new()));
        }
        let mut raw = vec![ibv_gid_entry::default(); capacity];
        // SAFETY: the context stays open, and `raw` contains `capacity`
        // writable entries with the layout generated from the same header
        // used to compile the C shim. No outputs are read on failure.
        let count =
            unsafe { ruapc_ibv_query_gid_table_native(ctx.as_ptr(), raw.as_mut_ptr(), capacity) };
        if count < 0 {
            return Err(query_error(
                ErrorKind::IBQueryGidFail,
                io::Error::from_raw_os_error((-count) as i32),
            ));
        }
        let entries = raw.get(..count as usize).ok_or_else(|| {
            query_error(
                ErrorKind::IBQueryGidFail,
                io::Error::from_raw_os_error(libc::EPROTO),
            )
        })?;
        Ok(Some(
            entries
                .iter()
                .filter_map(|raw| {
                    gid_table_entry(raw.gid, raw.gid_index, raw.port_num, raw.gid_type)
                })
                .collect(),
        ))
    }

    #[cfg(test)]
    pub(super) fn fallback_is_expected(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::open_device;

    #[test]
    fn native_queries_ignore_sysfs_arguments() {
        let dev = open_device();
        let ctx = dev.context();
        let mut checked = false;
        for port in &dev.info().ports {
            for gid in &port.gids {
                let entry = ctx
                    .query_gid_entry(
                        port.port_num,
                        gid.index,
                        Path::new("/dev/null"),
                        &port.port_attr,
                    )
                    .unwrap();
                assert_eq!(entry.gid, gid.gid);
                assert_eq!(entry.gid_type, gid.gid_type);
                assert_eq!(ctx.query_gid(port.port_num, gid.index).unwrap(), gid.gid);
                assert_eq!(
                    ctx.query_gid_type(
                        port.port_num,
                        gid.index,
                        Path::new("/dev/null"),
                        &port.port_attr,
                    )
                    .unwrap(),
                    gid.gid_type
                );
                checked = true;
            }
        }
        assert!(checked, "no usable GID");
    }
}
