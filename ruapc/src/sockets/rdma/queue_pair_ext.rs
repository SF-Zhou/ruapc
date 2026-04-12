//! Extension methods for [`QueuePair`] that accept [`Buffer`] arguments.
//!
//! These convenience wrappers extract the raw address, length, and lkey
//! from a [`Buffer`] and delegate to the `_raw` methods on [`QueuePair`].

use ruapc_rdma::{QueuePair, verbs};

use crate::device::DevicesExt;
use crate::memory::{Buffer, BufferExt, MemoryKey};
use crate::{Error, ErrorKind, Result};

/// Extracts the RDMA lkey from a [`Buffer`] for the given QueuePair's device.
fn buffer_lkey(buf: &Buffer, qp: &QueuePair) -> Result<u32> {
    let device = buf
        .pool()
        .devices()
        .find_by_rdma_device(&qp.device)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                "QP device not found in buffer pool's device set".into(),
            )
        })?;
    match buf.memory_key(device)? {
        MemoryKey::Rdma { lkey, .. } => Ok(lkey),
        _ => Err(Error::new(
            ErrorKind::InvalidArgument,
            "buffer is not registered on an RDMA device".into(),
        )),
    }
}

/// Extension trait adding [`Buffer`]-aware methods to [`QueuePair`].
#[allow(dead_code)]
pub(crate) trait QueuePairExt {
    /// Posts a receive work request using the buffer's full capacity.
    fn recv(&self, wr_id: verbs::WRID, buf: &Buffer) -> Result<()>;

    /// Posts a send work request using the buffer's current `len`.
    fn send(&self, wr_id: verbs::WRID, buf: &Buffer) -> Result<()>;

    /// Issues an RDMA Read into `local_buf` from a remote address.
    fn rdma_read(
        &self,
        wr_id: verbs::WRID,
        local_buf: &Buffer,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<()>;

    /// Issues an RDMA Write from `local_buf` to a remote address.
    fn rdma_write(
        &self,
        wr_id: verbs::WRID,
        local_buf: &Buffer,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<()>;
}

impl QueuePairExt for QueuePair {
    fn recv(&self, wr_id: verbs::WRID, buf: &Buffer) -> Result<()> {
        let lkey = buffer_lkey(buf, self)?;
        self.recv_raw(wr_id, buf.as_ptr() as u64, buf.capacity() as u32, lkey)
            .map_err(Into::into)
    }

    fn send(&self, wr_id: verbs::WRID, buf: &Buffer) -> Result<()> {
        let lkey = buffer_lkey(buf, self)?;
        self.send_raw(wr_id, buf.as_ptr() as u64, buf.len() as u32, lkey)
            .map_err(Into::into)
    }

    fn rdma_read(
        &self,
        wr_id: verbs::WRID,
        local_buf: &Buffer,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<()> {
        let lkey = buffer_lkey(local_buf, self)?;
        self.rdma_read_raw(
            wr_id,
            local_buf.as_ptr() as u64,
            local_buf.capacity() as u32,
            lkey,
            remote_addr,
            rkey,
        )
        .map_err(Into::into)
    }

    fn rdma_write(
        &self,
        wr_id: verbs::WRID,
        local_buf: &Buffer,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<()> {
        let lkey = buffer_lkey(local_buf, self)?;
        self.rdma_write_raw(
            wr_id,
            local_buf.as_ptr() as u64,
            local_buf.len() as u32,
            lkey,
            remote_addr,
            rkey,
        )
        .map_err(Into::into)
    }
}
