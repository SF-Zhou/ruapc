//! Client-side pre-pinned destination for server-initiated remote writes.

use std::sync::{Arc, Mutex};

use ruapc_bufpool::{AsDeviceIndex, RemoteBufferInfo};

use crate::{Buffer, Error, ErrorKind, Result, core::scatter::SpaceLayout};

/// The buffers a client hands over for the duration of one request so the
/// server can write into them (via reverse-RPC pull or inline push).
///
/// Shared as `Arc<WriteTarget>` between the pending request's waiter entry
/// and any in-flight `MemoryService::pull` / `push` handler: **whoever
/// still holds a clone keeps the memory alive**, so a request timing out
/// while a pull's RDMA READ is in flight can never hand the underlying
/// memory back to the pool early. The buffers materialize for the caller
/// only when the last clone is unwrapped ([`try_into_buffers`]); if that
/// fails (a handler still holds a clone), they simply drop back to the
/// pool once it finishes.
///
/// The logical write space is defined by each buffer's logical length at
/// construction time (the usual rule: spaces are concatenations of
/// `Buffer::len()`), and is immutable afterwards.
///
/// [`try_into_buffers`]: WriteTarget::try_into_buffers
#[derive(Debug)]
pub(crate) struct WriteTarget {
    /// The pinned buffers. Locked only for the brief moments a writer
    /// needs `&mut` access (TCP push memcpy) or address/key export; the
    /// RDMA pull path writes through the NIC and never takes `&mut`.
    buffers: Mutex<Vec<Buffer>>,
    /// Segment lengths frozen at construction.
    layout: SpaceLayout,
}

impl WriteTarget {
    /// Wraps the buffers, freezing their logical lengths as the write
    /// space definition.
    pub fn new(buffers: Vec<Buffer>) -> Result<Arc<Self>> {
        let layout = SpaceLayout::from_lens(buffers.iter().map(|b| b.len() as u64))?;
        Ok(Arc::new(Self {
            buffers: Mutex::new(buffers),
            layout,
        }))
    }

    /// Total length of the write space in bytes.
    pub fn total_len(&self) -> u64 {
        self.layout.total()
    }

    /// Layout of the write space (consumed by the RDMA read planner).
    #[cfg_attr(not(feature = "rdma"), allow(dead_code))]
    pub fn layout(&self) -> &SpaceLayout {
        &self.layout
    }

    /// Exports the per-segment region info for the given device, in
    /// segment order. Used by the client to advertise the write space in
    /// request metadata.
    pub fn export_regions(
        &self,
        device_index: &impl AsDeviceIndex,
    ) -> Result<Vec<RemoteBufferInfo>> {
        let buffers = self.buffers.lock().unwrap();
        buffers
            .iter()
            .map(|buf| {
                buf.remote_buffer_info(device_index)
                    .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))
            })
            .collect()
    }

    /// Exports each segment's `(base address, lkey)` for the given device,
    /// in segment order. Used by the RDMA pull path to build READ scatter
    /// lists; the caller must hold an `Arc<WriteTarget>` clone until the
    /// reads complete (the addresses stay valid exactly as long as the
    /// pin does).
    #[cfg_attr(not(feature = "rdma"), allow(dead_code))]
    pub fn export_sge_bases(&self, device_index: &impl AsDeviceIndex) -> Result<Vec<(u64, u32)>> {
        let buffers = self.buffers.lock().unwrap();
        buffers
            .iter()
            .map(|buf| {
                let lkey = buf
                    .memory_key(device_index)
                    .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?
                    .lkey;
                Ok((buf.as_ptr() as u64, lkey))
            })
            .collect()
    }

    /// Copies `data` into the write space at `dst_offset` (TCP push path).
    ///
    /// The range must have been validated against
    /// [`total_len`](Self::total_len) beforehand.
    pub fn copy_in(&self, dst_offset: u64, data: &[u8]) -> Result<()> {
        let mut buffers = self.buffers.lock().unwrap();
        let mut cursor = 0usize;
        self.layout
            .for_each_slice(dst_offset, data.len() as u64, |seg, off, slice| {
                let (off, slice) = (off as usize, slice as usize);
                buffers[seg][off..off + slice].copy_from_slice(&data[cursor..cursor + slice]);
                cursor += slice;
                Ok(())
            })
    }

    /// Recovers the buffers if this is the last live clone; otherwise the
    /// buffers stay pinned by the remaining clones and fall back to the
    /// pool when the last one drops.
    pub fn try_into_buffers(this: Arc<Self>) -> Option<Vec<Buffer>> {
        Arc::try_unwrap(this)
            .ok()
            .map(|target| target.buffers.into_inner().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Devices;

    fn pool() -> Arc<crate::BufferPool> {
        let devices = Arc::new(Devices::default());
        ruapc_bufpool::BufferPoolBuilder::new(devices).build()
    }

    #[test]
    fn test_copy_in_spans_buffers() {
        let pool = pool();
        let mut a = pool.allocate(64 * 1024).unwrap();
        let mut b = pool.allocate(64 * 1024).unwrap();
        a.set_len(4);
        b.set_len(4);
        let target = WriteTarget::new(vec![a, b]).unwrap();
        assert_eq!(target.total_len(), 8);
        target.copy_in(2, b"wxyz").unwrap();
        let buffers = WriteTarget::try_into_buffers(target).unwrap();
        assert_eq!(&buffers[0][2..4], b"wx");
        assert_eq!(&buffers[1][..2], b"yz");
    }

    #[test]
    fn test_try_into_buffers_requires_uniqueness() {
        let pool = pool();
        let mut a = pool.allocate(64 * 1024).unwrap();
        a.set_len(1);
        let target = WriteTarget::new(vec![a]).unwrap();
        let clone = target.clone();
        assert!(WriteTarget::try_into_buffers(target).is_none());
        assert!(WriteTarget::try_into_buffers(clone).is_some());
    }
}
