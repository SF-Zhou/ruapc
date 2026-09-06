//! Client-side pre-pinned destination for server-initiated remote writes.

use std::sync::{Arc, Mutex};

use ruapc_bufpool::{AsDeviceIndex, RemoteBufferInfo};

use crate::{Buffer, Error, ErrorKind, Result, remote_memory::scatter::SpaceLayout};

/// The buffers a client hands over for the duration of one request so the
/// server can write into them (via `read_into_target` or `write_inline`).
///
/// Shared as `Arc<WriteTarget>` between the pending request's waiter entry
/// and any in-flight `MemoryService::read_into_target` / `write_inline`
/// handler. CPU writers borrow the buffers under the mutex. An RDMA READ
/// moves them into the queue pair, leaving this target empty until completion;
/// competing writers cannot access the destination during DMA. The queue pair
/// retains the allocation through every completion, including timeout flushes.
/// Completed buffers are restored here and can be recovered only when the last
/// target clone is unwrapped ([`try_into_buffers`]). Failed in-flight transfers
/// leave recovery unavailable and release their memory after DMA completes.
///
/// The logical write space is defined by each buffer's logical length at
/// construction time (the usual rule: spaces are concatenations of
/// `Buffer::len()`), and is immutable afterwards.
///
/// [`try_into_buffers`]: WriteTarget::try_into_buffers
#[derive(Debug)]
pub(crate) struct WriteTarget {
    /// The pinned buffers. Locked only for the brief moments a writer
    /// needs CPU access or moves ownership into an RDMA operation. `None`
    /// excludes CPU access and competing DMA while the QP owns the buffers.
    buffers: Mutex<Option<Vec<Buffer>>>,
    /// Segment lengths frozen at construction.
    layout: SpaceLayout,
}

impl WriteTarget {
    /// Wraps the buffers, freezing their logical lengths as the write
    /// space definition.
    pub fn new(buffers: Vec<Buffer>) -> Result<Arc<Self>> {
        let layout = SpaceLayout::from_lens(buffers.iter().map(|b| b.len() as u64))?;
        Ok(Arc::new(Self {
            buffers: Mutex::new(Some(buffers)),
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
            .as_ref()
            .ok_or_else(Self::busy)?
            .iter()
            .map(|buf| {
                buf.remote_buffer_info(device_index)
                    .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))
            })
            .collect()
    }

    /// Moves the destination into the transport until all DMA has completed.
    /// CPU copies and competing transfers cannot access a checked-out target.
    #[cfg(feature = "rdma")]
    pub fn take_for_read(&self) -> Result<Vec<Buffer>> {
        self.buffers.lock().unwrap().take().ok_or_else(Self::busy)
    }

    #[cfg(feature = "rdma")]
    pub fn restore_after_read(&self, buffers: Vec<Buffer>) {
        let mut slot = self.buffers.lock().unwrap();
        assert!(slot.is_none(), "write target already contains buffers");
        *slot = Some(buffers);
    }

    fn busy() -> Error {
        Error::new(
            ErrorKind::BuffersInUse,
            "write target is owned by an in-flight transfer".into(),
        )
    }

    /// Copies `data` into the write space at `dst_offset` (`write_inline` path).
    ///
    /// The range must have been validated against
    /// [`total_len`](Self::total_len) beforehand.
    pub fn copy_in(&self, dst_offset: u64, data: &[u8]) -> Result<()> {
        let mut buffers = self.buffers.lock().unwrap();
        let buffers = buffers.as_mut().ok_or_else(Self::busy)?;
        let mut cursor = 0usize;
        self.layout
            .for_each_slice(dst_offset, data.len() as u64, |seg, off, slice| {
                let (off, slice) = (off as usize, slice as usize);
                buffers[seg][off..off + slice].copy_from_slice(&data[cursor..cursor + slice]);
                cursor += slice;
                Ok(())
            })
    }

    /// Recovers completed buffers when this is the last live clone. An active
    /// handler or a destination still owned by the queue pair prevents recovery.
    pub fn try_into_buffers(this: Arc<Self>) -> Option<Vec<Buffer>> {
        Arc::try_unwrap(this)
            .ok()
            .and_then(|target| target.buffers.into_inner().unwrap())
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

    #[cfg(feature = "rdma")]
    #[test]
    fn dma_ownership_excludes_cpu_and_competing_writers() {
        let pool = pool();
        let mut buffer = pool.allocate(64 * 1024).unwrap();
        buffer.set_len(4);
        let target = WriteTarget::new(vec![buffer]).unwrap();
        let buffers = target.take_for_read().unwrap();
        assert_eq!(
            target.copy_in(0, b"data").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        assert_eq!(
            target.take_for_read().unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        target.restore_after_read(buffers);
        target.copy_in(0, b"data").unwrap();
        let buffers = WriteTarget::try_into_buffers(target).unwrap();
        assert_eq!(buffers[0].as_slice(), b"data");
    }
}
