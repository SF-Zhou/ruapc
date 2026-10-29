//! RPC-owned destination checkout around a checked buffer set.

use std::sync::{Arc, Mutex};

use ruapc_bufpool::{AsDeviceIndex, RemoteBufferInfo};
#[cfg(feature = "rdma")]
use ruapc_memory::SpaceLayout;
use ruapc_memory::{BufferSet, CopyBatch};

#[cfg(test)]
use crate::CopyOp;
use crate::{Buffer, Error, ErrorKind, Result};

/// The owned destination shared by the original waiter and reverse-RPC handlers.
///
/// CPU copies borrow the complete set under one mutex. RDMA checkout removes
/// the set and leaves the target unavailable until settled buffers return from
/// the dependency. Request expiry, cancellation and ERR state do not authorize
/// restoring buffers still owned by DMA. Failed transfers may leave no set to
/// recover, even after the QP eventually recycles its buffers.
#[derive(Debug)]
pub(crate) struct WriteTarget {
    buffers: Mutex<Option<BufferSet>>,
    /// The advertised logical length remains available during DMA checkout.
    total_len: u64,
}

impl WriteTarget {
    /// Installs an already checked destination set.
    pub fn new(buffers: BufferSet) -> Arc<Self> {
        let total_len = buffers.layout().total();
        Arc::new(Self {
            buffers: Mutex::new(Some(buffers)),
            total_len,
        })
    }

    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Exports actual available buffers for metadata on the selected device.
    pub fn export_regions(
        &self,
        device_index: &impl AsDeviceIndex,
    ) -> Result<Vec<RemoteBufferInfo>> {
        let buffers = self.buffers.lock().unwrap();
        buffers
            .as_ref()
            .ok_or_else(Self::busy)?
            .buffers()
            .iter()
            .map(|buffer| {
                buffer
                    .remote_buffer_info(device_index)
                    .map_err(|error| Error::new(ErrorKind::InvalidArgument, error.to_string()))
            })
            .collect()
    }

    /// Plans against the available set's layout, then removes the whole set
    /// under the same lock, excluding CPU copies and competing DMA. Planning
    /// is synchronous; an error leaves the original buffers available.
    #[cfg(feature = "rdma")]
    pub fn checkout<R>(
        &self,
        plan: impl FnOnce(&SpaceLayout) -> Result<R>,
    ) -> Result<(R, BufferSet)> {
        let mut slot = self.buffers.lock().unwrap();
        let buffers = slot.as_ref().ok_or_else(Self::busy)?;
        let planned = plan(buffers.layout())?;
        let buffers = slot
            .take()
            .expect("write target remains available while locked");
        Ok((planned, buffers))
    }

    /// Restores owned buffers returned by a settled transfer or a pre-post error.
    #[cfg(feature = "rdma")]
    pub fn restore(&self, buffers: BufferSet) {
        assert_eq!(
            buffers.layout().total(),
            self.total_len,
            "restored write target length changed"
        );
        let mut slot = self.buffers.lock().unwrap();
        assert!(slot.is_none(), "write target already contains buffers");
        *slot = Some(buffers);
    }

    fn busy() -> Error {
        Error::new(
            ErrorKind::BuffersInUse,
            "write target buffers are unavailable".into(),
        )
    }

    /// Validates destination bounds and packed length before any target access.
    /// A zero-byte batch succeeds after validation without borrowing DMA-owned
    /// buffers. Nonempty copies hold the mutex for the whole batch.
    pub fn copy_inline(&self, batch: &CopyBatch<'_>, bytes: &[u8]) -> Result<()> {
        batch.check_destination(self.total_len)?;
        if bytes.len() as u64 != batch.byte_count() {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!(
                    "inline payload contains {} bytes but copy operations describe {}",
                    bytes.len(),
                    batch.byte_count()
                ),
            ));
        }
        if batch.byte_count() == 0 {
            return Ok(());
        }
        let mut buffers = self.buffers.lock().unwrap();
        let buffers = buffers.as_mut().ok_or_else(Self::busy)?;
        Ok(buffers.scatter(batch, bytes)?)
    }

    #[cfg(test)]
    pub fn copy_in(&self, dst_offset: u64, data: &[u8]) -> Result<()> {
        let operations = [CopyOp::new(0, dst_offset, data.len() as u64)];
        self.copy_inline(&CopyBatch::new(&operations)?, data)
    }

    /// Returns available destinations only when no waiter or handler shares them.
    pub fn try_into_buffers(this: Arc<Self>) -> Option<Vec<Buffer>> {
        Arc::try_unwrap(this)
            .ok()
            .and_then(|target| target.buffers.into_inner().unwrap())
            .map(BufferSet::into_buffers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BufferPool, Devices};

    fn pool() -> Arc<BufferPool> {
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
        let target = WriteTarget::new(BufferSet::try_new(vec![a, b]).unwrap());
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
        let target = WriteTarget::new(BufferSet::try_new(vec![a]).unwrap());
        let clone = target.clone();
        assert!(WriteTarget::try_into_buffers(target).is_none());
        assert!(WriteTarget::try_into_buffers(clone).is_some());
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn failed_planning_preserves_the_complete_target() {
        let pool = pool();
        let mut a = pool.allocate(4).unwrap();
        let mut b = pool.allocate(4).unwrap();
        a.set_len(4);
        b.set_len(4);
        a.copy_from_slice(b"keep");
        b.copy_from_slice(b"both");
        let pointers = [a.as_ptr(), b.as_ptr()];
        let target = WriteTarget::new(BufferSet::try_new(vec![a, b]).unwrap());
        let error = target
            .checkout::<()>(|layout| {
                assert_eq!(layout.total(), 8);
                Err(Error::kind(ErrorKind::InvalidCopyOp))
            })
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
        let buffers = WriteTarget::try_into_buffers(target).unwrap();
        assert_eq!(buffers.len(), 2);
        assert_eq!(buffers[0].as_ptr(), pointers[0]);
        assert_eq!(buffers[1].as_ptr(), pointers[1]);
        assert_eq!(buffers[0].as_slice(), b"keep");
        assert_eq!(buffers[1].as_slice(), b"both");
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn dma_ownership_excludes_cpu_and_competing_writers() {
        let pool = pool();
        let mut buffer = pool.allocate(64 * 1024).unwrap();
        buffer.set_len(4);
        let target = WriteTarget::new(BufferSet::try_new(vec![buffer]).unwrap());
        let (planned_len, buffers) = target.checkout(|layout| Ok(layout.total())).unwrap();
        assert_eq!(planned_len, 4);
        assert_eq!(
            target.copy_in(0, b"data").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        assert_eq!(
            target
                .checkout::<()>(|_| panic!("busy targets must not invoke the planner"))
                .unwrap_err()
                .kind,
            ErrorKind::BuffersInUse
        );
        target.restore(buffers);
        target.copy_in(0, b"data").unwrap();
        let buffers = WriteTarget::try_into_buffers(target).unwrap();
        assert_eq!(buffers[0].as_slice(), b"data");
    }
}
