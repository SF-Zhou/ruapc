//! RPC-owned destination checkout around a checked buffer set.

use std::sync::{Arc, Mutex};

use ruapc_bufpool::{RemoteBufferInfo, WriteBuffer};
#[cfg(feature = "rdma")]
use ruapc_memory::SpaceLayout;
use ruapc_memory::{BufferSet, CopyBatch};
use thin_vec::ThinVec;
use tokio::sync::Notify;

#[cfg(test)]
use crate::CopyOp;
#[cfg(feature = "rdma")]
use crate::rdma::RdmaSocket;
use crate::{Error, ErrorKind, Result, Socket};

/// Whether the caller can leave locally outstanding destinations to the runtime.
/// Only adapters that received actual ownership select `DetachOwned`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recovery {
    Wait,
    DetachOwned,
}

/// The owned destination shared by the original waiter and reverse-RPC handlers.
///
/// CPU copies borrow the complete set under one mutex. RDMA checkout removes
/// the set and leaves the target unavailable until settled buffers return from
/// the dependency. Request expiry, cancellation and ERR state do not authorize
/// restoring buffers still owned by DMA. Failed transfers restore the complete
/// original set after settlement, or reclaim it if an owning caller abandoned
/// recovery. Closing the target excludes new operations even through a retained
/// handle, without depending on Arc uniqueness.
#[derive(Debug)]
pub(crate) struct WriteTarget {
    buffers: Mutex<TargetState>,
    restored: Notify,
    /// The advertised logical length remains available during DMA checkout.
    total_len: u64,
    #[cfg(feature = "rdma")]
    recovery: Recovery,
}

#[derive(Debug)]
struct TargetState {
    storage: TargetStorage,
    access: TargetAccess,
    failure: Option<Error>,
}

#[derive(Debug)]
enum TargetStorage {
    Available(BufferSet<WriteBuffer>),
    #[cfg(feature = "rdma")]
    CheckedOut {
        socket: Option<Arc<RdmaSocket>>,
        recovery_deadline: Option<tokio::time::Instant>,
    },
    Taken,
}

#[derive(Debug, PartialEq, Eq)]
enum TargetAccess {
    Open,
    Closed,
    #[cfg(feature = "rdma")]
    Detached,
}

impl TargetState {
    /// Seal access and report whether local storage has settled. Detachment is
    /// permanent even after its late restoration has reclaimed the allocation.
    fn seal(&mut self) -> Result<bool> {
        #[cfg(feature = "rdma")]
        if self.access == TargetAccess::Detached {
            return Err(WriteTarget::recovery_timeout());
        }
        self.access = TargetAccess::Closed;
        match self.storage {
            TargetStorage::Available(_) | TargetStorage::Taken => Ok(true),
            #[cfg(feature = "rdma")]
            TargetStorage::CheckedOut { .. } => Ok(false),
        }
    }

    fn take(&mut self) -> Option<BufferSet<WriteBuffer>> {
        if !matches!(self.storage, TargetStorage::Available(_)) {
            return None;
        }
        let TargetStorage::Available(buffers) =
            std::mem::replace(&mut self.storage, TargetStorage::Taken)
        else {
            unreachable!("available storage was checked while locked");
        };
        Some(buffers)
    }
}

impl WriteTarget {
    #[cfg(test)]
    pub(crate) fn from_owned(buffers: BufferSet) -> Arc<Self> {
        Self::new_with_recovery(
            BufferSet::try_new(
                buffers
                    .into_buffers()
                    .into_iter()
                    .map(WriteBuffer::from)
                    .collect(),
            )
            .unwrap(),
            Recovery::Wait,
        )
    }

    /// Installs an already checked destination set.
    pub(crate) fn new_with_recovery(
        buffers: BufferSet<WriteBuffer>,
        _recovery: Recovery,
    ) -> Arc<Self> {
        let total_len = buffers.layout().total();
        Arc::new(Self {
            buffers: Mutex::new(TargetState {
                storage: TargetStorage::Available(buffers),
                access: TargetAccess::Open,
                failure: None,
            }),
            restored: Notify::new(),
            total_len,
            #[cfg(feature = "rdma")]
            recovery: _recovery,
        })
    }

    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Exports available destinations for the selected transport while locked.
    pub fn export_regions(&self, socket: &Socket) -> Result<ThinVec<RemoteBufferInfo>> {
        let buffers = self.buffers.lock().unwrap();
        if buffers.access != TargetAccess::Open {
            return Err(Self::busy());
        }
        let TargetStorage::Available(buffers) = &buffers.storage else {
            return Err(Self::busy());
        };
        socket.export_regions(buffers.buffers())
    }

    /// Plans against the available set's layout, then removes the whole set
    /// under the same lock, excluding CPU copies and competing DMA. Planning
    /// is synchronous; an error leaves the original buffers available.
    #[cfg(all(test, feature = "rdma"))]
    pub fn checkout<R>(
        &self,
        plan: impl FnOnce(&SpaceLayout) -> Result<R>,
    ) -> Result<(R, BufferSet<WriteBuffer>)> {
        self.checkout_on(None, plan)
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn checkout_rdma<R>(
        &self,
        socket: Arc<RdmaSocket>,
        plan: impl FnOnce(&SpaceLayout) -> Result<R>,
    ) -> Result<(R, BufferSet<WriteBuffer>)> {
        self.checkout_on(Some(socket), plan)
    }

    #[cfg(feature = "rdma")]
    fn checkout_on<R>(
        &self,
        socket: Option<Arc<RdmaSocket>>,
        plan: impl FnOnce(&SpaceLayout) -> Result<R>,
    ) -> Result<(R, BufferSet<WriteBuffer>)> {
        let mut slot = self.buffers.lock().unwrap();
        if slot.access != TargetAccess::Open {
            return Err(Self::busy());
        }
        let TargetStorage::Available(buffers) = &slot.storage else {
            return Err(Self::busy());
        };
        let planned = plan(buffers.layout())?;
        let checked_out = TargetStorage::CheckedOut {
            socket,
            recovery_deadline: None,
        };
        let TargetStorage::Available(buffers) = std::mem::replace(&mut slot.storage, checked_out)
        else {
            unreachable!("write target remains available while locked");
        };
        Ok((planned, buffers))
    }

    /// Restores owned buffers returned by a settled transfer or a pre-post error.
    #[cfg(all(test, feature = "rdma"))]
    pub fn restore(&self, buffers: BufferSet<WriteBuffer>) {
        self.restore_with_error(buffers, None);
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn restore_with_error(&self, buffers: BufferSet<WriteBuffer>, error: Option<Error>) {
        assert_eq!(
            buffers.layout().total(),
            self.total_len,
            "restored write target length changed"
        );
        let mut slot = self.buffers.lock().unwrap();
        assert!(
            matches!(slot.storage, TargetStorage::CheckedOut { .. }),
            "only checked-out storage can be restored"
        );
        // Late restoration after detachment reclaims the allocation; retained
        // target handles cannot take it later or postpone its pool return.
        let (storage, released) = if slot.access == TargetAccess::Detached {
            (TargetStorage::Taken, Some(buffers))
        } else {
            (TargetStorage::Available(buffers), None)
        };
        let checked_out = std::mem::replace(&mut slot.storage, storage);
        if slot.failure.is_none() {
            slot.failure = error;
        }
        drop(slot);
        // Socket and allocation destructors must run outside the target lock.
        drop(checked_out);
        drop(released);
        self.restored.notify_waiters();
    }

    pub(crate) fn failure(&self) -> Option<Error> {
        self.buffers.lock().unwrap().failure.clone()
    }

    /// Seal access before recovering. Existing synchronous CPU copies finish
    /// under the mutex; checked-out destinations return through `restore`.
    /// An old Arc can no longer start another copy after this point.
    pub(crate) async fn close_and_take(&self) -> Result<Option<Vec<WriteBuffer>>> {
        {
            let mut slot = self.buffers.lock().unwrap();
            if slot.seal()? {
                return Ok(slot.take().map(BufferSet::into_buffers));
            }
        }
        self.close().await?;
        Ok(self
            .buffers
            .lock()
            .unwrap()
            .take()
            .map(BufferSet::into_buffers))
    }

    pub(crate) async fn close(&self) -> Result<()> {
        // Most requests already have their destinations. Avoid registering a
        // notification waiter unless storage is actually checked out.
        if self.buffers.lock().unwrap().seal()? {
            return Ok(());
        }
        loop {
            let notified = self.restored.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            #[cfg(feature = "rdma")]
            let recovery;
            {
                let mut slot = self.buffers.lock().unwrap();
                // Recheck after registering to cover restoration between the
                // first observation and notification setup.
                if slot.seal()? {
                    return Ok(());
                }
                #[cfg(feature = "rdma")]
                {
                    let TargetStorage::CheckedOut {
                        socket,
                        recovery_deadline,
                    } = &mut slot.storage
                    else {
                        unreachable!("unsettled target storage is checked out");
                    };
                    recovery = socket.clone().map(|socket| {
                        // Retrying cancelled recovery preserves the deadline for
                        // this checkout instead of starting another grace.
                        let deadline = *recovery_deadline.get_or_insert_with(|| {
                            tokio::time::Instant::now() + socket.settle_grace()
                        });
                        (socket, deadline)
                    });
                }
            }
            #[cfg(feature = "rdma")]
            if let Some((socket, at)) = recovery {
                if tokio::time::timeout_at(at, notified.as_mut())
                    .await
                    .is_err()
                {
                    if self.recovery == Recovery::DetachOwned {
                        {
                            let mut slot = self.buffers.lock().unwrap();
                            // Prefer storage that was restored concurrently
                            // with the timer firing.
                            if slot.seal()? {
                                return Ok(());
                            }
                            slot.access = TargetAccess::Detached;
                        }
                        socket.set_error();
                        return Err(Self::recovery_timeout());
                    }
                    socket.close_and_wait().await;
                    // Completion processing authorizes restoration. The task
                    // delivering the buffers may not have run yet.
                    notified.await;
                }
                continue;
            }
            notified.await;
        }
    }

    #[cfg(feature = "rdma")]
    fn recovery_timeout() -> Error {
        Error::new(
            ErrorKind::BufferRecoveryTimeout,
            "owned destination recovery grace expired; buffers remain with the runtime".into(),
        )
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
        if buffers.access != TargetAccess::Open {
            return Err(Self::busy());
        }
        let TargetStorage::Available(buffers) = &mut buffers.storage else {
            return Err(Self::busy());
        };
        Ok(buffers.scatter(batch, bytes)?)
    }

    #[cfg(test)]
    pub fn copy_in(&self, dst_offset: u64, data: &[u8]) -> Result<()> {
        let operations = [CopyOp::new(0, dst_offset, data.len() as u64)];
        self.copy_inline(&CopyBatch::new(&operations)?, data)
    }

    /// Seals and returns an available target without waiting. Retained handles
    /// remain closed and cannot access recovered destinations.
    #[cfg(test)]
    pub fn try_into_buffers(this: Arc<Self>) -> Option<Vec<WriteBuffer>> {
        let mut slot = this.buffers.lock().unwrap();
        slot.seal().ok()?;
        slot.take().map(BufferSet::into_buffers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BufferPool;

    fn pool() -> Arc<BufferPool> {
        ruapc_bufpool::BufferPoolBuilder::new(Vec::new()).build()
    }

    #[test]
    fn test_copy_in_spans_buffers() {
        let pool = pool();
        let mut a = pool.allocate(64 * 1024).unwrap();
        let mut b = pool.allocate(64 * 1024).unwrap();
        a.set_len(4);
        b.set_len(4);
        let target = WriteTarget::from_owned(BufferSet::try_new(vec![a, b]).unwrap());
        assert_eq!(target.total_len(), 8);
        target.copy_in(2, b"wxyz").unwrap();
        let buffers = WriteTarget::try_into_buffers(target).unwrap();
        assert_eq!(&buffers[0][2..4], b"wx");
        assert_eq!(&buffers[1][..2], b"yz");
    }

    #[test]
    fn recovery_revokes_retained_handles() {
        let pool = pool();
        let mut a = pool.allocate(64 * 1024).unwrap();
        a.set_len(1);
        let target = WriteTarget::from_owned(BufferSet::try_new(vec![a]).unwrap());
        let clone = target.clone();
        assert!(WriteTarget::try_into_buffers(target).is_some());
        assert_eq!(
            clone.copy_in(0, b"x").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        assert!(WriteTarget::try_into_buffers(clone).is_none());
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn recovery_waits_for_original_destinations_and_survives_cancelled_wait() {
        let pool = pool();
        let buffer = pool.allocate(4).unwrap();
        let pointer = buffer.as_ptr();
        let target = WriteTarget::from_owned(BufferSet::try_new(vec![buffer]).unwrap());
        let (_, buffers) = target.checkout(|_| Ok(())).unwrap();
        let mut recovering = Box::pin(target.close_and_take());
        assert!(futures_util::poll!(recovering.as_mut()).is_pending());
        drop(recovering);
        assert_eq!(
            target.copy_in(0, b"x").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        target.restore(buffers);
        let recovered = target.close_and_take().await.unwrap().unwrap();
        assert_eq!(recovered[0].as_ptr(), pointer);
        assert!(target.close_and_take().await.unwrap().is_none());
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
        let target = WriteTarget::from_owned(BufferSet::try_new(vec![a, b]).unwrap());
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
        let target = WriteTarget::from_owned(BufferSet::try_new(vec![buffer]).unwrap());
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
