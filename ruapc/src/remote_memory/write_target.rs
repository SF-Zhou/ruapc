//! RPC-owned write destinations shared by a call and its reverse-RPC handlers.

#[cfg(feature = "rdma")]
use std::sync::Weak;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use ruapc_bufpool::RemoteBufferInfo;
#[cfg(feature = "rdma")]
use ruapc_memory::SpaceLayout;
use ruapc_memory::{BufferSet, CopyBatch};
#[cfg(feature = "rdma")]
use ruapc_rdma::ReadRecovery;
use thin_vec::ThinVec;

#[cfg(test)]
use crate::CopyOp;
#[cfg(feature = "rdma")]
use crate::rdma::RdmaSocket;
use crate::{Buffer, Error, ErrorKind, Result, Socket};

/// The owned destination shared by the original call and reverse-RPC handlers.
///
/// CPU copies borrow the complete set under one mutex. An RDMA READ moves the
/// set into its plan and leaves the receiver that returns it once DMA has
/// settled, whatever the transfer's result. Request expiry, cancellation and
/// ERR state never return buffers. The original call finally seals the target,
/// which revokes every handler still holding a reference.
#[derive(Debug)]
pub(crate) struct WriteTarget {
    destinations: Mutex<Destinations>,
    /// The advertised logical length remains available during a READ.
    total_len: u64,
}

#[derive(Debug)]
enum Destinations {
    Available(BufferSet),
    /// A READ plan on `socket` owns the set until `recovery` returns it.
    #[cfg(feature = "rdma")]
    Reading {
        recovery: ReadRecovery,
        socket: Weak<RdmaSocket>,
    },
    /// The original call took the set or its recovery; writers are revoked.
    Sealed,
}

impl Destinations {
    /// Borrows the set, first restoring one whose READ has settled.
    fn available(&mut self) -> Result<&mut BufferSet> {
        #[cfg(feature = "rdma")]
        if let Self::Reading { recovery, .. } = self
            && let Some(buffers) = recovery.try_recv()
        {
            *self = Self::Available(BufferSet::new(buffers));
        }
        match self {
            Self::Available(buffers) => Ok(buffers),
            _ => Err(Error::new(
                ErrorKind::BuffersInUse,
                "write target buffers are unavailable".into(),
            )),
        }
    }
}

impl WriteTarget {
    /// Installs a destination set; the request validates it before publication.
    pub fn new(buffers: BufferSet) -> Arc<Self> {
        Arc::new(Self {
            total_len: buffers.layout().total(),
            destinations: Mutex::new(Destinations::Available(buffers)),
        })
    }

    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Checks the attached destination space before the request publishes it.
    pub fn check_region_limit(&self) -> Result<()> {
        let mut destinations = self.destinations.lock().unwrap();
        Ok(destinations.available()?.layout().check_region_limit()?)
    }

    /// Exports available destinations for the selected transport while locked.
    pub fn export_regions(&self, socket: &Socket) -> Result<ThinVec<RemoteBufferInfo>> {
        let mut destinations = self.destinations.lock().unwrap();
        socket.export_regions(destinations.available()?.buffers())
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
        let mut destinations = self.destinations.lock().unwrap();
        Ok(destinations.available()?.scatter(batch, bytes)?)
    }

    #[cfg(test)]
    pub fn copy_in(&self, dst_offset: u64, data: &[u8]) -> Result<()> {
        let operations = [CopyOp::new(0, dst_offset, data.len() as u64)];
        self.copy_inline(&CopyBatch::new(&operations)?, data)
    }

    /// Plans a READ against the available set's layout without taking it.
    ///
    /// The layout never changes: a READ always returns the original vector.
    /// A plan therefore stays valid until [`Self::start_read`] takes the set,
    /// which fails if a competing READ or the sealing call got there first.
    #[cfg(feature = "rdma")]
    pub fn plan_read<T>(&self, plan: impl FnOnce(&SpaceLayout) -> Result<T>) -> Result<T> {
        let mut destinations = self.destinations.lock().unwrap();
        plan(destinations.available()?.layout())
    }

    /// Moves the whole set into a READ plan under one lock, excluding CPU
    /// copies and competing READs until its recovery returns the set.
    ///
    /// `prepare` is synchronous. Its error returns every buffer, which leaves
    /// the target available. A prepared plan leaves `recovery` here, so the
    /// set comes back after settlement even if the reading handler is gone.
    /// `socket` is the connection to close when the original call stops
    /// waiting for that READ.
    #[cfg(feature = "rdma")]
    pub fn start_read<T>(
        &self,
        socket: Weak<RdmaSocket>,
        prepare: impl FnOnce(BufferSet) -> std::result::Result<(T, ReadRecovery), (Error, Vec<Buffer>)>,
    ) -> Result<T> {
        let mut destinations = self.destinations.lock().unwrap();
        destinations.available()?;
        let Destinations::Available(buffers) =
            std::mem::replace(&mut *destinations, Destinations::Sealed)
        else {
            unreachable!("the set was available under this lock");
        };
        match prepare(buffers) {
            Ok((prepared, recovery)) => {
                *destinations = Destinations::Reading { recovery, socket };
                Ok(prepared)
            }
            Err((error, buffers)) => {
                *destinations = Destinations::Available(BufferSet::new(buffers));
                Err(error)
            }
        }
    }

    /// Revokes every writer for good and returns the complete original set.
    ///
    /// A set held by a READ is unavailable without a grace period. With one,
    /// this waits for the READ to settle, closes its connection if the grace
    /// expires first, and then keeps waiting without a deadline: neither the
    /// timer nor ERR state proves that DMA has stopped. Waiting holds only a
    /// weak connection reference, so it cannot delay QP destruction.
    /// Cancellation leaves the set with the READ, which then releases it.
    #[cfg_attr(not(feature = "rdma"), allow(unused_variables))]
    pub async fn seal(&self, grace: Option<Duration>) -> Option<Vec<Buffer>> {
        let taken = std::mem::replace(
            &mut *self.destinations.lock().unwrap(),
            Destinations::Sealed,
        );
        match taken {
            Destinations::Available(buffers) => Some(buffers.into_buffers()),
            #[cfg(feature = "rdma")]
            Destinations::Reading {
                mut recovery,
                socket,
            } => {
                // The recovery is checked only here, after the target is
                // sealed: a READ that has settled by now returns the set with
                // or without a grace period, and no other access can race it.
                let Some(grace) = grace else {
                    return recovery.try_recv();
                };
                if let Ok(buffers) = tokio::time::timeout(grace, &mut recovery).await {
                    return Some(buffers);
                }
                if let Some(socket) = socket.upgrade() {
                    socket.set_error();
                }
                Some(recovery.await)
            }
            Destinations::Sealed => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BufferPool;

    fn new_target(pool: &Arc<BufferPool>, contents: &[&[u8]]) -> Arc<WriteTarget> {
        let buffers = contents
            .iter()
            .map(|bytes| {
                let mut buffer = pool.allocate(bytes.len()).unwrap();
                buffer.set_len(bytes.len());
                buffer.copy_from_slice(bytes);
                buffer
            })
            .collect();
        WriteTarget::new(BufferSet::new(buffers))
    }

    fn pool() -> Arc<BufferPool> {
        ruapc_bufpool::BufferPoolBuilder::new(Vec::new()).build()
    }

    #[tokio::test]
    async fn copy_in_spans_buffers() {
        let target = new_target(&pool(), &[b"....", b"...."]);
        assert_eq!(target.total_len(), 8);
        target.copy_in(2, b"wxyz").unwrap();
        let buffers = target.seal(None).await.unwrap();
        assert_eq!(buffers[0].as_slice(), b"..wx");
        assert_eq!(buffers[1].as_slice(), b"yz..");
    }

    #[tokio::test]
    async fn sealing_returns_the_set_and_revokes_stale_handlers() {
        let target = new_target(&pool(), &[b"keep"]);
        let stale_handler = target.clone();
        let buffers = target.seal(Some(Duration::ZERO)).await.unwrap();
        assert_eq!(
            stale_handler.copy_in(0, b"late").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        #[cfg(feature = "rdma")]
        {
            // A READ planned before the seal cannot take the set afterwards.
            assert_eq!(
                stale_handler
                    .plan_read::<()>(|_| panic!("sealed targets plan no READ"))
                    .unwrap_err()
                    .kind,
                ErrorKind::BuffersInUse
            );
            assert_eq!(
                stale_handler
                    .start_read::<()>(Weak::new(), |_| panic!("sealed targets start no READ"))
                    .unwrap_err()
                    .kind,
                ErrorKind::BuffersInUse
            );
        }
        assert_eq!(buffers[0].as_slice(), b"keep");
        assert!(stale_handler.seal(None).await.is_none());
    }

    #[tokio::test]
    async fn sealing_preserves_an_attached_empty_set() {
        for grace in [None, Some(Duration::ZERO)] {
            let target = WriteTarget::new(BufferSet::new(Vec::new()));
            assert!(target.seal(grace).await.unwrap().is_empty());
        }
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn failed_planning_or_preparation_preserves_the_complete_target() {
        let target = new_target(&pool(), &[b"keep", b"both"]);
        let error = target
            .plan_read::<()>(|layout| {
                assert_eq!(layout.total(), 8);
                Err(Error::kind(ErrorKind::InvalidCopyOp))
            })
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
        let error = target
            .start_read::<()>(Weak::new(), |destinations| {
                assert_eq!(destinations.layout().total(), 8);
                Err((
                    Error::kind(ErrorKind::InvalidCopyOp),
                    destinations.into_buffers(),
                ))
            })
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
        target.copy_in(0, b"kept").unwrap();
        let buffers = target.seal(None).await.unwrap();
        assert_eq!(buffers[0].as_slice(), b"kept");
        assert_eq!(buffers[1].as_slice(), b"both");
    }

    #[cfg(feature = "rdma")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_owns_the_set_until_it_settles() {
        use crate::rdma::test_utils::RdmaConnection;

        let connection = RdmaConnection::establish().await;
        let pool = &connection.context.state.buffer_pool;

        // The plan excludes CPU copies and competing READs, then returns the
        // set to the target without help from the handler that started it.
        let target = new_target(pool, &[b"keep"]);
        let posting = connection.hold(&target);
        assert_eq!(
            target.copy_in(0, b"busy").unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        assert_eq!(
            target
                .plan_read::<()>(|_| panic!("busy targets plan no READ"))
                .unwrap_err()
                .kind,
            ErrorKind::BuffersInUse
        );
        assert_eq!(
            target
                .start_read::<()>(Weak::new(), |_| panic!("busy targets start no READ"))
                .unwrap_err()
                .kind,
            ErrorKind::BuffersInUse
        );
        drop(posting);
        target.copy_in(0, b"next").unwrap();
        assert_eq!(target.seal(None).await.unwrap()[0].as_slice(), b"next");

        // Without a grace period, sealing does not wait for the READ, but
        // still returns a set whose READ has already settled.
        let target = new_target(pool, &[b"keep"]);
        let posting = connection.hold(&target);
        assert!(target.seal(None).await.is_none());
        drop(posting);
        let target = new_target(pool, &[b"keep"]);
        drop(connection.hold(&target));
        assert_eq!(target.seal(None).await.unwrap()[0].as_slice(), b"keep");

        // Settlement within the grace period leaves the connection usable.
        let target = new_target(pool, &[b"keep"]);
        let posting = connection.hold(&target);
        let mut sealing = Box::pin(target.seal(Some(Duration::from_secs(30))));
        assert!(futures_util::poll!(&mut sealing).is_pending());
        drop(posting);
        assert_eq!(sealing.await.unwrap()[0].as_slice(), b"keep");
        assert!(connection.socket.state.is_ok());

        // Expiry closes the connection, but only settlement returns the set.
        let target = new_target(pool, &[b"keep"]);
        let posting = connection.hold(&target);
        let mut sealing = Box::pin(target.seal(Some(Duration::ZERO)));
        assert!(futures_util::poll!(&mut sealing).is_pending());
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(futures_util::poll!(&mut sealing).is_pending());
        assert!(!connection.socket.state.is_ok());
        drop(posting);
        assert_eq!(sealing.await.unwrap()[0].as_slice(), b"keep");

        connection.shutdown().await;
    }
}
