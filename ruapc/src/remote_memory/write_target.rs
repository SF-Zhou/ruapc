//! RPC-owned write destinations shared by a call and its reverse-RPC handlers.

#[cfg(feature = "rdma")]
use std::sync::Weak;
use std::{
    io::IoSliceMut,
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration,
};

use ruapc_bufpool::RemoteBufferInfo;
#[cfg(feature = "rdma")]
use ruapc_memory::SpaceLayout;
use ruapc_memory::{BufferSet, CopyBatch, RangeCursor};
#[cfg(feature = "rdma")]
use ruapc_rdma::ReadRecovery;
use thin_vec::ThinVec;

#[cfg(feature = "rdma")]
use crate::rdma::RdmaSocket;
use crate::{Buffer, Error, ErrorKind, Result, Socket, msg::frame::DataPlacement};

/// Most destination buffers that one direct read fills. A range that crosses
/// more is filled by several reads.
const DIRECT_READ_SLICES: usize = 16;

/// The owned destination shared by the original call and the peer's writers:
/// data frames on stream connections, reverse-RPC handlers on RDMA.
///
/// CPU copies and direct socket reads borrow the set under one mutex, for one
/// contiguous piece at a time. An RDMA READ moves the set into its plan and
/// leaves the receiver that returns it once DMA has settled, whatever the
/// transfer's result. Request expiry, cancellation and ERR state never return
/// buffers. The original call finally seals the target, which revokes every
/// writer still holding a reference.
#[derive(Debug)]
pub(crate) struct WriteTarget {
    state: Mutex<TargetState>,
    /// The advertised logical length remains available during a READ.
    total_len: u64,
}

/// Held by the call that owns these destinations. Cancelling the call revokes
/// writers even when a connection is still receiving an admitted frame.
pub(crate) struct WriteTargetOwner(Arc<WriteTarget>);

impl WriteTargetOwner {
    pub(crate) fn new(target: Arc<WriteTarget>) -> Self {
        Self(target)
    }
}

impl Deref for WriteTargetOwner {
    type Target = Arc<WriteTarget>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for WriteTargetOwner {
    fn drop(&mut self) {
        // Available buffers return to the pool. Dropping a recovery receiver
        // leaves the RDMA plan responsible for the buffers it still owns.
        drop(self.0.take());
    }
}

/// Everything writers change, under the target's one mutex.
#[derive(Debug)]
struct TargetState {
    destinations: Destinations,
    frames: FrameProgress,
}

/// What data frames have done to a target so far.
#[derive(Debug, Default)]
struct FrameProgress {
    /// For a reverse read, destinations in local copy order and the complete
    /// number of bytes owed. A normal write attachment accepts addressed frames.
    packed: Option<(RangeCursor, u64)>,
    received: u64,
    refusal: Option<Error>,
}

impl FrameProgress {
    fn admit(
        &mut self,
        placement: DataPlacement,
        len: usize,
        space_len: u64,
    ) -> Result<RangeCursor> {
        if let Some(first) = &self.refusal {
            return Err(first.clone());
        }
        let result = match (&mut self.packed, placement) {
            (Some((cursor, _)), DataPlacement::Packed) if len as u64 <= cursor.remaining() => {
                Ok(cursor.split_to(len as u64))
            }
            (Some((cursor, _)), DataPlacement::Packed) => Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!(
                    "packed frame carries {len} bytes with only {} bytes remaining",
                    cursor.remaining()
                ),
            )),
            (None, DataPlacement::Ranges { ranges, .. }) => {
                RangeCursor::new(ranges, space_len).map_err(Error::from)
            }
            (Some(_), DataPlacement::Ranges { .. }) => Err(Error::new(
                ErrorKind::InvalidCopyOp,
                "addressed write frame for a packed read".into(),
            )),
            (None, DataPlacement::Packed) => Err(Error::new(
                ErrorKind::InvalidCopyOp,
                "packed read frame for an addressed write target".into(),
            )),
        };
        if let Err(error) = &result {
            self.refusal = Some(error.clone());
        }
        result
    }
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
    /// Data frames may write any range within it.
    pub fn new(buffers: BufferSet) -> Arc<Self> {
        Self::with_frames(buffers, FrameProgress::default())
    }

    /// Installs a reverse read's local scatter plan. Only packed frames may
    /// advance it, and its destinations are never advertised to the peer.
    pub fn expecting(buffers: BufferSet, batch: &CopyBatch<'_>) -> Arc<Self> {
        let cursor = batch.destination_cursor();
        Self::with_frames(
            buffers,
            FrameProgress {
                packed: Some((cursor, batch.byte_count())),
                ..Default::default()
            },
        )
    }

    fn with_frames(buffers: BufferSet, frames: FrameProgress) -> Arc<Self> {
        Arc::new(Self {
            total_len: buffers.layout().total(),
            state: Mutex::new(TargetState {
                destinations: Destinations::Available(buffers),
                frames,
            }),
        })
    }

    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Checks the attached destination space before the request publishes it.
    pub fn check_region_limit(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        Ok(state
            .destinations
            .available()?
            .layout()
            .check_region_limit()?)
    }

    /// Exports available destinations for the selected transport while locked.
    pub fn export_regions(&self, socket: &Socket) -> Result<ThinVec<RemoteBufferInfo>> {
        let mut state = self.state.lock().unwrap();
        if state.frames.packed.is_some() {
            return Ok(ThinVec::new());
        }
        socket.export_regions(state.destinations.available()?.buffers())
    }

    /// Resolves a complete frame's destinations before any bytes are written.
    pub(super) fn admit(&self, placement: DataPlacement, len: usize) -> Result<RangeCursor> {
        let mut state = self.state.lock().unwrap();
        state.destinations.available()?;
        state.frames.admit(placement, len, self.total_len)
    }

    /// Copies one contiguous piece of a data frame to `offset`. Fails once
    /// the target is sealed, and for a piece outside it.
    pub fn push(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.destinations.available()?.write_at(offset, bytes)?;
        state.frames.received += bytes.len() as u64;
        Ok(())
    }

    /// Lets `fill` write straight into the start of the destination range
    /// `[offset, offset + len)` and returns its result: the number of bytes
    /// it wrote from the range's start, as a vectored socket read reports.
    ///
    /// `fill` gets the range's first [`DIRECT_READ_SLICES`] buffers at most,
    /// without any allocation, so it may be unable to fill the whole range.
    /// It runs under the mutex and must not block; the outer error means the
    /// target is sealed or the range lies outside it, and `fill` did not run.
    /// Bytes that `fill` reports are counted as received.
    pub fn push_with(
        &self,
        offset: u64,
        len: u64,
        fill: impl FnOnce(&mut [IoSliceMut<'_>]) -> std::io::Result<usize>,
    ) -> Result<std::io::Result<usize>> {
        let mut state = self.state.lock().unwrap();
        let TargetState {
            destinations,
            frames,
        } = &mut *state;
        let mut slices: [IoSliceMut<'_>; DIRECT_READ_SLICES] =
            std::array::from_fn(|_| IoSliceMut::new(&mut []));
        let mut count = 0;
        for (slot, slice) in slices
            .iter_mut()
            .zip(destinations.available()?.slices_mut(offset, len)?)
        {
            *slot = IoSliceMut::new(slice);
            count += 1;
        }
        let filled = fill(&mut slices[..count]);
        if let Ok(count) = filled {
            frames.received += count as u64;
        }
        Ok(filled)
    }

    /// Whether data frames delivered what this target was created for.
    ///
    /// Fails with the first refusal, if the target refused a frame. A target
    /// that [expects](Self::expecting) destinations also fails until frames
    /// have written every one of them completely: an admitted frame whose
    /// bytes did not all arrive does not count.
    pub fn delivery(&self) -> Result<()> {
        let state = self.state.lock().unwrap();
        let frames = &state.frames;
        if let Some(refusal) = &frames.refusal {
            return Err(refusal.clone());
        }
        match &frames.packed {
            Some((_, total)) if frames.received != *total => Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!(
                    "data frames delivered {} of {} bytes to the requested destinations",
                    frames.received, total
                ),
            )),
            _ => Ok(()),
        }
    }

    /// Total bytes that data frames have written into this target. Sealing
    /// stops the count.
    #[cfg(test)]
    pub fn received(&self) -> u64 {
        self.state.lock().unwrap().frames.received
    }

    #[cfg(test)]
    pub fn copy_in(&self, dst_offset: u64, data: &[u8]) -> Result<()> {
        self.push(dst_offset, data)
    }

    /// Plans a READ against the available set's layout without taking it.
    ///
    /// The layout never changes: a READ always returns the original vector.
    /// A plan therefore stays valid until [`Self::start_read`] takes the set,
    /// which fails if a competing READ or the sealing call got there first.
    #[cfg(feature = "rdma")]
    pub fn plan_read<T>(&self, plan: impl FnOnce(&SpaceLayout) -> Result<T>) -> Result<T> {
        let mut state = self.state.lock().unwrap();
        plan(state.destinations.available()?.layout())
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
        let mut state = self.state.lock().unwrap();
        let destinations = &mut state.destinations;
        destinations.available()?;
        let Destinations::Available(buffers) =
            std::mem::replace(destinations, Destinations::Sealed)
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
        // A READ that has settled by now returns the set with or without a
        // grace period; once sealed, no other access can race its recovery.
        let Some(grace) = grace else {
            return self.seal_now();
        };
        match self.take() {
            Destinations::Available(buffers) => Some(buffers.into_buffers()),
            #[cfg(feature = "rdma")]
            Destinations::Reading {
                mut recovery,
                socket,
            } => {
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

    /// Seals without waiting, as [`Self::seal`] does without a grace period.
    pub fn seal_now(&self) -> Option<Vec<Buffer>> {
        match self.take() {
            Destinations::Available(buffers) => Some(buffers.into_buffers()),
            #[cfg(feature = "rdma")]
            Destinations::Reading { mut recovery, .. } => recovery.try_recv(),
            Destinations::Sealed => None,
        }
    }

    fn take(&self) -> Destinations {
        std::mem::replace(
            &mut self.state.lock().unwrap().destinations,
            Destinations::Sealed,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BufferPool;

    fn filled(pool: &Arc<BufferPool>, bytes: &[u8]) -> Buffer {
        let mut buffer = pool.allocate(bytes.len()).unwrap();
        buffer.set_len(bytes.len());
        buffer.copy_from_slice(bytes);
        buffer
    }

    fn new_target(pool: &Arc<BufferPool>, contents: &[&[u8]]) -> Arc<WriteTarget> {
        let buffers = contents.iter().map(|bytes| filled(pool, bytes)).collect();
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

    fn write_range(offset: u64, len: u64) -> DataPlacement {
        DataPlacement::Ranges {
            ranges: vec![(offset, len)],
            ack: None,
        }
    }

    #[test]
    fn packed_frame_admission_uses_local_destinations_and_counts_actual_bytes() {
        use crate::CopyOp;
        let ops = [CopyOp::new(0, 4, 2), CopyOp::new(0, 0, 2)];
        let pool = pool();
        let target = WriteTarget::expecting(
            BufferSet::new(vec![filled(&pool, b"........")]),
            &CopyBatch::new(&ops).unwrap(),
        );
        let mut first = target.admit(DataPlacement::Packed, 3).unwrap();
        assert_eq!(first.next_span(u64::MAX), Some((4, 2)));
        assert_eq!(first.next_span(u64::MAX), Some((0, 1)));
        let last = target.admit(DataPlacement::Packed, 1).unwrap();
        assert_eq!(last.front(), Some((1, 1)));
        assert!(target.delivery().is_err());
        target.push(4, b"cd").unwrap();
        target.push(0, b"ab").unwrap();
        target.delivery().unwrap();
        assert_eq!(target.seal_now().unwrap()[0].as_slice(), b"ab..cd..");
    }

    #[test]
    fn packed_frame_for_normal_write_attachment_refuses_later_writes() {
        let target = new_target(&pool(), &[b"...."]);
        let first = target.admit(DataPlacement::Packed, 1).unwrap_err();
        assert_eq!(first.kind, ErrorKind::InvalidCopyOp);
        assert_eq!(target.admit(write_range(0, 1), 1).unwrap_err(), first);
    }

    /// A range outside the write space refuses any target for good, and a
    /// frame for an unavailable target is refused without making that stick.
    #[tokio::test]
    async fn out_of_bounds_frames_refuse_the_target_and_sealing_does_not() {
        let target = new_target(&pool(), &[b"...."]);
        for (offset, len) in [(3, 2), (4, 1), (u64::MAX, 2)] {
            let refusal = target
                .admit(write_range(offset, len), len as usize)
                .unwrap_err();
            assert_eq!(refusal.kind, ErrorKind::InvalidCopyOp);
        }
        let first = target.delivery().unwrap_err();
        assert!(first.msg.contains("offset 3 with length 2"), "{first}");
        assert_eq!(
            target.admit(write_range(0, 1), 1).unwrap_err().kind,
            ErrorKind::InvalidCopyOp
        );

        let target = new_target(&pool(), &[b"...."]);
        target.seal(None).await.unwrap();
        assert_eq!(
            target.admit(write_range(0, 1), 1).unwrap_err().kind,
            ErrorKind::BuffersInUse
        );
        target.delivery().unwrap();
    }

    /// A direct read gets the start of its range without allocating: as many
    /// buffers as fit the fixed slice array, and the rest in later reads.
    #[test]
    fn direct_reads_get_a_bounded_number_of_buffers() {
        let contents = vec![&b"ab"[..]; DIRECT_READ_SLICES + 3];
        let target = new_target(&pool(), &contents);
        let total = target.total_len();
        let mut offset = 1;
        let mut reads = Vec::new();
        while offset < total {
            let read = target
                .push_with(offset, total - offset, |slices| {
                    let mut count = 0;
                    for slice in slices.iter_mut() {
                        slice.fill(b'x');
                        count += slice.len();
                    }
                    reads.push(slices.len());
                    Ok(count)
                })
                .unwrap()
                .unwrap();
            offset += read as u64;
        }
        assert_eq!(reads, [DIRECT_READ_SLICES, 3]);
        assert_eq!(target.received(), total - 1);
        // A failed read counts nothing, and an empty range offers no buffer.
        let failed = target
            .push_with(0, 1, |_| Err(std::io::ErrorKind::WouldBlock.into()))
            .unwrap();
        assert!(failed.is_err());
        let empty = target.push_with(total, 0, |slices| Ok(slices.len()));
        assert_eq!(empty.unwrap().unwrap(), 0);
        assert_eq!(target.received(), total - 1);
        assert_eq!(
            target
                .push_with(total, 1, |_| unreachable!())
                .unwrap_err()
                .kind,
            ErrorKind::InvalidCopyOp
        );
        let buffers = target.seal_now().unwrap();
        assert_eq!(buffers[0].as_slice(), b"ax");
        assert!(buffers[1..].iter().all(|buffer| buffer.as_slice() == b"xx"));
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
