//! Immutable, owned source buffers shared by a client and its pending reads.

use std::{convert::Infallible, ops::Range, sync::Arc};

use ruapc_bufpool::Buffer;

use crate::{BufferSet, BufferSetError, CopyBatch, MemoryResult, SpaceLayout};

/// Owned immutable source buffers, shareable through an `Arc`.
///
/// Local shared ownership protects CPU readers. It is not evidence that a
/// remote DMA operation has completed or that a request remains valid.
#[derive(Debug)]
pub struct ReadSource {
    buffers: BufferSet,
}

/// An owned immutable view of one contiguous range of a [`ReadSource`].
///
/// The view retains the complete source, so its bytes remain valid even after
/// the original source owner is dropped. Source buffers cannot be recovered
/// while any view remains alive. Views are constructed only by
/// [`ReadSource::slices`], which checks the entire batch before creating them.
/// This ownership protects CPU access; it is not remote DMA completion evidence.
#[derive(Debug)]
pub struct ReadSlice {
    source: Arc<ReadSource>,
    segment: usize,
    range: Range<usize>,
}

impl AsRef<[u8]> for ReadSlice {
    fn as_ref(&self) -> &[u8] {
        &self.source.buffers()[self.segment][self.range.clone()]
    }
}

impl From<BufferSet> for ReadSource {
    /// Makes an already checked buffer set immutable without rebuilding its layout.
    fn from(buffers: BufferSet) -> Self {
        Self { buffers }
    }
}

impl ReadSource {
    /// Checks and caches the source layout, retaining all buffers on failure.
    pub fn try_new(buffers: Vec<Buffer>) -> Result<Self, BufferSetError> {
        BufferSet::try_new(buffers).map(Self::from)
    }

    /// Returns an immutable view of the source buffers.
    #[must_use]
    pub fn buffers(&self) -> &[Buffer] {
        self.buffers.buffers()
    }

    /// Returns the immutable cached source layout.
    #[must_use]
    pub fn layout(&self) -> &SpaceLayout {
        self.buffers.layout()
    }

    /// Recovers local ownership of all source buffers.
    #[must_use]
    pub fn into_buffers(self) -> Vec<Buffer> {
        self.buffers.into_buffers()
    }

    /// Gathers source ranges into bytes, concatenated in operation order.
    ///
    /// Checks the batch's source bounds against the cached layout. Destination
    /// offsets do not determine output order. No peer-provided address or
    /// registration key can select a different allocation.
    pub fn gather(&self, batch: &CopyBatch<'_>) -> MemoryResult<Vec<u8>> {
        self.buffers.gather(batch)
    }

    /// Returns owned source slices in operation order without copying bytes.
    ///
    /// All source ranges are checked against the cached layout first. Each
    /// operation is then split at buffer boundaries; zero-length operations
    /// produce no views. Destination offsets do not determine output order,
    /// and overlapping source ranges may appear more than once.
    ///
    /// Each returned slice shares ownership of this immutable source. Recovery
    /// through [`Arc::try_unwrap`] remains unavailable until every slice and
    /// other source owner has been dropped.
    pub fn slices(self: &Arc<Self>, batch: &CopyBatch<'_>) -> MemoryResult<Vec<ReadSlice>> {
        let layout = self.layout();
        batch.check_source(layout.total())?;
        let mut slices = Vec::with_capacity(batch.ops().len());
        for op in batch.ops() {
            layout
                .for_each_slice_in_bounds::<Infallible>(
                    op.src_offset,
                    op.len,
                    |segment, offset, len| {
                        // The layout was derived from these actual buffers,
                        // so each segment-local bound fits usize.
                        let (offset, len) = (offset as usize, len as usize);
                        slices.push(ReadSlice {
                            source: self.clone(),
                            segment,
                            range: offset..offset + len,
                        });
                        Ok(())
                    },
                )
                .unwrap();
        }
        Ok(slices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CopyOp, MemoryErrorKind};

    fn source(parts: &[&[u8]]) -> Arc<ReadSource> {
        let pool = ruapc_bufpool::BufferPool::new(Arc::new(ruapc_bufpool::EmptyDevices));
        let buffers = parts
            .iter()
            .map(|bytes| {
                let mut buffer = pool.allocate(bytes.len().max(1)).unwrap();
                buffer.set_len(bytes.len());
                buffer.copy_from_slice(bytes);
                buffer
            })
            .collect();
        Arc::new(ReadSource::try_new(buffers).unwrap())
    }

    #[test]
    fn slices_preserve_storage_operation_order_and_repeated_source_ranges() {
        let source = source(&[b"abcd", b"", b"efgh"]);
        let operations = [
            CopyOp::new(2, 10, 4),
            CopyOp::new(0, 0, 3),
            CopyOp::new(2, 20, 2),
            CopyOp::new(u64::MAX, u64::MAX, 0),
        ];
        let slices = source
            .slices(&CopyBatch::new(&operations).unwrap())
            .unwrap();
        assert_eq!(slices.len(), 4);
        let expected = [
            &source.buffers()[0][2..4],
            &source.buffers()[2][..2],
            &source.buffers()[0][..3],
            &source.buffers()[0][2..4],
        ];
        for (slice, expected) in slices.iter().zip(expected) {
            assert_eq!(slice.as_ref(), expected);
            assert_eq!(slice.as_ref().as_ptr(), expected.as_ptr());
        }
        assert_eq!(
            slices
                .iter()
                .flat_map(|slice| slice.as_ref().iter().copied())
                .collect::<Vec<_>>(),
            b"cdefabccd"
        );
    }

    #[test]
    fn slices_retain_immutable_source_until_the_last_view_is_dropped() {
        let source = source(&[b"abcd", b"efgh"]);
        let operations = [CopyOp::new(2, 0, 4)];
        let mut slices = source
            .slices(&CopyBatch::new(&operations).unwrap())
            .unwrap();
        let weak = Arc::downgrade(&source);
        let source = Arc::try_unwrap(source).unwrap_err();
        drop(source);
        assert_eq!(slices.remove(0).as_ref(), b"cd");
        assert!(weak.upgrade().is_some());
        assert_eq!(slices[0].as_ref(), b"ef");
        drop(slices);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn invalid_and_empty_batches_do_not_retain_the_source() {
        let source = source(&[b"abcd"]);
        let operations = [CopyOp::new(0, 0, 1), CopyOp::new(4, 1, 1)];
        assert_eq!(
            source
                .slices(&CopyBatch::new(&operations).unwrap())
                .unwrap_err()
                .kind,
            MemoryErrorKind::InvalidCopyOp
        );
        for operations in [vec![], vec![CopyOp::new(u64::MAX, u64::MAX, 0)]] {
            assert!(
                source
                    .slices(&CopyBatch::new(&operations).unwrap())
                    .unwrap()
                    .is_empty()
            );
        }
        let buffers = Arc::try_unwrap(source).unwrap().into_buffers();
        assert_eq!(buffers[0].as_slice(), b"abcd");
    }

    #[test]
    fn buffers_can_be_recovered_after_slices_are_dropped() {
        let source = source(&[b"abcd"]);
        let operations = [CopyOp::new(0, 0, 4)];
        let slices = source
            .slices(&CopyBatch::new(&operations).unwrap())
            .unwrap();
        let source = Arc::try_unwrap(source).unwrap_err();
        drop(slices);
        let mut buffers = Arc::try_unwrap(source).unwrap().into_buffers();
        buffers[0].copy_from_slice(b"next");
        assert_eq!(buffers[0].as_slice(), b"next");
    }

    #[test]
    fn reads_cross_buffer_boundaries_and_rejects_out_of_bounds() {
        let pool = ruapc_bufpool::BufferPool::new(Arc::new(ruapc_bufpool::EmptyDevices));
        let buffers = [b"abcd", b"efgh"]
            .into_iter()
            .map(|bytes| {
                let mut buffer = pool.allocate(bytes.len()).unwrap();
                buffer.set_len(bytes.len());
                buffer.copy_from_slice(bytes);
                buffer
            })
            .collect();
        let source = ReadSource::try_new(buffers).unwrap();
        let operations = [CopyOp::new(2, 0, 4), CopyOp::new(0, 4, 2)];
        assert_eq!(
            source
                .gather(&CopyBatch::new(&operations).unwrap())
                .unwrap(),
            b"cdefab"
        );
        assert_eq!(
            source
                .gather(&CopyBatch::new(&[CopyOp::new(7, 0, 2)]).unwrap())
                .unwrap_err()
                .kind,
            MemoryErrorKind::InvalidCopyOp
        );
    }
}
