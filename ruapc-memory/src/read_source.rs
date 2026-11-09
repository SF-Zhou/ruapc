//! Immutable owned sources and checked borrowed source views.

use ruapc_bufpool::Buffer;

use crate::{BufferSet, BufferSetError, BufferView, CopyBatch, MemoryResult, SpaceLayout};

/// An immutable borrowed source with a checked, cached logical layout.
///
/// The slice remains borrowed while this view exists. It neither moves buffer
/// handles nor allocates a vector of per-buffer references.
#[derive(Debug)]
pub struct BorrowedSource<'a, B = Buffer> {
    buffers: &'a [B],
    layout: SpaceLayout,
}

impl<'a, B: BufferView> BorrowedSource<'a, B> {
    /// Checks the source layout without moving or cloning its buffers.
    pub fn new(buffers: &'a [B]) -> MemoryResult<Self> {
        Ok(Self {
            buffers,
            layout: SpaceLayout::from_buffers(buffers)?,
        })
    }

    /// Returns the immutable layout checked against the borrowed buffers.
    #[must_use]
    pub fn layout(&self) -> &SpaceLayout {
        &self.layout
    }

    /// Returns the original source slice.
    #[must_use]
    pub fn buffers(&self) -> &'a [B] {
        self.buffers
    }

    /// Gathers operation payloads using the source's cached layout.
    pub fn gather(&self, batch: &CopyBatch<'_>) -> MemoryResult<Vec<u8>> {
        batch.gather_from_layout(&self.layout, self.buffers)
    }
}

/// Owned immutable source buffers, shareable through an `Arc`.
///
/// Local shared ownership protects CPU readers. It is not evidence that a
/// remote DMA operation has completed or that a request remains valid.
#[derive(Debug)]
pub struct ReadSource<B = Buffer> {
    buffers: BufferSet<B>,
}

impl<B> From<BufferSet<B>> for ReadSource<B> {
    /// Makes an already checked buffer set immutable without rebuilding its layout.
    fn from(buffers: BufferSet<B>) -> Self {
        Self { buffers }
    }
}

impl<B: BufferView> ReadSource<B> {
    /// Checks and caches the source layout, retaining all buffers on failure.
    pub fn try_new(buffers: Vec<B>) -> Result<Self, BufferSetError<B>> {
        BufferSet::try_new(buffers).map(Self::from)
    }

    /// Returns an immutable view of the source buffers.
    #[must_use]
    pub fn buffers(&self) -> &[B] {
        self.buffers.buffers()
    }

    /// Returns the immutable cached source layout.
    #[must_use]
    pub fn layout(&self) -> &SpaceLayout {
        self.buffers.layout()
    }

    /// Recovers local ownership of all source buffers.
    #[must_use]
    pub fn into_buffers(self) -> Vec<B> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CopyOp, MemoryErrorKind};
    use ruapc_bufpool::BufferPool;

    #[test]
    fn borrowed_source_checks_and_copies_without_moving_handles() {
        let pool = BufferPool::new(Vec::new());
        let buffers: Vec<_> = [b"ab", b"cd"]
            .into_iter()
            .map(|bytes| {
                let mut buffer = pool.allocate(bytes.len()).unwrap();
                buffer.set_len(bytes.len());
                buffer.copy_from_slice(bytes);
                buffer
            })
            .collect();
        let source = BorrowedSource::new(&buffers).unwrap();
        assert!(std::ptr::eq(source.buffers(), buffers.as_slice()));
        assert_eq!(source.layout().total(), 4);
        let ops = [CopyOp::new(1, 0, 2)];
        assert_eq!(
            source.gather(&CopyBatch::new(&ops).unwrap()).unwrap(),
            b"bc"
        );
        let ops = [CopyOp::new(3, 0, 2)];
        assert_eq!(
            source
                .gather(&CopyBatch::new(&ops).unwrap())
                .unwrap_err()
                .kind,
            MemoryErrorKind::InvalidCopyOp
        );
    }

    #[test]
    fn reads_cross_buffer_boundaries_and_rejects_out_of_bounds() {
        let pool = BufferPool::new(Vec::new());
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
