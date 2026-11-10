//! Immutable borrowed source buffers with a cached layout.

use ruapc_bufpool::Buffer;

use crate::{BufferSet, CopyBatch, MemoryResult, SpaceLayout};

/// An immutable borrow of source buffers and their cached logical layout.
///
/// The borrow prevents changes to the buffers and their lengths while this
/// source is used. It is not remote DMA completion or request-validity evidence.
///
/// ```compile_fail
/// use ruapc_bufpool::Buffer;
/// use ruapc_memory::ReadSource;
/// fn change_length(buffers: &mut [Buffer]) {
///     let source = ReadSource::new(buffers);
///     buffers[0].set_len(0);
///     assert!(!source.buffers().is_empty());
/// }
/// ```
#[derive(Debug)]
pub struct ReadSource<'a> {
    buffers: &'a [Buffer],
    layout: SpaceLayout,
}

impl<'a> From<&'a BufferSet> for ReadSource<'a> {
    /// Borrows a buffer set without rebuilding its layout.
    fn from(buffers: &'a BufferSet) -> Self {
        Self {
            buffers: buffers.buffers(),
            layout: buffers.layout().clone(),
        }
    }
}

impl<'a> ReadSource<'a> {
    /// Borrows sources and caches their layout. Operations check the region limit.
    pub fn new(buffers: &'a [Buffer]) -> Self {
        Self {
            buffers,
            layout: SpaceLayout::from_buffers(buffers),
        }
    }

    /// Returns an immutable view of the source buffers.
    #[must_use]
    pub fn buffers(&self) -> &'a [Buffer] {
        self.buffers
    }

    /// Returns the immutable cached source layout; its region limit is unchecked.
    #[must_use]
    pub fn layout(&self) -> &SpaceLayout {
        &self.layout
    }

    /// Gathers source ranges into bytes, concatenated in operation order.
    ///
    /// Checks the batch's source bounds against the cached layout. Destination
    /// offsets do not determine output order. No peer-provided address or
    /// registration key can select a different allocation.
    pub fn gather(&self, batch: &CopyBatch<'_>) -> MemoryResult<Vec<u8>> {
        batch.gather_from_layout(&self.layout, self.buffers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CopyOp, MemoryErrorKind};

    #[test]
    fn reads_cross_buffer_boundaries_and_rejects_out_of_bounds() {
        let pool = ruapc_bufpool::BufferPool::new(Vec::new());
        let buffers = [b"abcd", b"efgh"]
            .into_iter()
            .map(|bytes| {
                let mut buffer = pool.allocate(bytes.len()).unwrap();
                buffer.set_len(bytes.len());
                buffer.copy_from_slice(bytes);
                buffer
            })
            .collect::<Vec<_>>();
        let source = ReadSource::new(&buffers);
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
