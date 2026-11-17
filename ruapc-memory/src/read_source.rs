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

    /// Invokes `f` with every buffer-contiguous slice of the logical range
    /// `[offset, offset + len)`, in order, `len` bytes in total.
    ///
    /// Checks the region limit and the range before the first call. An empty
    /// range calls nothing and permits any offset.
    pub fn for_each_slice(
        &self,
        offset: u64,
        len: u64,
        mut f: impl FnMut(&[u8]),
    ) -> MemoryResult<()> {
        self.layout
            .for_each_slice(offset, len, |segment, offset, len| {
                let (offset, len) = (offset as usize, len as usize);
                f(&self.buffers[segment][offset..offset + len]);
                Ok(())
            })
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
    #[test]
    fn range_slices_follow_buffer_boundaries_and_skip_empty_buffers() {
        let pool = ruapc_bufpool::BufferPool::new(Vec::new());
        let mut buffers = [&b"abcd"[..], b"", b"efgh"]
            .into_iter()
            .map(|bytes| {
                let mut buffer = pool.allocate(4).unwrap();
                buffer.set_len(bytes.len());
                buffer.copy_from_slice(bytes);
                buffer
            })
            .collect::<Vec<_>>();
        let slices = |offset, len| {
            let source = ReadSource::new(&buffers);
            let mut slices = Vec::new();
            source
                .for_each_slice(offset, len, |slice| slices.push(slice.to_vec()))
                .map(|()| slices)
        };
        assert_eq!(slices(2, 4).unwrap(), [b"cd", b"ef"]);
        assert_eq!(slices(4, 4).unwrap(), [b"efgh"]);
        assert!(slices(u64::MAX, 0).unwrap().is_empty());
        for (offset, len) in [(7, 2), (8, 1), (u64::MAX, 2)] {
            assert_eq!(
                slices(offset, len).unwrap_err().kind,
                MemoryErrorKind::InvalidCopyOp
            );
        }

        // An owned set exposes the same ranges for writing.
        let mut owned = BufferSet::new(std::mem::take(&mut buffers));
        let lens = |owned: &mut BufferSet, offset, len| {
            owned
                .slices_mut(offset, len)
                .map(|slices| slices.map(|slice| slice.len()).collect::<Vec<_>>())
        };
        assert_eq!(lens(&mut owned, 0, 8).unwrap(), [4, 4]);
        assert_eq!(lens(&mut owned, 3, 2).unwrap(), [1, 1]);
        assert_eq!(lens(&mut owned, 5, 2).unwrap(), [2]);
        assert!(lens(&mut owned, u64::MAX, 0).unwrap().is_empty());
        owned.write_at(3, b"XYZ").unwrap();
        owned.write_at(8, b"").unwrap();
        for (offset, bytes) in [(6, &b"123"[..]), (8, b"1"), (u64::MAX, b"12")] {
            assert_eq!(
                owned.write_at(offset, bytes).unwrap_err().kind,
                MemoryErrorKind::InvalidCopyOp
            );
        }
        // Rejected ranges wrote nothing, and no length changed.
        let buffers = owned.into_buffers();
        assert_eq!(buffers[0].as_slice(), b"abcX");
        assert!(buffers[1].is_empty());
        assert_eq!(buffers[2].as_slice(), b"YZgh");
    }
}
