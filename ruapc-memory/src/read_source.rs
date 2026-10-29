//! Immutable, owned source buffers shared by a client and its pending reads.

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CopyOp, MemoryErrorKind};
    use std::sync::Arc;

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
