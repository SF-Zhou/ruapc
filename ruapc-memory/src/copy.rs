//! Validated copy batches and buffer sets with cached logical layouts.

use std::convert::Infallible;

use ruapc_bufpool::Buffer;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{MemoryError, MemoryErrorKind, MemoryResult, SpaceLayout};

/// Maximum number of operations in one copy batch.
pub const MAX_COPY_OPS: usize = 1024;

/// One copy operation between two logical spaces.
///
/// For `remote_read` (server pulls client memory): `src_offset` addresses
/// the client's read space, `dst_offset` the server's local buffers.
/// For `remote_write` (server pushes to client memory): `src_offset`
/// addresses the server's local buffers, `dst_offset` the client's write
/// space.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CopyOp {
    /// Byte offset into the source space.
    pub src_offset: u64,
    /// Byte offset into the destination space.
    pub dst_offset: u64,
    /// Number of bytes to copy. Zero-length ops are permitted and skipped.
    pub len: u64,
}

impl CopyOp {
    /// Creates a copy operation.
    #[must_use]
    pub const fn new(src_offset: u64, dst_offset: u64, len: u64) -> Self {
        Self {
            src_offset,
            dst_offset,
            len,
        }
    }
}

/// An immutable batch whose operation structure has been checked once.
///
/// Construction checks operation count, arithmetic overflow, total byte count
/// and destination overlap. It does not bind the batch to particular buffers
/// or claim either peer's space exists. Execution checks the actual accessed
/// space: gather checks the source, scatter checks the destination, and planning
/// checks both. Source overlap is allowed; zero-length operations perform no
/// access regardless of their offsets.
///
/// This value provides no memory ownership, request validity or DMA completion
/// evidence. The operation slice cannot change while the batch remains in use:
///
/// ```compile_fail
/// use ruapc_memory::{CopyBatch, CopyOp};
/// let mut operations = [CopyOp::new(0, 0, 4)];
/// let batch = CopyBatch::new(&operations).unwrap();
/// operations[0].len = 8;
/// assert_eq!(batch.ops()[0].len, 4);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct CopyBatch<'a> {
    ops: &'a [CopyOp],
    byte_count: u64,
    max_src_end: u64,
    max_dst_end: u64,
}

impl<'a> CopyBatch<'a> {
    /// Validates the operation structure without requiring either space's length.
    pub fn new(ops: &'a [CopyOp]) -> MemoryResult<Self> {
        let (byte_count, max_src_end, max_dst_end) = validate_structure(ops)?;
        Ok(Self {
            ops,
            byte_count,
            max_src_end,
            max_dst_end,
        })
    }

    /// Returns the original immutable operation slice.
    #[must_use]
    pub fn ops(&self) -> &'a [CopyOp] {
        self.ops
    }

    /// Returns the sum of operation lengths, not the extent of either space.
    #[must_use]
    pub fn byte_count(&self) -> u64 {
        self.byte_count
    }

    /// Checks that every source range fits the provided logical space.
    pub fn check_source(&self, source_len: u64) -> MemoryResult<()> {
        if self.max_src_end > source_len {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                format!(
                    "copy operations require {} source bytes but the space has {source_len}",
                    self.max_src_end
                ),
            ));
        }
        Ok(())
    }

    /// Checks that every destination range fits the provided logical space.
    pub fn check_destination(&self, destination_len: u64) -> MemoryResult<()> {
        if self.max_dst_end > destination_len {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                format!(
                    "copy operations require {} destination bytes but the space has {destination_len}",
                    self.max_dst_end
                ),
            ));
        }
        Ok(())
    }

    /// Checks both spaces without repeating structural validation or sorting.
    pub fn check_bounds(&self, source_len: u64, destination_len: u64) -> MemoryResult<()> {
        self.check_source(source_len)?;
        self.check_destination(destination_len)
    }

    /// Gathers actual source buffers into packed bytes in operation order.
    ///
    /// Destination offsets do not index the result. The actual source layout
    /// is constructed and checked before copying. For repeated copies from an
    /// owned buffer list, [`BufferSet::gather`] reuses its cached layout.
    pub fn gather(&self, source: &[Buffer]) -> MemoryResult<Vec<u8>> {
        let layout = SpaceLayout::from_buffers(source)?;
        self.gather_from_layout(&layout, source)
    }

    /// Scatters packed operation payloads into actual destination buffers.
    ///
    /// Packed bytes must contain exactly [`byte_count`](Self::byte_count) bytes
    /// in operation order; source offsets do not index them. The actual layout,
    /// all destination ranges and packed length are checked before any write.
    /// Destination gaps remain unchanged. [`BufferSet::scatter`] reuses a cached
    /// layout when the caller owns the buffers.
    pub fn scatter(&self, packed: &[u8], destination: &mut [Buffer]) -> MemoryResult<()> {
        let layout = SpaceLayout::from_buffers(destination)?;
        self.scatter_into_layout(&layout, packed, destination)
    }

    // Only layouts derived from these actual buffers, or owned beside them in
    // BufferSet, reach these private helpers. No public API accepts a caller's
    // promise that a separate layout matches mutable buffers.
    fn gather_from_layout(&self, layout: &SpaceLayout, source: &[Buffer]) -> MemoryResult<Vec<u8>> {
        self.check_source(layout.total())?;
        let mut packed = Vec::with_capacity(usize::try_from(self.byte_count)?);
        for op in self.ops {
            layout
                .for_each_slice_in_bounds::<Infallible>(
                    op.src_offset,
                    op.len,
                    |segment, offset, len| {
                        let (offset, len) = (offset as usize, len as usize);
                        packed.extend_from_slice(&source[segment][offset..offset + len]);
                        Ok(())
                    },
                )
                .unwrap();
        }
        Ok(packed)
    }

    fn scatter_into_layout(
        &self,
        layout: &SpaceLayout,
        packed: &[u8],
        destination: &mut [Buffer],
    ) -> MemoryResult<()> {
        self.check_destination(layout.total())?;
        if packed.len() as u64 != self.byte_count {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                format!(
                    "packed payload contains {} bytes but copy operations describe {}",
                    packed.len(),
                    self.byte_count
                ),
            ));
        }
        let mut cursor = 0usize;
        for op in self.ops {
            layout
                .for_each_slice_in_bounds::<Infallible>(
                    op.dst_offset,
                    op.len,
                    |segment, offset, len| {
                        // Both the destination and packed slice bounds were checked
                        // for the whole batch before the first write.
                        let (offset, len) = (offset as usize, len as usize);
                        destination[segment][offset..offset + len]
                            .copy_from_slice(&packed[cursor..cursor + len]);
                        cursor += len;
                        Ok(())
                    },
                )
                .unwrap();
        }
        Ok(())
    }
}

/// Owned buffers with a logical layout derived and checked at construction.
///
/// Shared access exposes immutable buffers. CPU writes go through checked batch
/// operations, which cannot change buffer lengths. Recovering mutable buffer
/// ownership consumes the set, so the cached layout cannot become stale.
///
/// ```compile_fail
/// use ruapc_memory::BufferSet;
/// fn change_length(set: &mut BufferSet) {
///     set.buffers()[0].set_len(0);
/// }
/// ```
#[derive(Debug)]
pub struct BufferSet {
    buffers: Vec<Buffer>,
    layout: SpaceLayout,
}

impl BufferSet {
    /// Checks the buffers' logical layout, preserving all buffers on failure.
    pub fn try_new(buffers: Vec<Buffer>) -> Result<Self, BufferSetError> {
        match SpaceLayout::from_buffers(&buffers) {
            Ok(layout) => Ok(Self { buffers, layout }),
            Err(error) => Err(BufferSetError { error, buffers }),
        }
    }

    /// Returns the immutable cached logical layout.
    #[must_use]
    pub fn layout(&self) -> &SpaceLayout {
        &self.layout
    }

    /// Returns an immutable view of the owned buffers.
    #[must_use]
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    /// Consumes the set and recovers ownership of all buffers.
    #[must_use]
    pub fn into_buffers(self) -> Vec<Buffer> {
        self.buffers
    }

    /// Gathers a batch while reusing the checked layout.
    pub fn gather(&self, batch: &CopyBatch<'_>) -> MemoryResult<Vec<u8>> {
        batch.gather_from_layout(&self.layout, &self.buffers)
    }

    /// Scatters a batch while reusing the checked layout.
    pub fn scatter(&mut self, batch: &CopyBatch<'_>, packed: &[u8]) -> MemoryResult<()> {
        batch.scatter_into_layout(&self.layout, packed, &mut self.buffers)
    }
}

/// A buffer-set construction failure retaining the original owned buffers.
#[derive(Debug)]
pub struct BufferSetError {
    /// The layout validation failure.
    pub error: MemoryError,
    buffers: Vec<Buffer>,
}

impl BufferSetError {
    /// Borrows the buffers that could not form a valid set.
    #[must_use]
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    /// Returns the validation error and every original buffer in original order.
    #[must_use]
    pub fn into_parts(self) -> (MemoryError, Vec<Buffer>) {
        (self.error, self.buffers)
    }
}

impl std::fmt::Display for BufferSetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for BufferSetError {}

fn validate_structure(ops: &[CopyOp]) -> MemoryResult<(u64, u64, u64)> {
    if ops.len() > MAX_COPY_OPS {
        return Err(MemoryError::new(
            MemoryErrorKind::InvalidCopyOp,
            format!("too many copy ops: {} (limit {MAX_COPY_OPS})", ops.len()),
        ));
    }
    let mut total = 0u64;
    let mut max_src_end = 0u64;
    let mut dst_ranges: Vec<(u64, u64)> = Vec::with_capacity(ops.len());
    for (i, op) in ops.iter().enumerate() {
        if op.len == 0 {
            continue;
        }
        let src_end = op.src_offset.checked_add(op.len);
        let dst_end = op.dst_offset.checked_add(op.len);
        let (Some(src_end), Some(dst_end)) = (src_end, dst_end) else {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                format!("copy op {i} overflows: {op:?}"),
            ));
        };
        max_src_end = max_src_end.max(src_end);
        total = total.checked_add(op.len).ok_or_else(|| {
            MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                "copy ops total length overflows u64".into(),
            )
        })?;
        dst_ranges.push((op.dst_offset, dst_end));
    }
    dst_ranges.sort_unstable();
    for pair in dst_ranges.windows(2) {
        if pair[1].0 < pair[0].1 {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                format!(
                    "copy ops have overlapping destination ranges: \
                     [{}, {}) and [{}, {})",
                    pair[0].0, pair[0].1, pair[1].0, pair[1].1
                ),
            ));
        }
    }
    // Sorted, nonoverlapping ranges end in increasing order.
    let max_dst_end = dst_ranges.last().map_or(0, |(_, end)| *end);
    Ok((total, max_src_end, max_dst_end))
}
