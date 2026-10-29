//! Logical space layouts and checked segment traversal.

use ruapc_bufpool::Buffer;

use crate::{MemoryError, MemoryErrorKind, MemoryResult};

/// Maximum number of segments in a logical memory space.
pub const MAX_REGIONS: usize = 64;

/// Prefix-sum view of a logical space built from segment lengths.
#[derive(Debug, Clone)]
pub struct SpaceLayout {
    /// `starts[i]` is the space offset where segment `i` begins;
    /// `starts[segments]` is the total length.
    starts: Vec<u64>,
}

impl SpaceLayout {
    /// Builds a layout from segment lengths. Fails on total-length
    /// overflow or too many segments.
    pub fn from_lens(lens: impl IntoIterator<Item = u64>) -> MemoryResult<Self> {
        let mut starts = vec![0u64];
        for len in lens {
            let last = *starts.last().unwrap();
            let next = last.checked_add(len).ok_or_else(|| {
                MemoryError::new(
                    MemoryErrorKind::InvalidCopyOp,
                    "logical space length overflows u64".into(),
                )
            })?;
            starts.push(next);
            if starts.len() > MAX_REGIONS + 1 {
                return Err(MemoryError::new(
                    MemoryErrorKind::InvalidCopyOp,
                    format!("logical space exceeds {MAX_REGIONS} segments"),
                ));
            }
        }
        Ok(Self { starts })
    }

    /// Builds a layout from the buffers' logical lengths, not their capacities.
    pub fn from_buffers(buffers: &[Buffer]) -> MemoryResult<Self> {
        Self::from_lens(buffers.iter().map(|buffer| buffer.len() as u64))
    }

    /// Total length of the space in bytes.
    pub fn total(&self) -> u64 {
        *self.starts.last().unwrap()
    }

    /// Length of segment `i`.
    fn seg_len(&self, i: usize) -> u64 {
        self.starts[i + 1] - self.starts[i]
    }

    /// Resolves a space offset to `(segment index, offset within it)`.
    ///
    /// `offset` must be strictly below [`total`](Self::total). Zero-length
    /// segments are skipped (they occupy no offsets).
    fn locate(&self, offset: u64) -> (usize, u64) {
        debug_assert!(offset < self.total());
        // partition_point: first segment whose start is > offset, minus 1.
        let idx = self.starts.partition_point(|&s| s <= offset) - 1;
        (idx, offset - self.starts[idx])
    }

    /// Invokes `f(segment, offset_in_segment, slice_len)` for every
    /// segment-contiguous slice of `[offset, offset + len)`, in order.
    ///
    /// Validates the complete range before calling `f`. Empty ranges perform
    /// no access and permit any offset, matching zero-length [`crate::CopyOp`]s.
    /// Errors from `f` stop iteration immediately and are returned unchanged.
    pub fn for_each_slice<E: From<MemoryError>>(
        &self,
        offset: u64,
        len: u64,
        f: impl FnMut(usize, u64, u64) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if len != 0 && offset.checked_add(len).is_none_or(|end| end > self.total()) {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                format!(
                    "range at offset {offset} with length {len} exceeds logical space ({} bytes)",
                    self.total()
                ),
            )
            .into());
        }
        self.for_each_slice_in_bounds(offset, len, f)
    }

    /// Iterates a range checked by this module or a batch's whole-space check.
    /// This is an implementation detail; public callers use `for_each_slice`.
    #[inline]
    pub(crate) fn for_each_slice_in_bounds<E>(
        &self,
        offset: u64,
        len: u64,
        mut f: impl FnMut(usize, u64, u64) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        debug_assert!(
            len == 0
                || offset
                    .checked_add(len)
                    .is_some_and(|end| end <= self.total())
        );
        let mut pos = offset;
        let mut remaining = len;
        while remaining > 0 {
            let (seg, off) = self.locate(pos);
            let slice = remaining.min(self.seg_len(seg) - off);
            debug_assert!(slice > 0);
            f(seg, off, slice)?;
            pos += slice;
            remaining -= slice;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn layout(lens: &[u64]) -> SpaceLayout {
        SpaceLayout::from_lens(lens.iter().copied()).unwrap()
    }
    #[test]
    fn test_layout_total_and_locate() {
        let l = layout(&[10, 0, 5]);
        assert_eq!(l.total(), 15);
        assert_eq!(l.locate(0), (0, 0));
        assert_eq!(l.locate(9), (0, 9));
        // Offset 10 lands in segment 2 (segment 1 is empty).
        assert_eq!(l.locate(10), (2, 0));
        assert_eq!(l.locate(14), (2, 4));
    }
    #[test]
    fn test_layout_overflow_rejected() {
        assert!(SpaceLayout::from_lens([u64::MAX, 1]).is_err());
        assert!(SpaceLayout::from_lens(std::iter::repeat_n(1, MAX_REGIONS + 1)).is_err());
    }
    #[test]
    fn test_for_each_slice_spans_segments() {
        let l = layout(&[4, 4, 4]);
        let mut seen = Vec::new();
        l.for_each_slice::<MemoryError>(2, 8, |seg, off, len| {
            seen.push((seg, off, len));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![(0, 2, 2), (1, 0, 4), (2, 0, 2)]);
    }
}
