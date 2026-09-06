//! Logical-space math for vectored remote memory transfers.
//!
//! Both sides of a remote read/write describe memory as a *logical
//! contiguous space*: an ordered list of segments (registered buffers or
//! advertised remote regions) concatenated by their logical lengths.
//! A transfer is a batch of [`CopyOp`]s, each copying `len` bytes from
//! `src_offset` in the source space to `dst_offset` in the destination
//! space.
//!
//! This module provides:
//! - [`SpaceLayout`]: prefix sums over segment lengths, offset→segment
//!   resolution and slice iteration;
//! - [`validate_ops`]: bounds/overflow/overlap checking of an op batch
//!   against two spaces;
//! - [`plan_chunks`]: fragmentation of an op batch into pieces that are
//!   contiguous in the source space with a bounded scatter list in the
//!   destination space — exactly the shape of an RDMA READ work request
//!   (one contiguous remote region, up to `max_sge` local segments).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Error, ErrorKind, Result};

/// Maximum number of regions a request may carry per logical space
/// (read regions and write regions are counted separately). Bounds the
/// metadata size and validation cost.
pub const MAX_REGIONS: usize = 64;

/// Maximum number of [`CopyOp`]s per `remote_read` / `remote_write` call.
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

/// Prefix-sum view of a logical space built from segment lengths.
#[derive(Debug, Clone)]
pub(crate) struct SpaceLayout {
    /// `starts[i]` is the space offset where segment `i` begins;
    /// `starts[segments]` is the total length.
    starts: Vec<u64>,
}

impl SpaceLayout {
    /// Builds a layout from segment lengths. Fails on total-length
    /// overflow or too many segments.
    pub fn from_lens(lens: impl IntoIterator<Item = u64>) -> Result<Self> {
        let mut starts = vec![0u64];
        for len in lens {
            let last = *starts.last().unwrap();
            let next = last.checked_add(len).ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidCopyOp,
                    "logical space length overflows u64".into(),
                )
            })?;
            starts.push(next);
            if starts.len() > MAX_REGIONS + 1 {
                return Err(Error::new(
                    ErrorKind::InvalidCopyOp,
                    format!("logical space exceeds {MAX_REGIONS} segments"),
                ));
            }
        }
        Ok(Self { starts })
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
    /// The range must have been validated against the space beforehand.
    pub fn for_each_slice<E>(
        &self,
        offset: u64,
        len: u64,
        mut f: impl FnMut(usize, u64, u64) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
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

/// Validates an op batch against a source and a destination space.
///
/// Checks (initiator and executor both run this — neither trusts the
/// peer):
/// - op count is within [`MAX_COPY_OPS`];
/// - no offset arithmetic overflows;
/// - every op fits its space (`src_offset + len <= src_total`, same for
///   dst);
/// - destination ranges do not overlap (concurrent fragments writing the
///   same bytes is a data race). Source overlap is permitted.
///
/// Returns the total number of bytes moved by the batch.
pub(crate) fn validate_ops(ops: &[CopyOp], src_total: u64, dst_total: u64) -> Result<u64> {
    if ops.len() > MAX_COPY_OPS {
        return Err(Error::new(
            ErrorKind::InvalidCopyOp,
            format!("too many copy ops: {} (limit {MAX_COPY_OPS})", ops.len()),
        ));
    }
    let mut total = 0u64;
    let mut dst_ranges: Vec<(u64, u64)> = Vec::with_capacity(ops.len());
    for (i, op) in ops.iter().enumerate() {
        if op.len == 0 {
            continue;
        }
        let src_end = op.src_offset.checked_add(op.len);
        let dst_end = op.dst_offset.checked_add(op.len);
        let (Some(src_end), Some(dst_end)) = (src_end, dst_end) else {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!("copy op {i} overflows: {op:?}"),
            ));
        };
        if src_end > src_total {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!("copy op {i} exceeds source space ({src_total} bytes): {op:?}"),
            ));
        }
        if dst_end > dst_total {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!("copy op {i} exceeds destination space ({dst_total} bytes): {op:?}"),
            ));
        }
        total = total.checked_add(op.len).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidCopyOp,
                "copy ops total length overflows u64".into(),
            )
        })?;
        dst_ranges.push((op.dst_offset, dst_end));
    }
    dst_ranges.sort_unstable();
    for pair in dst_ranges.windows(2) {
        if pair[1].0 < pair[0].1 {
            return Err(Error::new(
                ErrorKind::InvalidCopyOp,
                format!(
                    "copy ops have overlapping destination ranges: \
                     [{}, {}) and [{}, {})",
                    pair[0].0, pair[0].1, pair[1].0, pair[1].1
                ),
            ));
        }
    }
    Ok(total)
}

/// One destination slice of a [`SrcChunk`]: contiguous within a single
/// destination segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(feature = "rdma"), allow(dead_code))]
pub(crate) struct DstSlice {
    pub seg: usize,
    pub off: u64,
    pub len: u64,
}

/// A source-contiguous piece of a planned transfer: one contiguous range
/// of a single source segment, scattered over at most `max_sge`
/// destination slices. Maps 1:1 onto an RDMA READ work request (remote =
/// source, local scatter list = destination).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "rdma"), allow(dead_code))]
pub(crate) struct SrcChunk {
    pub seg: usize,
    pub off: u64,
    pub len: u64,
    pub dst: Vec<DstSlice>,
}

/// Fragments an op batch into [`SrcChunk`]s: split at source segment
/// boundaries (mandatory — the source side of each chunk must be
/// contiguous), gather destination slices per chunk up to `max_sge`
/// (splitting further when the destination is more fragmented).
///
/// `ops` must have been validated with [`validate_ops`] first.
#[cfg_attr(not(feature = "rdma"), allow(dead_code))]
pub(crate) fn plan_chunks(
    src: &SpaceLayout,
    dst: &SpaceLayout,
    ops: &[CopyOp],
    max_sge: usize,
) -> Vec<SrcChunk> {
    debug_assert!(max_sge >= 1);
    let mut chunks: Vec<SrcChunk> = Vec::new();
    for op in ops {
        if op.len == 0 {
            continue;
        }
        let mut dst_pos = op.dst_offset;
        // Split at source segment boundaries first.
        let _ = src.for_each_slice::<std::convert::Infallible>(
            op.src_offset,
            op.len,
            |src_seg, src_off, src_len| {
                let mut chunk = SrcChunk {
                    seg: src_seg,
                    off: src_off,
                    len: 0,
                    dst: Vec::new(),
                };
                let _ = dst.for_each_slice::<std::convert::Infallible>(
                    dst_pos,
                    src_len,
                    |dst_seg, dst_off, dst_len| {
                        if chunk.dst.len() == max_sge {
                            // Scatter list full: emit and continue with a
                            // new chunk starting right after the emitted
                            // bytes.
                            let next_off = chunk.off + chunk.len;
                            let prev = std::mem::replace(
                                &mut chunk,
                                SrcChunk {
                                    seg: src_seg,
                                    off: next_off,
                                    len: 0,
                                    dst: Vec::new(),
                                },
                            );
                            chunks.push(prev);
                        }
                        chunk.dst.push(DstSlice {
                            seg: dst_seg,
                            off: dst_off,
                            len: dst_len,
                        });
                        chunk.len += dst_len;
                        Ok(())
                    },
                );
                debug_assert_eq!(chunk.len + (chunk.off - src_off), src_len);
                chunks.push(chunk);
                dst_pos += src_len;
                Ok(())
            },
        );
    }
    chunks
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
        l.for_each_slice::<std::convert::Infallible>(2, 8, |seg, off, len| {
            seen.push((seg, off, len));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![(0, 2, 2), (1, 0, 4), (2, 0, 2)]);
    }

    #[test]
    fn test_validate_ops_bounds() {
        let ops = [CopyOp::new(0, 0, 10)];
        assert_eq!(validate_ops(&ops, 10, 10).unwrap(), 10);
        assert!(validate_ops(&ops, 9, 10).is_err());
        assert!(validate_ops(&ops, 10, 9).is_err());
        // Overflowing offsets are rejected.
        assert!(validate_ops(&[CopyOp::new(u64::MAX, 0, 1)], u64::MAX, 10).is_err());
        // Zero-length ops are ignored, even out of range.
        assert_eq!(validate_ops(&[CopyOp::new(100, 100, 0)], 1, 1).unwrap(), 0);
    }

    #[test]
    fn test_validate_ops_dst_overlap_rejected() {
        let ops = [CopyOp::new(0, 0, 8), CopyOp::new(8, 4, 8)];
        assert!(validate_ops(&ops, 16, 16).is_err());
        // Adjacent (touching) ranges are fine.
        let ops = [CopyOp::new(0, 0, 8), CopyOp::new(0, 8, 8)];
        assert_eq!(validate_ops(&ops, 16, 16).unwrap(), 16);
        // Source overlap is fine.
        let ops = [CopyOp::new(0, 0, 8), CopyOp::new(0, 8, 8)];
        assert!(validate_ops(&ops, 8, 16).is_ok());
    }

    #[test]
    fn test_validate_ops_count_cap() {
        let ops = vec![CopyOp::new(0, 0, 0); MAX_COPY_OPS + 1];
        assert!(validate_ops(&ops, 0, 0).is_err());
    }

    #[test]
    fn test_plan_single_contiguous() {
        let src = layout(&[100]);
        let dst = layout(&[100]);
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(10, 20, 30)], 16);
        assert_eq!(
            chunks,
            vec![SrcChunk {
                seg: 0,
                off: 10,
                len: 30,
                dst: vec![DstSlice {
                    seg: 0,
                    off: 20,
                    len: 30
                }],
            }]
        );
    }

    #[test]
    fn test_plan_splits_at_src_boundary() {
        let src = layout(&[8, 8]);
        let dst = layout(&[16]);
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(4, 0, 8)], 16);
        assert_eq!(chunks.len(), 2);
        assert_eq!((chunks[0].seg, chunks[0].off, chunks[0].len), (0, 4, 4));
        assert_eq!((chunks[1].seg, chunks[1].off, chunks[1].len), (1, 0, 4));
        assert_eq!(
            chunks[1].dst,
            vec![DstSlice {
                seg: 0,
                off: 4,
                len: 4
            }]
        );
    }

    #[test]
    fn test_plan_gathers_dst_slices() {
        // One contiguous source range scattering over three destination
        // segments becomes a single chunk with three slices.
        let src = layout(&[24]);
        let dst = layout(&[8, 8, 8]);
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(0, 0, 24)], 16);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].dst.len(), 3);
        assert_eq!(chunks[0].len, 24);
    }

    #[test]
    fn test_plan_respects_max_sge() {
        let src = layout(&[32]);
        let dst = layout(&[8, 8, 8, 8]);
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(0, 0, 32)], 2);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].dst.len(), 2);
        assert_eq!(chunks[1].dst.len(), 2);
        assert_eq!((chunks[1].seg, chunks[1].off, chunks[1].len), (0, 16, 16));
    }

    #[test]
    fn test_plan_repeated_sge_splits_advance_src_offset() {
        let src = layout(&[32]);
        let dst = layout(&[8, 8, 8, 8]);
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(0, 0, 32)], 1);
        assert_eq!(chunks.len(), 4);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!((chunk.seg, chunk.off, chunk.len), (0, 8 * i as u64, 8));
        }
    }

    #[test]
    fn test_plan_total_bytes_conserved() {
        let src = layout(&[7, 13, 5]);
        let dst = layout(&[3, 3, 19]);
        let ops = [CopyOp::new(2, 1, 20), CopyOp::new(23, 21, 2)];
        validate_ops(&ops, src.total(), dst.total()).unwrap();
        let chunks = plan_chunks(&src, &dst, &ops, 2);
        let total: u64 = chunks.iter().map(|c| c.len).sum();
        assert_eq!(total, 22);
        for chunk in &chunks {
            assert_eq!(chunk.dst.iter().map(|s| s.len).sum::<u64>(), chunk.len);
            assert!(chunk.dst.len() <= 2);
        }
    }
}
