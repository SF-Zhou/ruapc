//! Source-contiguous transfer planning with bounded destination scatter lists.

use std::convert::Infallible;

use crate::{CopyBatch, MemoryError, MemoryErrorKind, MemoryResult, SpaceLayout};

/// One destination slice of a [`SrcChunk`]: contiguous within a single
/// destination segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DstSlice {
    pub seg: usize,
    pub off: u64,
    pub len: u64,
}

/// A source-contiguous piece of a planned transfer: one contiguous range
/// of a single source segment, scattered over at most `max_sge`
/// destination slices. Maps 1:1 onto an RDMA READ work request (remote =
/// source, local scatter list = destination).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SrcChunk {
    pub seg: usize,
    pub off: u64,
    pub len: u64,
    pub dst: Vec<DstSlice>,
}

impl CopyBatch<'_> {
    /// Splits the batch into source-contiguous chunks with at most `max_sge`
    /// destination slices each. Checks both actual layout bounds without
    /// repeating structural validation. `max_sge` must be nonzero even for an
    /// empty batch. Planning performs no transfer and conveys no DMA authority.
    pub fn plan_chunks(
        &self,
        src: &SpaceLayout,
        dst: &SpaceLayout,
        max_sge: usize,
    ) -> MemoryResult<Vec<SrcChunk>> {
        if max_sge == 0 {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidArgument,
                "maximum destination segments per chunk must be nonzero".into(),
            ));
        }
        self.check_bounds(src.total(), dst.total())?;
        let mut chunks: Vec<SrcChunk> = Vec::new();
        for op in self.ops() {
            if op.len == 0 {
                continue;
            }
            let mut dst_pos = op.dst_offset;
            // Split at source segment boundaries first.
            src.for_each_slice_in_bounds::<Infallible>(
                op.src_offset,
                op.len,
                |src_seg, src_off, src_len| {
                    let mut chunk = SrcChunk {
                        seg: src_seg,
                        off: src_off,
                        len: 0,
                        dst: Vec::new(),
                    };
                    dst.for_each_slice_in_bounds::<Infallible>(
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
                    )
                    .unwrap();
                    debug_assert_eq!(chunk.len + (chunk.off - src_off), src_len);
                    chunks.push(chunk);
                    dst_pos += src_len;
                    Ok(())
                },
            )
            .unwrap();
        }
        Ok(chunks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CopyOp, MAX_COPY_OPS};
    fn validate_ops(ops: &[CopyOp], src: u64, dst: u64) -> MemoryResult<u64> {
        let batch = CopyBatch::new(ops)?;
        batch.check_bounds(src, dst)?;
        Ok(batch.byte_count())
    }
    fn plan_chunks(
        src: &SpaceLayout,
        dst: &SpaceLayout,
        ops: &[CopyOp],
        max_sge: usize,
    ) -> MemoryResult<Vec<SrcChunk>> {
        CopyBatch::new(ops)?.plan_chunks(src, dst, max_sge)
    }

    fn layout(lens: &[u64]) -> SpaceLayout {
        SpaceLayout::from_lens(lens.iter().copied()).unwrap()
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
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(10, 20, 30)], 16).unwrap();
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
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(4, 0, 8)], 16).unwrap();
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
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(0, 0, 24)], 16).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].dst.len(), 3);
        assert_eq!(chunks[0].len, 24);
    }

    #[test]
    fn test_plan_respects_max_sge() {
        let src = layout(&[32]);
        let dst = layout(&[8, 8, 8, 8]);
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(0, 0, 32)], 2).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].dst.len(), 2);
        assert_eq!(chunks[1].dst.len(), 2);
        assert_eq!((chunks[1].seg, chunks[1].off, chunks[1].len), (0, 16, 16));
    }

    #[test]
    fn test_plan_repeated_sge_splits_advance_src_offset() {
        let src = layout(&[32]);
        let dst = layout(&[8, 8, 8, 8]);
        let chunks = plan_chunks(&src, &dst, &[CopyOp::new(0, 0, 32)], 1).unwrap();
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
        let chunks = plan_chunks(&src, &dst, &ops, 2).unwrap();
        let total: u64 = chunks.iter().map(|c| c.len).sum();
        assert_eq!(total, 22);
        for chunk in &chunks {
            assert_eq!(chunk.dst.iter().map(|s| s.len).sum::<u64>(), chunk.len);
            assert!(chunk.dst.len() <= 2);
        }
    }
}
