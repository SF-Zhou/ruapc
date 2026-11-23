//! Ordered spans of a single logical space, consumed in packed byte order.

use crate::{CopyBatch, MAX_COPY_OPS, MemoryError, MemoryErrorKind, MemoryResult};

/// A checked sequence of logical (offset, length) ranges. Ranges may overlap
/// or appear out of offset order. Empty ranges occupy no bytes.
#[derive(Debug, Default)]
pub struct RangeCursor {
    first: Option<(u64, u64)>,
    rest: std::vec::IntoIter<(u64, u64)>,
    remaining: u64,
}

impl RangeCursor {
    /// Checks the ranges and takes ownership of their allocation.
    pub fn new(mut ranges: Vec<(u64, u64)>, space_len: u64) -> MemoryResult<Self> {
        let remaining = Self::checked_len(&ranges, space_len)?;
        ranges.retain(|&(_, len)| len != 0);
        let mut rest = ranges.into_iter();
        Ok(Self {
            first: rest.next(),
            rest,
            remaining,
        })
    }

    /// Builds an owned cursor from a slice after checking its count, bounds
    /// and total length. A single nonempty range fits inline.
    pub fn from_slice(ranges: &[(u64, u64)], space_len: u64) -> MemoryResult<Self> {
        let remaining = Self::checked_len(ranges, space_len)?;
        let nonempty = ranges.iter().copied().filter(|&(_, len)| len != 0);
        Ok(Self::collect_nonempty(nonempty, remaining))
    }

    fn checked_len(ranges: &[(u64, u64)], space_len: u64) -> MemoryResult<u64> {
        if ranges.len() > MAX_COPY_OPS {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                "too many ranges".into(),
            ));
        }
        let mut remaining = 0u64;
        for &(offset, len) in ranges {
            if len == 0 {
                continue;
            }
            if offset.checked_add(len).is_none_or(|end| end > space_len) {
                return Err(MemoryError::new(
                    MemoryErrorKind::InvalidCopyOp,
                    format!(
                        "range at offset {offset} with length {len} exceeds logical space ({space_len} bytes)"
                    ),
                ));
            }
            remaining = remaining.checked_add(len).ok_or_else(|| {
                MemoryError::new(
                    MemoryErrorKind::InvalidCopyOp,
                    "total range length overflows u64".into(),
                )
            })?;
        }
        Ok(remaining)
    }

    fn collect_nonempty(mut spans: impl Iterator<Item = (u64, u64)>, remaining: u64) -> Self {
        let first = spans.next();
        let mut rest = Vec::new();
        if remaining > first.map_or(0, |(_, len)| len) {
            // Filtering out empty ranges preserves the bounded input's upper size hint.
            rest.reserve_exact(spans.size_hint().1.unwrap_or(0));
            rest.extend(spans);
        }
        Self {
            first,
            rest: rest.into_iter(),
            remaining,
        }
    }

    /// Number of bytes that have not yet been consumed.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Next nonempty contiguous span.
    pub fn front(&self) -> Option<(u64, u64)> {
        self.first
    }

    /// Takes at most `limit` bytes of the next span.
    pub fn next_span(&mut self, limit: u64) -> Option<(u64, u64)> {
        let (offset, len) = self.first?;
        if limit == 0 {
            return None;
        }
        let n = len.min(limit);
        self.first = if n == len {
            self.rest.next()
        } else {
            Some((offset + n, len - n))
        };
        self.remaining -= n;
        Some((offset, n))
    }

    /// Removes up to `limit` bytes and returns their cursor. Taking the whole
    /// remainder moves its storage. A prefix within one span needs no allocation.
    pub fn split_to(&mut self, limit: u64) -> Self {
        if limit >= self.remaining {
            return std::mem::take(self);
        }
        let first = self.next_span(limit);
        let mut left = limit - first.map_or(0, |(_, len)| len);
        let rest = std::iter::from_fn(|| {
            let span = self.next_span(left)?;
            left -= span.1;
            Some(span)
        })
        .collect::<Vec<_>>()
        .into_iter();
        Self {
            first,
            rest,
            remaining: limit,
        }
    }
}

impl CopyBatch<'_> {
    /// Destination ranges in copy order for unpacking a stream of source bytes.
    pub fn destination_cursor(&self) -> RangeCursor {
        let ranges = self
            .ops()
            .iter()
            .filter(|op| op.len != 0)
            .map(|op| (op.dst_offset, op.len));
        RangeCursor::collect_nonempty(ranges, self.byte_count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_preserves_order_overlap_and_boundaries() {
        let mut plan = RangeCursor::new(vec![(8, 4), (u64::MAX, 0), (1, 5), (2, 4)], 12).unwrap();
        let mut first = plan.split_to(7);
        assert_eq!(first.next_span(8), Some((8, 4)));
        assert_eq!(first.next_span(8), Some((1, 3)));
        assert_eq!(first.remaining(), 0);
        assert_eq!(plan.split_to(0).remaining(), 0);
        let mut last = plan.split_to(u64::MAX);
        assert_eq!(last.next_span(1), Some((4, 1)));
        assert_eq!(last.next_span(5), Some((5, 1)));
        assert_eq!(last.next_span(5), Some((2, 4)));
        assert_eq!(plan.front(), None);
    }

    #[test]
    fn owned_cursor_reuses_the_range_vector() {
        let ranges = vec![(6, 1), (0, 0), (4, 2)];
        let tail = ranges.as_ptr().wrapping_add(1);
        let plan = RangeCursor::new(ranges, 7).unwrap();
        assert_eq!(plan.front(), Some((6, 1)));
        assert_eq!(plan.rest.as_slice(), &[(4, 2)]);
        assert_eq!(plan.rest.as_slice().as_ptr(), tail);
    }

    #[test]
    fn borrowed_ranges_produce_an_independent_ordered_cursor() {
        let mut input = [(6, 1), (u64::MAX, 0), (0, 3), (1, 1)];
        let mut plan = RangeCursor::from_slice(&input, 7).unwrap();
        input.fill((u64::MAX, 0));
        assert_eq!(plan.remaining(), 5);
        assert_eq!(plan.next_span(5), Some((6, 1)));
        assert_eq!(plan.next_span(5), Some((0, 3)));
        assert_eq!(plan.next_span(5), Some((1, 1)));
        assert_eq!(plan.front(), None);
        assert_eq!(RangeCursor::from_slice(&input, 0).unwrap().front(), None);
    }

    #[test]
    fn both_constructors_reject_invalid_bounds_totals_and_counts() {
        for (ranges, space_len) in [
            (vec![(1, 2)], 2),
            (vec![(u64::MAX, 1)], u64::MAX),
            (vec![(0, u64::MAX), (0, 1)], u64::MAX),
            (vec![(0, 0); MAX_COPY_OPS + 1], 0),
        ] {
            assert!(RangeCursor::from_slice(&ranges, space_len).is_err());
            assert!(RangeCursor::new(ranges, space_len).is_err());
        }
    }
}
