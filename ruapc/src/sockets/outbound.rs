//! Frames queued for a stream connection's send loop.

use bytes::{Bytes, BytesMut};
use tokio::sync::OwnedSemaphorePermit;

/// One frame in a stream connection's send queue.
///
/// A message frame is a single encoded segment. A data frame can follow its
/// head with segments that share the sender's buffers, so its bytes reach a
/// vectored write without first being copied into one allocation. The send
/// loop writes a frame's segments back to back and never interleaves frames.
///
/// A data frame also owns the queued bytes that its connection admitted it
/// with. Whichever form a carrier takes the frame in keeps them until the
/// frame's last byte is written, and no longer.
#[derive(Debug)]
pub(crate) struct Outbound {
    head: Bytes,
    data: Vec<Bytes>,
    /// The frame's share of its connection's queued data bytes.
    queued: Option<OwnedSemaphorePermit>,
}

impl From<Bytes> for Outbound {
    fn from(head: Bytes) -> Self {
        Self::with_data(head, Vec::new())
    }
}

/// One nonempty segment of a queued frame, in wire order.
///
/// The frame's last segment owns the frame's queued bytes: dropping it
/// releases them, so a carrier that drops each segment as soon as it is
/// written releases the bytes exactly when the frame is.
#[derive(Debug)]
pub(crate) struct Segment {
    pub(crate) bytes: Bytes,
    queued: Option<OwnedSemaphorePermit>,
}

impl Segment {
    /// The segment as `Bytes` that still own what the segment owns, for
    /// carriers whose sink takes nothing else.
    pub(crate) fn into_bytes(self) -> Bytes {
        holding(self.bytes, self.queued)
    }
}

/// `bytes` that keep `queued` until they are dropped.
fn holding(bytes: Bytes, queued: Option<OwnedSemaphorePermit>) -> Bytes {
    match queued {
        Some(queued) => Bytes::from_owner(Held {
            bytes,
            _queued: queued,
        }),
        None => bytes,
    }
}

/// Bytes together with the queued-byte share of the frame they end.
struct Held {
    bytes: Bytes,
    _queued: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for Held {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Outbound {
    /// A frame whose encoded `head` is followed by `data`, in order.
    pub(crate) fn with_data(head: Bytes, data: Vec<Bytes>) -> Self {
        Self {
            head,
            data,
            queued: None,
        }
    }

    /// Attaches the queued bytes that the connection admitted this frame
    /// with. They are released when the frame is written or dropped.
    pub(crate) fn admitted(mut self, queued: OwnedSemaphorePermit) -> Self {
        self.queued = Some(queued);
        self
    }

    /// Number of bytes the frame carries on the wire.
    pub(crate) fn byte_len(&self) -> usize {
        self.head.len() + self.data.iter().map(Bytes::len).sum::<usize>()
    }

    /// The frame's nonempty segments in wire order. The last one owns the
    /// frame's queued bytes.
    pub(crate) fn segments(self) -> impl Iterator<Item = Segment> {
        let mut queued = self.queued;
        let mut segments = std::iter::once(self.head)
            .chain(self.data)
            .filter(|segment| !segment.is_empty())
            .peekable();
        std::iter::from_fn(move || {
            let bytes = segments.next()?;
            let queued = match segments.peek() {
                Some(_) => None,
                None => queued.take(),
            };
            Some(Segment { bytes, queued })
        })
    }

    /// The frame as one contiguous allocation that owns the frame's queued
    /// bytes, for carriers that send a frame as a single unit. Only a frame
    /// with several segments is copied.
    pub(crate) fn into_bytes(self) -> Bytes {
        if self.data.is_empty() {
            return holding(self.head, self.queued);
        }
        let mut frame = BytesMut::with_capacity(self.byte_len());
        frame.extend_from_slice(&self.head);
        for segment in &self.data {
            frame.extend_from_slice(segment);
        }
        holding(frame.freeze(), self.queued)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Semaphore;

    use super::*;

    /// A frame that was admitted with all `permits` of the returned budget.
    fn admitted(frame: Outbound, permits: u32) -> (Outbound, Arc<Semaphore>) {
        let budget = Arc::new(Semaphore::new(permits as usize));
        let queued = budget.clone().try_acquire_many_owned(permits).unwrap();
        (frame.admitted(queued), budget)
    }

    #[test]
    fn single_segment_frames_keep_their_storage() {
        let head = Bytes::from_static(b"message");
        let pointer = head.as_ptr();
        assert_eq!(Outbound::from(head.clone()).into_bytes().as_ptr(), pointer);
        let segments: Vec<_> = Outbound::from(head).segments().collect();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].bytes.as_ptr(), pointer);
    }

    #[test]
    fn segments_keep_wire_order_and_skip_empty_ones() {
        let data = vec![
            Bytes::from_static(b"ab"),
            Bytes::new(),
            Bytes::from_static(b"cd"),
        ];
        let pointers: Vec<_> = [&data[0], &data[2]].map(|bytes| bytes.as_ptr()).into();
        let frame = || Outbound::with_data(Bytes::from_static(b"head"), data.clone());
        assert_eq!(frame().byte_len(), 8);
        let segments: Vec<_> = frame().segments().map(|segment| segment.bytes).collect();
        assert_eq!(segments, [&b"head"[..], b"ab", b"cd"]);
        // Segments are shared with the sender, not copied.
        assert_eq!(
            segments[1..].iter().map(|s| s.as_ptr()).collect::<Vec<_>>(),
            pointers
        );
        assert_eq!(frame().into_bytes(), b"headabcd"[..]);
    }

    /// A carrier drops segments as it writes them: the queued bytes last
    /// until the final nonempty one is dropped, whatever the frame's shape.
    #[test]
    fn queued_bytes_are_released_with_the_last_segment() {
        for (data, count) in [
            (vec![], 1),
            (vec![Bytes::new()], 1),
            (vec![Bytes::from_static(b"ab"), Bytes::new()], 2),
            (
                vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
                3,
            ),
        ] {
            let frame = Outbound::with_data(Bytes::from_static(b"head"), data);
            let (frame, budget) = admitted(frame, 5);
            let mut segments: Vec<_> = frame.segments().collect();
            assert_eq!(segments.len(), count);
            // Every segment but the last is written and dropped.
            segments.drain(..count - 1);
            assert_eq!(budget.available_permits(), 0);
            // The `Bytes` form of the last one still owns the queued bytes.
            let last = segments.pop().unwrap().into_bytes();
            assert_eq!(budget.available_permits(), 0);
            drop(last);
            assert_eq!(budget.available_permits(), 5);
        }
    }

    /// A frame without any byte has no segment to carry its queued bytes:
    /// taking its segments releases them at once.
    #[test]
    fn a_frame_without_bytes_releases_its_queued_bytes_when_taken() {
        let (frame, budget) = admitted(Outbound::from(Bytes::new()), 1);
        assert_eq!(frame.segments().count(), 0);
        assert_eq!(budget.available_permits(), 1);
    }

    #[test]
    fn the_joined_frame_owns_the_queued_bytes_until_it_is_dropped() {
        for data in [
            vec![],
            vec![Bytes::new()],
            vec![Bytes::from_static(b"x"), Bytes::from_static(b"y")],
        ] {
            let expected: Vec<u8> = std::iter::once(&b"head"[..])
                .chain(data.iter().map(|segment| &segment[..]))
                .flatten()
                .copied()
                .collect();
            let frame = Outbound::with_data(Bytes::from_static(b"head"), data);
            let (frame, budget) = admitted(frame, 3);
            let joined = frame.into_bytes();
            assert_eq!(joined, expected[..]);
            assert_eq!(budget.available_permits(), 0);
            drop(joined);
            assert_eq!(budget.available_permits(), 3);
        }
    }

    /// An admitted frame that never reaches a carrier returns its bytes.
    #[test]
    fn dropping_a_queued_frame_releases_its_queued_bytes() {
        let (frame, budget) = admitted(Outbound::from(Bytes::from_static(b"head")), 4);
        assert_eq!(budget.available_permits(), 0);
        drop(frame);
        assert_eq!(budget.available_permits(), 4);
    }
}
