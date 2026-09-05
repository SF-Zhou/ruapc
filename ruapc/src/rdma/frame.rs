//! RDMA wire framing shared by serialization and dispatch workers.

use crate::{Buffer, Result};
use bytes::Bytes;

/// Size of the per-frame header: a big-endian u32 frame length.
///
/// Every RDMA send is a sequence of `[4B frame_len][4B meta_len][meta]
/// [payload]` frames — usually one. Uniform framing makes messages
/// self-delimiting, so aggregation is plain concatenation and the receive
/// path has a single parse loop.
pub(crate) const FRAME_HEADER: usize = 4;

/// Serializes a message as one wire frame: `[4B frame_len][4B meta_len]
/// [meta][payload]`.
///
/// Every RDMA send is a sequence of such frames (usually one). The frame
/// header makes messages self-delimiting, so the poll thread can aggregate
/// window-blocked sends by plain concatenation and the receive side always
/// walks the same frame loop — no aggregation magic, no special cases.
pub(super) struct FramedBuffer<'a>(pub(super) &'a mut Buffer);

impl crate::msg::SendMsg for FramedBuffer<'_> {
    fn size(&self) -> usize {
        self.0.len()
    }

    fn prepare(&mut self) -> Result<()> {
        self.0.set_len(0);
        // Reserve the frame length header; patched in `finish`.
        self.0.extend_from_slice(&0u32.to_be_bytes())?;
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        let meta_len = u32::try_from(payload_offset - meta_offset - FRAME_HEADER)?;
        self.0[meta_offset..meta_offset + FRAME_HEADER].copy_from_slice(&meta_len.to_be_bytes());
        let frame_len = u32::try_from(self.0.len() - FRAME_HEADER)?;
        self.0[..FRAME_HEADER].copy_from_slice(&frame_len.to_be_bytes());
        Ok(())
    }

    fn writer(&mut self) -> impl std::io::Write {
        #[repr(transparent)]
        struct Writer<'a>(&'a mut Buffer);

        impl std::io::Write for Writer<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                self.0.extend_from_slice(buf).map_err(std::io::Error::other)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        Writer(self.0)
    }
}

/// Walks the `[4B len][message]` frames of one received buffer, invoking
/// `f` with each frame (a zero-copy slice of the refcounted buffer).
pub(super) fn for_each_frame(frames: &Bytes, mut f: impl FnMut(Bytes)) {
    let mut offset = 0;
    while offset < frames.len() {
        let Some(header) = frames.get(offset..offset + FRAME_HEADER) else {
            tracing::error!("truncated frame header at {offset}");
            return;
        };
        let frame_len = u32::from_be_bytes(header.try_into().unwrap()) as usize;
        let start = offset + FRAME_HEADER;
        let Some(end) = start
            .checked_add(frame_len)
            .filter(|end| *end <= frames.len())
        else {
            tracing::error!("truncated frame ({frame_len}B) at {offset}");
            return;
        };
        f(frames.slice(start..end));
        offset = end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_each_frame_walks_all_frames() {
        let mut buf = Vec::new();
        let frames: [&[u8]; 3] = [b"first", b"", b"third-frame"];
        for frame in frames {
            buf.extend_from_slice(&u32::try_from(frame.len()).unwrap().to_be_bytes());
            buf.extend_from_slice(frame);
        }
        let mut seen = Vec::new();
        for_each_frame(&Bytes::from(buf), |frame| seen.push(frame));
        assert_eq!(seen, frames.map(Bytes::from_static).to_vec());
    }

    #[test]
    fn test_for_each_frame_stops_on_truncation() {
        // Header claims 100 bytes but only 3 follow.
        let mut buf = 100u32.to_be_bytes().to_vec();
        buf.extend_from_slice(b"abc");
        let mut count = 0;
        for_each_frame(&Bytes::from(buf), |_| count += 1);
        assert_eq!(count, 0);

        // One valid frame, then a truncated header.
        let mut buf = 1u32.to_be_bytes().to_vec();
        buf.extend_from_slice(b"x");
        buf.extend_from_slice(&[0u8, 0]);
        let mut count = 0;
        for_each_frame(&Bytes::from(buf), |_| count += 1);
        assert_eq!(count, 1);
    }
}
