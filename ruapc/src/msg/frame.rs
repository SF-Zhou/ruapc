//! Stream framing shared by TCP and HTTP/2.

use bytes::{Buf, Bytes, BytesMut};

use super::SendMsg;
use crate::{Error, ErrorKind, MsgMeta, Result};
use serde::Serialize;

pub(crate) const MAGIC_NUM: u32 = u32::from_be_bytes(*b"RUA!");
pub(crate) const MAX_MSG_SIZE: usize = 64 << 20;
const HEADER_LEN: usize = 8;

/// Encodes directly into the final allocation, including both length prefixes.
/// The same size limit applies to both stream senders and receivers.
pub(crate) fn encode<P: Serialize>(meta: &MsgMeta, payload: &P) -> Result<Bytes> {
    let mut frame = StreamFrame(BytesMut::with_capacity(512));
    meta.serialize_to(payload, &mut frame)?;
    Ok(frame.0.freeze())
}

struct StreamFrame(BytesMut);

impl SendMsg for StreamFrame {
    fn size(&self) -> usize {
        self.0.len()
    }

    fn prepare(&mut self) -> Result<()> {
        self.0.extend_from_slice(&MAGIC_NUM.to_be_bytes());
        self.0.extend_from_slice(&0u32.to_be_bytes());
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        self.0.finish(meta_offset, payload_offset)?;
        check_size(self.0.len() - HEADER_LEN)?;
        let body_len = u32::try_from(self.0.len() - HEADER_LEN)?;
        self.0[4..HEADER_LEN].copy_from_slice(&body_len.to_be_bytes());
        Ok(())
    }

    fn writer(&mut self) -> impl std::io::Write {
        self.0.writer()
    }
}

fn check_size(body_len: usize) -> Result<()> {
    if body_len >= MAX_MSG_SIZE - HEADER_LEN {
        return Err(Error::new(
            ErrorKind::TcpParseMsgFailed,
            format!("msg is too long: {body_len}"),
        ));
    }
    Ok(())
}

// Keep the successful header path in its callers, including the TCP parser;
// outlining this Result would add a call and obscure the checked header length.
#[inline]
fn parse_header(buffer: &[u8]) -> Result<Option<usize>> {
    if buffer.len() < HEADER_LEN {
        return Ok(None);
    }
    let header = u64::from_be_bytes(buffer[..HEADER_LEN].try_into().unwrap());
    if (header >> 32) as u32 != MAGIC_NUM {
        return Err(Error::new(
            ErrorKind::TcpParseMsgFailed,
            format!("invalid header: {header:08X}"),
        ));
    }

    let len = usize::try_from(header & u64::from(u32::MAX))?;
    check_size(len)?;
    Ok(Some(len))
}

/// Parse a single framed message from the buffer.
///
/// Wire format: `[4B magic][4B len][len bytes body]`.
/// Returns `Ok(None)` if the buffer doesn't contain a complete message yet.
pub(crate) fn parse_message(buffer: &mut BytesMut) -> Result<Option<Bytes>> {
    let Some(len) = parse_header(buffer)? else {
        return Ok(None);
    };
    if buffer.len() < HEADER_LEN + len {
        Ok(None)
    } else {
        buffer.advance(HEADER_LEN);
        Ok(Some(buffer.split_to(len).into()))
    }
}

/// Decodes owned chunks without copying frames contained in a single chunk.
/// Only an incomplete frame is buffered; allocation follows received bytes.
#[derive(Default)]
pub(crate) struct FrameDecoder {
    partial: BytesMut,
}

impl FrameDecoder {
    /// Consumes up to one frame, leaving any following frames in `input`.
    /// `Ok(None)` means all input was consumed into an incomplete frame.
    pub(crate) fn decode(&mut self, input: &mut Bytes) -> Result<Option<Bytes>> {
        if self.partial.is_empty() {
            if let Some(len) = parse_header(input)?
                && input.len() >= HEADER_LEN + len
            {
                input.advance(HEADER_LEN);
                return Ok(Some(input.split_to(len)));
            }
            self.partial.extend_from_slice(input);
            input.clear();
            return Ok(None);
        }

        // Finish and validate a split header before buffering any body bytes.
        if self.partial.len() < HEADER_LEN {
            let count = (HEADER_LEN - self.partial.len()).min(input.len());
            self.partial.extend_from_slice(&input[..count]);
            input.advance(count);
        }
        let Some(len) = parse_header(&self.partial)? else {
            return Ok(None);
        };

        // A split header does not require copying a body contained in this chunk.
        if self.partial.len() == HEADER_LEN && input.len() >= len {
            self.partial.clear();
            return Ok(Some(input.split_to(len)));
        }

        let remaining = HEADER_LEN + len - self.partial.len();
        let count = remaining.min(input.len());
        self.partial.extend_from_slice(&input[..count]);
        input.advance(count);
        if count < remaining {
            return Ok(None);
        }

        // Transfer the entire allocation: leaving an empty split tail here
        // would retain a large message even after its handler has finished.
        let mut frame = std::mem::take(&mut self.partial);
        frame.advance(HEADER_LEN);
        Ok(Some(frame.freeze()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_frames_survive_every_split_and_concatenation() {
        for flags in [
            crate::MsgFlags::IsReq,
            crate::MsgFlags::IsReq | crate::MsgFlags::UseMessagePack,
        ] {
            let meta = MsgMeta {
                flags,
                method: "Echo/echo".into(),
                ..Default::default()
            };
            let encoded = encode(&meta, &"hello").unwrap();
            for split in 0..encoded.len() {
                let mut received = BytesMut::from(&encoded[..split]);
                assert!(parse_message(&mut received).unwrap().is_none());
                received.extend_from_slice(&encoded[split..]);
                received.extend_from_slice(&encoded);
                for _ in 0..2 {
                    let message =
                        crate::Message::parse(parse_message(&mut received).unwrap().unwrap())
                            .unwrap();
                    assert_eq!(message.meta, meta);
                    assert_eq!(
                        message
                            .payload
                            .deserialize::<String>(&message.meta)
                            .unwrap(),
                        "hello"
                    );
                }
                assert!(received.is_empty());
            }
        }
    }

    fn make_header(magic: u32, len: u32) -> [u8; 8] {
        let h: u64 = ((magic as u64) << 32) | (len as u64);
        h.to_be_bytes()
    }

    #[test]
    fn chunk_decoder_preserves_complete_frame_storage() {
        let encoded = encode(&MsgMeta::default(), &"hello").unwrap();
        let mut input = encoded.clone();
        let mut decoder = FrameDecoder::default();
        let body = decoder.decode(&mut input).unwrap().unwrap();
        assert_eq!(body, encoded.slice(HEADER_LEN..));
        assert_eq!(body.as_ptr(), encoded.slice(HEADER_LEN..).as_ptr());
        assert!(input.is_empty());
        assert_eq!(decoder.partial.capacity(), 0);
    }

    #[test]
    fn chunk_decoder_survives_every_split_before_another_frame() {
        for flags in [
            crate::MsgFlags::IsReq,
            crate::MsgFlags::IsReq | crate::MsgFlags::UseMessagePack,
        ] {
            let meta = MsgMeta {
                flags,
                method: "Echo/echo".into(),
                ..Default::default()
            };
            let encoded = encode(&meta, &"hello").unwrap();
            for split in 0..encoded.len() {
                let mut decoder = FrameDecoder::default();
                let mut first = encoded.slice(..split);
                assert!(decoder.decode(&mut first).unwrap().is_none());
                assert!(first.is_empty());

                let mut tail = BytesMut::from(&encoded[split..]);
                tail.extend_from_slice(&encoded);
                let mut input = tail.freeze();
                let next_body = input.slice(encoded.len() - split + HEADER_LEN..);
                for index in 0..2 {
                    let body = decoder.decode(&mut input).unwrap().unwrap();
                    if index == 1 {
                        // Completing a fragmented frame must leave the next
                        // complete frame available without copying its bytes.
                        assert_eq!(body.as_ptr(), next_body.as_ptr());
                    }
                    let message = crate::Message::parse(body).unwrap();
                    assert_eq!(message.meta, meta);
                    assert_eq!(
                        message
                            .payload
                            .deserialize::<String>(&message.meta)
                            .unwrap(),
                        "hello"
                    );
                }
                assert!(input.is_empty());
                assert!(decoder.decode(&mut input).unwrap().is_none());
                assert!(decoder.partial.is_empty());
            }
        }
    }

    #[test]
    fn chunk_decoder_survives_repeated_fragmentation() {
        let meta = MsgMeta {
            flags: crate::MsgFlags::IsReq | crate::MsgFlags::UseMessagePack,
            method: "Echo/echo".into(),
            ..Default::default()
        };
        let encoded = encode(&meta, &"fragmented payload").unwrap();
        let mut joined = BytesMut::new();
        for _ in 0..3 {
            joined.extend_from_slice(&encoded);
        }
        let joined = joined.freeze();
        for chunk_size in 1..=joined.len() {
            let mut decoder = FrameDecoder::default();
            let mut messages = 0;
            for start in (0..joined.len()).step_by(chunk_size) {
                let mut input = joined.slice(start..(start + chunk_size).min(joined.len()));
                while let Some(body) = decoder.decode(&mut input).unwrap() {
                    assert_eq!(body, encoded.slice(HEADER_LEN..));
                    messages += 1;
                }
                assert!(input.is_empty());
                // Empty chunks do not discard buffered header/body fragments.
                assert!(decoder.decode(&mut Bytes::new()).unwrap().is_none());
            }
            assert_eq!(messages, 3, "chunk size {chunk_size}");
            assert!(decoder.partial.is_empty());
        }
    }

    #[test]
    fn chunk_decoder_preserves_body_storage_after_split_header() {
        let encoded = encode(&MsgMeta::default(), &"hello").unwrap();
        for split in 1..=HEADER_LEN {
            let mut decoder = FrameDecoder::default();
            let mut first = encoded.slice(..split);
            assert!(decoder.decode(&mut first).unwrap().is_none());
            let mut rest = encoded.slice(split..);
            let body = decoder.decode(&mut rest).unwrap().unwrap();
            assert_eq!(body.as_ptr(), encoded.slice(HEADER_LEN..).as_ptr());
            assert!(rest.is_empty());
            assert!(decoder.partial.is_empty());
        }
    }

    #[test]
    fn chunk_decoder_releases_completed_assembly_storage() {
        let encoded = encode(&MsgMeta::default(), &vec![42u8; 64 << 10]).unwrap();
        let mut decoder = FrameDecoder::default();
        let mut first = encoded.slice(..encoded.len() / 2);
        assert!(decoder.decode(&mut first).unwrap().is_none());
        assert!(decoder.partial.capacity() >= encoded.len() / 2);

        let mut rest = encoded.slice(encoded.len() / 2..);
        let body = decoder.decode(&mut rest).unwrap().unwrap();
        assert_eq!(body, encoded.slice(HEADER_LEN..));
        assert_eq!(decoder.partial.capacity(), 0);
        drop(body);

        // A later complete frame must not recreate or retain assembly storage.
        let mut complete = encoded.clone();
        let body = decoder.decode(&mut complete).unwrap().unwrap();
        assert_eq!(body.as_ptr(), encoded.slice(HEADER_LEN..).as_ptr());
        assert_eq!(decoder.partial.capacity(), 0);
    }

    #[test]
    fn chunk_decoder_checks_headers_before_buffering_bodies() {
        for (magic, len) in [
            (0, 4),
            (MAGIC_NUM, (MAX_MSG_SIZE - HEADER_LEN) as u32),
            (MAGIC_NUM, (MAX_MSG_SIZE - HEADER_LEN + 1) as u32),
            (MAGIC_NUM, u32::MAX),
        ] {
            let header = Bytes::copy_from_slice(&make_header(magic, len));
            let mut buffer = BytesMut::from(header.as_ref());
            assert_eq!(
                parse_message(&mut buffer).unwrap_err().kind,
                ErrorKind::TcpParseMsgFailed
            );
            for split in 0..HEADER_LEN {
                let mut decoder = FrameDecoder::default();
                let mut first = header.slice(..split);
                assert!(decoder.decode(&mut first).unwrap().is_none());
                let mut rest = header.slice(split..);
                assert_eq!(
                    decoder.decode(&mut rest).unwrap_err().kind,
                    ErrorKind::TcpParseMsgFailed
                );
            }
        }

        // The largest allowed length remains incomplete without a body and
        // must not cause an eager allocation based on its advertised length.
        let header = make_header(MAGIC_NUM, (MAX_MSG_SIZE - HEADER_LEN - 1) as u32);
        assert!(
            parse_message(&mut BytesMut::from(header.as_slice()))
                .unwrap()
                .is_none()
        );
        let mut decoder = FrameDecoder::default();
        assert!(
            decoder
                .decode(&mut Bytes::copy_from_slice(&header))
                .unwrap()
                .is_none()
        );
        assert!(decoder.partial.capacity() < MAX_MSG_SIZE / 2);
    }

    #[test]
    fn chunk_decoder_accepts_empty_frames_without_losing_the_next_frame() {
        let mut encoded = BytesMut::from(make_header(MAGIC_NUM, 0).as_slice());
        encoded.extend_from_slice(&make_header(MAGIC_NUM, 1));
        encoded.extend_from_slice(b"x");
        let mut input = encoded.freeze();
        let mut decoder = FrameDecoder::default();
        assert!(decoder.decode(&mut input).unwrap().unwrap().is_empty());
        assert_eq!(decoder.decode(&mut input).unwrap().unwrap(), b"x"[..]);
        assert!(decoder.decode(&mut input).unwrap().is_none());
    }

    #[test]
    fn test_parse_message_too_short_returns_none() {
        // Less than 8 bytes → no complete header yet.
        let mut buf = BytesMut::from(&[0x52u8, 0x55, 0x41][..]); // only 3 bytes
        assert!(parse_message(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_parse_message_invalid_magic_returns_error() {
        let mut buf = BytesMut::new();
        // magic = 0x00000000, len = 4
        buf.extend_from_slice(&make_header(0x00000000, 4));
        buf.extend_from_slice(&[0u8; 4]);
        let result = parse_message(&mut buf);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().kind,
            crate::error::ErrorKind::TcpParseMsgFailed
        ));
    }

    #[test]
    fn test_parse_message_too_long_returns_error() {
        let mut buf = BytesMut::new();
        // The complete frame would exceed MAX_MSG_SIZE.
        let too_long = (MAX_MSG_SIZE - std::mem::size_of::<u64>() + 1) as u32;
        buf.extend_from_slice(&make_header(MAGIC_NUM, too_long));
        let result = parse_message(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_message_incomplete_body_returns_none() {
        let mut buf = BytesMut::new();
        // Header says body is 100 bytes but we only provide 10.
        buf.extend_from_slice(&make_header(MAGIC_NUM, 100));
        buf.extend_from_slice(&[0u8; 10]);
        assert!(parse_message(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_parse_message_valid_returns_body() {
        let body = b"hello world";
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&make_header(MAGIC_NUM, body.len() as u32));
        buf.extend_from_slice(body);
        let result = parse_message(&mut buf).unwrap();
        assert_eq!(result.unwrap(), &body[..]);
        // Buffer should be empty after consuming the message.
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_message_zero_len_body() {
        // len = 0 is valid: just an empty body.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&make_header(MAGIC_NUM, 0));
        let result = parse_message(&mut buf).unwrap();
        assert_eq!(result.unwrap(), &[][..]);
    }
}
