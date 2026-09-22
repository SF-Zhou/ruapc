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

/// Parse a single framed message from the buffer.
///
/// Wire format: `[4B magic][4B len][len bytes body]`.
/// Returns `Ok(None)` if the buffer doesn't contain a complete message yet.
pub(crate) fn parse_message(buffer: &mut BytesMut) -> Result<Option<Bytes>> {
    const S: usize = std::mem::size_of::<u64>();
    if buffer.len() < S {
        return Ok(None);
    }
    let header = u64::from_be_bytes(buffer[..S].try_into().unwrap());
    if (header >> 32) as u32 != MAGIC_NUM {
        return Err(Error::new(
            ErrorKind::TcpParseMsgFailed,
            format!("invalid header: {header:08X}"),
        ));
    }

    let len = usize::try_from(header & u64::from(u32::MAX))?;
    check_size(len)?;

    if buffer.len() < S + len {
        Ok(None)
    } else {
        buffer.advance(S);
        Ok(Some(buffer.split_to(len).into()))
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
