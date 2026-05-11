pub(crate) const MAGIC_NUM: u32 = u32::from_be_bytes(*b"RUA!");
pub(crate) const MAX_MSG_SIZE: usize = 64 << 20;

mod tcp_socket;
pub(crate) use tcp_socket::TcpSocket;

mod tcp_socket_pool;
pub(crate) use tcp_socket_pool::TcpSocketPool;

use bytes::{Buf, Bytes, BytesMut};

use crate::error::{Error, ErrorKind, Result};

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
    if S + len >= MAX_MSG_SIZE {
        return Err(Error::new(
            ErrorKind::TcpParseMsgFailed,
            format!("msg is too long: {len}"),
        ));
    }

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
        // len = MAX_MSG_SIZE - 8 + 1 means S + len == MAX_MSG_SIZE which triggers the error.
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
