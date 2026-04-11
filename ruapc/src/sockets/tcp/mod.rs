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
