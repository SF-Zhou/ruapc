//! Control values with an unencoded trailing byte region.
//!
//! A raw payload is `[control_len: u32 BE][control][raw bytes]`. The control
//! value uses named-field MessagePack when `UseMessagePack` is set, otherwise
//! JSON. The outer frame bounds the raw region; callers validate its exact
//! expected length before using it. Encoding returns only the small prefix so
//! transports can retain and send the raw storage separately.

use std::io::Cursor;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::{CodecError, CodecErrorKind, CodecResult, MAX_DECODE_DEPTH, MsgFlags};

/// Encodes only the control prefix, without allocating or copying raw bytes.
/// The enclosing message must carry [`MsgFlags::HasRawBytes`].
pub fn encode_header<P: Serialize>(control: &P, flags: MsgFlags) -> CodecResult<Bytes> {
    let mut bytes = BytesMut::new();
    encode_into(control, flags, &mut bytes)?;
    Ok(bytes.freeze())
}

/// Appends a control prefix directly to the frame's metadata allocation.
pub(crate) fn encode_into<P: Serialize>(
    control: &P,
    flags: MsgFlags,
    bytes: &mut BytesMut,
) -> CodecResult<()> {
    check_flags(flags, CodecErrorKind::InvalidArgument)?;
    let start = bytes.len();
    bytes.extend_from_slice(&[0; 4]);
    if flags.contains(MsgFlags::UseMessagePack) {
        rmp_serde::encode::write_named(&mut bytes.writer(), control)?;
    } else {
        serde_json::to_writer(bytes.writer(), control)?;
    }
    let len = u32::try_from(bytes.len() - start - 4)?;
    if len == 0 {
        return Err(CodecError::new(
            CodecErrorKind::SerializeFailed,
            "raw control value must not be empty".into(),
        ));
    }
    bytes[start..start + 4].copy_from_slice(&len.to_be_bytes());
    Ok(())
}

/// Decodes the control value and borrows the raw region from the input.
/// Rejects missing/truncated prefixes, trailing control values and excessive
/// nesting. Neither decoding nor returning the raw region copies its bytes.
pub fn decode<P: for<'de> Deserialize<'de>>(
    payload: &[u8],
    flags: MsgFlags,
) -> CodecResult<(P, &[u8])> {
    check_flags(flags, CodecErrorKind::DeserializeFailed)?;
    let prefix: [u8; 4] = payload
        .get(..4)
        .and_then(|v| v.try_into().ok())
        .ok_or_else(invalid)?;
    let len = u32::from_be_bytes(prefix) as usize;
    let rest = &payload[4..];
    let control = rest
        .get(..len)
        .filter(|v| !v.is_empty())
        .ok_or_else(invalid)?;
    let value = if flags.contains(MsgFlags::UseMessagePack) {
        let mut decoder = rmp_serde::Deserializer::new(Cursor::new(control));
        decoder.set_max_depth(MAX_DECODE_DEPTH);
        let value = P::deserialize(&mut decoder)?;
        if decoder.position() != len as u64 {
            return Err(invalid());
        }
        value
    } else {
        serde_json::from_slice(control)?
    };
    Ok((value, &rest[len..]))
}

fn check_flags(flags: MsgFlags, kind: CodecErrorKind) -> CodecResult<()> {
    if !flags.contains(MsgFlags::HasRawBytes) {
        return Err(CodecError::new(kind, "missing raw payload flag".into()));
    }
    Ok(())
}

fn invalid() -> CodecError {
    CodecError::new(
        CodecErrorKind::DeserializeFailed,
        "invalid raw control prefix".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Control {
        request_id: u64,
    }

    #[test]
    fn raw_tail_is_borrowed_for_both_codecs() {
        for flags in [
            MsgFlags::HasRawBytes,
            MsgFlags::HasRawBytes | MsgFlags::UseMessagePack,
        ] {
            let control = Control { request_id: 42 };
            let mut bytes = encode_header(&control, flags).unwrap().to_vec();
            let offset = bytes.len();
            bytes.extend_from_slice(&[0, 255, 193, 42]);
            let (decoded, raw) = decode::<Control>(&bytes, flags).unwrap();
            assert_eq!(decoded, control);
            assert_eq!(raw, &[0, 255, 193, 42]);
            assert_eq!(raw.as_ptr(), bytes[offset..].as_ptr());
            assert!(crate::deserialize_payload::<Control>(&bytes, flags).is_err());
        }
    }

    #[test]
    fn invalid_control_never_exposes_raw_bytes() {
        let flags = MsgFlags::HasRawBytes | MsgFlags::UseMessagePack;
        let header = encode_header(&Control { request_id: 42 }, flags).unwrap();
        for end in 0..header.len() {
            assert!(decode::<Control>(&header[..end], flags).is_err());
        }
        assert!(decode::<Control>(&header, MsgFlags::UseMessagePack).is_err());
        for body in [vec![], vec![0xc0, 0xc0], vec![0xc1]] {
            let mut bytes = (body.len() as u32).to_be_bytes().to_vec();
            bytes.extend(body);
            assert!(decode::<()>(&bytes, flags).is_err());
        }
        let body = [vec![0x91; MAX_DECODE_DEPTH + 8], vec![0xc0]].concat();
        let mut bytes = (body.len() as u32).to_be_bytes().to_vec();
        bytes.extend(body);
        assert!(decode::<serde::de::IgnoredAny>(&bytes, flags).is_err());
    }

    #[test]
    fn raw_encoding_reports_invalid_arguments_and_units_roundtrip() {
        for codec in [MsgFlags::empty(), MsgFlags::UseMessagePack] {
            assert_eq!(
                encode_header(&(), codec).unwrap_err().kind,
                CodecErrorKind::InvalidArgument
            );
            assert_eq!(
                decode::<()>(&[], codec).unwrap_err().kind,
                CodecErrorKind::DeserializeFailed
            );
            let flags = codec | MsgFlags::HasRawBytes;
            let header = encode_header(&(), flags).unwrap();
            assert!(header.len() > 4);
            assert_eq!(decode::<()>(&header, flags).unwrap(), ((), &[][..]));
        }
    }
}
