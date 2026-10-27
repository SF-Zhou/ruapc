//! Metadata decoding from a borrowed message body.

use crate::{CodecError, CodecErrorKind, CodecResult, MsgMeta};

/// Decodes metadata and returns the start of the payload in `body`.
///
/// The body is `[meta_len: 4B][named-field MessagePack metadata][payload]`.
/// The returned offset is within `body`; the payload bytes are neither copied
/// nor decoded. Storage ownership and the lifetime of any payload view remain
/// with the caller.
///
/// # Errors
///
/// Returns an error for a missing length prefix, empty or out-of-bounds
/// metadata, or invalid MessagePack metadata.
#[inline]
pub fn parse_body(body: &[u8]) -> CodecResult<(MsgMeta, usize)> {
    const S: usize = std::mem::size_of::<u32>();
    let len = body.len();
    let meta_len = if let Some(bytes) = body.get(..S).and_then(|bytes| bytes.try_into().ok()) {
        u32::from_be_bytes(bytes) as usize
    } else {
        return Err(CodecError::new(
            CodecErrorKind::DeserializeFailed,
            format!("invalid msg length: {len}"),
        ));
    };

    if meta_len == 0 {
        return Err(CodecError::new(
            CodecErrorKind::DeserializeFailed,
            format!("invalid meta length: {meta_len}"),
        ));
    }

    if meta_len > len - S {
        return Err(CodecError::new(
            CodecErrorKind::DeserializeFailed,
            format!("invalid meta length: {meta_len}, msg length: {len}"),
        ));
    }

    let offset = S + meta_len;
    let meta = MsgMeta::decode(&body[S..offset])?;
    Ok((meta, offset))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
