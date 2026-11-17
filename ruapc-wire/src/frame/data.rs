//! Data frames: raw bytes pushed into a pending request's write space.

use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};

use super::HEADER_LEN;
use crate::{CodecError, CodecErrorKind, CodecResult, MAX_DECODE_DEPTH};

/// Magic prefix of a data frame, in place of [`MAGIC_NUM`](super::MAGIC_NUM).
pub const DATA_MAGIC_NUM: u32 = u32::from_be_bytes(*b"RUAD");
/// Most raw bytes one data frame carries. Larger transfers span several
/// frames, so no single frame holds a stream for long.
pub const MAX_DATA_LEN: usize = 1 << 20;
/// Longest encoded head of a data frame.
pub const MAX_DATA_HEAD_LEN: usize = 64 << 10;

/// Size of a data frame's head length prefix.
const HEAD_LEN_PREFIX: usize = std::mem::size_of::<u32>();
/// Where the encoded head starts in a data frame.
const HEAD_OFFSET: usize = HEADER_LEN + HEAD_LEN_PREFIX;

/// A contiguous destination range in a write space.
///
/// Encoded as the two-element array `[offset, len]`: a head lists up to one
/// range per copy operation, so field names would dominate it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "(u64, u64)", into = "(u64, u64)")]
pub struct DataRange {
    /// Byte offset into the receiver's write space.
    pub offset: u64,
    /// Number of bytes for this range.
    pub len: u64,
}

impl From<(u64, u64)> for DataRange {
    fn from((offset, len): (u64, u64)) -> Self {
        Self { offset, len }
    }
}

impl From<DataRange> for (u64, u64) {
    fn from(range: DataRange) -> Self {
        (range.offset, range.len)
    }
}

/// The head of a data frame: where the frame's raw bytes go.
///
/// A data frame is `[RUAD: 4B][body_len: 4B][head_len: 4B][head][raw bytes]`,
/// with big-endian lengths and `body_len` counting everything after it. The
/// head is named-field MessagePack. The raw bytes fill [`ranges`](Self::ranges)
/// in order, so their count is the sum of the range lengths, at most
/// [`MAX_DATA_LEN`].
///
/// A frame describes bytes; it is no evidence that the receiver has a request
/// `id`, that the request lent a write space, or that the ranges fit it.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataHead {
    /// Message ID of a request pending at the receiver. The bytes go to the
    /// write space which that request attached, on this connection.
    pub id: u64,
    /// Message ID that the receiver answers with an ordinary response message
    /// once it has consumed this frame, reporting whether the write space
    /// accepted it. `None` requests no answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack: Option<u64>,
    /// Destination ranges, filled in order by the frame's raw bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<DataRange>,
}

impl DataHead {
    /// Number of raw bytes a frame with this head carries.
    ///
    /// # Errors
    ///
    /// Returns [`CodecErrorKind::InvalidArgument`] when the ranges describe
    /// more than [`MAX_DATA_LEN`] bytes.
    pub fn data_len(&self) -> CodecResult<usize> {
        self.ranges
            .iter()
            .try_fold(0u64, |total, range| total.checked_add(range.len))
            .and_then(|total| usize::try_from(total).ok())
            .filter(|&total| total <= MAX_DATA_LEN)
            .ok_or_else(|| {
                CodecError::new(
                    CodecErrorKind::InvalidArgument,
                    format!("data frame ranges exceed {MAX_DATA_LEN} bytes"),
                )
            })
    }

    /// Decodes a head whose frame carries `len` raw bytes.
    pub(super) fn decode(encoded: &[u8], len: usize) -> CodecResult<Self> {
        let mut deserializer = rmp_serde::Deserializer::from_read_ref(encoded);
        deserializer.set_max_depth(MAX_DECODE_DEPTH);
        let head = Self::deserialize(&mut deserializer)?;
        // The ranges alone say where each byte goes, so they must account
        // for exactly the bytes that follow.
        if head.data_len().ok() != Some(len) {
            return Err(CodecError::new(
                CodecErrorKind::InvalidFrame,
                format!("data frame carries {len} bytes that its ranges do not describe"),
            ));
        }
        Ok(head)
    }
}

/// Appends a data frame's header and head to `out` and returns the number of
/// raw bytes that complete the frame.
///
/// The caller sends exactly that many bytes after the appended ones, either
/// in the same storage or as separate segments of the same frame.
///
/// # Errors
///
/// Returns [`CodecErrorKind::InvalidArgument`] when the ranges describe more
/// than [`MAX_DATA_LEN`] bytes or the encoded head exceeds
/// [`MAX_DATA_HEAD_LEN`]. `out` then keeps a partial encoding.
pub fn encode_data_head(head: &DataHead, out: &mut BytesMut) -> CodecResult<usize> {
    let data_len = head.data_len()?;
    let start = out.len();
    out.put_bytes(0, HEAD_OFFSET);
    rmp_serde::encode::write_named(&mut out.writer(), head)?;
    let head_len = out.len() - start - HEAD_OFFSET;
    if head_len > MAX_DATA_HEAD_LEN {
        return Err(CodecError::new(
            CodecErrorKind::InvalidArgument,
            format!("data frame head of {head_len} bytes exceeds {MAX_DATA_HEAD_LEN}"),
        ));
    }
    let body_len = u32::try_from(HEAD_LEN_PREFIX + head_len + data_len)?;
    let prefix = &mut out[start..start + HEAD_OFFSET];
    prefix[..4].copy_from_slice(&DATA_MAGIC_NUM.to_be_bytes());
    prefix[4..HEADER_LEN].copy_from_slice(&body_len.to_be_bytes());
    prefix[HEADER_LEN..].copy_from_slice(&u32::try_from(head_len)?.to_be_bytes());
    Ok(data_len)
}

/// Checks a data frame's body length before any of the body is buffered.
#[inline]
pub(super) fn check_body_len(body_len: usize) -> CodecResult<()> {
    if !(HEAD_LEN_PREFIX..=HEAD_LEN_PREFIX + MAX_DATA_HEAD_LEN + MAX_DATA_LEN).contains(&body_len) {
        return Err(CodecError::new(
            CodecErrorKind::InvalidFrame,
            format!("invalid data frame length: {body_len}"),
        ));
    }
    Ok(())
}

/// How far the bytes at the start of a data frame get its head.
pub(super) enum HeadParse {
    /// The head is complete. `prefix` bytes precede the frame's `len` raw
    /// bytes.
    Complete {
        head: DataHead,
        prefix: usize,
        len: usize,
    },
    /// The head needs the frame's first `needed` bytes, as far as the bytes
    /// held so far tell: more than are held.
    Needs(usize),
}

/// Parses the head of a data frame whose header announced `body_len`, from
/// the bytes of the frame received so far. The one place that checks the
/// head length and decodes the head, for every carrier.
#[inline]
pub(super) fn parse_head(frame: &[u8], body_len: usize) -> CodecResult<HeadParse> {
    let Some(head_len) = frame.get(HEADER_LEN..HEAD_OFFSET) else {
        return Ok(HeadParse::Needs(HEAD_OFFSET));
    };
    let head_len = usize::try_from(u32::from_be_bytes(head_len.try_into().unwrap()))?;
    let len = body_len
        .checked_sub(HEAD_LEN_PREFIX + head_len)
        .filter(|&len| (1..=MAX_DATA_HEAD_LEN).contains(&head_len) && len <= MAX_DATA_LEN)
        .ok_or_else(|| {
            CodecError::new(
                CodecErrorKind::InvalidFrame,
                format!("invalid data frame head length {head_len} in a body of {body_len}"),
            )
        })?;
    let prefix = HEAD_OFFSET + head_len;
    let Some(encoded) = frame.get(HEAD_OFFSET..prefix) else {
        return Ok(HeadParse::Needs(prefix));
    };
    let head = DataHead::decode(encoded, len)?;
    Ok(HeadParse::Complete { head, prefix, len })
}
