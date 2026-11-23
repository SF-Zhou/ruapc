//! Data frames: raw bytes pushed into a pending request's write space.

use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Deserializer, Serialize, de};

use super::HEADER_LEN;
use crate::{CodecError, CodecErrorKind, CodecResult, MAX_DECODE_DEPTH};

/// Magic prefix of a data frame, in place of [`MAGIC_NUM`](super::MAGIC_NUM).
pub const DATA_MAGIC_NUM: u32 = u32::from_be_bytes(*b"RUAD");
/// Most raw bytes one data frame carries. Larger transfers span several
/// frames, so no single frame holds a stream for long.
pub const MAX_DATA_LEN: usize = 1 << 20;
/// Longest encoded head of a data frame.
pub const MAX_DATA_HEAD_LEN: usize = 64 << 10;
/// Most explicit destination ranges in one write frame.
pub const MAX_DATA_RANGES: usize = 1024;

/// Size of a data frame's head length prefix.
const HEAD_LEN_PREFIX: usize = std::mem::size_of::<u32>();
/// Where the encoded head starts in a data frame.
const HEAD_OFFSET: usize = HEADER_LEN + HEAD_LEN_PREFIX;

/// A destination's `(byte offset, length)` in the write space, encoded as
/// the two-element MessagePack array `[offset, len]`.
pub type DataRange = (u64, u64);

/// The head of a data frame: where the frame's raw bytes go.
///
/// A data frame is `[RUAD: 4B][body_len: 4B][head_len: 4B][head][raw bytes]`,
/// with big-endian lengths and `body_len` counting everything after it. The
/// named-field MessagePack head selects how the receiver places the raw bytes.
/// The two length prefixes determine the raw byte count for both placements.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataHead {
    /// Message ID of a request pending on the connection that carries the frame.
    pub id: u64,
    /// How the receiver places the bytes in that request's write space.
    pub placement: DataPlacement,
}

/// Destination selection for the raw bytes of a data frame.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataPlacement {
    /// Continue a read through the receiver's own ordered destination plan.
    #[default]
    Packed,
    /// Write into the sender-specified ranges of a published write space.
    #[serde(rename = "write")]
    Ranges {
        /// Destination ranges, filled in order. Their lengths sum to the raw byte count.
        #[serde(deserialize_with = "deserialize_ranges::<MAX_DATA_RANGES, _>")]
        ranges: Vec<DataRange>,
        /// Message ID to answer once the receiver has consumed this frame.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ack: Option<u64>,
    },
}

/// Decodes a sequence of `[offset, len]` pairs with at most `LIMIT` entries.
/// A counted sequence is checked before allocating. A streaming sequence is
/// checked before storing each pair.
pub fn deserialize_ranges<'de, const LIMIT: usize, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<DataRange>, D::Error> {
    struct Ranges<const LIMIT: usize>;

    impl<'de, const LIMIT: usize> de::Visitor<'de> for Ranges<LIMIT> {
        type Value = Vec<DataRange>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "at most {LIMIT} data ranges")
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let count = seq.size_hint().unwrap_or(0);
            if count > LIMIT {
                return Err(de::Error::invalid_length(count, &self));
            }
            let mut ranges = Vec::with_capacity(count);
            while let Some(range) = seq.next_element()? {
                if ranges.len() == LIMIT {
                    return Err(de::Error::invalid_length(ranges.len() + 1, &self));
                }
                ranges.push(range);
            }
            Ok(ranges)
        }
    }

    deserializer.deserialize_seq(Ranges::<LIMIT>)
}

impl DataHead {
    fn check_data_len(&self, len: usize, kind: CodecErrorKind) -> CodecResult<()> {
        if len > MAX_DATA_LEN {
            return Err(CodecError::new(
                kind,
                format!("data frame exceeds {MAX_DATA_LEN} raw bytes"),
            ));
        }
        if let DataPlacement::Ranges { ranges, .. } = &self.placement {
            if ranges.len() > MAX_DATA_RANGES {
                return Err(CodecError::new(
                    kind,
                    format!("data frame exceeds {MAX_DATA_RANGES} ranges"),
                ));
            }
            let total = ranges
                .iter()
                .try_fold(0u64, |sum, &(_, len)| sum.checked_add(len));
            if total != Some(len as u64) {
                return Err(CodecError::new(
                    kind,
                    format!("data frame carries {len} bytes that its ranges do not describe"),
                ));
            }
        }
        Ok(())
    }

    /// Decodes a head whose frame carries `len` raw bytes.
    pub(super) fn decode(encoded: &[u8], len: usize) -> CodecResult<Self> {
        let mut deserializer = rmp_serde::Deserializer::from_read_ref(encoded);
        deserializer.set_max_depth(MAX_DECODE_DEPTH);
        let head = Self::deserialize(&mut deserializer)?;
        head.check_data_len(len, CodecErrorKind::InvalidFrame)?;
        Ok(head)
    }
}

/// Appends the header and head of a frame carrying `data_len` raw bytes to `out`.
/// The caller sends those bytes next, possibly as separate segments.
///
/// # Errors
///
/// Returns [`CodecErrorKind::InvalidArgument`] for a frame over any size or
/// range limit or whose explicit ranges describe a different raw length.
pub fn encode_data_head(head: &DataHead, data_len: usize, out: &mut BytesMut) -> CodecResult<()> {
    head.check_data_len(data_len, CodecErrorKind::InvalidArgument)?;
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
    Ok(())
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
        .checked_sub(HEAD_LEN_PREFIX)
        .and_then(|rest| rest.checked_sub(head_len))
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
