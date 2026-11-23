//! Message and data framing shared by TCP, HTTP/2, WebSocket and RDMA.

use bytes::{Buf, Bytes, BytesMut};

use super::SendMsg;
use crate::{CodecError, CodecErrorKind, CodecResult, MsgMeta};
use serde::Serialize;

mod data;
use data::HeadParse;
pub use data::{
    DATA_MAGIC_NUM, DataHead, DataPlacement, DataRange, MAX_DATA_HEAD_LEN, MAX_DATA_LEN,
    MAX_DATA_RANGES, deserialize_ranges, encode_data_head,
};

/// Magic prefix of a message frame on all framed transports.
pub const MAGIC_NUM: u32 = u32::from_be_bytes(*b"RUA!");
/// Exclusive upper bound on the complete frame size, in bytes.
pub const MAX_MSG_SIZE: usize = 64 << 20;
/// Size of `[4B magic][4B body length]`, excluding the metadata length prefix.
pub const HEADER_LEN: usize = 8;
/// Largest reservation a receiver makes for a frame beyond the bytes of that
/// frame it already holds. A header alone therefore cannot make a receiver
/// allocate its advertised length; larger frames grow with the received bytes.
pub const EAGER_RESERVE: usize = 1 << 20;

/// Encodes a frame header after validating the shared complete-frame size limit.
/// Direct serializers can reserve [`HEADER_LEN`] bytes and backfill this header
/// once their body has been written into transport-owned storage.
#[inline]
pub fn encode_header(body_len: usize) -> CodecResult<[u8; HEADER_LEN]> {
    check_size(body_len)?;
    let body_len = u32::try_from(body_len)?;
    Ok(((u64::from(MAGIC_NUM) << 32) | u64::from(body_len)).to_be_bytes())
}

/// Encodes directly into the final allocation, including both length prefixes.
/// The same size limit applies to all framed senders and receivers.
///
/// TCP and HTTP/2 streams carry frames back to back; each WebSocket binary
/// message carries exactly one frame. One RDMA SEND can carry multiple frames.
pub fn encode<P: Serialize>(meta: &MsgMeta, payload: &P) -> CodecResult<Bytes> {
    let mut frame = StreamFrame(BytesMut::with_capacity(512));
    meta.serialize_to(payload, &mut frame)?;
    Ok(frame.0.freeze())
}

struct StreamFrame(BytesMut);

impl SendMsg for StreamFrame {
    type Error = CodecError;

    #[inline]
    fn size(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn prepare(&mut self) -> CodecResult<()> {
        self.0.clear();
        self.0.extend_from_slice(&[0; HEADER_LEN]);
        Ok(())
    }

    #[inline]
    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> CodecResult<()> {
        self.0.finish(meta_offset, payload_offset)?;
        let header = encode_header(self.0.len() - HEADER_LEN)?;
        self.0[..HEADER_LEN].copy_from_slice(&header);
        Ok(())
    }

    #[inline]
    fn writer(&mut self) -> impl std::io::Write {
        self.0.writer()
    }
}

#[inline]
fn check_size(body_len: usize) -> CodecResult<()> {
    if body_len >= MAX_MSG_SIZE - HEADER_LEN {
        return Err(CodecError::new(
            CodecErrorKind::InvalidFrame,
            format!("msg is too long: {body_len}"),
        ));
    }
    Ok(())
}

/// A validated frame header: the frame's kind and body length.
#[derive(Clone, Copy)]
enum Header {
    Message(usize),
    Data(usize),
}

// Keep the successful header path in its callers, including the TCP parser;
// outlining this result would add a call and obscure the checked header length.
#[inline]
fn parse_any_header(buffer: &[u8]) -> CodecResult<Option<Header>> {
    if buffer.len() < HEADER_LEN {
        return Ok(None);
    }
    let header = u64::from_be_bytes(buffer[..HEADER_LEN].try_into().unwrap());
    let len = usize::try_from(header & u64::from(u32::MAX))?;
    match (header >> 32) as u32 {
        MAGIC_NUM => {
            check_size(len)?;
            Ok(Some(Header::Message(len)))
        }
        DATA_MAGIC_NUM => {
            data::check_body_len(len)?;
            Ok(Some(Header::Data(len)))
        }
        _ => Err(CodecError::new(
            CodecErrorKind::InvalidFrame,
            format!("invalid header: {header:08X}"),
        )),
    }
}

/// Parses the header of a message frame. Carriers that never transport data
/// frames reject them here.
#[inline]
fn parse_header(buffer: &[u8]) -> CodecResult<Option<usize>> {
    match parse_any_header(buffer)? {
        None => Ok(None),
        Some(Header::Message(len)) => Ok(Some(len)),
        Some(Header::Data(_)) => Err(unexpected_data_frame()),
    }
}

#[cold]
fn unexpected_data_frame() -> CodecError {
    CodecError::new(
        CodecErrorKind::InvalidFrame,
        "data frame where only message frames are accepted".into(),
    )
}

/// The next frame of a stream.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// The complete body of a message frame: `[meta_len][meta][payload]`.
    Message(Bytes),
    /// The decoded head of a data frame. The frame's `len` raw bytes are not
    /// consumed: they are the next `len` bytes of the stream, and the caller
    /// must take exactly that many before parsing another frame.
    Data {
        /// What the bytes are for.
        head: DataHead,
        /// Number of raw bytes that follow the head.
        len: usize,
    },
}

/// Parse a single framed message from the buffer.
///
/// Wire format: `[4B magic][4B len][len bytes body]`.
/// Returns `Ok(None)` if the buffer doesn't contain a complete message yet.
/// A data frame is an error; streams that carry them use [`parse_frame`].
#[inline]
pub fn parse_message(buffer: &mut BytesMut) -> CodecResult<Option<Bytes>> {
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

/// What [`parse_frame`] found at the start of a stream buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum Parsed {
    /// A frame, consumed from the buffer as [`Frame`] describes.
    Frame(Frame),
    /// The buffer holds only the start of a frame. At least `missing` more
    /// bytes are needed before another call can succeed, as far as the
    /// buffered bytes tell; the buffer has room for them, within the
    /// [`EAGER_RESERVE`] bound.
    Incomplete {
        /// Bytes still missing from the header, the message frame or the
        /// data frame's head, whichever the buffered bytes identify.
        missing: usize,
    },
}

impl Parsed {
    /// The parsed frame, or `None` for an incomplete one.
    #[inline]
    pub fn frame(self) -> Option<Frame> {
        match self {
            Self::Frame(frame) => Some(frame),
            Self::Incomplete { .. } => None,
        }
    }
}

/// Parses the next frame of a stream that carries message and data frames.
///
/// A message frame is complete once the buffer holds all of it. A data frame
/// is complete with its header and head: its raw bytes stay in the stream,
/// see [`Frame::Data`], and are never buffered on its behalf.
///
/// An incomplete frame reports how many bytes it still needs and reserves
/// room for them, so that it is received into one allocation instead of
/// repeatedly doubled ones. Nothing is reserved until the header is complete,
/// and at most the larger of [`EAGER_RESERVE`] and the bytes already
/// buffered: allocation follows received bytes, never the advertised length
/// alone.
#[inline]
pub fn parse_frame(buffer: &mut BytesMut) -> CodecResult<Parsed> {
    let needed = match parse_any_header(buffer)? {
        None => {
            return Ok(Parsed::Incomplete {
                missing: HEADER_LEN - buffer.len(),
            });
        }
        Some(Header::Message(len)) => {
            if buffer.len() >= HEADER_LEN + len {
                buffer.advance(HEADER_LEN);
                return Ok(Parsed::Frame(Frame::Message(buffer.split_to(len).into())));
            }
            HEADER_LEN + len
        }
        Some(Header::Data(body_len)) => match data::parse_head(buffer, body_len)? {
            HeadParse::Complete { head, prefix, len } => {
                buffer.advance(prefix);
                return Ok(Parsed::Frame(Frame::Data { head, len }));
            }
            HeadParse::Needs(needed) => needed,
        },
    };
    let missing = needed - buffer.len();
    reserve_missing(buffer, missing, buffer.len());
    Ok(Parsed::Incomplete { missing })
}

/// Makes room for a frame's `missing` bytes once `received` bytes of it are
/// held, reserving at most the larger of [`EAGER_RESERVE`] and `received`.
#[inline]
fn reserve_missing(buffer: &mut BytesMut, missing: usize, received: usize) {
    if missing > buffer.capacity() - buffer.len() {
        buffer.reserve(missing.min(received.max(EAGER_RESERVE)));
    }
}

/// Decodes all complete frames in a bounded transport message, such as an
/// RDMA SEND. Each callback receives a zero-copy body that owns shared input
/// storage and remains valid after `frames` is dropped.
///
/// Unlike the incremental parsers, this rejects an incomplete header or body,
/// and accepts message frames only.
/// Callbacks for earlier complete frames may already have run when a later
/// frame fails validation. Callback errors are returned unchanged and stop
/// subsequent decoding; framing errors convert through `From<CodecError>`.
#[inline]
pub fn decode_frames<E: From<CodecError>>(
    frames: &Bytes,
    mut f: impl FnMut(Bytes) -> Result<(), E>,
) -> Result<(), E> {
    let mut offset = 0;
    while offset < frames.len() {
        let body_len = parse_header(&frames[offset..])?.ok_or_else(|| {
            CodecError::new(
                CodecErrorKind::InvalidFrame,
                format!("truncated frame header at {offset}"),
            )
        })?;
        let body_start = offset + HEADER_LEN;
        let end = body_start
            .checked_add(body_len)
            .filter(|&end| end <= frames.len())
            .ok_or_else(|| {
                CodecError::new(
                    CodecErrorKind::InvalidFrame,
                    format!("truncated frame body ({body_len}B) at {body_start}"),
                )
            })?;
        f(frames.slice(body_start..end))?;
        offset = end;
    }
    Ok(())
}

/// Decodes owned chunks without copying frames contained in a single chunk.
/// Only an incomplete message frame, or the incomplete head of a data frame,
/// is buffered; allocation follows received bytes, reserving at most
/// [`EAGER_RESERVE`] ahead of them.
#[derive(Default)]
pub struct FrameDecoder {
    partial: BytesMut,
}

impl FrameDecoder {
    /// Whether the decoder holds bytes of an incomplete frame that a caller
    /// still has to supply, as after its input ended inside a header, a
    /// message body or a data head.
    #[inline]
    pub fn has_partial(&self) -> bool {
        !self.partial.is_empty()
    }

    /// Consumes up to one message frame, leaving any following frames in
    /// `input`. `Ok(None)` means all input was consumed into an incomplete
    /// frame. A data frame is an error; streams that carry them use
    /// [`decode_frame`](Self::decode_frame).
    #[inline]
    pub fn decode(&mut self, input: &mut Bytes) -> CodecResult<Option<Bytes>> {
        match self.decode_frame(input)? {
            None => Ok(None),
            Some(Frame::Message(body)) => Ok(Some(body)),
            Some(Frame::Data { .. }) => Err(unexpected_data_frame()),
        }
    }

    /// Consumes up to one frame, leaving any following bytes in `input`.
    /// `Ok(None)` means all input was consumed into an incomplete frame.
    ///
    /// After [`Frame::Data`], `input` starts with that frame's raw bytes,
    /// which may continue in later chunks. The caller takes them itself and
    /// calls this again only once it has taken all of them.
    #[inline]
    pub fn decode_frame(&mut self, input: &mut Bytes) -> CodecResult<Option<Frame>> {
        if self.partial.is_empty() {
            match parse_any_header(input)? {
                Some(Header::Message(len)) => {
                    let total = HEADER_LEN + len;
                    if input.len() >= total {
                        input.advance(HEADER_LEN);
                        return Ok(Some(Frame::Message(input.split_to(len))));
                    }
                    reserve_missing(&mut self.partial, total, input.len());
                }
                Some(Header::Data(body_len)) => {
                    if let HeadParse::Complete { head, prefix, len } =
                        data::parse_head(input, body_len)?
                    {
                        input.advance(prefix);
                        return Ok(Some(Frame::Data { head, len }));
                    }
                }
                None => {}
            }
            self.partial.extend_from_slice(input);
            input.clear();
            return Ok(None);
        }

        // Finish and validate a split header before buffering any body bytes.
        self.fill(input, HEADER_LEN);
        match parse_any_header(&self.partial)? {
            None => Ok(None),
            Some(Header::Message(len)) => self.finish_message(input, len),
            // Only the head is assembled; the raw bytes stay in `input`.
            Some(Header::Data(body_len)) => loop {
                match data::parse_head(&self.partial, body_len)? {
                    HeadParse::Complete { head, len, .. } => {
                        self.partial = BytesMut::new();
                        return Ok(Some(Frame::Data { head, len }));
                    }
                    HeadParse::Needs(needed) => {
                        self.fill(input, needed);
                        if self.partial.len() < needed {
                            return Ok(None);
                        }
                    }
                }
            },
        }
    }

    /// Moves input into the assembly buffer until it holds `target` bytes.
    #[inline]
    fn fill(&mut self, input: &mut Bytes, target: usize) {
        if self.partial.len() < target {
            let count = (target - self.partial.len()).min(input.len());
            self.partial.extend_from_slice(&input[..count]);
            input.advance(count);
        }
    }

    /// Completes a message frame whose header is in the assembly buffer.
    #[inline]
    fn finish_message(&mut self, input: &mut Bytes, len: usize) -> CodecResult<Option<Frame>> {
        // A split header does not require copying a body contained in this chunk.
        if self.partial.len() == HEADER_LEN && input.len() >= len {
            self.partial.clear();
            return Ok(Some(Frame::Message(input.split_to(len))));
        }

        let remaining = HEADER_LEN + len - self.partial.len();
        let count = remaining.min(input.len());
        let received = self.partial.len() + count;
        reserve_missing(&mut self.partial, remaining, received);
        self.partial.extend_from_slice(&input[..count]);
        input.advance(count);
        if count < remaining {
            return Ok(None);
        }

        // Transfer the entire allocation: leaving an empty split tail here
        // would retain a large message even after its handler has finished.
        let mut frame = std::mem::take(&mut self.partial);
        frame.advance(HEADER_LEN);
        Ok(Some(Frame::Message(frame.freeze())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MsgFlags, deserialize_payload, parse_body};

    #[test]
    fn reused_stream_frame_replaces_header_body_and_old_tail() {
        for flags in [MsgFlags::IsReq, MsgFlags::IsReq | MsgFlags::UseMessagePack] {
            let mut frame = StreamFrame(BytesMut::with_capacity(8192));
            let pointer = frame.0.as_ptr();
            let capacity = frame.0.capacity();
            let mut meta = MsgMeta {
                flags,
                method: "Echo/first".into(),
                msgid: 1,
                ..Default::default()
            };
            meta.serialize_to(&"x".repeat(4096), &mut frame).unwrap();
            let previous_len = frame.0.len();

            meta.method = "Echo/second".into();
            meta.msgid = 2;
            meta.serialize_to(&"new payload", &mut frame).unwrap();

            assert!(frame.0.len() < previous_len);
            assert_eq!(
                frame.0.as_ref(),
                encode(&meta, &"new payload").unwrap().as_ref()
            );
            assert_eq!(frame.0.as_ptr(), pointer);
            assert_eq!(frame.0.capacity(), capacity);
        }
    }

    #[test]
    fn encode_and_decode_share_the_frame_size_limit() {
        // The exclusive limit rejects the first body length that would make a
        // complete frame reach MAX_MSG_SIZE; the decoder tests pin the same
        // boundary from the receive side.
        assert!(check_size(MAX_MSG_SIZE - HEADER_LEN - 1).is_ok());
        assert_eq!(
            check_size(MAX_MSG_SIZE - HEADER_LEN).unwrap_err().kind,
            CodecErrorKind::InvalidFrame
        );
    }

    #[test]
    fn encoded_frames_survive_every_split_and_concatenation() {
        for flags in [MsgFlags::IsReq, MsgFlags::IsReq | MsgFlags::UseMessagePack] {
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
                    let body = parse_message(&mut received).unwrap().unwrap();
                    let (decoded, offset) = parse_body(&body).unwrap();
                    assert_eq!(decoded, meta);
                    assert_eq!(
                        deserialize_payload::<String>(&body[offset..], decoded.flags).unwrap(),
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

    /// How many bytes an incomplete buffer still needs.
    fn missing(buffer: &mut BytesMut) -> usize {
        match parse_frame(buffer).unwrap() {
            Parsed::Incomplete { missing } => missing,
            Parsed::Frame(frame) => panic!("expected an incomplete frame, got {frame:?}"),
        }
    }

    #[test]
    fn incomplete_frames_reserve_their_remaining_bytes_once() {
        let encoded = encode(&MsgMeta::default(), &vec![7u8; 300 << 10]).unwrap();
        let mut buffer = BytesMut::with_capacity(64);
        // An incomplete header reserves nothing.
        buffer.extend_from_slice(&encoded[..HEADER_LEN - 1]);
        assert_eq!(missing(&mut buffer), 1);
        assert_eq!(buffer.capacity(), 64);

        buffer.extend_from_slice(&encoded[HEADER_LEN - 1..100]);
        assert_eq!(missing(&mut buffer), encoded.len() - 100);
        assert!(buffer.capacity() >= encoded.len());
        let storage = buffer.as_ptr();
        // Asking again keeps the reservation.
        assert_eq!(missing(&mut buffer), encoded.len() - 100);
        buffer.extend_from_slice(&encoded[100..]);
        assert_eq!(buffer.as_ptr(), storage);

        assert_eq!(
            parse_frame(&mut buffer).unwrap(),
            Parsed::Frame(Frame::Message(encoded.slice(HEADER_LEN..)))
        );
        // An empty buffer misses a whole header.
        assert_eq!(missing(&mut buffer), HEADER_LEN);
    }

    #[test]
    fn reservations_follow_received_bytes_not_the_advertised_length() {
        let header = make_header(MAGIC_NUM, (MAX_MSG_SIZE - HEADER_LEN - 1) as u32);
        let mut buffer = BytesMut::from(header.as_slice());
        assert_eq!(missing(&mut buffer), MAX_MSG_SIZE - HEADER_LEN - 1);
        assert!(buffer.capacity() >= EAGER_RESERVE);
        assert!(buffer.capacity() < 4 * EAGER_RESERVE);

        // Once more than the eager bound has arrived, growth at most doubles.
        buffer.resize(3 * EAGER_RESERVE, 0);
        let mut full = BytesMut::from(&buffer[..]);
        missing(&mut full);
        assert!(full.capacity() >= 6 * EAGER_RESERVE);
        assert!(full.capacity() < 12 * EAGER_RESERVE);

        let mut invalid = BytesMut::from(make_header(0, 4).as_slice());
        assert_eq!(
            parse_frame(&mut invalid).unwrap_err().kind,
            CodecErrorKind::InvalidFrame
        );
    }

    #[test]
    fn chunk_decoder_assembles_a_fragmented_frame_in_one_allocation() {
        let encoded = encode(&MsgMeta::default(), &vec![42u8; 64 << 10]).unwrap();
        for first in [1, HEADER_LEN - 1, HEADER_LEN, HEADER_LEN + 1, 4096] {
            let mut decoder = FrameDecoder::default();
            let mut storage = None;
            let mut body = None;
            let mut start = 0;
            let mut chunk = first;
            while body.is_none() {
                let end = (start + chunk).min(encoded.len());
                let mut input = encoded.slice(start..end);
                body = decoder.decode(&mut input).unwrap();
                assert!(input.is_empty());
                if body.is_none() && decoder.partial.len() >= HEADER_LEN {
                    // The header fixed the assembly buffer for the whole frame.
                    assert!(decoder.partial.capacity() >= encoded.len());
                    let pointer = decoder.partial.as_ptr();
                    assert_eq!(*storage.get_or_insert(pointer), pointer, "first {first}");
                }
                start = end;
                chunk = 1000;
            }
            assert_eq!(body.unwrap(), encoded.slice(HEADER_LEN..));
            assert_eq!(decoder.partial.capacity(), 0);
        }
    }

    fn data_head(lens: &[u64]) -> DataHead {
        DataHead {
            id: 7,
            placement: DataPlacement::Ranges {
                ack: Some(u64::MAX),
                ranges: lens
                    .iter()
                    .enumerate()
                    .map(|(index, &len)| (100 * index as u64, len))
                    .collect(),
            },
        }
    }

    /// A data frame carrying `data`, and the length of everything before it.
    fn data_frame(head: &DataHead, data: &[u8]) -> (Bytes, usize) {
        let mut frame = BytesMut::new();
        encode_data_head(head, data.len(), &mut frame).unwrap();
        let prefix = frame.len();
        frame.extend_from_slice(data);
        (frame.freeze(), prefix)
    }

    #[test]
    fn data_frames_leave_their_bytes_in_the_stream_at_every_split() {
        let head = data_head(&[3, 0, 5]);
        let (encoded, prefix) = data_frame(&head, b"abcdefgh");
        assert_eq!(&encoded[..4], b"RUAD");
        assert_eq!(
            u32::from_be_bytes(encoded[4..HEADER_LEN].try_into().unwrap()) as usize,
            encoded.len() - HEADER_LEN
        );
        let message = encode(&MsgMeta::default(), &"next").unwrap();
        for split in 0..encoded.len() {
            let mut received = BytesMut::from(&encoded[..split]);
            let data = || Frame::Data {
                head: head.clone(),
                len: 8,
            };
            if split < prefix {
                // Only the head is awaited, never the raw bytes.
                let missing = missing(&mut received);
                assert!((1..=prefix - split).contains(&missing), "split {split}");
                received.extend_from_slice(&encoded[split..]);
                assert_eq!(parse_frame(&mut received).unwrap().frame(), Some(data()));
            } else {
                assert_eq!(parse_frame(&mut received).unwrap().frame(), Some(data()));
                received.extend_from_slice(&encoded[split..]);
            }
            // The parser consumed the head and nothing of the raw bytes.
            assert_eq!(&received[..], b"abcdefgh");
            received.advance(8);
            received.extend_from_slice(&message);
            assert_eq!(
                parse_frame(&mut received).unwrap().frame(),
                Some(Frame::Message(message.slice(HEADER_LEN..)))
            );
            assert!(received.is_empty());
        }
    }

    #[test]
    fn chunk_decoder_yields_data_heads_without_buffering_their_bytes() {
        let head = data_head(&[4, 4]);
        let (encoded, prefix) = data_frame(&head, b"01234567");
        let message = encode(&MsgMeta::default(), &"next").unwrap();
        let mut joined = BytesMut::from(encoded.as_ref());
        joined.extend_from_slice(&message);
        joined.extend_from_slice(&encoded);
        let joined = joined.freeze();
        for chunk_size in 1..=joined.len() {
            let mut decoder = FrameDecoder::default();
            let mut frames = Vec::new();
            let mut data = Vec::new();
            let mut awaited = 0;
            for start in (0..joined.len()).step_by(chunk_size) {
                let mut input = joined.slice(start..(start + chunk_size).min(joined.len()));
                while !input.is_empty() {
                    if awaited > 0 {
                        let taken = input.split_to(awaited.min(input.len()));
                        awaited -= taken.len();
                        data.extend_from_slice(&taken);
                        continue;
                    }
                    match decoder.decode_frame(&mut input).unwrap() {
                        Some(Frame::Data { head, len }) => {
                            // Nothing but the head was ever assembled.
                            assert!(decoder.partial.is_empty());
                            awaited = len;
                            frames.push(Some(head));
                        }
                        Some(Frame::Message(body)) => {
                            assert_eq!(body, message.slice(HEADER_LEN..));
                            frames.push(None);
                        }
                        None => assert!(decoder.partial.len() < prefix),
                    }
                }
            }
            assert_eq!(
                frames,
                [Some(head.clone()), None, Some(head.clone())],
                "chunk size {chunk_size}"
            );
            assert_eq!(data, b"0123456701234567");
        }
    }

    #[test]
    fn data_heads_must_describe_exactly_the_bytes_that_follow() {
        // A head for 8 bytes in a frame that announces 7 or 9.
        let head = data_head(&[8]);
        for announced in [7u32, 9] {
            let (encoded, prefix) = data_frame(&head, b"01234567");
            let mut frame = BytesMut::from(&encoded[..prefix]);
            let body_len = (prefix - HEADER_LEN) as u32 + announced;
            frame[4..HEADER_LEN].copy_from_slice(&body_len.to_be_bytes());
            assert_eq!(
                parse_frame(&mut frame.clone()).unwrap_err().kind,
                CodecErrorKind::InvalidFrame
            );
            assert_eq!(
                FrameDecoder::default()
                    .decode_frame(&mut frame.freeze())
                    .unwrap_err()
                    .kind,
                CodecErrorKind::InvalidFrame
            );
        }

        // Range lengths that overflow or exceed one frame cannot be encoded.
        for lens in [
            &[MAX_DATA_LEN as u64 + 1][..],
            &[MAX_DATA_LEN as u64, 1],
            &[u64::MAX, 2],
        ] {
            assert_eq!(
                encode_data_head(&data_head(lens), MAX_DATA_LEN, &mut BytesMut::new())
                    .unwrap_err()
                    .kind,
                CodecErrorKind::InvalidArgument
            );
        }
        let ranges = vec![DataRange::default(); MAX_DATA_RANGES + 1];
        let oversized = DataHead {
            placement: DataPlacement::Ranges { ranges, ack: None },
            ..Default::default()
        };
        assert_eq!(
            encode_data_head(&oversized, 0, &mut BytesMut::new())
                .unwrap_err()
                .kind,
            CodecErrorKind::InvalidArgument
        );
        // An independently encoded head is subject to the same range limit.
        let encoded = rmp_serde::to_vec_named(&oversized).unwrap();
        assert!(
            DataHead::decode(&encoded, 0)
                .unwrap_err()
                .msg
                .contains("1024")
        );
        let at_limit = data_head(&vec![0; MAX_DATA_RANGES]);
        let encoded = rmp_serde::to_vec_named(&at_limit).unwrap();
        assert_eq!(DataHead::decode(&encoded, 0).unwrap(), at_limit);

        // A full frame is accepted; an empty one needs no ranges.
        let full = data_head(&[MAX_DATA_LEN as u64]);
        let mut frame = BytesMut::new();
        encode_data_head(&full, MAX_DATA_LEN, &mut frame).unwrap();
        assert_eq!(
            parse_frame(&mut frame).unwrap().frame(),
            Some(Frame::Data {
                head: full,
                len: MAX_DATA_LEN
            })
        );
        let empty = DataHead {
            id: 3,
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_data_head(&empty, 0, &mut frame).unwrap();
        assert_eq!(
            parse_frame(&mut frame).unwrap().frame(),
            Some(Frame::Data {
                head: empty,
                len: 0
            })
        );
    }

    #[test]
    fn packed_frames_take_their_length_from_the_prefixes() {
        let head = DataHead {
            id: 9,
            placement: DataPlacement::Packed,
        };
        let mut prefix = BytesMut::new();
        encode_data_head(&head, MAX_DATA_LEN, &mut prefix).unwrap();
        let mut decoder = FrameDecoder::default();
        for byte in &prefix[..prefix.len() - 1] {
            assert!(
                decoder
                    .decode_frame(&mut Bytes::copy_from_slice(&[*byte]))
                    .unwrap()
                    .is_none()
            );
        }
        let mut tail = prefix.split_off(prefix.len() - 1);
        tail.extend_from_slice(b"abc");
        let mut tail = tail.freeze();
        assert_eq!(
            decoder.decode_frame(&mut tail).unwrap(),
            Some(Frame::Data {
                head: head.clone(),
                len: MAX_DATA_LEN
            })
        );
        assert_eq!(&tail[..], b"abc");
        assert_eq!(
            encode_data_head(&head, MAX_DATA_LEN + 1, &mut prefix)
                .unwrap_err()
                .kind,
            CodecErrorKind::InvalidArgument
        );
    }

    #[test]
    fn data_frame_lengths_are_checked_before_buffering() {
        let largest = 4 + MAX_DATA_HEAD_LEN + MAX_DATA_LEN;
        for body_len in [0, 3, largest + 1, u32::MAX as usize] {
            let header = make_header(DATA_MAGIC_NUM, body_len as u32);
            assert_eq!(
                parse_frame(&mut BytesMut::from(header.as_slice()))
                    .unwrap_err()
                    .kind,
                CodecErrorKind::InvalidFrame,
                "body length {body_len}"
            );
        }
        // The head length must leave at most one frame of raw bytes.
        for (body_len, head_len) in [
            (largest, 0),
            (largest, MAX_DATA_HEAD_LEN + 1),
            (largest, MAX_DATA_HEAD_LEN - 1),
            (100, 97),
            (100, u32::MAX as usize),
        ] {
            let mut frame = BytesMut::from(make_header(DATA_MAGIC_NUM, body_len as u32).as_slice());
            frame.extend_from_slice(&(head_len as u32).to_be_bytes());
            assert_eq!(
                parse_frame(&mut frame).unwrap_err().kind,
                CodecErrorKind::InvalidFrame,
                "head length {head_len} in {body_len}"
            );
        }
        // An announced head reserves its own bytes, not the frame's.
        let mut frame = BytesMut::from(make_header(DATA_MAGIC_NUM, largest as u32).as_slice());
        frame.extend_from_slice(&(MAX_DATA_HEAD_LEN as u32).to_be_bytes());
        assert_eq!(missing(&mut frame), MAX_DATA_HEAD_LEN);
        assert!(frame.capacity() < 4 * MAX_DATA_HEAD_LEN);
    }

    #[test]
    fn message_only_parsers_reject_data_frames() {
        let (encoded, _) = data_frame(&data_head(&[2]), b"xy");
        assert_eq!(
            parse_message(&mut BytesMut::from(encoded.as_ref()))
                .unwrap_err()
                .kind,
            CodecErrorKind::InvalidFrame
        );
        assert_eq!(
            FrameDecoder::default()
                .decode(&mut encoded.clone())
                .unwrap_err()
                .kind,
            CodecErrorKind::InvalidFrame
        );
        assert_eq!(
            decode_frames::<CodecError>(&encoded, |_| panic!("no message frame"))
                .unwrap_err()
                .kind,
            CodecErrorKind::InvalidFrame
        );
    }

    #[test]
    fn data_heads_omit_absent_fields_and_ignore_unknown_ones() {
        let minimal = DataHead {
            id: 9,
            ..Default::default()
        };
        let encoded = rmp_serde::to_vec_named(&minimal).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<serde_json::Value>(&encoded).unwrap(),
            serde_json::json!({"id": 9, "placement": "packed"})
        );

        #[derive(Serialize)]
        struct Extended {
            id: u64,
            future: Vec<u8>,
            placement: DataPlacement,
        }
        let placement = DataPlacement::Ranges {
            ranges: vec![(4, 2)],
            ack: None,
        };
        let encoded = rmp_serde::to_vec_named(&Extended {
            id: 9,
            future: vec![1, 2, 3],
            placement: placement.clone(),
        })
        .unwrap();
        let decoded = DataHead::decode(&encoded, 2).unwrap();
        assert_eq!(decoded.id, 9);
        assert_eq!(decoded.placement, placement);
        let encoded = rmp_serde::to_vec_named(&decoded).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<serde_json::Value>(&encoded).unwrap(),
            serde_json::json!({"id": 9, "placement": {"write": {"ranges": [[4, 2]]}}})
        );
    }

    /// A range is the array `[offset, len]`: a head can list one per copy
    /// operation, and field names would be most of it.
    #[test]
    fn data_ranges_encode_as_two_element_arrays() {
        let range: DataRange = (4, 2);
        assert_eq!(rmp_serde::to_vec_named(&range).unwrap(), [0x92, 4, 2]);
        let widest: DataRange = (u64::MAX, u64::MAX);
        let encoded = rmp_serde::to_vec_named(&widest).unwrap();
        assert_eq!(encoded.len(), 19);
        assert_eq!(
            rmp_serde::from_slice::<DataRange>(&encoded).unwrap(),
            widest
        );
        // A map, or an array of another length, is not a range.
        #[derive(Serialize)]
        struct Named {
            offset: u64,
            len: u64,
        }
        let named = rmp_serde::to_vec_named(&Named { offset: 4, len: 2 }).unwrap();
        assert!(rmp_serde::from_slice::<DataRange>(&named).is_err());
        let triple = rmp_serde::to_vec_named(&(4u64, 2u64, 1u64)).unwrap();
        assert!(rmp_serde::from_slice::<DataRange>(&triple).is_err());
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
        for flags in [MsgFlags::IsReq, MsgFlags::IsReq | MsgFlags::UseMessagePack] {
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
                    let (decoded, offset) = parse_body(&body).unwrap();
                    assert_eq!(decoded, meta);
                    assert_eq!(
                        deserialize_payload::<String>(&body[offset..], decoded.flags).unwrap(),
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
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
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
                CodecErrorKind::InvalidFrame
            );
            for split in 0..HEADER_LEN {
                let mut decoder = FrameDecoder::default();
                let mut first = header.slice(..split);
                assert!(decoder.decode(&mut first).unwrap().is_none());
                let mut rest = header.slice(split..);
                assert_eq!(
                    decoder.decode(&mut rest).unwrap_err().kind,
                    CodecErrorKind::InvalidFrame
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
    fn partial_state_is_reported_while_a_frame_is_incomplete() {
        let encoded = encode(&MsgMeta::default(), &"hello").unwrap();
        let mut decoder = FrameDecoder::default();
        let mut first = encoded.slice(..3);
        assert!(decoder.decode(&mut first).unwrap().is_none());
        assert!(decoder.has_partial());
        let mut rest = encoded.slice(3..);
        assert!(decoder.decode(&mut rest).unwrap().is_some());
        assert!(!decoder.has_partial());
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
            CodecErrorKind::InvalidFrame
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
