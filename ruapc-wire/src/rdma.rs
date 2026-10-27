//! RDMA wire framing shared by serialization and dispatch workers.
//!
//! These types describe bytes on the wire and require no verbs resources.
//! Decoding a frame grants no authority to dispatch a private control message,
//! release advertised memory, or complete a transfer. The transport must enforce
//! connection identity, negotiated capabilities, ownership and completion rules.
//!
//! Public control types and codecs support transport-runtime integration;
//! private controls bypass application RPC routing. This integration API may
//! change between alpha releases, and `ruapc` pins this crate to its exact
//! matching release. Such pairing applies within one build. Compatibility with
//! remote peers still requires compatible wire encodings or explicit protocol
//! version and capability negotiation before using a changed format.

use crate::{CodecError, CodecErrorKind, CodecResult, MsgMeta};
use bytes::Bytes;
use ruapc_bufpool::RemoteBufferInfo;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::Cursor;

/// Size of the per-frame header: a big-endian u32 frame length.
///
/// Ordinary frames retain `[4B frame_len][4B meta_len][meta][payload]`.
/// A zero frame length escapes to a length-prefixed control frame after
/// negotiating large-payload support. Both forms are self-delimiting and
/// can be concatenated into one SEND.
pub const FRAME_HEADER: usize = 4;

const CONTROL_HEADER: usize = 2 * FRAME_HEADER;
const OFFER: u8 = 1;
const FINISH: u8 = 2;
const FINISH_REPLY: u8 = 3;

/// A serialized payload exposed for a peer READ. The metadata travels in
/// this offer; the advertised regions contain only the serialized payload.
///
/// The transport must validate the negotiated payload size and the complete
/// remote-region layout before allocating destinations or posting any READs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargePayloadOffer {
    /// Transfer identity scoped to the sending connection.
    pub transfer_id: u64,
    /// Length of the serialized payload in the advertised regions.
    pub payload_len: u64,
    /// Registered source regions in logical payload order.
    pub regions: Vec<RemoteBufferInfo>,
    /// RPC metadata associated with the serialized payload.
    pub meta: MsgMeta,
}

/// Control messages are enabled only after negotiating large-payload support.
/// Their bodies use named-field MessagePack independently of the payload flag.
/// These values are protocol data, not completion evidence. The transport must
/// validate their connection and its transfer state before acting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlFrame {
    /// Advertises the serialized source of a large RPC payload.
    Offer(LargePayloadOffer),
    /// Asks the source to finish the identified transfer.
    Finish {
        /// Transfer identity scoped to the sending connection.
        transfer_id: u64,
    },
    /// Reports whether the identified transfer remains valid.
    FinishReply {
        /// Transfer identity scoped to the sending connection.
        transfer_id: u64,
        /// The source's result for this transfer.
        completed: bool,
    },
}

/// One decoded frame from a completed SEND's byte storage.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// An ordinary RPC body sharing the input storage, still requiring parsing.
    Message(Bytes),
    /// A private control message requiring transport-level validation.
    Control(ControlFrame),
}

#[derive(Serialize, Deserialize)]
struct Finish {
    transfer_id: u64,
}

#[derive(Serialize, Deserialize)]
struct FinishReply {
    transfer_id: u64,
    completed: bool,
}

/// Encodes `[0: 4B][control_len: 4B][kind: 1B][named MessagePack body]`.
/// `control_len` includes the kind byte. Zero cannot be a valid ordinary RPC
/// frame because an RPC body starts with a nonempty metadata length prefix.
/// The caller checks that the result fits the negotiated receive buffer.
pub fn encode_control(control: &ControlFrame) -> CodecResult<Bytes> {
    let mut bytes = vec![0; CONTROL_HEADER];
    match control {
        ControlFrame::Offer(offer) => {
            if offer.payload_len == 0 {
                return Err(invalid_frame("large-payload offer has an empty payload"));
            }
            bytes.push(OFFER);
            rmp_serde::encode::write_named(&mut bytes, offer)?;
        }
        ControlFrame::Finish { transfer_id } => {
            bytes.push(FINISH);
            rmp_serde::encode::write_named(
                &mut bytes,
                &Finish {
                    transfer_id: *transfer_id,
                },
            )?;
        }
        ControlFrame::FinishReply {
            transfer_id,
            completed,
        } => {
            bytes.push(FINISH_REPLY);
            rmp_serde::encode::write_named(
                &mut bytes,
                &FinishReply {
                    transfer_id: *transfer_id,
                    completed: *completed,
                },
            )?;
        }
    }
    let len = u32::try_from(bytes.len() - CONTROL_HEADER)?;
    bytes[FRAME_HEADER..CONTROL_HEADER].copy_from_slice(&len.to_be_bytes());
    Ok(bytes.into())
}

/// Decodes all frames in one completed SEND, retaining zero-copy views for
/// ordinary RPC messages. A control frame may be aggregated with ordinary
/// messages or other controls. Truncation and unknown controls are errors;
/// callbacks for preceding complete frames may already have run.
/// A callback error is returned unchanged and stops decoding subsequent frames.
/// Framing errors are converted into the callback's error type through
/// `From<CodecError>`. Callers using only codec errors can select
/// `decode_frames::<CodecError>` explicitly.
/// Message bodies own shared input storage and remain valid after `frames` is
/// dropped. Controls are parsed data and do not constitute completion evidence.
#[inline]
pub fn decode_frames<E: From<CodecError>>(
    frames: &Bytes,
    mut f: impl FnMut(Frame) -> Result<(), E>,
) -> Result<(), E> {
    let mut offset = 0;
    while offset < frames.len() {
        let frame_len = take_length(frames, &mut offset)?;
        if frame_len != 0 {
            let end = frame_end(frames, offset, frame_len)?;
            f(Frame::Message(frames.slice(offset..end)))?;
            offset = end;
            continue;
        }

        let control_len = take_length(frames, &mut offset)?;
        let end = frame_end(frames, offset, control_len)?;
        let (&kind, body) = frames[offset..end]
            .split_first()
            .ok_or_else(|| invalid_frame("empty RDMA control frame"))?;
        let control = match kind {
            OFFER => {
                let offer: LargePayloadOffer = decode_control_body(body)?;
                if offer.payload_len == 0 {
                    return Err(invalid_frame("large-payload offer has an empty payload").into());
                }
                ControlFrame::Offer(offer)
            }
            FINISH => {
                let finish: Finish = decode_control_body(body)?;
                ControlFrame::Finish {
                    transfer_id: finish.transfer_id,
                }
            }
            FINISH_REPLY => {
                let reply: FinishReply = decode_control_body(body)?;
                ControlFrame::FinishReply {
                    transfer_id: reply.transfer_id,
                    completed: reply.completed,
                }
            }
            _ => return Err(invalid_frame(format!("unknown RDMA control kind {kind}")).into()),
        };
        f(Frame::Control(control))?;
        offset = end;
    }
    Ok(())
}

#[inline]
fn take_length(frames: &[u8], offset: &mut usize) -> CodecResult<usize> {
    let end = frame_end(frames, *offset, FRAME_HEADER)?;
    let len = u32::from_be_bytes(frames[*offset..end].try_into().unwrap());
    *offset = end;
    usize::try_from(len).map_err(Into::into)
}

#[inline]
fn frame_end(frames: &[u8], offset: usize, len: usize) -> CodecResult<usize> {
    offset
        .checked_add(len)
        .filter(|end| *end <= frames.len())
        .ok_or_else(|| invalid_frame(format!("truncated RDMA frame ({len}B) at {offset}")))
}

fn decode_control_body<T: DeserializeOwned>(body: &[u8]) -> CodecResult<T> {
    if !matches!(body.first(), Some(0x80..=0x8f | 0xde | 0xdf)) {
        return Err(invalid_frame("RDMA control body must be a MessagePack map"));
    }
    let mut decoder = rmp_serde::Deserializer::new(Cursor::new(body));
    decoder.set_max_depth(64);
    let value = T::deserialize(&mut decoder)?;
    if decoder.position() != body.len() as u64 {
        return Err(invalid_frame("trailing bytes in RDMA control body"));
    }
    Ok(value)
}

fn invalid_frame(message: impl Into<String>) -> CodecError {
    CodecError::new(CodecErrorKind::DeserializeFailed, message.into())
}

#[cfg(test)]
#[path = "rdma/tests.rs"]
mod tests;
