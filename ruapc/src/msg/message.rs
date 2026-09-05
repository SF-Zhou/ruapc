//! Decoding a message body without copying its payload.

use super::MsgMeta;
use crate::{Error, ErrorKind, Payload, Result};

/// RPC message containing metadata and payload.
///
/// A message consists of:
/// 1. Metadata (method name, flags, message ID)
/// 2. Payload (the actual request or response data)
///
/// Messages are serialized in a custom binary format:
/// - 4 bytes: metadata length (big-endian u32)
/// - N bytes: serialized metadata (always MessagePack, named fields)
/// - M bytes: serialized payload (JSON, or MessagePack when the
///   `UseMessagePack` flag is set)
#[derive(Debug, Default)]
pub struct Message {
    /// Message metadata.
    pub meta: MsgMeta,
    /// Message payload data.
    pub payload: Payload,
}

impl Message {
    /// Creates a new message with the given metadata and payload.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc::{Message, MsgMeta, Payload};
    /// let meta = MsgMeta::default();
    /// let payload = Payload::default();
    /// let msg = Message::new(meta, payload);
    /// ```
    pub fn new(meta: MsgMeta, payload: Payload) -> Self {
        Self { meta, payload }
    }

    /// Parses a message from raw bytes.
    ///
    /// This method decodes the binary message metadata and extracts the
    /// payload.
    ///
    /// # Message Format
    ///
    /// ```text
    /// | 4 bytes    | N bytes  | M bytes |
    /// | meta_len   | metadata | payload |
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The message is too short to contain valid metadata
    /// - The metadata length is invalid
    /// - Metadata deserialization fails
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Message, Payload};
    /// # use bytes::Bytes;
    /// # let raw_bytes = Bytes::new();
    /// let msg = Message::parse(raw_bytes).unwrap();
    /// ```
    pub fn parse(payload: impl Into<Payload>) -> Result<Self> {
        const S: usize = std::mem::size_of::<u32>();
        let mut payload: Payload = payload.into();

        let len = payload.len();
        let meta_len = if let Some(b) = payload.get(..S).and_then(|b| b.try_into().ok()) {
            u32::from_be_bytes(b) as usize
        } else {
            return Err(Error::new(
                ErrorKind::DeserializeFailed,
                format!("invalid msg length: {len}"),
            ));
        };

        if meta_len == 0 {
            return Err(Error::new(
                ErrorKind::DeserializeFailed,
                format!("invalid meta length: {meta_len}"),
            ));
        }

        if meta_len > len - S {
            return Err(Error::new(
                ErrorKind::DeserializeFailed,
                format!("invalid meta length: {meta_len}, msg length: {len}"),
            ));
        }

        let offset = S + meta_len;
        let meta = MsgMeta::decode(&payload[S..offset])?;
        payload.advance(offset);
        Ok(Message { meta, payload })
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
