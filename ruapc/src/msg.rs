use std::io::Write;

use bitflags::bitflags;
use bytes::BytesMut;
use serde::{Deserialize, Serialize};

use crate::{
    Payload,
    error::{Error, ErrorKind, Result},
};

/// Message flags for RPC communication.
///
/// Flags control message behavior and serialization format:
/// - `IsReq`: Indicates this is a request (vs. response)
/// - `UseMessagePack`: Use MessagePack instead of JSON for serialization
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone, Copy)]
#[repr(transparent)]
#[serde(transparent)]
pub struct MsgFlags(u8);

bitflags! {
    impl MsgFlags: u8 {
        /// Message is a request.
        const IsReq = 1;
        /// Message is a response.
        const IsRsp = 2;
        /// Use MessagePack serialization format.
        const UseMessagePack = 4;
    }
}

/// Message metadata containing routing and control information.
///
/// The metadata is serialized at the beginning of each message and contains:
/// - Method name for routing
/// - Flags controlling message behavior
/// - Message ID for request/response correlation
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct MsgMeta {
    /// The fully qualified method name (e.g., "ServiceName/method_name").
    pub method: String,
    /// Message flags controlling behavior and format.
    pub flags: MsgFlags,
    /// Message ID for correlating requests and responses.
    pub msgid: u64,
}

impl MsgMeta {
    /// Checks if this message is a request.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc::{MsgMeta, MsgFlags};
    /// let mut meta = MsgMeta::default();
    /// meta.flags = MsgFlags::IsReq;
    /// assert!(meta.is_req());
    /// ```
    #[must_use]
    pub fn is_req(&self) -> bool {
        self.flags.contains(MsgFlags::IsReq)
    }

    #[must_use]
    pub fn is_rsp(&self) -> bool {
        self.flags.contains(MsgFlags::IsRsp)
    }
}

/// RPC message containing metadata and payload.
///
/// A message consists of:
/// 1. Metadata (method name, flags, message ID)
/// 2. Payload (the actual request or response data)
///
/// Messages are serialized in a custom binary format:
/// - 4 bytes: metadata length (big-endian u32)
/// - N bytes: serialized metadata (JSON or MessagePack)
/// - M bytes: serialized payload (JSON or MessagePack)
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
    /// This method deserializes the message metadata and extracts the payload.
    /// The metadata can be in JSON or MessagePack format (auto-detected).
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
        let meta_len = if let Ok(b) = payload[..S].try_into() {
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

        let offset = S + meta_len;
        if offset > len {
            return Err(Error::new(
                ErrorKind::DeserializeFailed,
                format!("invalid meta length: {meta_len}, msg length: {len}"),
            ));
        }

        let meta: MsgMeta = if payload[S] == b'{' {
            serde_json::from_slice(&payload[S..offset])?
        } else {
            rmp_serde::from_slice(&payload[S..offset])?
        };

        payload.advance(offset);
        Ok(Message { meta, payload })
    }

    /// Deserializes the message payload into a typed value.
    ///
    /// The deserialization format is determined by the `UseMessagePack` flag
    /// in the message metadata. If the payload is empty, it's treated as null.
    ///
    /// # Type Parameters
    ///
    /// * `P` - The type to deserialize into
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::Message;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)]
    /// # struct MyResponse { value: String }
    /// # let msg = Message::default();
    /// let response: MyResponse = msg.deserialize().unwrap();
    /// ```
    pub fn deserialize<P: for<'c> Deserialize<'c>>(self) -> Result<P> {
        if self.payload.is_empty() {
            // for an empty payload, treat it as a null value, which allows using curl to send body-less requests.
            Ok(serde_json::from_value(serde_json::Value::Null)?)
        } else if self.meta.flags.contains(MsgFlags::UseMessagePack) {
            Ok(rmp_serde::from_slice(&self.payload)?)
        } else {
            Ok(serde_json::from_slice(&self.payload)?)
        }
    }
}

/// Trait for types that can send serialized messages.
///
/// Implementors of this trait can be used as message targets for serialization.
/// The trait provides methods for preparing the message buffer, writing data,
/// and finalizing the message with proper length prefixes.
pub trait SendMsg {
    /// Returns the current size of the message buffer.
    fn size(&self) -> usize;

    /// Prepares the message buffer for writing.
    ///
    /// # Errors
    ///
    /// Returns an error if preparation fails.
    fn prepare(&mut self) -> Result<()>;

    /// Finalizes the message by updating length prefixes.
    ///
    /// # Arguments
    ///
    /// * `meta_offset` - Offset where metadata length is stored
    /// * `payload_offset` - Offset where payload begins
    ///
    /// # Errors
    ///
    /// Returns an error if finalization fails.
    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()>;

    /// Returns a writer for appending data to the message.
    fn writer(&mut self) -> impl std::io::Write;
}

impl MsgMeta {
    /// Serializes the metadata and payload into a message buffer.
    ///
    /// This method handles the complete serialization process:
    /// 1. Writes a 4-byte length prefix for the metadata
    /// 2. Serializes the metadata (JSON or MessagePack based on flags)
    /// 3. Serializes the payload (JSON or MessagePack based on flags)
    /// 4. Updates the length prefix with the actual metadata size
    ///
    /// # Type Parameters
    ///
    /// * `M` - The message buffer type implementing `SendMsg`
    /// * `P` - The payload type to serialize
    ///
    /// # Arguments
    ///
    /// * `payload` - The data to serialize as the message payload
    /// * `msg` - The message buffer to write to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Buffer preparation fails
    /// - Serialization fails
    /// - Message finalization fails
    pub fn serialize_to<M: SendMsg, P: Serialize>(&self, payload: &P, msg: &mut M) -> Result<()> {
        msg.prepare()?;

        // serialize meta.
        let meta_offset = msg.size();
        // reserve for meta len.
        msg.writer()
            .write_all(&0u32.to_be_bytes())
            .map_err(|e| Error::new(ErrorKind::SerializeFailed, e.to_string()))?;
        if self.flags.contains(MsgFlags::UseMessagePack) {
            rmp_serde::encode::write_named(&mut msg.writer(), self)?;
        } else {
            serde_json::to_writer(msg.writer(), self)?;
        }

        // serialize payload.
        let payload_offset = msg.size();
        if self.flags.contains(MsgFlags::UseMessagePack) {
            rmp_serde::encode::write_named(&mut msg.writer(), payload)?;
        } else {
            serde_json::to_writer(msg.writer(), payload)?;
        }

        msg.finish(meta_offset, payload_offset)?;

        Ok(())
    }
}

impl SendMsg for BytesMut {
    fn size(&self) -> usize {
        self.len()
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        const S: usize = std::mem::size_of::<u32>();
        let meta_len = u32::try_from(payload_offset - meta_offset - S)?;
        self[meta_offset..meta_offset + S].copy_from_slice(&meta_len.to_be_bytes());
        Ok(())
    }

    fn writer(&mut self) -> impl std::io::Write {
        #[repr(transparent)]
        struct Writer<'a>(&'a mut BytesMut);

        impl std::io::Write for Writer<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                self.0.extend_from_slice(buf);
                Ok(())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        Writer(self)
    }
}
