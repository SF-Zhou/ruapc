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

    /// Checks if this message is a response.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc::{MsgMeta, MsgFlags};
    /// let mut meta = MsgMeta::default();
    /// meta.flags = MsgFlags::IsRsp;
    /// assert!(meta.is_rsp());
    /// ```
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

impl SendMsg for crate::Buffer {
    fn size(&self) -> usize {
        self.len()
    }

    fn prepare(&mut self) -> Result<()> {
        self.set_len(0);
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
        struct Writer<'a>(&'a mut crate::Buffer);

        impl std::io::Write for Writer<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                self.0.extend_from_slice(buf).map_err(std::io::Error::other)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        Writer(self)
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_meta(method: &str, use_msgpack: bool) -> MsgMeta {
        let mut flags = MsgFlags::IsReq;
        if use_msgpack {
            flags |= MsgFlags::UseMessagePack;
        }
        MsgMeta {
            method: method.to_string(),
            flags,
            msgid: 42,
        }
    }

    /// Serialize `meta` + `payload` into a `BytesMut` and return the bytes.
    fn serialize_to_bytes<P: serde::Serialize>(meta: &MsgMeta, payload: &P) -> BytesMut {
        let mut buf = BytesMut::new();
        meta.serialize_to(payload, &mut buf).unwrap();
        buf
    }

    #[test]
    fn test_msgflags_is_req_is_rsp() {
        let mut meta = MsgMeta::default();
        assert!(!meta.is_req());
        assert!(!meta.is_rsp());

        meta.flags = MsgFlags::IsReq;
        assert!(meta.is_req());
        assert!(!meta.is_rsp());

        meta.flags = MsgFlags::IsRsp;
        assert!(!meta.is_req());
        assert!(meta.is_rsp());
    }

    #[test]
    fn test_serialize_parse_json_roundtrip() {
        let meta = make_meta("TestService/hello", false);
        let payload_value = serde_json::json!({"key": "value", "num": 123});

        let buf = serialize_to_bytes(&meta, &payload_value);

        let msg = Message::parse(Bytes::from(buf)).unwrap();
        assert_eq!(msg.meta.method, "TestService/hello");
        assert_eq!(msg.meta.msgid, 42);
        assert!(msg.meta.is_req());
        assert!(!msg.meta.flags.contains(MsgFlags::UseMessagePack));

        let recovered: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!(recovered, payload_value);
    }

    #[test]
    fn test_serialize_parse_msgpack_roundtrip() {
        let meta = make_meta("TestService/hello", true);
        let payload_str = "hello msgpack";

        let buf = serialize_to_bytes(&meta, &payload_str);

        let msg = Message::parse(Bytes::from(buf)).unwrap();
        assert_eq!(msg.meta.method, "TestService/hello");
        assert!(msg.meta.flags.contains(MsgFlags::UseMessagePack));

        let recovered: String = rmp_serde::from_slice(&msg.payload).unwrap();
        assert_eq!(recovered, payload_str);
    }

    #[test]
    fn test_parse_too_short_returns_error() {
        // 4 bytes of zeros → meta_len == 0, which is explicitly rejected.
        let four_zero_bytes = Bytes::from_static(&[0u8, 0, 0, 0]);
        assert!(Message::parse(four_zero_bytes).is_err());
    }

    #[test]
    fn test_parse_zero_meta_len_returns_error() {
        // 4 bytes of zeros => meta_len == 0, which is invalid.
        let buf = Bytes::from_static(&[0u8, 0, 0, 0]);
        assert!(Message::parse(buf).is_err());
    }

    #[test]
    fn test_parse_meta_len_exceeds_payload_returns_error() {
        // meta_len says 100, but total is only 8 bytes.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&100u32.to_be_bytes()); // meta_len = 100
        buf.extend_from_slice(b"short");
        assert!(Message::parse(Bytes::from(buf)).is_err());
    }

    #[test]
    fn test_message_new_and_default() {
        let msg = Message::default();
        assert!(msg.meta.method.is_empty());
        assert!(msg.payload.is_empty());

        let meta = MsgMeta {
            method: "Svc/method".into(),
            flags: MsgFlags::IsRsp,
            msgid: 7,
        };
        let payload = crate::Payload::from(bytes::Bytes::from_static(b"data"));
        let msg2 = Message::new(meta, payload);
        assert_eq!(msg2.meta.method, "Svc/method");
        assert!(!msg2.payload.is_empty());
    }

    #[test]
    fn test_msgmeta_default() {
        let meta = MsgMeta::default();
        assert!(meta.method.is_empty());
        assert_eq!(meta.flags, MsgFlags::default());
        assert_eq!(meta.msgid, 0);
    }

    #[test]
    fn test_msgflags_serde_roundtrip() {
        let flags = MsgFlags::IsReq | MsgFlags::UseMessagePack;
        let json = serde_json::to_string(&flags).unwrap();
        let recovered: MsgFlags = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, flags);
    }

    #[test]
    fn test_bytesmut_sendmsg_prepare_is_noop() {
        // BytesMut::prepare does nothing — the buffer is not cleared.
        let mut buf = BytesMut::from(&b"existing"[..]);
        buf.prepare().unwrap();
        assert_eq!(&buf[..], b"existing");
    }

    #[test]
    fn test_bytesmut_writer_write_and_flush() {
        use std::io::Write as _;
        let mut bm = BytesMut::new();
        {
            let mut w = bm.writer();
            // `write()` calls `write_all()` internally.
            w.write(b"hello").unwrap();
            // `flush()` is a no-op but must be reachable.
            w.flush().unwrap();
        }
        assert_eq!(&bm[..], b"hello");
    }

    #[test]
    fn test_buffer_sendmsg_serialize() {
        use crate::{BufferPool, Devices};
        use std::sync::Arc;
        let devices = Arc::new(Devices::default());
        let pool = BufferPool::new(devices, 4096, 4096, 0);
        let mut buf = pool.allocate().unwrap();

        let meta = make_meta("SomeService/rpc", false);
        // serialize_to exercises Buffer::prepare, Buffer::writer, and Buffer::finish.
        meta.serialize_to(&serde_json::json!({"x": 1}), &mut buf)
            .unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_buffer_sendmsg_writer_write_and_flush() {
        use crate::{BufferPool, Devices};
        use std::io::Write as _;
        use std::sync::Arc;
        let devices = Arc::new(Devices::default());
        let pool = BufferPool::new(devices, 4096, 4096, 0);
        let mut buf = pool.allocate().unwrap();
        buf.set_len(0);
        {
            let mut w = buf.writer();
            // Explicitly call `write()` (not `write_all()`).
            w.write(b"test").unwrap();
            // Explicitly call `flush()` (no-op but must be reachable).
            w.flush().unwrap();
        }
        assert_eq!(buf.len(), 4);
    }
}
