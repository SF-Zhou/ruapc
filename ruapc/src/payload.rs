use bytes::{Buf, Bytes, BytesMut};
use serde::Deserialize;

use crate::{MsgFlags, MsgMeta, Result};

/// Message payload supporting different memory backends.
///
/// The payload can be:
/// - **Empty**: No data
/// - **Normal**: Standard heap-allocated bytes
/// - **RDMA**: Zero-copy registered buffer (requires "rdma" feature)
///
/// This abstraction allows efficient handling of different memory types
/// while providing a uniform interface.
#[derive(Debug, Default)]
pub enum Payload {
    /// Empty payload (no data).
    #[default]
    Empty,
    /// Normal heap-allocated payload.
    Normal(Bytes),
    /// Registered buffer payload with offset (requires "rdma" feature).
    #[cfg(feature = "rdma")]
    RDMA(crate::memory::Buffer, usize),
}

impl Payload {
    /// Returns the length of the payload in bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc::Payload;
    /// # use bytes::Bytes;
    /// let payload = Payload::from(Bytes::from("hello"));
    /// assert_eq!(payload.len(), 5);
    /// ```
    pub fn len(&self) -> usize {
        match self {
            Payload::Empty => 0,
            Payload::Normal(bytes) => bytes.len(),
            #[cfg(feature = "rdma")]
            Payload::RDMA(buffer, off) => buffer.len() - off,
        }
    }

    /// Checks if the payload is empty.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc::Payload;
    /// let payload = Payload::default();
    /// assert!(payload.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        match self {
            Payload::Empty => true,
            Payload::Normal(bytes) => bytes.is_empty(),
            #[cfg(feature = "rdma")]
            Payload::RDMA(buffer, off) => buffer.len() == *off,
        }
    }

    /// Returns the payload data as a byte slice.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc::Payload;
    /// # use bytes::Bytes;
    /// let payload = Payload::from(Bytes::from("hello"));
    /// assert_eq!(payload.as_slice(), b"hello");
    /// ```
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Payload::Empty => &[],
            Payload::Normal(bytes) => bytes,
            #[cfg(feature = "rdma")]
            Payload::RDMA(buffer, off) => &buffer[*off..],
        }
    }

    /// Advances the internal position by the specified offset.
    ///
    /// This is used during message parsing to skip over already-processed data.
    ///
    /// # Arguments
    ///
    /// * `offset` - Number of bytes to advance
    pub fn advance(&mut self, offset: usize) {
        match self {
            Payload::Empty => {}
            Payload::Normal(bytes) => bytes.advance(offset),
            #[cfg(feature = "rdma")]
            Payload::RDMA(_, off) => *off += offset,
        }
    }

    /// Deserializes the payload into a specific type.
    ///
    /// This method handles different serialization formats based on message flags.
    /// It supports JSON (default) and MessagePack.
    ///
    /// # Type Parameters
    ///
    /// * `P` - The type to deserialize into
    ///
    /// # Arguments
    ///
    /// * `meta` - Message metadata containing flags indicating the serialization format
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn deserialize<P: for<'c> Deserialize<'c>>(&self, meta: &MsgMeta) -> Result<P> {
        if self.is_empty() {
            // for an empty payload, treat it as a null value, which allows using curl to send body-less requests.
            Ok(serde_json::from_value(serde_json::Value::Null)?)
        } else if meta.flags.contains(MsgFlags::UseMessagePack) {
            Ok(rmp_serde::from_slice(self)?)
        } else {
            Ok(serde_json::from_slice(self)?)
        }
    }
}

impl std::ops::Deref for Payload {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<Bytes> for Payload {
    fn from(value: Bytes) -> Self {
        Payload::Normal(value)
    }
}

impl From<BytesMut> for Payload {
    fn from(value: BytesMut) -> Self {
        Payload::Normal(value.into())
    }
}

impl From<Payload> for Bytes {
    fn from(value: Payload) -> Self {
        match value {
            Payload::Empty => Bytes::new(),
            Payload::Normal(bytes) => bytes,
            #[cfg(feature = "rdma")]
            Payload::RDMA(buffer, off) => Bytes::copy_from_slice(&buffer[off..]),
        }
    }
}

#[cfg(feature = "rdma")]
impl From<crate::memory::Buffer> for Payload {
    fn from(value: crate::memory::Buffer) -> Self {
        Payload::RDMA(value, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn json_meta() -> MsgMeta {
        MsgMeta::default()
    }

    fn msgpack_meta() -> MsgMeta {
        MsgMeta {
            flags: MsgFlags::UseMessagePack,
            ..Default::default()
        }
    }

    #[test]
    fn test_empty_payload_properties() {
        let p = Payload::default();
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
        assert_eq!(p.as_slice(), b"");
        assert_eq!(&*p, b"");
    }

    #[test]
    fn test_normal_payload_from_bytes() {
        let p = Payload::from(Bytes::from_static(b"hello"));
        assert!(!p.is_empty());
        assert_eq!(p.len(), 5);
        assert_eq!(p.as_slice(), b"hello");
    }

    #[test]
    fn test_normal_payload_from_bytes_mut() {
        let mut bm = BytesMut::new();
        bm.extend_from_slice(b"world");
        let p = Payload::from(bm);
        assert_eq!(p.len(), 5);
        assert_eq!(p.as_slice(), b"world");
    }

    #[test]
    fn test_payload_into_bytes_empty() {
        let p = Payload::Empty;
        let b: Bytes = p.into();
        assert!(b.is_empty());
    }

    #[test]
    fn test_payload_into_bytes_normal() {
        let p = Payload::from(Bytes::from_static(b"data"));
        let b: Bytes = p.into();
        assert_eq!(&b[..], b"data");
    }

    #[test]
    fn test_advance_normal() {
        let mut p = Payload::from(Bytes::from_static(b"abcdef"));
        p.advance(3);
        assert_eq!(p.as_slice(), b"def");
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn test_advance_empty_is_noop() {
        let mut p = Payload::Empty;
        p.advance(0); // should not panic
        assert!(p.is_empty());
    }

    #[test]
    fn test_deserialize_empty_payload_as_null() {
        // Empty payload is treated as JSON null (→ Option<String>::None etc.)
        let p = Payload::Empty;
        let v: Option<String> = p.deserialize(&json_meta()).unwrap();
        assert!(v.is_none());
    }

    #[test]
    fn test_deserialize_json() {
        let data = serde_json::to_vec(&"hello json").unwrap();
        let p = Payload::from(Bytes::from(data));
        let s: String = p.deserialize(&json_meta()).unwrap();
        assert_eq!(s, "hello json");
    }

    #[test]
    fn test_deserialize_msgpack() {
        let data = rmp_serde::to_vec_named(&42u64).unwrap();
        let p = Payload::from(Bytes::from(data));
        let n: u64 = p.deserialize(&msgpack_meta()).unwrap();
        assert_eq!(n, 42);
    }

    #[test]
    fn test_deserialize_invalid_json_returns_error() {
        let p = Payload::from(Bytes::from_static(b"not valid json!"));
        let result: Result<String> = p.deserialize(&json_meta());
        assert!(result.is_err());
    }

    #[test]
    fn test_deref_coercion() {
        let p = Payload::from(Bytes::from_static(b"deref"));
        let slice: &[u8] = &p;
        assert_eq!(slice, b"deref");
    }
}
