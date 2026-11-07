use bytes::{Buf, Bytes, BytesMut};
use serde::Deserialize;

use crate::{MsgMeta, Result};

#[cfg(feature = "rdma")]
use crate::Buffer;

/// Owned, immutable message payload.
///
/// Parsing and conversion to [`Bytes`] retain the underlying allocation without
/// copying its contents, including registered buffers and custom byte owners.
#[derive(Debug, Default)]
pub struct Payload(Bytes);

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
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
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
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Advances the internal position by the specified offset.
    ///
    /// This is used during message parsing to skip over already-processed data.
    ///
    /// # Arguments
    ///
    /// * `offset` - Number of bytes to advance
    ///
    /// # Panics
    ///
    /// Panics if `offset` exceeds the remaining payload length.
    #[inline]
    pub fn advance(&mut self, offset: usize) {
        self.0.advance(offset);
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
        ruapc_wire::deserialize_payload(self.as_slice(), meta.flags).map_err(Into::into)
    }
}

impl std::ops::Deref for Payload {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<Bytes> for Payload {
    #[inline]
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<BytesMut> for Payload {
    #[inline]
    fn from(value: BytesMut) -> Self {
        Self(value.into())
    }
}

impl From<Payload> for Bytes {
    #[inline]
    fn from(value: Payload) -> Self {
        value.0
    }
}

#[cfg(feature = "rdma")]
impl From<Buffer> for Payload {
    #[inline]
    fn from(value: Buffer) -> Self {
        Self(Bytes::from_owner(value))
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::MsgFlags;
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
        let p = Payload::default();
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
        p.advance(3);
        assert!(p.is_empty());
        p.advance(0);
    }

    #[test]
    fn test_advance_empty_is_noop() {
        let mut p = Payload::default();
        p.advance(0); // should not panic
        assert!(p.is_empty());
    }

    #[test]
    fn advancing_past_the_end_panics_without_changing_the_payload() {
        for mut payload in [
            Payload::default(),
            Payload::from(Bytes::from_static(b"abc")),
        ] {
            let original = payload.as_slice().to_vec();
            let offset = payload.len() + 1;
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    payload.advance(offset);
                }))
                .is_err()
            );
            assert_eq!(payload.as_slice(), original);
        }
    }

    #[test]
    fn test_deserialize_empty_payload_as_null() {
        // Empty payload is treated as JSON null (→ Option<String>::None etc.)
        let p = Payload::default();
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
