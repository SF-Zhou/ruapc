use bytes::{Buf, Bytes, BytesMut};
use serde::Deserialize;

use crate::{MsgFlags, MsgMeta, Result};

/// Message payload supporting different memory backends.
///
/// The payload can be:
/// - **Empty**: No data
/// - **Normal**: Standard heap-allocated bytes
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
}

impl Payload {
    /// Returns the length of the payload in bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc_core::Payload;
    /// # use bytes::Bytes;
    /// let payload = Payload::from(Bytes::from("hello"));
    /// assert_eq!(payload.len(), 5);
    /// ```
    pub fn len(&self) -> usize {
        match self {
            Payload::Empty => 0,
            Payload::Normal(bytes) => bytes.len(),
        }
    }

    /// Checks if the payload is empty.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc_core::Payload;
    /// let payload = Payload::default();
    /// assert!(payload.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        match self {
            Payload::Empty => true,
            Payload::Normal(bytes) => bytes.is_empty(),
        }
    }

    /// Returns the payload data as a byte slice.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc_core::Payload;
    /// # use bytes::Bytes;
    /// let payload = Payload::from(Bytes::from("hello"));
    /// assert_eq!(payload.as_slice(), b"hello");
    /// ```
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Payload::Empty => &[],
            Payload::Normal(bytes) => bytes,
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
        }
    }
}
