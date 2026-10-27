//! Serialization directly into transport-owned storage.

use super::{MsgFlags, MsgMeta};
use crate::{Buffer, CodecError, CodecErrorKind, CodecResult};
use bytes::BytesMut;
use serde::Serialize;
use std::io::Write;

/// Transport-owned storage with framing hooks for direct serialization.
///
/// Each serialization starts a new message through [`prepare`](Self::prepare).
/// The serializer uses exclusive mutable access to the storage and statically
/// dispatches these hooks. Writers append bytes and update the size reported by
/// [`size`](Self::size). Implementations may reserve or grow storage, but these
/// hooks do not submit a transport operation or certify transmission or DMA
/// completion. Buffer ownership remains with the storage implementation.
///
/// An error can leave partially encoded bytes in the storage. The caller must
/// discard or reset that content before treating it as a complete message.
pub trait SendMsg {
    /// Storage errors, including conversion from codec failures.
    ///
    /// Hooks return this type directly, so transport-specific errors retain
    /// their identity without boxing or conversion through a codec error.
    type Error: From<CodecError>;

    /// Returns the current size of the message buffer.
    fn size(&self) -> usize;

    /// Resets storage for a new message, discarding any previous message or
    /// partial serialization, then prepares any transport-specific header.
    ///
    /// On success, only that header may precede the bytes appended by the
    /// serializer. The built-in `Buffer` and `BytesMut` implementations clear
    /// their contents while retaining capacity and write no header.
    fn prepare(&mut self) -> Result<(), Self::Error>;

    /// Backfills lengths: `meta_offset` points to the metadata length prefix,
    /// and `payload_offset` marks the first payload byte.
    ///
    /// The serializer supplies offsets within the current storage, with a
    /// four-byte length prefix followed by nonempty metadata before
    /// `payload_offset`. The `Buffer` and `BytesMut` implementations reject
    /// invalid offsets or empty metadata with [`CodecErrorKind::InvalidArgument`]
    /// and leave the bytes unchanged. They check the structure and representable
    /// length only; they do not decode or validate the metadata contents.
    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<(), Self::Error>;

    /// Returns a writer for appending data to the message.
    fn writer(&mut self) -> impl std::io::Write;
}

impl MsgMeta {
    /// Starts a new message with [`SendMsg::prepare`], writes named-field
    /// MessagePack metadata and a flag-selected payload, then backfills lengths.
    /// Propagates storage, encoding and framing errors.
    ///
    /// Reusing `msg` replaces its previous message, including after a failed
    /// serialization. Failure can leave partially encoded content in `msg`.
    /// Success only completes serialization; it does not send the message or
    /// certify DMA completion.
    pub fn serialize_to<M: SendMsg, P: Serialize>(
        &self,
        payload: &P,
        msg: &mut M,
    ) -> Result<(), M::Error> {
        msg.prepare()?;

        let meta_offset = msg.size();
        // Reserve the metadata length; `finish` backfills it after encoding.
        msg.writer()
            .write_all(&0u32.to_be_bytes())
            .map_err(|e| CodecError::new(CodecErrorKind::SerializeFailed, e.to_string()))?;
        self.encode_to(msg.writer())?;

        let payload_offset = msg.size();
        if self.flags.contains(MsgFlags::UseMessagePack) {
            rmp_serde::encode::write_named(&mut msg.writer(), payload).map_err(CodecError::from)?;
        } else {
            serde_json::to_writer(msg.writer(), payload).map_err(CodecError::from)?;
        }

        msg.finish(meta_offset, payload_offset)?;

        Ok(())
    }
}

#[inline]
fn finish_metadata(bytes: &mut [u8], meta_offset: usize, payload_offset: usize) -> CodecResult<()> {
    let prefix_end = meta_offset
        .checked_add(std::mem::size_of::<u32>())
        .filter(|&end| end < payload_offset && payload_offset <= bytes.len())
        .ok_or_else(|| CodecError::kind(CodecErrorKind::InvalidArgument))?;
    let meta_len = u32::try_from(payload_offset - prefix_end)?;
    bytes[meta_offset..prefix_end].copy_from_slice(&meta_len.to_be_bytes());
    Ok(())
}

impl SendMsg for Buffer {
    type Error = CodecError;

    #[inline]
    fn size(&self) -> usize {
        self.len()
    }

    #[inline]
    fn prepare(&mut self) -> CodecResult<()> {
        self.set_len(0);
        Ok(())
    }

    #[inline]
    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> CodecResult<()> {
        finish_metadata(self, meta_offset, payload_offset)
    }

    #[inline]
    fn writer(&mut self) -> impl std::io::Write {
        #[repr(transparent)]
        struct Writer<'a>(&'a mut Buffer);

        impl std::io::Write for Writer<'_> {
            #[inline]
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            #[inline]
            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                self.0.extend_from_slice(buf).map_err(std::io::Error::other)
            }

            #[inline]
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        Writer(self)
    }
}

impl SendMsg for BytesMut {
    type Error = CodecError;

    #[inline]
    fn size(&self) -> usize {
        self.len()
    }

    #[inline]
    fn prepare(&mut self) -> CodecResult<()> {
        self.clear();
        Ok(())
    }

    #[inline]
    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> CodecResult<()> {
        finish_metadata(self, meta_offset, payload_offset)
    }

    #[inline]
    fn writer(&mut self) -> impl std::io::Write {
        #[repr(transparent)]
        struct Writer<'a>(&'a mut BytesMut);

        impl std::io::Write for Writer<'_> {
            #[inline]
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            #[inline]
            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                self.0.extend_from_slice(buf);
                Ok(())
            }

            #[inline]
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        Writer(self)
    }
}
