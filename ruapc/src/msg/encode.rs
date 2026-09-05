//! Serialization directly into transport-owned storage.

use super::{MsgFlags, MsgMeta};
use crate::{Error, ErrorKind, Result};
use bytes::BytesMut;
use serde::Serialize;
use std::io::Write;

/// Trait for types that can send serialized messages.
///
/// Implementors of this trait can be used as message targets for serialization.
/// The trait provides methods for preparing the message buffer, writing data,
/// and finalizing the message with proper length prefixes.
pub(crate) trait SendMsg {
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
    /// 2. Serializes the metadata (always MessagePack)
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
    pub(crate) fn serialize_to<M: SendMsg, P: Serialize>(
        &self,
        payload: &P,
        msg: &mut M,
    ) -> Result<()> {
        msg.prepare()?;

        // serialize meta (compact binary layout; the `UseMessagePack` flag
        // only affects the payload).
        let meta_offset = msg.size();
        // reserve for meta len.
        msg.writer()
            .write_all(&0u32.to_be_bytes())
            .map_err(|e| Error::new(ErrorKind::SerializeFailed, e.to_string()))?;
        self.encode_to(msg.writer())?;

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
