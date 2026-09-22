//! Serialization directly into transport-owned storage.

use super::{MsgFlags, MsgMeta};
use crate::{Error, ErrorKind, Result};
use bytes::BytesMut;
use serde::Serialize;
use std::io::Write;

/// Transport-owned storage with framing hooks for direct serialization.
pub(crate) trait SendMsg {
    /// Returns the current size of the message buffer.
    fn size(&self) -> usize;

    /// Prepares storage and any transport-specific header.
    fn prepare(&mut self) -> Result<()>;

    /// Backfills lengths: `meta_offset` points to the metadata length prefix,
    /// and `payload_offset` marks the first payload byte.
    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()>;

    /// Returns a writer for appending data to the message.
    fn writer(&mut self) -> impl std::io::Write;
}

impl MsgMeta {
    /// Writes named-field MessagePack metadata and a flag-selected payload,
    /// then backfills lengths. Propagates storage, encoding and framing errors.
    pub(crate) fn serialize_to<M: SendMsg, P: Serialize>(
        &self,
        payload: &P,
        msg: &mut M,
    ) -> Result<()> {
        msg.prepare()?;

        let meta_offset = msg.size();
        // Reserve the metadata length; `finish` backfills it after encoding.
        msg.writer()
            .write_all(&0u32.to_be_bytes())
            .map_err(|e| Error::new(ErrorKind::SerializeFailed, e.to_string()))?;
        self.encode_to(msg.writer())?;

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
