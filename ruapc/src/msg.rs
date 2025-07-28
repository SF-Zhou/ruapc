use std::io::Write;

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

use crate::{
    Bytes,
    error::{Error, ErrorKind, Result},
};

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone, Copy)]
#[repr(transparent)]
#[serde(transparent)]
pub struct MsgFlags(u8);

bitflags! {
    impl MsgFlags: u8 {
        const IsReq = 1;
        const UseMessagePack = 2;
    }
}

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct MsgMeta {
    pub method: String,
    pub flags: MsgFlags,
    pub msgid: u64,
}

#[derive(Debug, Default)]
pub struct RecvMsg {
    pub meta: MsgMeta,
    payload: Bytes,
}

impl RecvMsg {
    /// # Errors
    pub fn parse(bytes: impl Into<Bytes>) -> Result<Self> {
        const S: usize = std::mem::size_of::<u32>();
        let mut bytes: Bytes = bytes.into();

        let len = bytes.len();
        let meta_len = if let Ok(b) = bytes[..S].try_into() {
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

        let meta: MsgMeta = if bytes[S] == b'{' {
            serde_json::from_slice(&bytes[S..offset])?
        } else {
            rmp_serde::from_slice(&bytes[S..offset])?
        };

        bytes.advance(offset);
        Ok(RecvMsg {
            meta,
            payload: bytes,
        })
    }

    /// # Errors
    pub fn deserialize<P: for<'c> Deserialize<'c>>(self) -> Result<P> {
        if self.meta.flags.contains(MsgFlags::UseMessagePack) {
            Ok(rmp_serde::from_slice(&self.payload)?)
        } else {
            Ok(serde_json::from_slice(&self.payload)?)
        }
    }
}

pub trait SendMsg {
    fn size(&self) -> usize;

    fn prepare(&mut self) -> Result<()>;

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()>;

    fn writer(&mut self) -> impl std::io::Write;
}

impl MsgMeta {
    /// # Errors
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

impl SendMsg for bytes::BytesMut {
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
        struct Writer<'a>(&'a mut bytes::BytesMut);

        impl std::io::Write for Writer<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.write_all(buf)?;
                Ok(buf.len())
            }

            #[inline]
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
