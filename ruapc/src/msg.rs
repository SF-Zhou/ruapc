use bitflags::bitflags;
use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorKind, Result};

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone, Copy)]
#[repr(transparent)]
#[serde(transparent)]
pub struct MsgFlags(u8);

bitflags! {
    impl MsgFlags: u8 {
        const IsReq = 1;
    }
}

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct MsgMeta {
    pub method: String,
    pub flags: MsgFlags,
}

#[derive(Debug)]
pub struct RecvMsg {
    pub meta: MsgMeta,
    payload: serde_json::Value,
}

impl Default for RecvMsg {
    fn default() -> Self {
        Self {
            meta: MsgMeta::default(),
            payload: serde_json::Value::Null,
        }
    }
}

impl RecvMsg {
    /// # Errors
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        const S: usize = std::mem::size_of::<u32>();

        let len = bytes.len();
        let meta_len = if let Ok(b) = bytes[..S].try_into() {
            u32::from_be_bytes(b) as usize
        } else {
            return Err(Error::new(
                ErrorKind::DeserializeFailed,
                format!("invalid msg length: {len}"),
            ));
        };

        let offset = S + meta_len;
        if offset > len {
            return Err(Error::new(
                ErrorKind::DeserializeFailed,
                format!("invalid meta length: {meta_len}, msg length: {len}"),
            ));
        }

        let meta: MsgMeta = serde_json::from_slice(&bytes[S..offset])?;
        let payload = serde_json::from_slice(&bytes[offset..])?;
        Ok(RecvMsg { meta, payload })
    }

    /// # Errors
    pub fn deserialize<P: for<'c> Deserialize<'c>>(self) -> Result<P> {
        Ok(serde_json::from_value(self.payload)?)
    }
}
