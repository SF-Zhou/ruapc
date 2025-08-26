use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    Receiver, State,
    error::{Error, ErrorKind, Result},
    msg::{MsgMeta, SendMsg},
};

#[derive(Debug, Clone)]
pub struct TcpSocket {
    stream: mpsc::Sender<Bytes>,
}

impl TcpSocket {
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self { stream }
    }

    pub async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<Receiver> {
        struct TcpSocketBytes(BytesMut);

        impl SendMsg for TcpSocketBytes {
            fn size(&self) -> usize {
                self.0.size()
            }

            fn prepare(&mut self) -> Result<()> {
                self.0.extend_from_slice(&super::MAGIC_NUM.to_be_bytes());
                self.0.extend_from_slice(&0u32.to_be_bytes());
                self.0.prepare()
            }

            fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
                const S: usize = std::mem::size_of::<u32>();
                if meta_offset < S {
                    return Err(Error::new(
                        ErrorKind::SerializeFailed,
                        format!("invalid meta offset: {meta_offset}"),
                    ));
                }

                self.0.finish(meta_offset, payload_offset)?;
                let total_len = u32::try_from(self.size() - meta_offset)?;
                self.0[meta_offset - S..meta_offset].copy_from_slice(&total_len.to_be_bytes());
                Ok(())
            }

            fn writer(&mut self) -> impl std::io::Write {
                self.0.writer()
            }
        }

        let receiver = if meta.is_req() {
            let (msgid, rx) = state.waiter.alloc();
            meta.msgid = msgid;
            rx
        } else {
            Receiver::None
        };

        let mut bytes = TcpSocketBytes(BytesMut::with_capacity(512));
        meta.serialize_to(payload, &mut bytes)?;
        if bytes.0.len() >= super::MAX_MSG_SIZE {
            return Err(Error::new(
                ErrorKind::TcpParseMsgFailed,
                format!("msg is too long: {}", bytes.0.len()),
            ));
        }

        self.stream
            .send(bytes.0.into())
            .await
            .map_err(|e| Error::new(ErrorKind::TcpSendMsgFailed, e.to_string()))?;

        Ok(receiver)
    }
}
