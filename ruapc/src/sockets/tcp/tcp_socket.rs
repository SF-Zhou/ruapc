use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    SocketTrait, State,
    error::{Error, ErrorKind, Result},
    msg::{MsgMeta, SendMsg},
};

#[derive(Debug, Clone)]
pub struct TcpSocket {
    stream: mpsc::Sender<Bytes>,
    conn_id: u64,
}

impl TcpSocket {
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self {
            stream,
            conn_id: crate::task::next_conn_id(),
        }
    }

    /// Unique id of the underlying connection.
    pub(crate) fn conn_id(&self) -> u64 {
        self.conn_id
    }

    /// Whether `other` refers to the same underlying connection.
    pub(crate) fn same_socket(&self, other: &Self) -> bool {
        self.conn_id == other.conn_id
    }
}

impl SocketTrait for TcpSocket {
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
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

        let mut bytes = TcpSocketBytes(BytesMut::with_capacity(512));
        meta.serialize_to(payload, &mut bytes)?;
        if bytes.0.len() >= super::MAX_MSG_SIZE {
            return Err(Error::new(
                ErrorKind::TcpParseMsgFailed,
                format!("msg is too long: {}", bytes.0.len()),
            ));
        }

        // Bind the pending request to this connection so it fails eagerly
        // if the connection dies before the response arrives.
        if meta.is_req() {
            state.waiter.bind_connection(meta.msgid, self.conn_id);
        }

        self.stream
            .send(bytes.0.into())
            .await
            .map_err(|e| Error::new(ErrorKind::TcpSendMsgFailed, e.to_string()))?;

        Ok(())
    }
}
