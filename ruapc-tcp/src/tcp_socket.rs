use std::any::Any;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::{BoxFuture, Error, ErrorKind, MsgMeta, Result, RuapcSocket, SendMsg, SocketType};

/// TCP socket for sending messages.
///
/// This socket implementation sends messages through an MPSC channel,
/// which is connected to a background task that handles the actual TCP I/O.
#[derive(Debug, Clone)]
pub struct TcpSocket {
    stream: mpsc::Sender<Bytes>,
}

impl TcpSocket {
    /// Creates a new TCP socket with the given channel sender.
    pub fn new(stream: mpsc::Sender<Bytes>) -> Self {
        Self { stream }
    }

    /// Internal method to serialize and send a message.
    pub async fn send_internal(&self, meta: MsgMeta, payload: Bytes) -> Result<()> {
        struct TcpSocketBytes(BytesMut);

        impl SendMsg for TcpSocketBytes {
            fn size(&self) -> usize {
                self.0.size()
            }

            fn prepare(&mut self) -> Result<()> {
                self.0.extend_from_slice(&crate::MAGIC_NUM.to_be_bytes());
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
        meta.serialize_to_bytes(payload, &mut bytes)?;
        if bytes.0.len() >= crate::MAX_MSG_SIZE {
            return Err(Error::new(
                ErrorKind::TcpParseMsgFailed,
                format!("msg is too long: {}", bytes.0.len()),
            ));
        }

        self.stream
            .send(bytes.0.into())
            .await
            .map_err(|e| Error::new(ErrorKind::TcpSendMsgFailed, e.to_string()))?;

        Ok(())
    }
}

impl RuapcSocket for TcpSocket {
    fn send_bytes(&self, meta: MsgMeta, payload: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.send_internal(meta, payload))
    }

    fn socket_type(&self) -> SocketType {
        SocketType::TCP
    }

    fn clone_boxed(&self) -> Box<dyn RuapcSocket> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
