use std::sync::Arc;

use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    SocketPool,
    error::{Error, ErrorKind, Result},
    msg::{MsgMeta, RecvMsg},
};

const MAGIC_NUM: u32 = u32::from_be_bytes(*b"RUA!");
const U32_BYTE_SIZE: usize = std::mem::size_of::<u32>();

pub struct Socket {
    pub socket_pool: Arc<SocketPool>,
    pub tcp_stream: Option<TcpStream>,
    pub for_send: bool,
}

impl Socket {
    /// # Errors
    pub async fn send<P: Serialize>(&mut self, meta: MsgMeta, payload: &P) -> Result<()> {
        #[repr(transparent)]
        struct Writer<'a>(&'a mut Vec<u8>);

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

        let mut bytes = vec![];
        bytes.extend_from_slice(&MAGIC_NUM.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        serde_json::to_writer(Writer(&mut bytes), &meta)?;
        let meta_len = u32::try_from(bytes.len() - U32_BYTE_SIZE * 3)?;
        bytes[U32_BYTE_SIZE * 2..U32_BYTE_SIZE * 3].copy_from_slice(&meta_len.to_be_bytes());
        serde_json::to_writer(Writer(&mut bytes), &payload)?;
        let total_len = u32::try_from(bytes.len() - U32_BYTE_SIZE * 2)?;
        bytes[U32_BYTE_SIZE..U32_BYTE_SIZE * 2].copy_from_slice(&total_len.to_be_bytes());

        let Some(mut stream) = self.tcp_stream.take() else {
            return Err(Error::kind(ErrorKind::InvalidArgument));
        };
        stream
            .write_all(&bytes)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpSendMsgFailed, e.to_string()))?;

        self.tcp_stream = Some(stream);
        Ok(())
    }

    /// # Errors
    pub async fn recv(&mut self) -> Result<RecvMsg> {
        let Some(mut stream) = self.tcp_stream.take() else {
            return Err(Error::kind(ErrorKind::InvalidArgument));
        };

        let header = stream
            .read_u64()
            .await
            .map_err(|e| Error::new(ErrorKind::TcpRecvMsgFailed, e.to_string()))?;

        let (magic_num, total_len) = (
            (header >> 32) as u32,
            usize::try_from(header & u64::from(u32::MAX))?,
        );
        if magic_num != MAGIC_NUM {
            return Err(Error::new(
                ErrorKind::TcpParseMsgFailed,
                format!("invalid magic num: {magic_num}"),
            ));
        }
        if total_len < U32_BYTE_SIZE {
            return Err(Error::new(
                ErrorKind::TcpParseMsgFailed,
                format!("total len is short: {total_len}"),
            ));
        }

        let mut bytes = vec![0u8; total_len];
        stream
            .read_exact(&mut bytes)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpRecvMsgFailed, e.to_string()))?;

        let msg = RecvMsg::parse(&bytes)?;
        self.tcp_stream = Some(stream);
        Ok(msg)
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        if let Some(stream) = self.tcp_stream.take() {
            if self.for_send {
                self.socket_pool.add_socket_for_send(stream);
            } else {
                self.socket_pool.add_socket_for_recv(stream);
            }
        }
    }
}
