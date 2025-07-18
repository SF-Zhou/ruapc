use serde::Serialize;

use crate::{MsgMeta, Receiver, Result, tcp::TcpSocket};

#[derive(Clone, Debug)]
pub enum Socket {
    Tcp(TcpSocket),
}

impl Socket {
    /// # Errors
    pub async fn send<P: Serialize>(&self, meta: MsgMeta, payload: &P) -> Result<Receiver> {
        match self {
            Socket::Tcp(tcp_socket) => tcp_socket.send(meta, payload).await,
        }
    }
}
