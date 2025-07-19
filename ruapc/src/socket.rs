use serde::Serialize;

use crate::{MsgMeta, Receiver, Result, tcp::TcpSocket, ws::WebSocket};

#[derive(Clone, Debug)]
pub enum Socket {
    Tcp(TcpSocket),
    WS(WebSocket),
}

impl Socket {
    /// # Errors
    pub async fn send<P: Serialize>(&self, meta: MsgMeta, payload: &P) -> Result<Receiver> {
        match self {
            Socket::Tcp(tcp_socket) => tcp_socket.send(meta, payload).await,
            Socket::WS(web_socket) => web_socket.send(meta, payload).await,
        }
    }
}
