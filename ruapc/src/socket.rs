use serde::Serialize;

use crate::{MsgMeta, Receiver, Result, Waiter, tcp::TcpSocket, ws::WebSocket};

#[derive(Clone, Debug)]
pub enum Socket {
    Tcp(TcpSocket),
    WS(WebSocket),
}

impl Socket {
    /// # Errors
    pub async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        waiter: &Waiter,
    ) -> Result<Receiver> {
        match self {
            Socket::Tcp(tcp_socket) => tcp_socket.send(meta, payload, waiter).await,
            Socket::WS(web_socket) => web_socket.send(meta, payload, waiter).await,
        }
    }
}
