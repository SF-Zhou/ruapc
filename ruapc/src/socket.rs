use std::sync::Arc;

use serde::Serialize;

use crate::{MsgMeta, Receiver, Result, State, http::HttpSocket, tcp::TcpSocket, ws::WebSocket};

#[derive(Clone, Debug)]
pub enum Socket {
    TCP(TcpSocket),
    WS(WebSocket),
    HTTP(HttpSocket),
}

impl Socket {
    /// # Errors
    pub async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<Receiver> {
        match self {
            Socket::TCP(tcp_socket) => tcp_socket.send(meta, payload, state).await,
            Socket::WS(web_socket) => web_socket.send(meta, payload, state).await,
            Socket::HTTP(http_socket) => http_socket.send(meta, payload, state),
        }
    }
}
