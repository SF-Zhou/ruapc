use std::sync::Arc;

use serde::Serialize;

use crate::{MsgMeta, Receiver, Result, State, http::HttpSocket, tcp::TcpSocket, ws::WebSocket};

#[derive(Clone, Debug)]
pub enum Socket {
    TCP(TcpSocket),
    WS(WebSocket),
    HTTP(HttpSocket),
    #[cfg(feature = "rdma")]
    RDMA(std::sync::Arc<crate::rdma::RdmaSocket>),
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
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => rdma_socket.send(meta, payload, state).await,
        }
    }
}

impl From<TcpSocket> for Socket {
    fn from(value: TcpSocket) -> Self {
        Socket::TCP(value)
    }
}

impl From<&TcpSocket> for Socket {
    fn from(value: &TcpSocket) -> Self {
        Socket::TCP(value.clone())
    }
}

impl From<WebSocket> for Socket {
    fn from(value: WebSocket) -> Self {
        Socket::WS(value)
    }
}

impl From<&WebSocket> for Socket {
    fn from(value: &WebSocket) -> Self {
        Socket::WS(value.clone())
    }
}

#[cfg(feature = "rdma")]
impl From<std::sync::Arc<crate::rdma::RdmaSocket>> for Socket {
    fn from(value: std::sync::Arc<crate::rdma::RdmaSocket>) -> Self {
        Socket::RDMA(value)
    }
}

#[cfg(feature = "rdma")]
impl From<&std::sync::Arc<crate::rdma::RdmaSocket>> for Socket {
    fn from(value: &std::sync::Arc<crate::rdma::RdmaSocket>) -> Self {
        Socket::RDMA(value.clone())
    }
}
