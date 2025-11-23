use std::sync::Arc;

use serde::Serialize;

use crate::{MsgMeta, Result, State, http::HttpSocket, tcp::TcpSocket, ws::WebSocket};

/// Socket abstraction supporting multiple transport protocols.
///
/// The `Socket` enum provides a unified interface for different transport types:
/// - TCP: Raw TCP socket
/// - WS: WebSocket connection
/// - HTTP: HTTP/1.1 connection
/// - RDMA: RDMA connection (requires "rdma" feature)
///
/// All socket types support the same `send` operation for transmitting messages.
#[derive(Clone, Debug)]
pub enum Socket {
    /// TCP socket.
    TCP(TcpSocket),
    /// WebSocket.
    WS(WebSocket),
    /// HTTP socket.
    HTTP(HttpSocket),
    /// RDMA socket (requires "rdma" feature).
    #[cfg(feature = "rdma")]
    RDMA(std::sync::Arc<crate::rdma::RdmaSocket>),
}

impl Socket {
    /// Sends a message through this socket.
    ///
    /// This method serializes and sends a message with the given metadata and payload.
    /// The actual transmission mechanism depends on the underlying socket type.
    ///
    /// # Type Parameters
    ///
    /// * `P` - The payload type to serialize
    ///
    /// # Arguments
    ///
    /// * `meta` - Message metadata (method name, flags, etc.)
    /// * `payload` - The data to send
    /// * `state` - Shared state for request/response correlation
    ///
    /// # Errors
    ///
    /// Returns an error if sending fails.
    pub async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
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
