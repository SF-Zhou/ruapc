use std::sync::Arc;

use ruapc_bufpool::RemoteBufferInfo;
use serde::Serialize;
use thin_vec::ThinVec;

use crate::{Buffer, MsgMeta, Result, State, http::HttpSocket, tcp::TcpSocket, ws::WebSocket};
#[cfg(feature = "rdma")]
use crate::{Error, ErrorKind, rdma::RdmaSocket};

/// Socket abstraction supporting multiple transport protocols.
///
/// The `Socket` enum provides a unified interface for different transport types:
/// - TCP: Raw TCP socket
/// - WS: WebSocket connection
/// - HTTP: HTTP/1.1 and HTTP/2 (h2c) connection
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
    RDMA(std::sync::Arc<RdmaSocket>),
}

impl Socket {
    #[cfg(test)]
    pub(crate) fn conn_id(&self) -> Option<u64> {
        match self {
            Self::TCP(socket) => Some(socket.conn_id()),
            Self::WS(socket) => Some(socket.conn_id()),
            Self::HTTP(HttpSocket::Stream(socket)) => Some(socket.conn_id()),
            Self::HTTP(HttpSocket::ForResponse(_)) => None,
            #[cfg(feature = "rdma")]
            Self::RDMA(socket) => Some(socket.conn_id),
        }
    }

    /// Remote RDMA device of this connection; `None` for every other
    /// transport (and in builds without the `rdma` feature).
    pub(crate) fn rdma_remote_device(&self) -> Option<&str> {
        match self {
            #[cfg(feature = "rdma")]
            Self::RDMA(socket) => Some(&socket.path.remote.device),
            _ => None,
        }
    }

    /// Reserves local SEND bandwidth for memory the peer is expected to read.
    #[allow(unused_variables)]
    pub(crate) async fn reserve_rdma_send_bandwidth(
        &self,
        bytes: u64,
        request_deadline: Option<std::time::Instant>,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "rdma")]
            Self::RDMA(socket) => socket.reserve_send_bandwidth(bytes, request_deadline).await,
            _ => Ok(()),
        }
    }
}

impl Socket {
    /// Exports attachment lengths for streams, or registered addresses and keys
    /// for the RDMA connection's local NIC. Stream copies resolve owned buffers
    /// through the request waiter and never use addresses or registration keys.
    pub(crate) fn export_regions(&self, buffers: &[Buffer]) -> Result<ThinVec<RemoteBufferInfo>> {
        match self {
            Socket::TCP(_) | Socket::WS(_) | Socket::HTTP(_) => Ok(buffers
                .iter()
                .map(|buffer| RemoteBufferInfo {
                    len: buffer.len() as u64,
                    ..Default::default()
                })
                .collect()),
            #[cfg(feature = "rdma")]
            Socket::RDMA(socket) => buffers
                .iter()
                .map(|buffer| {
                    buffer
                        .remote_buffer_info(socket.queue_pair.device_index)
                        .map_err(|error| Error::new(ErrorKind::InvalidArgument, error.to_string()))
                })
                .collect(),
        }
    }

    /// Sends a message through this socket.
    pub async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()> {
        match self {
            Socket::TCP(tcp_socket) => tcp_socket.send(meta, payload, state).await,
            Socket::WS(web_socket) => web_socket.send(meta, payload, state).await,
            Socket::HTTP(http_socket) => http_socket.send(meta, payload, state).await,
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

impl From<HttpSocket> for Socket {
    fn from(value: HttpSocket) -> Self {
        Socket::HTTP(value)
    }
}

impl From<&HttpSocket> for Socket {
    fn from(value: &HttpSocket) -> Self {
        Socket::HTTP(value.clone())
    }
}

#[cfg(feature = "rdma")]
impl From<std::sync::Arc<RdmaSocket>> for Socket {
    fn from(value: std::sync::Arc<RdmaSocket>) -> Self {
        Socket::RDMA(value)
    }
}

#[cfg(feature = "rdma")]
impl From<&std::sync::Arc<RdmaSocket>> for Socket {
    fn from(value: &std::sync::Arc<RdmaSocket>) -> Self {
        Socket::RDMA(value.clone())
    }
}
