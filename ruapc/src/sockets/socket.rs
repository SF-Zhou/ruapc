use std::sync::Arc;

use ruapc_bufpool::RemoteBufferInfo;
use serde::Serialize;

use crate::{
    Buffer, Context, MsgMeta, Result, State,
    http::HttpSocket,
    services::{MemoryReadReq, MemoryService, MemoryWriteReq},
    tcp::TcpSocket,
    ws::WebSocket,
};

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
    RDMA(std::sync::Arc<crate::rdma::RdmaSocket>),
}

/// Trait defining the interface for sending messages through different socket types.
///
/// `SocketTrait` provides a unified interface for message transmission across
/// different transport protocols (TCP, WebSocket, HTTP, RDMA). Each socket type
/// implements this trait to provide its own send mechanism while maintaining
/// a consistent API.
///
/// # Implementors
///
/// - [`Socket`] - The main enum implementing this trait for all transport types
/// - Individual socket types (TcpSocket, WebSocket, HttpSocket, etc.) also implement this
pub trait SocketTrait {
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
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()>;

    async fn remote_read(
        &self,
        ctx: &Context,
        mut local: Buffer,
        remote: &RemoteBufferInfo,
    ) -> Result<Buffer> {
        let req = MemoryReadReq {
            key: remote.key,
            addr: remote.addr,
            len: remote.len,
        };
        let client = crate::Client::default();
        let data: Vec<u8> = client.tcp_read(ctx, &req).await?;
        if data.len() > local.len() {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                format!(
                    "remote read returned {} bytes but local buffer is {} bytes",
                    data.len(),
                    local.len()
                ),
            ));
        }
        local[..data.len()].copy_from_slice(&data);
        Ok(local)
    }

    async fn remote_write(
        &self,
        ctx: &Context,
        local: Buffer,
        remote: &RemoteBufferInfo,
    ) -> Result<Buffer> {
        let req = MemoryWriteReq {
            key: remote.key,
            addr: remote.addr,
            data: local[..remote.len as usize].to_vec(),
        };
        let client = crate::Client::default();
        client.tcp_write(ctx, &req).await?;
        Ok(local)
    }
}

impl SocketTrait for Socket {
    async fn send<P: Serialize>(
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

    async fn remote_read(
        &self,
        ctx: &Context,
        local: Buffer,
        remote: &RemoteBufferInfo,
    ) -> Result<Buffer> {
        match self {
            Socket::TCP(tcp_socket) => tcp_socket.remote_read(ctx, local, remote).await,
            Socket::WS(web_socket) => web_socket.remote_read(ctx, local, remote).await,
            Socket::HTTP(http_socket) => http_socket.remote_read(ctx, local, remote).await,
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => rdma_socket.remote_read(ctx, local, remote).await,
        }
    }

    async fn remote_write(
        &self,
        ctx: &Context,
        local: Buffer,
        remote: &RemoteBufferInfo,
    ) -> Result<Buffer> {
        match self {
            Socket::TCP(tcp_socket) => tcp_socket.remote_write(ctx, local, remote).await,
            Socket::WS(web_socket) => web_socket.remote_write(ctx, local, remote).await,
            Socket::HTTP(http_socket) => http_socket.remote_write(ctx, local, remote).await,
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => rdma_socket.remote_write(ctx, local, remote).await,
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
