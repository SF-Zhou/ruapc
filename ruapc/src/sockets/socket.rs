use std::sync::Arc;

use ruapc_bufpool::DeviceIndex;
use serde::Serialize;

#[cfg(feature = "rdma")]
use crate::rdma::RdmaSocket;
use crate::{
    Buffer, Context, CopyOp, Error, ErrorKind, MsgMeta, RemoteIoError, RemoteSpace, Result, State,
    http::HttpSocket,
    remote_memory::{WriteTarget, inline, scatter::SpaceLayout},
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

/// Trait defining the interface for sending messages through different socket types.
pub trait SocketTrait {
    /// Sends a message through this socket.
    async fn send<P: Serialize>(
        &self,
        meta: &mut MsgMeta,
        payload: &P,
        state: &Arc<State>,
    ) -> Result<()>;

    /// Reads from the remote space; stream transports use reverse RPCs.
    async fn remote_read(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
        remote: &RemoteSpace<'_>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        inline::read(ctx, ops, local, remote).await
    }

    /// Writes to the remote space; RDMA overrides this with client-side READ.
    async fn remote_write(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        inline::write(ctx, ops, local).await
    }
}

impl Socket {
    /// Returns the device index associated with this socket.
    pub fn device_index(&self, state: &State) -> DeviceIndex {
        match self {
            Socket::TCP(_) | Socket::WS(_) | Socket::HTTP(_) => {
                ruapc_bufpool::Device::index(state.devices.tcp_device())
            }
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => rdma_socket.queue_pair.device_index,
        }
    }

    /// Executes the client side of `_ruapc.memory/read_into_target`: RDMA
    /// READs from the peer's advertised regions into the request's pinned
    /// write target. Only meaningful on RDMA connections.
    #[allow(unused_variables)]
    pub(crate) async fn read_into_target(
        &self,
        regions: &[ruapc_bufpool::RemoteBufferInfo],
        src_layout: &SpaceLayout,
        ops: &[CopyOp],
        target: std::sync::Arc<WriteTarget>,
        request_deadline: Option<std::time::Instant>,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => {
                rdma_socket
                    .read_into_target(regions, src_layout, ops, target, request_deadline)
                    .await
            }
            _ => Err(Error::new(
                ErrorKind::InvalidArgument,
                "read_into_target requires RDMA (other transports use write_inline)".into(),
            )),
        }
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
        ops: &[CopyOp],
        local: Vec<Buffer>,
        remote: &RemoteSpace<'_>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        match self {
            Socket::TCP(_) | Socket::WS(_) | Socket::HTTP(_) => {
                inline::read(ctx, ops, local, remote).await
            }
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => rdma_socket.remote_read(ctx, ops, local, remote).await,
        }
    }

    async fn remote_write(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        match self {
            Socket::TCP(_) | Socket::WS(_) | Socket::HTTP(_) => {
                inline::write(ctx, ops, local).await
            }
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => rdma_socket.remote_write(ctx, ops, local).await,
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
