use std::sync::Arc;

use ruapc_bufpool::DeviceIndex;
use serde::Serialize;

use crate::{
    Buffer, Context, CopyOp, MsgMeta, RemoteIoError, RemoteSpace, Result, State,
    core::scatter::SpaceLayout,
    http::HttpSocket,
    services::{MemoryPushReq, MemoryReadReq, MemoryService},
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

#[derive(Debug)]
pub(crate) enum SocketHealth {
    Tcp(std::sync::Weak<crate::tcp::TcpSocketInner>),
    Ws(std::sync::Weak<crate::ws::WebSocketInner>),
    Http(std::sync::Weak<crate::http::StreamSocketInner>),
    #[cfg(feature = "rdma")]
    RdmaPeer(std::sync::Weak<crate::rdma::RdmaPeerHealth>),
    #[cfg(feature = "rdma")]
    RdmaSocket(std::sync::Weak<crate::rdma::RdmaSocket>),
}

impl SocketHealth {
    pub(crate) fn is_connected(&self) -> bool {
        match self {
            Self::Tcp(socket) => socket.upgrade().is_some_and(|socket| !socket.is_closed()),
            Self::Ws(socket) => socket.upgrade().is_some_and(|socket| !socket.is_closed()),
            Self::Http(socket) => socket.upgrade().is_some_and(|socket| !socket.is_closed()),
            #[cfg(feature = "rdma")]
            Self::RdmaPeer(peer) => peer.upgrade().is_some_and(|peer| peer.is_connected()),
            #[cfg(feature = "rdma")]
            Self::RdmaSocket(socket) => socket.upgrade().is_some_and(|socket| socket.state.is_ok()),
        }
    }

    pub(crate) fn is_aggregate(&self) -> bool {
        #[cfg(feature = "rdma")]
        {
            matches!(self, Self::RdmaPeer(_))
        }
        #[cfg(not(feature = "rdma"))]
        {
            false
        }
    }

    pub(crate) fn same_scope(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Tcp(left), Self::Tcp(right)) => left.ptr_eq(right),
            (Self::Ws(left), Self::Ws(right)) => left.ptr_eq(right),
            (Self::Http(left), Self::Http(right)) => left.ptr_eq(right),
            #[cfg(feature = "rdma")]
            (Self::RdmaPeer(left), Self::RdmaPeer(right)) => left.ptr_eq(right),
            #[cfg(feature = "rdma")]
            (Self::RdmaSocket(left), Self::RdmaSocket(right)) => left.ptr_eq(right),
            _ => false,
        }
    }
}

impl Socket {
    pub(crate) fn conn_id(&self) -> Option<u64> {
        match self {
            Self::TCP(socket) => Some(socket.conn_id()),
            Self::WS(socket) => Some(socket.conn_id()),
            Self::HTTP(crate::http::HttpSocket::Stream(socket)) => Some(socket.conn_id()),
            Self::HTTP(crate::http::HttpSocket::ForResponse(_)) => None,
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

    /// Executes a validated batch of reads from the peer's read space into
    /// the `local` buffers (see [`Context::remote_read`] for the space and
    /// op semantics; validation already happened there).
    ///
    /// Default implementation (TCP/WS/HTTP): a reverse `MemoryService/read`
    /// RPC returns the requested byte ranges inline, which are then
    /// scattered into `local` according to the ops.
    async fn remote_read(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        mut local: Vec<Buffer>,
        remote: &RemoteSpace<'_>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        // Pass msgid so that the client verifies the original request is
        // still alive after reading its buffers.
        let req = MemoryReadReq {
            regions: remote.regions().to_vec(),
            ops: ops.to_vec(),
            msgid: ctx.msg_meta.msgid,
        };
        let client = crate::Client::default();
        let data: Vec<u8> = match client.read(ctx, &req).await {
            Ok(rsp) => rsp.data,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        let expected: u64 = ops.iter().map(|op| op.len).sum();
        if data.len() as u64 != expected {
            return Err(RemoteIoError::new(
                crate::Error::new(
                    crate::ErrorKind::InvalidCopyOp,
                    format!(
                        "remote read returned {} bytes but the ops requested {expected}",
                        data.len()
                    ),
                ),
                Some(local),
            ));
        }
        // Scatter the inline blob (op payloads concatenated in op order)
        // into the local space.
        let layout = match SpaceLayout::from_lens(local.iter().map(|b| b.len() as u64)) {
            Ok(layout) => layout,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        let mut cursor = 0usize;
        for op in ops {
            let _ = layout.for_each_slice::<std::convert::Infallible>(
                op.dst_offset,
                op.len,
                |seg, off, len| {
                    let (off, len) = (off as usize, len as usize);
                    local[seg][off..off + len].copy_from_slice(&data[cursor..cursor + len]);
                    cursor += len;
                    Ok(())
                },
            );
        }
        Ok(local)
    }

    /// Executes a validated batch of writes from the `local` buffers into
    /// the peer's write space (see [`Context::remote_write`]; validation
    /// already happened there).
    ///
    /// Default implementation (TCP/WS/HTTP): the op payloads travel inline
    /// in a reverse `MemoryService/push` RPC and the client copies them
    /// into its pinned write buffers.
    async fn remote_write(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        // Gather the op payloads (in op order) into one inline blob.
        let layout = match SpaceLayout::from_lens(local.iter().map(|b| b.len() as u64)) {
            Ok(layout) => layout,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        let total: u64 = ops.iter().map(|op| op.len).sum();
        let mut data = Vec::with_capacity(total as usize);
        for op in ops {
            let _ = layout.for_each_slice::<std::convert::Infallible>(
                op.src_offset,
                op.len,
                |seg, off, len| {
                    let (off, len) = (off as usize, len as usize);
                    data.extend_from_slice(&local[seg][off..off + len]);
                    Ok(())
                },
            );
        }
        let req = MemoryPushReq {
            msgid: ctx.msg_meta.msgid,
            ops: ops.to_vec(),
            data,
        };
        let client = crate::Client::default();
        match client.push(ctx, &req).await {
            Ok(()) => Ok(local),
            Err(e) => Err(RemoteIoError::new(e, Some(local))),
        }
    }
}

impl Socket {
    pub(crate) fn health(&self) -> Option<SocketHealth> {
        match self {
            Socket::TCP(socket) => Some(SocketHealth::Tcp(socket.health())),
            Socket::WS(socket) => Some(SocketHealth::Ws(socket.health())),
            Socket::HTTP(socket) => socket.health().map(SocketHealth::Http),
            #[cfg(feature = "rdma")]
            Socket::RDMA(socket) => Some(match socket.peer_health() {
                Some(peer) => SocketHealth::RdmaPeer(peer),
                None => SocketHealth::RdmaSocket(Arc::downgrade(socket)),
            }),
        }
    }

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

    /// Executes the client side of a `MemoryService/pull` request: RDMA
    /// READs from the peer's advertised regions into the request's pinned
    /// write target. Only meaningful on RDMA connections.
    #[allow(unused_variables)]
    pub(crate) async fn pull_into_target(
        &self,
        regions: &[ruapc_bufpool::RemoteBufferInfo],
        src_layout: &SpaceLayout,
        ops: &[CopyOp],
        target: std::sync::Arc<crate::core::WriteTarget>,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => {
                rdma_socket
                    .pull_into_target(regions, src_layout, ops, target)
                    .await
            }
            _ => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "pull requires an RDMA connection (non-RDMA transports use push)".into(),
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
            Socket::TCP(tcp_socket) => tcp_socket.remote_read(ctx, ops, local, remote).await,
            Socket::WS(web_socket) => web_socket.remote_read(ctx, ops, local, remote).await,
            Socket::HTTP(http_socket) => http_socket.remote_read(ctx, ops, local, remote).await,
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
            Socket::TCP(tcp_socket) => tcp_socket.remote_write(ctx, ops, local).await,
            Socket::WS(web_socket) => web_socket.remote_write(ctx, ops, local).await,
            Socket::HTTP(http_socket) => http_socket.remote_write(ctx, ops, local).await,
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
