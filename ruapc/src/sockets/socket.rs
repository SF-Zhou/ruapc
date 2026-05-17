use std::sync::Arc;

use ruapc_bufpool::{DeviceIndex, RemoteBufferInfo};
use serde::Serialize;

use crate::{
    Buffer, Context, MsgMeta, Result, State,
    http::HttpSocket,
    services::{MemoryPushReq, MemoryReadReq, MemoryService},
    tcp::TcpSocket,
    ws::WebSocket,
};

/// Options controlling `remote_read` behavior.
///
/// The `skip_verify` field is `pub(crate)` to prevent external code from
/// bypassing the UUID liveness check.
#[derive(Debug, Clone, Copy, Default)]
pub struct RemoteReadOptions {
    /// When true, skips the post-read UUID liveness verification.
    ///
    /// Only safe when the remote buffer's lifetime is guaranteed by the
    /// calling context (e.g., the server holds `&buf` across `.await`).
    /// Only used by the RDMA path; the TCP path always verifies inline.
    #[cfg_attr(not(feature = "rdma"), allow(dead_code))]
    pub(crate) skip_verify: bool,
}

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
pub trait SocketTrait {
    /// Sends a message through this socket.
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
        _options: &RemoteReadOptions,
    ) -> Result<Buffer> {
        // Pass msgid so that tcp_read on the client side verifies
        // the original request is still alive after reading the buffer.
        let req = MemoryReadReq {
            key: remote.key,
            addr: remote.addr,
            len: remote.len,
            msgid: ctx.msg_meta.msgid,
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

    async fn remote_write(&self, ctx: &Context, local: Buffer) -> Result<Buffer> {
        // Push data to the client via tcp_push.
        let req = MemoryPushReq {
            msgid: ctx.msg_meta.msgid,
            data: local[..].to_vec(),
        };
        let client = crate::Client::default();
        client.tcp_push(ctx, &req).await?;
        Ok(local)
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
        options: &RemoteReadOptions,
    ) -> Result<Buffer> {
        match self {
            Socket::TCP(tcp_socket) => tcp_socket.remote_read(ctx, local, remote, options).await,
            Socket::WS(web_socket) => web_socket.remote_read(ctx, local, remote, options).await,
            Socket::HTTP(http_socket) => http_socket.remote_read(ctx, local, remote, options).await,
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => rdma_socket.remote_read(ctx, local, remote, options).await,
        }
    }

    async fn remote_write(&self, ctx: &Context, local: Buffer) -> Result<Buffer> {
        match self {
            Socket::TCP(tcp_socket) => tcp_socket.remote_write(ctx, local).await,
            Socket::WS(web_socket) => web_socket.remote_write(ctx, local).await,
            Socket::HTTP(http_socket) => http_socket.remote_write(ctx, local).await,
            #[cfg(feature = "rdma")]
            Socket::RDMA(rdma_socket) => rdma_socket.remote_write(ctx, local).await,
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
