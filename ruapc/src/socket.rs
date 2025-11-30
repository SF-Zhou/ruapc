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

    /// Performs an RDMA Read operation to read data from a remote buffer.
    ///
    /// This method is only available when the socket is an RDMA socket. It initiates
    /// an RDMA Read operation to fetch data from the remote peer's registered memory
    /// region. The operation is asynchronous and returns a receiver that will provide
    /// the completed buffer with the read data.
    ///
    /// # Usage Pattern
    ///
    /// 1. Client prepares a buffer with data and calls `buffer.as_remote(&device)` to
    ///    get a `RemoteBuffer` descriptor.
    /// 2. Client sends the `RemoteBuffer` info to the server via an RPC request,
    ///    including the message ID.
    /// 3. Server receives the request and extracts the `RemoteBuffer` info.
    /// 4. Server calls `is_message_waiting(msgid)` to verify the client is still waiting
    ///    (this ensures the client's buffer is still valid).
    /// 5. Server calls `rdma_read(remote_buffer, request_id)` to initiate the read.
    /// 6. Server awaits the receiver to get the completed buffer.
    ///
    /// # Arguments
    ///
    /// * `remote_buffer` - Information about the remote buffer to read from
    /// * `request_id` - A unique ID for this RDMA Read request (usually the msgid)
    ///
    /// # Returns
    ///
    /// * `Ok(receiver)` - A receiver that will provide the completed buffer
    /// * `Err` - If this is not an RDMA socket or if the operation fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // On server side, after receiving a request with RemoteBuffer info:
    /// if ctx.state.waiter.contains_message_id(msgid) {
    ///     if let Ok(receiver) = socket.rdma_read(&remote_buffer, msgid) {
    ///         match receiver.await {
    ///             Ok(Ok(buffer)) => {
    ///                 // Use the data in buffer
    ///             }
    ///             _ => {
    ///                 // Handle error
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    #[cfg(feature = "rdma")]
    pub fn rdma_read(
        &self,
        remote_buffer: &ruapc_rdma::RemoteBuffer,
        request_id: u64,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<ruapc_rdma::Buffer>>> {
        match self {
            Socket::RDMA(rdma_socket) => rdma_socket.rdma_read(remote_buffer, request_id),
            _ => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "RDMA Read is only supported on RDMA sockets".to_string(),
            )),
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
