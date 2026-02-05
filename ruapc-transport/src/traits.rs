use std::{net::SocketAddr, pin::Pin, sync::Arc};

use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{Message, MsgMeta, RawStream, Result, SocketType};

/// Type alias for boxed futures.
pub type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Handler trait for processing received messages.
///
/// This trait enables dependency inversion - the transport layer doesn't know
/// about the RPC routing logic, it just notifies the handler when messages arrive.
/// The high-level RPC layer implements this trait to receive messages.
pub trait MessageHandler: Send + Sync + 'static {
    /// Called when a message is received from a socket.
    ///
    /// # Arguments
    ///
    /// * `socket` - The socket that received the message (as a dynamic trait object)
    /// * `msg` - The received message
    ///
    /// # Errors
    ///
    /// Returns an error if message processing fails.
    fn handle_recv(&self, socket: &dyn RuapcSocket, msg: Message) -> Result<()>;
}

/// Socket trait for sending messages over different transport protocols.
///
/// This trait provides a unified interface for all socket types (TCP, WebSocket, HTTP, RDMA).
/// Each transport implementation provides its own socket type that implements this trait.
///
/// # Example
///
/// ```rust,ignore
/// use ruapc_transport::{RuapcSocket, MsgMeta, Result};
/// use serde::Serialize;
///
/// async fn send_message<S: RuapcSocket>(socket: &S, meta: &mut MsgMeta, payload: &impl Serialize) -> Result<()> {
///     socket.send(meta, payload).await
/// }
/// ```
pub trait RuapcSocket: Send + Sync + std::fmt::Debug + 'static {
    /// Sends bytes through this socket.
    ///
    /// # Arguments
    ///
    /// * `meta` - Message metadata (method name, flags, message ID)
    /// * `payload` - The serialized payload bytes to send
    ///
    /// # Errors
    ///
    /// Returns an error if sending fails.
    fn send_bytes(&self, meta: MsgMeta, payload: bytes::Bytes) -> BoxFuture<'_, Result<()>>;

    /// Returns the socket type identifier.
    fn socket_type(&self) -> SocketType;

    /// Clones the socket into a boxed trait object.
    fn clone_boxed(&self) -> Box<dyn RuapcSocket>;

    /// Downcasts to a concrete socket type (for internal use).
    fn as_any(&self) -> &dyn std::any::Any;
}

impl Clone for Box<dyn RuapcSocket> {
    fn clone(&self) -> Self {
        self.clone_boxed()
    }
}

/// Socket pool trait for managing connections.
///
/// This trait provides a unified interface for socket pools that manage
/// connections for different transport protocols. It handles:
/// - Connection pooling and reuse
/// - New connection creation
/// - Lifecycle management (stop/join)
///
/// # Example
///
/// ```rust,ignore
/// use ruapc_transport::{RuapcSocketPool, SocketType, MessageHandler};
/// use std::net::SocketAddr;
/// use std::sync::Arc;
///
/// async fn get_socket<P: RuapcSocketPool>(
///     pool: &P,
///     addr: &SocketAddr,
///     handler: &Arc<dyn MessageHandler>,
/// ) -> Result<Box<dyn RuapcSocket>, ruapc_transport::Error> {
///     pool.acquire(addr, handler).await
/// }
/// ```
pub trait RuapcSocketPool: Send + Sync + std::fmt::Debug + 'static {
    /// Returns the socket type this pool manages.
    fn socket_type(&self) -> SocketType;

    /// Acquires a socket connection to the specified address.
    ///
    /// This method may reuse an existing connection from the pool
    /// or create a new connection if needed.
    ///
    /// # Arguments
    ///
    /// * `addr` - The target socket address
    /// * `handler` - Message handler for received messages
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    fn acquire(
        &self,
        addr: SocketAddr,
        handler: Arc<dyn MessageHandler>,
    ) -> BoxFuture<'_, Result<Box<dyn RuapcSocket>>>;

    /// Handles a new incoming connection stream.
    ///
    /// This method is called by the listener when a new connection is accepted.
    /// It processes the stream according to the pool's protocol type.
    ///
    /// # Arguments
    ///
    /// * `handler` - Message handler for received messages
    /// * `stream` - The raw network stream to handle
    /// * `addr` - The remote address of the connection
    ///
    /// # Errors
    ///
    /// Returns an error if stream handling fails.
    fn handle_new_stream(
        &self,
        handler: Arc<dyn MessageHandler>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> BoxFuture<'_, Result<()>>;

    /// Stops the socket pool and initiates connection cleanup.
    fn stop(&self);

    /// Creates a drop guard for this socket pool.
    fn drop_guard(&self) -> DropGuard;

    /// Waits for all connections in the pool to close.
    fn join(&self) -> BoxFuture<'_, ()>;
}

// Helper function for serializing payloads
pub fn serialize_payload<P: Serialize>(meta: &mut MsgMeta, payload: &P) -> Result<bytes::Bytes> {
    let mut bytes = bytes::BytesMut::with_capacity(512);
    meta.serialize_to(payload, &mut bytes)?;
    Ok(bytes.freeze())
}
