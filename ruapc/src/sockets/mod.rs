/// Socket abstraction layer.
mod socket;
pub use socket::{RemoteReadOptions, Socket, SocketTrait};

/// Socket pool management and configuration.
mod socket_pool;
pub use socket_pool::{RawStream, SocketPool, SocketPoolConfig, SocketPoolTrait, SocketType};
#[cfg(feature = "rdma")]
pub use socket_pool::{RdmaQueuePairConfig, RdmaSocketPoolConfig};

/// HTTP transport implementation.
pub mod http;
/// TCP transport implementation.
pub mod tcp;
/// Unified transport that supports multiple protocols simultaneously.
pub mod unified;
/// WebSocket transport implementation.
pub mod ws;
