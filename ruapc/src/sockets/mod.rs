/// Socket abstraction layer.
mod socket;
pub use socket::{Socket, SocketTrait};

/// Socket pool management and configuration.
mod socket_pool;
pub use socket_pool::{RawStream, SocketPool, SocketPoolConfig, SocketPoolTrait, SocketType};

/// HTTP transport implementation.
pub mod http;
/// RDMA (Remote Direct Memory Access) transport implementation.
#[cfg(feature = "rdma")]
pub mod rdma;
/// TCP transport implementation.
pub mod tcp;
/// Unified transport that supports multiple protocols simultaneously.
pub mod unified;
/// WebSocket transport implementation.
pub mod ws;
