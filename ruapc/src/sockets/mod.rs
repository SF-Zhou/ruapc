/// Socket abstraction layer.
mod socket;
pub(crate) use socket::SocketHealth;
pub use socket::{Socket, SocketTrait};

mod connect;
mod lifecycle;
pub(crate) use lifecycle::{ConnectionLifecycle, ConnectionMap, PoolConnection};

mod endpoint;
pub use endpoint::{Endpoint, ListenMode, Transport};

/// Socket pool management and configuration.
mod socket_pool;
pub(crate) use socket_pool::AcquireOptions;
pub use socket_pool::{RawStream, SocketPool, SocketPoolConfig, SocketPoolTrait};
#[cfg(feature = "rdma")]
pub use socket_pool::{RdmaQueuePairConfig, RdmaSocketPoolConfig, RdmaZoneConfig};

/// HTTP transport implementation.
pub mod http;
/// TCP transport implementation.
pub mod tcp;
/// WebSocket transport implementation.
pub mod ws;
