/// Socket abstraction layer.
mod socket;
pub use socket::Socket;

mod channel;
mod outbound;
pub(crate) use outbound::{Outbound, Segment};
mod send_queue;
#[cfg(test)]
pub(crate) mod test_util;
pub(crate) use send_queue::{SendRequest, reserve_send};
mod connect;
pub(crate) use channel::ChannelConnection;
#[cfg(test)]
pub(crate) use channel::MAX_DEFERRED_RESPONSES;
mod lifecycle;
pub(crate) use lifecycle::{ConnectionLifecycle, ConnectionMap, PoolConnection};

mod endpoint;
pub use endpoint::{Endpoint, ListenMode, Transport};

/// Socket pool configuration.
mod config;
pub use config::SocketPoolConfig;

/// Socket pool management.
mod socket_pool;
pub(crate) use socket_pool::AcquireOptions;
pub use socket_pool::{RawStream, SocketPool};

/// HTTP transport implementation.
pub mod http;
/// TCP transport implementation.
pub mod tcp;
/// WebSocket transport implementation.
pub mod ws;
