use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;

/// Transport protocol types supported by RuaPC.
///
/// Each socket type represents a different transport protocol with its own characteristics:
/// - **TCP**: Raw TCP sockets with custom protocol
/// - **WS**: WebSocket over HTTP
/// - **HTTP**: HTTP/1.1 with request/response semantics
/// - **UNIFIED**: Accepts all protocol types on the same port
/// - **RDMA**: High-performance RDMA (requires "rdma" feature)
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone, Copy, clap::ValueEnum)]
pub enum SocketType {
    /// Raw TCP transport.
    TCP,
    /// WebSocket transport.
    WS,
    /// HTTP transport.
    HTTP,
    /// Unified transport supporting multiple protocols.
    UNIFIED,
    /// RDMA transport (requires "rdma" feature).
    RDMA,
}

impl std::fmt::Display for SocketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

/// Socket pool configuration.
///
/// Specifies which transport protocol to use for the socket pool.
///
/// # Examples
///
/// ```rust
/// use ruapc_core::{SocketPoolConfig, SocketType};
///
/// let config = SocketPoolConfig {
///     socket_type: SocketType::TCP,
/// };
/// ```
#[serde_inline_default]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub struct SocketPoolConfig {
    /// The transport protocol type to use. Default is TCP.
    #[serde_inline_default(SocketType::TCP)]
    pub socket_type: SocketType,
}

impl Default for SocketPoolConfig {
    fn default() -> Self {
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default()))
            .expect("default SocketPoolConfig should deserialize successfully")
    }
}
