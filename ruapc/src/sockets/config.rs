//! Transport-independent socket pool configuration.

use serde::{Deserialize, Serialize};

#[cfg(feature = "rdma")]
use crate::rdma::RdmaSocketPoolConfig;
use crate::{DEFAULT_BUFFER_POOL_MEMORY, ListenMode};

/// Socket pool configuration.
///
/// Configures listener behavior and transport resources.
///
/// # Examples
///
/// ```rust
/// use ruapc::{ListenMode, SocketPoolConfig};
///
/// let config = SocketPoolConfig {
///     listen_mode: ListenMode::TCP,
///     ..Default::default()
/// };
/// ```
#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct SocketPoolConfig {
    /// How accepted TCP streams are interpreted. Outbound transport is part
    /// of each [`Endpoint`](crate::Endpoint), not this configuration.
    pub listen_mode: ListenMode,
    /// Maximum memory of the shared buffer pool in bytes.
    pub buffer_pool_memory: usize,
    /// Maximum number of server-side requests processed concurrently;
    /// excess requests are rejected immediately with an `Overloaded` error
    /// response (load shedding). `0` disables the cap.
    pub max_inflight_requests: usize,
    /// Base path for HTTP RPC, streaming, and documentation endpoints.
    /// Empty or `/` serves HTTP endpoints at the root.
    pub http_base_path: String,
    /// RDMA-specific settings. Enabled by default when the crate's `rdma`
    /// feature is active; set to `None` to disable device discovery, memory
    /// registration, and connection resources.
    #[cfg(feature = "rdma")]
    #[serde(default)]
    pub rdma: Option<RdmaSocketPoolConfig>,
}

impl Default for SocketPoolConfig {
    fn default() -> Self {
        Self {
            listen_mode: ListenMode::TCP,
            buffer_pool_memory: DEFAULT_BUFFER_POOL_MEMORY,
            max_inflight_requests: 0,
            http_base_path: String::new(),
            #[cfg(feature = "rdma")]
            rdma: Some(RdmaSocketPoolConfig::default()),
        }
    }
}

impl SocketPoolConfig {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.buffer_pool_memory == 0 {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "buffer_pool_memory must be nonzero".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn normalized_http_base_path(&self) -> crate::Result<String> {
        if self.http_base_path.contains(['?', '#']) {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "http_base_path must not contain a query or fragment".into(),
            ));
        }

        let path = self.http_base_path.trim_matches('/');
        if path.is_empty() {
            return Ok(String::new());
        }

        let path = format!("/{path}");
        format!("http://localhost{path}/_rpc")
            .parse::<hyper::Uri>()
            .map_err(|error| {
                crate::Error::new(
                    crate::ErrorKind::InvalidArgument,
                    format!("invalid http_base_path: {error}"),
                )
            })?;
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_uses_transport_defaults() {
        let config = SocketPoolConfig::default();
        assert_eq!(config.listen_mode, ListenMode::TCP);
        assert_eq!(config.buffer_pool_memory, DEFAULT_BUFFER_POOL_MEMORY);
        assert!(config.http_base_path.is_empty());
        #[cfg(feature = "rdma")]
        assert_eq!(config.rdma, Some(RdmaSocketPoolConfig::default()));
    }

    #[test]
    fn normalizes_http_base_path() {
        for (input, expected) in [("", ""), ("/", ""), ("api", "/api"), ("/api/", "/api")] {
            let config = SocketPoolConfig {
                http_base_path: input.to_string(),
                ..Default::default()
            };
            assert_eq!(config.normalized_http_base_path().unwrap(), expected);
        }

        for input in ["/api?version=1", "/api#docs", "/not valid"] {
            let config = SocketPoolConfig {
                http_base_path: input.to_string(),
                ..Default::default()
            };
            assert!(config.normalized_http_base_path().is_err());
        }
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = SocketPoolConfig {
            listen_mode: ListenMode::UNIFIED,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json).unwrap()["buffer_pool_memory"],
            DEFAULT_BUFFER_POOL_MEMORY
        );
        let recovered: SocketPoolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, config);
        assert!(serde_json::from_str::<SocketPoolConfig>(r#"{"socket_type":"UNIFIED"}"#).is_err());
        assert_eq!(
            serde_json::from_str::<SocketPoolConfig>("{}")
                .unwrap()
                .buffer_pool_memory,
            DEFAULT_BUFFER_POOL_MEMORY
        );
        let invalid = SocketPoolConfig {
            buffer_pool_memory: 0,
            ..Default::default()
        };
        assert!(invalid.validate().is_err());
    }
}
