//! Transport-independent socket pool configuration.

use serde::{Deserialize, Serialize};

#[cfg(feature = "rdma")]
use crate::rdma::RdmaSocketPoolConfig;
use crate::{DEFAULT_BUFFER_POOL_MEMORY, Error, ErrorKind, ListenMode, Result};

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
///     listen_mode: ListenMode::TCP | ListenMode::HTTP,
///     ..Default::default()
/// };
/// ```
#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct SocketPoolConfig {
    /// Nonempty set of accepted protocols, combined with `|`. Defaults to TCP.
    /// Outbound transport is part of each [`Endpoint`](crate::Endpoint), not
    /// this configuration.
    pub listen_mode: ListenMode,
    /// Maximum memory of the shared buffer pool in bytes.
    pub buffer_pool_memory: usize,
    /// Maximum number of server-side requests processed concurrently across
    /// all methods, including built-in payload-transfer RPCs;
    /// excess requests are rejected immediately with an `Overloaded` error
    /// response (load shedding). `0` disables the cap. Allow enough capacity
    /// for handlers awaiting nested or reverse calls, those calls, and their
    /// transport RPCs.
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
    pub(crate) fn validate(&self) -> Result<()> {
        if self.listen_mode.is_empty() || !ListenMode::UNIFIED.contains(self.listen_mode) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "listen_mode must contain at least one of TCP, HTTP or WS and no unknown bits"
                    .into(),
            ));
        }
        if self.buffer_pool_memory == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "buffer_pool_memory must be nonzero".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn normalized_http_base_path(&self) -> Result<String> {
        if self.http_base_path.contains(['?', '#']) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
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
                Error::new(
                    ErrorKind::InvalidArgument,
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
    fn listen_mode_requires_known_nonempty_protocols() {
        for listen_mode in [
            ListenMode::empty(),
            ListenMode::from_bits_retain(0x80),
            ListenMode::TCP | ListenMode::from_bits_retain(0x80),
        ] {
            let config = SocketPoolConfig {
                listen_mode,
                ..Default::default()
            };
            assert_eq!(
                config.validate().unwrap_err().kind,
                ErrorKind::InvalidArgument
            );
        }
        let config: SocketPoolConfig =
            serde_json::from_str(r#"{"listen_mode":"http|ws"}"#).unwrap();
        assert_eq!(config.listen_mode, ListenMode::HTTP | ListenMode::WS);
        config.validate().unwrap();
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
