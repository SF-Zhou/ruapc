mod attempt;
mod buffers;
#[doc(hidden)]
pub use buffers::BufferScope;
pub use buffers::{Completed, OwnedBuffers};
mod call;
pub use call::{RawCall, ResponseContract};
mod with_buffers;

pub use with_buffers::ClientWithBuffers;

use serde::{Deserialize, Serialize};
use std::time::Duration;

mod request;
pub(crate) use request::ReadAttachment;

/// Configuration shared by generated RPC client methods.
///
/// # Examples
///
/// ```rust,ignore
/// let client = Client::default();
/// let endpoint = "tcp://127.0.0.1:8000".parse().unwrap();
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap().with_endpoint(endpoint);
///
/// let rsp = client.echo(&ctx, &"hello".into()).await;
/// ```
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Client {
    /// Response timeout after a connection is available, including admission
    /// to the transport's send queue. Default is 1 second.
    ///
    /// The effective response budget is the minimum of this value and
    /// the remaining deadline of the context (for nested RPCs issued while
    /// handling a request). The budget travels with the request so the
    /// server can drop work the client no longer waits for.
    /// Queue admission observes the absolute deadline; response waiting uses
    /// a shared expiry sweep, so timeout notification may occur later.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Total budget for connection establishment across the endpoint's addresses.
    /// Connection setup does not consume the response timeout; nested calls
    /// still cap both budgets at the parent context's remaining deadline.
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// Whether to use MessagePack serialization. Default is true.
    /// When false, JSON serialization is used.
    pub use_msgpack: bool,
    /// Retry limit for pre-wire connection or send-queue failures. Default: 2.
    ///
    /// The connection pool tries equivalent addresses within each acquisition;
    /// those address attempts do not consume this limit. Retries stay within
    /// the same logical endpoint. Timeouts and connection failures while waiting
    /// for a response are never retried: the request may have executed.
    pub max_retries: u32,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(5),
            use_msgpack: true,
            max_retries: 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Result};

    #[crate::service]
    pub(super) trait TestService {
        async fn test_call(&self, ctx: &Context, req: &()) -> Result<()>;
    }

    #[test]
    fn test_default_config() {
        let client = Client::default();
        assert_eq!(client.timeout, Duration::from_secs(1));
        assert_eq!(client.connect_timeout, Duration::from_secs(5));
        assert!(client.use_msgpack);
        assert_eq!(client.max_retries, 2);
    }

    #[test]
    fn test_client_serde_roundtrip() {
        let client = Client {
            timeout: Duration::from_millis(500),
            connect_timeout: Duration::from_secs(2),
            use_msgpack: false,
            max_retries: 4,
        };
        let json = serde_json::to_string(&client).unwrap();
        let recovered: Client = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, client);
    }

    #[test]
    fn test_client_serde_defaults_from_empty_object() {
        let client: Client =
            serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap();
        assert_eq!(client.timeout, Duration::from_secs(1));
        assert_eq!(client.connect_timeout, Duration::from_secs(5));
        assert!(client.use_msgpack);
    }

    #[test]
    fn test_client_debug_format() {
        let client = Client::default();
        let debug = format!("{:?}", client);
        assert!(debug.contains("Client"));
    }

    #[tokio::test]
    async fn test_ruapc_request_invalid_endpoint_returns_err() {
        use crate::{ErrorKind, SocketPoolConfig};
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let client = Client::default();
        let result = client.test_call(&ctx, &()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
    }
}
