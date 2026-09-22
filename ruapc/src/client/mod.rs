mod attempt;
mod call;
pub use call::{CallPlain, CallWithBuffer, RawCall, RpcCall};
mod with_buffers;

pub use with_buffers::ClientWithBuffers;

use crate::Buffer;
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
    /// Response timeout after a connection is available. Default is 1 second.
    ///
    /// The effective response budget is the minimum of this value and
    /// the remaining deadline of the context (for nested RPCs issued while
    /// handling a request). The budget travels with the request so the
    /// server can drop work the client no longer waits for.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Total budget for connection establishment and endpoint failover.
    /// Connection setup does not consume the response timeout; nested calls
    /// still cap both budgets at the parent context's remaining deadline.
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// Whether to use MessagePack serialization. Default is true.
    /// When false, JSON serialization is used.
    pub use_msgpack: bool,
    /// Retry limit for pre-wire connection or send-queue failures. Default: 2.
    ///
    /// With [`Context::with_endpoints`](crate::Context::with_endpoints), retries
    /// prefer untried endpoints, ranked by current health, and may cycle once
    /// all candidates have been tried. Timeouts and connection failures while
    /// waiting for a response are never retried: the request may have executed.
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

impl Client {
    /// Creates a [`ClientWithBuffers`] wrapper attaching one *read* buffer
    /// to requests; see [`with_read_buffers`](Self::with_read_buffers).
    pub fn with_read_buffer(&self, buffer: Buffer) -> ClientWithBuffers<'_> {
        ClientWithBuffers::new(self).with_read_buffer(buffer)
    }

    /// Creates a [`ClientWithBuffers`] wrapper attaching *read* buffers to
    /// requests.
    ///
    /// The buffers' `RemoteBufferInfo` (one region per buffer, in order)
    /// is included in each request's metadata as the request's *read
    /// space*: a logically contiguous concatenation the server can read
    /// from with [`Context::remote_read`](crate::Context::remote_read).
    ///
    /// Each buffer contributes its logical length (`Buffer::len()`): call
    /// `set_len` after filling so the space covers exactly the valid data
    /// bytes. The wrapper, pending requests, and local inline readers share
    /// the immutable source. Reuse the wrapper or recover its buffers with
    /// [`ClientWithBuffers::take_read_buffers`]. This ownership protects local
    /// CPU copies; it does not track completion of remote one-sided RDMA reads.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let rsp = client.with_read_buffers(bufs).upload(&ctx, &req).await?;
    /// ```
    pub fn with_read_buffers(&self, buffers: Vec<Buffer>) -> ClientWithBuffers<'_> {
        ClientWithBuffers::new(self).with_read_buffers(buffers)
    }

    /// Creates a [`ClientWithBuffers`] wrapper attaching *write* buffers
    /// to requests.
    ///
    /// The buffers form a logical write space for
    /// [`Context::remote_write`](crate::Context::remote_write). Set each
    /// buffer's logical length to its receivable size before attaching.
    /// The next call consumes the set; in-flight RDMA retains destination
    /// ownership even if that call times out or is cancelled.
    ///
    /// A `Result<WithBuffers<T>, E>` response returns the buffers when the
    /// target is available and uniquely held. After failure, recover any
    /// available set with [`ClientWithBuffers::take_write_buffers`].
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let (rsp, bufs) = client
    ///     .with_write_buffers(vec![buf_a, buf_b])
    ///     .download(&ctx, &req)
    ///     .await?
    ///     .into_parts();
    /// ```
    pub fn with_write_buffers(&self, buffers: Vec<Buffer>) -> ClientWithBuffers<'_> {
        ClientWithBuffers::new(self).with_write_buffers(buffers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        use crate::{
            SocketPoolConfig,
            services::{DescribeRequest, ReflectionService as _},
        };
        let ctx = crate::Context::create(&SocketPoolConfig::default()).unwrap();
        let client = Client::default();
        let result = client.describe(&ctx, &DescribeRequest::default()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::InvalidArgument);
    }
}
