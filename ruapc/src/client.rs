use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use std::time::Duration;

use crate::{
    SocketType,
    context::{Context, SocketEndpoint},
    error::{Error, ErrorKind},
    msg::{MsgFlags, MsgMeta},
};

/// RPC client configuration and request handler.
///
/// The `Client` struct is used to make RPC requests to remote services.
/// It handles connection management, request serialization, and response
/// deserialization with configurable timeout and serialization format.
///
/// # Examples
///
/// ```rust,ignore
/// let client = Client::default();
/// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap().with_addr(addr);
///
/// let rsp = client.echo(&ctx, &Request("hello".into())).await;
/// ```
#[serde_inline_default]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Client {
    /// Timeout duration for RPC requests. Default is 1 second.
    #[serde_inline_default(Duration::from_secs(1))]
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Whether to use MessagePack serialization. Default is true.
    /// When false, JSON serialization is used.
    #[serde_inline_default(true)]
    pub use_msgpack: bool,
    /// Optional socket type to override the default from context.
    /// If None, uses the socket type from the context's socket pool.
    #[serde_inline_default(None)]
    pub socket_type: Option<SocketType>,
}

impl Default for Client {
    fn default() -> Self {
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap()
    }
}

impl Client {
    /// Makes an RPC request to a remote service.
    ///
    /// This is the internal method used by service trait implementations to
    /// send requests and receive responses. It handles:
    /// - Socket acquisition from the context
    /// - Request serialization
    /// - Response waiting with timeout
    /// - Response deserialization
    ///
    /// # Type Parameters
    ///
    /// * `Req` - The request type, must be serializable and have a JSON schema
    /// * `Rsp` - The response type, must be deserializable and have a JSON schema
    /// * `E` - The error type that can be returned
    ///
    /// # Arguments
    ///
    /// * `ctx` - The RPC context containing connection information
    /// * `req` - The request to send
    /// * `method_name` - The name of the RPC method to invoke
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The context doesn't have a valid endpoint
    /// - Socket acquisition fails
    /// - Request sending fails
    /// - Response timeout occurs
    /// - Response deserialization fails
    ///
    /// # Examples
    ///
    /// This method is typically called by generated service trait implementations,
    /// not directly by user code.
    pub async fn ruapc_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<crate::Error> + for<'c> Deserialize<'c>,
    {
        // 1. get socket.
        let socket = match &ctx.endpoint {
            SocketEndpoint::Invalid => {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "client context without address".to_string(),
                )
                .into());
            }
            SocketEndpoint::Connected(socket) => socket.clone(),
            SocketEndpoint::Address(socket_addr) => {
                let socket_type = self
                    .socket_type
                    .unwrap_or(ctx.state.socket_pool.socket_type());
                ctx.state
                    .socket_pool
                    .acquire(socket_addr, socket_type, &ctx.state)
                    .await?
            }
        };

        // 2. send request.
        let mut flags = MsgFlags::IsReq;
        if self.use_msgpack {
            flags |= MsgFlags::UseMessagePack;
        }

        let (msgid, receiver) = ctx.state.waiter.alloc();
        let mut meta = MsgMeta {
            method: method_name.into(),
            flags,
            msgid,
        };
        socket.send(&mut meta, req, &ctx.state).await?;

        // 3. recv response with timeout.
        if let Ok(result) = tokio::time::timeout(self.timeout, receiver.recv()).await {
            result?.deserialize()?
        } else {
            Err(Error::kind(ErrorKind::Timeout).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let client = Client::default();
        assert_eq!(client.timeout, Duration::from_secs(1));
    }
}
