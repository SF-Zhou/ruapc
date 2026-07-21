use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use std::sync::Mutex;
use std::time::Duration;

use crate::{
    Buffer, Context, SocketEndpoint, SocketTrait, SocketType,
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
/// let rsp = client.echo(&ctx, &"hello".into()).await;
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
    /// Creates a [`ClientWithBuffer`] that attaches a read buffer to requests.
    ///
    /// The returned wrapper implements the same service traits as `Client`, but
    /// includes the buffer's `RemoteBufferInfo` in each request's metadata,
    /// allowing the server to `remote_read` the client's registered memory.
    ///
    /// The advertised length is the buffer's logical length (`buffer.len()`):
    /// call `set_len` after filling the buffer so the server transfers exactly
    /// the valid data bytes.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut buf = pool.allocate(1024 * 1024)?;
    /// buf[..data.len()].copy_from_slice(data);
    /// buf.set_len(data.len());
    /// let rsp = client.with_read_buffer(&buf).upload(&ctx, &req).await?;
    /// ```
    pub fn with_read_buffer<'a>(&'a self, buffer: &'a Buffer) -> ClientWithBuffer<'a> {
        ClientWithBuffer {
            client: self,
            read_buffer: Some(buffer),
            write_buffer: Mutex::new(None),
        }
    }

    /// Makes an RPC request to a remote service.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The RPC context containing connection information
    /// * `req` - The request payload to send
    /// * `read_buffer` - Optional registered memory buffer for the server to read.
    /// * `write_buffer_slot` - Optional slot to receive a buffer written by the server.
    ///   If the server performs a `remote_write` during handling, the received buffer
    ///   will be placed here after the response arrives.
    /// * `method_name` - The name of the RPC method to invoke
    pub async fn ruapc_request<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        read_buffer: Option<&Buffer>,
        write_buffer_slot: Option<&mut Option<Buffer>>,
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

        // Extract buffer info if a read buffer is provided.
        let buffer_info = if let Some(buf) = read_buffer {
            let device_index = socket.device_index(&ctx.state);
            Some(
                buf.remote_buffer_info(&device_index)
                    .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?,
            )
        } else {
            None
        };

        // The waiter entry expires after `self.timeout` (coarse, swept
        // periodically); no per-request timer is registered.
        let (msgid, receiver) = ctx.state.waiter.alloc(self.timeout);
        let mut meta = MsgMeta {
            method: method_name.into(),
            flags,
            msgid,
            buffer_info,
        };
        socket.send(&mut meta, req, &ctx.state).await?;

        // 3. recv response (fails with Timeout once the entry expires).
        let (response, write_buffer) = receiver.recv().await?;
        // Pass the write buffer to the caller if a slot was provided.
        if let Some(slot) = write_buffer_slot {
            *slot = write_buffer;
        }
        response.payload.deserialize(&response.meta)?
    }
}

/// A client wrapper that attaches a read buffer to RPC calls.
///
/// Created via [`Client::with_read_buffer`]. Implements the same service
/// traits as `Client` (generated by the `#[service]` macro).
///
/// # Read buffer
///
/// The attached buffer's `RemoteBufferInfo` is included in every request's
/// metadata, allowing the server to `remote_read` the client's memory.
///
/// # Write buffer
///
/// A buffer pushed by the server (via a `Result<WithBuffer<T>, E>` service
/// method) is delivered through the return value of the call itself; no
/// manual retrieval is involved.
///
/// # Examples
///
/// ```rust,ignore
/// // Server reads from the client's buffer:
/// let rsp = client.with_read_buffer(&buf).upload(&ctx, &req).await?;
///
/// // Read buffer attached and a server-pushed buffer received, in one call:
/// let (rsp, out) = client.with_read_buffer(&buf).transform(&ctx, &req).await?.into_parts();
/// ```
pub struct ClientWithBuffer<'a> {
    client: &'a Client,
    read_buffer: Option<&'a Buffer>,
    write_buffer: Mutex<Option<Buffer>>,
}

impl<'a> ClientWithBuffer<'a> {
    /// Attaches a read buffer to this wrapper.
    ///
    /// The buffer's `RemoteBufferInfo` will be included in request metadata,
    /// allowing the server to `remote_read` the client's memory.
    pub fn with_read_buffer(mut self, buffer: &'a Buffer) -> Self {
        self.read_buffer = Some(buffer);
        self
    }

    /// Takes the write buffer received from the server, if any.
    ///
    /// Crate-internal: buffers pushed by the server are surfaced to users
    /// through `Result<WithBuffer<T>, E>` return values (see
    /// `core::contract`), never via manual retrieval.
    pub(crate) fn take_write_buffer(&self) -> Option<Buffer> {
        self.write_buffer.lock().unwrap().take()
    }

    /// Makes an RPC request with the configured buffers.
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
        let mut slot = self.write_buffer.lock().unwrap().take();
        let result = self
            .client
            .ruapc_request(ctx, req, self.read_buffer, Some(&mut slot), method_name)
            .await;
        *self.write_buffer.lock().unwrap() = slot;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let client = Client::default();
        assert_eq!(client.timeout, Duration::from_secs(1));
        assert!(client.use_msgpack);
        assert!(client.socket_type.is_none());
    }

    #[test]
    fn test_client_serde_roundtrip() {
        let client = Client {
            timeout: Duration::from_millis(500),
            use_msgpack: false,
            socket_type: Some(SocketType::TCP),
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
        assert!(client.use_msgpack);
        assert!(client.socket_type.is_none());
    }

    #[test]
    fn test_client_debug_format() {
        let client = Client::default();
        let debug = format!("{:?}", client);
        assert!(debug.contains("Client"));
    }

    #[tokio::test]
    async fn test_ruapc_request_invalid_endpoint_returns_err() {
        use crate::{SocketPoolConfig, services::MetaService as _};
        let ctx = crate::Context::create(&SocketPoolConfig::default()).unwrap();
        let client = Client::default();
        let result = client.list_methods(&ctx, &()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::InvalidArgument);
    }
}
