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

#[serde_inline_default]
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Client {
    #[serde_inline_default(Duration::from_secs(1))]
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde_inline_default(true)]
    pub use_msgpack: bool,
    #[serde_inline_default(None)]
    pub socket_type: Option<SocketType>,
}

impl Default for Client {
    fn default() -> Self {
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::default())).unwrap()
    }
}

impl Client {
    /// # Errors
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
        let mut meta = MsgMeta {
            method: method_name.into(),
            flags,
            msgid: 0,
        };
        let receiver = socket.send(&mut meta, req, &ctx.state).await?;

        // 3. recv response with timeout.
        if let Ok(result) = tokio::time::timeout(self.timeout, receiver.recv()).await {
            result?.deserialize()?
        } else {
            ctx.state.waiter.set_timeout(meta.msgid);
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
