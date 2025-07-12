use std::{net::SocketAddr, sync::Arc};

use serde::Serialize;

use crate::{Socket, SocketPool, error::Error, msg::{MsgMeta, MsgFlags}};

pub enum SocketEndpoint {
    Connected(Socket),
    Address(SocketAddr),
}

pub struct Context {
    pub(crate) socket_pool: Arc<SocketPool>,
    pub(crate) endpoint: SocketEndpoint,
}

impl Context {
    #[must_use] pub fn create_for_client(addr: SocketAddr) -> Self {
        Self {
            socket_pool: Arc::default(),
            endpoint: SocketEndpoint::Address(addr),
        }
    }

    pub async fn send_rsp<Rsp, E>(&mut self, mut meta: MsgMeta, rsp: std::result::Result<Rsp, E>)
    where
        Rsp: Serialize,
        E: std::error::Error + From<Error> + Serialize,
    {
        meta.flags.remove(MsgFlags::IsReq);
        match &mut self.endpoint {
            SocketEndpoint::Connected(socket) => {
                let _ = socket.send(meta, &rsp).await;
            }
            SocketEndpoint::Address(_) => {
                tracing::error!("invalid argument: send rsp without connected socket");
            }
        }
    }

    pub async fn send_err_rsp(&mut self, meta: MsgMeta, err: Error) {
        self.send_rsp::<(), Error>(meta, Err(err)).await;
    }
}
