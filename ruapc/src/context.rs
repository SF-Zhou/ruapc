use std::{net::SocketAddr, sync::Arc};

use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{
    Error, Router, Socket, SocketPoolConfig, State,
    msg::{MsgFlags, MsgMeta},
};

#[derive(Clone, Debug, Default)]
pub enum SocketEndpoint {
    #[default]
    Invalid,
    Connected(Socket),
    Address(SocketAddr),
}

#[derive(Clone)]
pub struct Context {
    pub state: Arc<State>,
    pub(crate) endpoint: SocketEndpoint,
    pub(crate) drop_guard: Option<Arc<DropGuard>>,
}

impl Default for Context {
    fn default() -> Self {
        let state = Arc::new(State::default());
        Self {
            drop_guard: Some(Arc::new(state.drop_guard())),
            state,
            endpoint: SocketEndpoint::Invalid,
        }
    }
}

impl Context {
    #[must_use]
    pub fn create(config: &SocketPoolConfig) -> Self {
        let state = Arc::new(State::create(Router::default(), config));
        Self {
            drop_guard: Some(Arc::new(state.drop_guard())),
            state,
            endpoint: SocketEndpoint::Invalid,
        }
    }

    #[must_use]
    pub fn create_with_router(router: Router, config: &SocketPoolConfig) -> Self {
        let state = Arc::new(State::create(router, config));
        Self {
            drop_guard: Some(Arc::new(state.drop_guard())),
            state,
            endpoint: SocketEndpoint::Invalid,
        }
    }

    #[must_use]
    pub fn with_addr(&self, addr: SocketAddr) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: SocketEndpoint::Address(addr),
            drop_guard: self.drop_guard.clone(),
        }
    }

    #[must_use]
    pub fn with_socket(&self, socket: Socket) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: SocketEndpoint::Connected(socket),
            drop_guard: self.drop_guard.clone(),
        }
    }

    #[must_use]
    pub(crate) fn server_ctx(state: &Arc<State>, socket: Socket) -> Self {
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Connected(socket),
            drop_guard: None,
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
                let _ = socket.send(&mut meta, &rsp, &self.state.waiter).await;
            }
            _ => {
                tracing::error!("invalid argument: send rsp without connected socket");
            }
        }
    }

    pub async fn send_err_rsp(&mut self, meta: MsgMeta, err: Error) {
        self.send_rsp::<(), Error>(meta, Err(err)).await;
    }
}
