use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{
    Context, Message, RawStream, Result, Router, Socket, SocketPool, SocketPoolConfig, Waiter,
};

#[derive(Default)]
pub struct State {
    pub router: Router,
    pub(crate) waiter: Arc<Waiter>,
    pub(crate) socket_pool: SocketPool,
}

impl State {
    pub(crate) fn create(router: Router, config: &SocketPoolConfig) -> Self {
        Self {
            router,
            waiter: Arc::default(),
            socket_pool: SocketPool::create(config),
        }
    }

    /// # Errors
    pub fn handle_recv(self: &Arc<Self>, socket: &Socket, msg: Message) -> Result<()> {
        if msg.meta.is_req() {
            let ctx = Context::server_ctx(self, socket.clone());
            self.router.dispatch(ctx, msg);
        } else {
            self.waiter.post(msg.meta.msgid, msg);
        }
        Ok(())
    }

    pub async fn handle_new_stream(self: Arc<Self>, stream: RawStream, addr: SocketAddr) {
        if let Err(e) = self
            .socket_pool
            .handle_new_stream(&self, stream, addr)
            .await
        {
            tracing::error!("handle new tcp stream error: {e}");
        }
    }

    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.socket_pool.drop_guard()
    }
}
