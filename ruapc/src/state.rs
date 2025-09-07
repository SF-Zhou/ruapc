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
    /// # Errors
    pub(crate) fn create(
        mut router: Router,
        config: &SocketPoolConfig,
    ) -> Result<(Arc<Self>, DropGuard)> {
        router.build_open_api()?;
        let state = Self {
            router,
            waiter: Arc::default(),
            socket_pool: SocketPool::create(config)?,
        };
        let state = Arc::new(state);
        let drop_guard = state.drop_guard();
        Ok((state, drop_guard))
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

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State").finish()
    }
}
