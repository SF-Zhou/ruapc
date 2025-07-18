use std::sync::Arc;

use tokio_util::sync::DropGuard;

use crate::{Context, MsgFlags, RecvMsg, Result, Router, Socket, SocketPool, Waiter};

#[derive(Default)]
pub struct State {
    pub router: Router,
    pub(crate) waiter: Arc<Waiter>,
    pub(crate) socket_pool: SocketPool,
}

impl State {
    /// # Errors
    pub fn handle_recv(self: &Arc<Self>, socket: &Socket, msg: RecvMsg) -> Result<()> {
        if msg.meta.flags.contains(MsgFlags::IsReq) {
            let ctx = Context::server_ctx(self, socket.clone());
            self.router.dispatch(ctx, msg);
        } else {
            self.waiter.post(msg.meta.msgid, msg);
        }
        Ok(())
    }

    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.socket_pool.drop_guard()
    }
}
