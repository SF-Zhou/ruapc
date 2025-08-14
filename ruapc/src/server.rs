use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{Listener, Result, Router, SocketPoolConfig, State};

pub struct Server {
    state: Arc<State>,
    listener: Listener,
    _drop_guard: DropGuard,
}

impl Server {
    #[must_use]
    pub fn create(router: Router, config: &SocketPoolConfig) -> Self {
        let state = State::create(router, config);
        let drop_guard = state.drop_guard();

        Self {
            state: Arc::new(state),
            listener: Listener::new(),
            _drop_guard: drop_guard,
        }
    }

    pub fn stop(&self) {
        self.listener.stop();
        self.state.socket_pool.stop();
    }

    pub async fn join(&self) {
        self.listener.join().await;
        self.state.socket_pool.join().await;
    }

    /// # Errors
    pub async fn listen(&self, addr: SocketAddr) -> Result<SocketAddr> {
        self.listener.start_listen(addr, &self.state).await
    }
}
