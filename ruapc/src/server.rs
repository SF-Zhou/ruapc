use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{Result, Router, SocketPool, State};

pub struct Server {
    state: Arc<State>,
    _drop_guard: DropGuard,
}

impl Server {
    #[must_use]
    pub fn create(router: Router) -> Self {
        let state = State {
            router,
            waiter: Arc::default(),
            socket_pool: SocketPool::default(),
        };
        let drop_guard = state.drop_guard();

        Self {
            state: Arc::new(state),
            _drop_guard: drop_guard,
        }
    }

    pub fn stop(&self) {
        self.state.socket_pool.stop();
    }

    pub async fn join(&self) {
        self.state.socket_pool.join().await;
    }

    /// # Errors
    pub async fn listen(&self, addr: SocketAddr) -> Result<SocketAddr> {
        let state = self.state.clone();
        state.socket_pool.start_listen(addr, &state).await
    }
}
