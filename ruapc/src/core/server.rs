use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{Listener, Result, Router, SocketPoolConfig, State};

/// RPC server owning listeners, connection pools and a service [`Router`].
///
/// Register services before [`create`](Self::create), then call
/// [`listen`](Self::listen) for each address. Shutdown uses
/// [`stop`](Self::stop) followed by [`join`](Self::join); it does not drain
/// application handlers.
///
/// # Examples
///
/// ```rust,no_run
/// use ruapc::{Router, Server, SocketPoolConfig};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let server = Server::create(Router::default(), &SocketPoolConfig::default())?;
/// let addr = server.listen("127.0.0.1:0".parse()?).await?;
/// println!("Listening on {addr}");
/// tokio::signal::ctrl_c().await?;
/// server.stop();
/// server.join().await;
/// # Ok(())
/// # }
/// ```
pub struct Server {
    state: Arc<State>,
    listener: Listener,
    _drop_guard: DropGuard,
}

impl Server {
    /// Creates shared state and connection pools without binding a listener.
    ///
    /// Requires a Tokio runtime. Use [`listen`](Self::listen) to bind addresses.
    ///
    /// # Errors
    ///
    /// Returns configuration, schema-generation or socket-pool initialization
    /// errors.
    ///
    /// With RDMA enabled, include the client's bootstrap transport (TCP, WS
    /// or HTTP) in the configured [`ListenMode`](crate::ListenMode).
    pub fn create(router: Router, config: &SocketPoolConfig) -> Result<Self> {
        let (state, drop_guard) = State::create(router, config)?;

        Ok(Self {
            state,
            listener: Listener::default(),
            _drop_guard: drop_guard,
        })
    }

    /// Shared state for buffer allocation, router inspection and, with the
    /// `rdma` feature, `State::rdma_path_report`.
    pub fn state(&self) -> &Arc<State> {
        &self.state
    }

    /// Requests listener shutdown and connection cleanup.
    ///
    /// Returns immediately. Call [`join`](Self::join) to await infrastructure
    /// shutdown. Running application handlers are not cancelled or drained.
    pub fn stop(&self) {
        self.listener.stop();
        self.state.socket_pool.stop();
    }

    /// Waits for listener and connection-pool tasks to stop.
    ///
    /// Call [`stop`](Self::stop) first. This does not wait for application
    /// handlers; applications that need a handler drain must track it themselves.
    pub async fn join(&self) {
        self.listener.join().await;
        self.state.socket_pool.join().await;
    }

    /// Binds an address and starts accepting connections using the configured
    /// listener mode. Returns the actual bound address, including an assigned
    /// port when `addr` uses port zero. May be called for multiple addresses.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::TcpBindFailed`](crate::ErrorKind::TcpBindFailed) if
    /// binding or reading the bound address fails, or
    /// [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed) after
    /// the listener has stopped.
    pub async fn listen(&self, addr: SocketAddr) -> Result<SocketAddr> {
        self.listener.start_listen(addr, &self.state).await
    }
}

#[cfg(all(test, feature = "rdma"))]
mod tests {
    use super::*;
    use crate::ListenMode;

    #[tokio::test]
    async fn rdma_enabled_server_accepts_every_listener_mode() {
        for listen_mode in [
            ListenMode::TCP,
            ListenMode::WS,
            ListenMode::HTTP,
            ListenMode::TCP | ListenMode::WS,
            ListenMode::TCP | ListenMode::HTTP,
            ListenMode::HTTP | ListenMode::WS,
            ListenMode::UNIFIED,
        ] {
            let config = SocketPoolConfig {
                listen_mode,
                ..Default::default()
            };
            let server = Server::create(Router::default(), &config)
                .unwrap_or_else(|err| panic!("{listen_mode} rejected: {err}"));
            server.stop();
            server.join().await;
        }
    }
}
