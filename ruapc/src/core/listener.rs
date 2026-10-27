use std::{net::SocketAddr, sync::Arc};

use crate::sockets::tcp::configure_stream;
use crate::{Error, ErrorKind, RawStream, Result, State, TaskSupervisor};

/// TCP accept loops and supervised connection-setup tasks.
///
/// [`Server`](crate::Server) normally owns this lifecycle together with the
/// connection pools. Stopping a standalone listener does not stop those pools
/// or drain application handlers.
pub struct Listener {
    task_supervisor: TaskSupervisor,
}

impl Default for Listener {
    fn default() -> Self {
        Self::new()
    }
}

impl Listener {
    /// Creates a listener supervisor without binding an address.
    /// Requires a Tokio runtime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            task_supervisor: TaskSupervisor::create(),
        }
    }

    /// Binds a TCP listener and forwards accepted streams to
    /// [`State::handle_new_stream`]. May be called for multiple addresses.
    ///
    /// Returns the actual bound address, including an assigned port when `addr`
    /// uses port zero. The state's socket-pool configuration selects the protocol.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::TcpBindFailed`] if binding or reading the bound
    /// address fails, or [`ErrorKind::ConnectionClosed`] after shutdown begins.
    pub async fn start_listen(&self, addr: SocketAddr, state: &Arc<State>) -> Result<SocketAddr> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpBindFailed, e.to_string()))?;
        let listener_addr = listener
            .local_addr()
            .map_err(|e| Error::new(ErrorKind::TcpBindFailed, e.to_string()))?;
        let state = state.clone();
        let supervisor = self.task_supervisor.handle();

        let task_supervisor = self
            .task_supervisor
            .try_start_async_task()
            .ok_or_else(|| Error::new(ErrorKind::ConnectionClosed, "listener stopped".into()))?;
        tokio::spawn(async move {
            tokio::select! {
                () = task_supervisor.stopped() => {
                    tracing::info!("stop accept loop");
                }
                () = async {
                    tracing::info!("start listening: {listener_addr}");
                    while let Ok((stream, addr)) = listener.accept().await {
                        configure_stream(&stream);
                        let state = state.clone();
                        let _ = supervisor.try_spawn(async move {
                            state.handle_new_stream(RawStream::TCP(stream), addr).await;
                        });
                    }
                } => {}
            }
        });

        Ok(listener_addr)
    }

    /// Requests cancellation of accept loops and supervised connection-setup
    /// tasks. Use [`join`](Self::join) to await their exit.
    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    /// Waits for accept loops and supervised connection-setup tasks to exit.
    /// Call [`stop`](Self::stop) first; connection pools have separate lifecycles.
    pub async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }
}
