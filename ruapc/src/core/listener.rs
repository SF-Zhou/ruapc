use std::{net::SocketAddr, sync::Arc};

use crate::{Error, ErrorKind, RawStream, Result, State, TaskSupervisor};

/// Network listener for accepting incoming connections.
///
/// The `Listener` manages TCP listener lifecycle and spawns tasks to handle
/// incoming connections. It provides graceful shutdown capabilities through
/// the task supervisor.
pub struct Listener {
    task_supervisor: TaskSupervisor,
}

impl Default for Listener {
    fn default() -> Self {
        Self::new()
    }
}

impl Listener {
    /// Creates a new listener.
    #[must_use]
    pub fn new() -> Self {
        Self {
            task_supervisor: TaskSupervisor::create(),
        }
    }

    /// Starts listening for connections on the specified address.
    ///
    /// This method binds a TCP listener to the given address and spawns a task
    /// to accept incoming connections. Each accepted connection is handled by
    /// the state's connection handler.
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address to bind to
    /// * `state` - Shared state for handling connections
    ///
    /// # Returns
    ///
    /// Returns the actual address the listener is bound to, which may differ
    /// from the requested address if port 0 was specified.
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP bind fails.
    pub async fn start_listen(&self, addr: SocketAddr, state: &Arc<State>) -> Result<SocketAddr> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::TcpBindFailed, e.to_string()))?;
        let listener_addr = listener
            .local_addr()
            .map_err(|e| Error::new(ErrorKind::TcpBindFailed, e.to_string()))?;
        let state = state.clone();

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            tokio::select! {
                () = task_supervisor.stopped() => {
                    tracing::info!("stop accept loop");
                }
                () = async {
                    tracing::info!("start listening: {listener_addr}");
                    while let Ok((stream, addr)) = listener.accept().await {
                        tokio::spawn(state.clone().handle_new_stream(RawStream::TCP(stream), addr));
                    }
                } => {}
            }
        });

        Ok(listener_addr)
    }

    /// Stops accepting new connections.
    ///
    /// This initiates shutdown of the listener task.
    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    /// Waits for the listener to fully stop.
    ///
    /// This method blocks until the listener task has completed.
    pub async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }
}
