use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::DropGuard;

use crate::{Error, ErrorKind, RawStream, Result, State, TaskSupervisor};

pub struct Listener {
    task_supervisor: TaskSupervisor,
}

impl Default for Listener {
    fn default() -> Self {
        Self::new()
    }
}

impl Listener {
    #[must_use]
    pub fn new() -> Self {
        Self {
            task_supervisor: TaskSupervisor::create(),
        }
    }

    /// # Errors
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

    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    #[must_use]
    pub fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    pub async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }
}
