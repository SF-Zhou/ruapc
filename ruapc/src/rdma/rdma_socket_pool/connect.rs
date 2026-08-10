use std::sync::Arc;

use super::{Client, ConnectionControl, Context, RdmaService as _, RdmaSocket};

pub(super) struct SocketRegistrationGuard {
    socket: Arc<RdmaSocket>,
    armed: bool,
    abort: Option<(
        crate::TaskSupervisorHandle,
        Client,
        Context,
        ConnectionControl,
    )>,
}

impl SocketRegistrationGuard {
    pub(super) fn new(
        socket: &Arc<RdmaSocket>,
        supervisor: crate::TaskSupervisorHandle,
        client: Client,
        context: Context,
        control: ConnectionControl,
    ) -> Self {
        Self {
            socket: socket.clone(),
            armed: true,
            abort: Some((supervisor, client, context, control)),
        }
    }

    pub(super) fn commit(&mut self) {
        self.armed = false;
        self.abort = None;
    }
}

impl Drop for SocketRegistrationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.socket.set_error();
            if let Some((supervisor, client, context, control)) = self.abort.take() {
                let _ = supervisor.try_spawn(async move {
                    if let Err(err) = client.abort(&context, &control).await {
                        tracing::debug!(connection_id = control.connection_id, %err, "RDMA abort cleanup failed");
                    }
                });
            }
        }
    }
}

pub(super) struct EstablishedSocket {
    pub(super) socket: Arc<RdmaSocket>,
    pub(super) registration: SocketRegistrationGuard,
}
