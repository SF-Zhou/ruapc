use std::{sync::Arc, time::Duration};

use crate::TaskSupervisor;

/// Background task that periodically refreshes RDMA port attributes.
///
/// Managed by [`TaskSupervisor`]; calling `task_supervisor.stop()` or
/// dropping the supervisor will signal this refresher to exit.
pub(crate) struct RdmaDeviceRefresher;

impl RdmaDeviceRefresher {
    const REFRESH_INTERVAL: Duration = Duration::from_secs(15);

    pub(crate) fn start(devices: Arc<crate::Devices>, task_supervisor: &TaskSupervisor) -> Self {
        let guard = task_supervisor.start_async_task();
        tokio::spawn(async move {
            let _guard = guard;
            let mut interval = tokio::time::interval(Self::REFRESH_INTERVAL);
            loop {
                tokio::select! {
                    _ = _guard.stopped() => break,
                    _ = interval.tick() => {
                        Self::refresh_all(&devices);
                    }
                }
            }
        });

        Self
    }

    fn refresh_all(devices: &crate::Devices) {
        for dev in devices.rdma_devices() {
            if let Err(err) = dev.refresh_port_attrs() {
                let info = dev.info();
                tracing::warn!(
                    device = %info.name,
                    error = %err,
                    "failed to refresh RDMA port attributes"
                );
            }
        }
    }
}
