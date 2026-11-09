use std::{sync::Arc, time::Duration};

use crate::{TaskSupervisor, rdma::RdmaDevice};

/// Starts a background task that periodically refreshes RDMA port attributes.
///
/// Managed by [`TaskSupervisor`]; calling `task_supervisor.stop()` or
/// dropping the supervisor will signal this refresher to exit.
pub(crate) fn start_device_refresher(devices: Arc<[RdmaDevice]>, task_supervisor: &TaskSupervisor) {
    const REFRESH_INTERVAL: Duration = Duration::from_secs(15);

    let guard = task_supervisor.start_async_task();
    tokio::spawn(async move {
        let _guard = guard;
        let mut interval = tokio::time::interval(REFRESH_INTERVAL);
        loop {
            tokio::select! {
                _ = _guard.stopped() => break,
                _ = interval.tick() => {
                    refresh_all(&devices);
                }
            }
        }
    });
}

fn refresh_all(devices: &[RdmaDevice]) {
    for dev in devices {
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
