use std::{sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;

pub(crate) struct RdmaDeviceRefresher {
    cancel: CancellationToken,
}

impl RdmaDeviceRefresher {
    const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

    pub(crate) fn start(devices: Arc<crate::Devices>) -> Self {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Self::REFRESH_INTERVAL);
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = interval.tick() => {
                        Self::refresh_all(&devices);
                    }
                }
            }
        });

        Self { cancel }
    }

    pub(crate) fn stop(&self) {
        self.cancel.cancel();
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

impl Drop for RdmaDeviceRefresher {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
