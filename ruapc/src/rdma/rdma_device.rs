use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use arc_swap::ArcSwap;
use ruapc_bufpool::DeviceIndex;
use ruapc_rdma::{ActiveDevice, Context, DeviceInfo, ProtectionDomain};

use super::RdmaBandwidthLimiter;
use super::RdmaSocketPoolConfig;
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Copy, Debug, PartialEq)]
struct BandwidthLimitConfig {
    ratio: f64,
    burst: Duration,
    max_wait: Duration,
}

pub struct RdmaDevice {
    index: DeviceIndex,
    inner: ActiveDevice,
    info: ArcSwap<DeviceInfo>,
    bandwidth_config: OnceLock<BandwidthLimitConfig>,
    bandwidth_limiters: dashmap::DashMap<u8, Arc<RdmaBandwidthLimiter>>,
}

impl RdmaDevice {
    pub fn new(inner: ActiveDevice) -> Self {
        let info = Arc::new(inner.info().clone());
        Self {
            index: DeviceIndex::default(),
            inner,
            info: ArcSwap::from(info),
            bandwidth_config: OnceLock::new(),
            bandwidth_limiters: dashmap::DashMap::new(),
        }
    }

    pub fn context(&self) -> &Arc<Context> {
        self.inner.context()
    }

    pub fn pd(&self) -> &Arc<ProtectionDomain> {
        self.inner.pd()
    }

    pub fn info(&self) -> Arc<DeviceInfo> {
        self.info.load_full()
    }

    pub(crate) fn configure_bandwidth_limit(&self, config: &RdmaSocketPoolConfig) -> Result<()> {
        let bandwidth_config = BandwidthLimitConfig {
            ratio: config.remote_memory.bandwidth_limit_ratio,
            burst: Duration::from_millis(config.remote_memory.bandwidth_limit_burst_ms),
            max_wait: Duration::from_millis(config.remote_memory.bandwidth_limit_max_wait_ms),
        };
        if let Err(requested) = self.bandwidth_config.set(bandwidth_config)
            && self.bandwidth_config.get() != Some(&requested)
        {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA device bandwidth limiter is already configured differently".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn bandwidth_limiter(&self, port_num: u8) -> Result<Arc<RdmaBandwidthLimiter>> {
        if let Some(limiter) = self.bandwidth_limiters.get(&port_num) {
            return Ok(limiter.clone());
        }
        let config = self.bandwidth_config.get().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                "RDMA device bandwidth limiter is not configured".into(),
            )
        })?;
        let info = self.info();
        let port = info
            .ports
            .iter()
            .find(|port| port.port_num == port_num)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("RDMA device {} has no port {port_num}", info.name),
                )
            })?;
        let bytes_per_sec = if config.ratio == 0.0 {
            0
        } else {
            let bandwidth_bps = port.bandwidth_bps().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "cannot determine bandwidth of RDMA device {} port {port_num} (active_width={}, active_speed={})",
                        info.name,
                        port.port_attr.active_width,
                        port.port_attr.active_speed_raw(),
                    ),
                )
            })?;
            ((bandwidth_bps as f64 * config.ratio / 8.0) as u64).max(1)
        };
        Ok(self
            .bandwidth_limiters
            .entry(port_num)
            .or_insert_with(|| {
                Arc::new(RdmaBandwidthLimiter::new(
                    info.name.clone(),
                    port_num,
                    bytes_per_sec,
                    config.burst,
                    config.max_wait,
                ))
            })
            .clone())
    }

    /// Refreshes the cached device info snapshot from the hardware.
    ///
    /// GID filtering (RoCE v2 loopback / link-local) happens at collection
    /// time inside [`ActiveDevice::query_device_info`].
    pub fn refresh_port_attrs(&self) -> ruapc_rdma::Result<()> {
        let info = Arc::new(self.inner.query_device_info()?);
        self.info.store(info);
        Ok(())
    }
}

impl ruapc_bufpool::Device for RdmaDevice {
    fn index(&self) -> DeviceIndex {
        self.index
    }

    fn set_index(&mut self, idx: DeviceIndex) {
        self.index = idx;
    }

    type Registrar = ActiveDevice;

    fn registrar(&self) -> &Self::Registrar {
        &self.inner
    }
}

impl std::fmt::Debug for RdmaDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaDevice")
            .field("index", &self.index)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruapc_bufpool::Device as _;

    #[test]
    fn test_rdma_device_debug_format() {
        let mut rdma = RdmaDevice::new(crate::rdma::test_utils::open_rdma_device());
        rdma.set_index(DeviceIndex { magic: 0, index: 3 });
        let debug = format!("{rdma:?}");
        assert!(debug.contains("RdmaDevice"));
    }

    #[test]
    fn test_rdma_device_index_and_inner() {
        let mut rdma = RdmaDevice::new(crate::rdma::test_utils::open_rdma_device());
        rdma.set_index(DeviceIndex {
            magic: 0,
            index: 42,
        });
        assert_eq!(rdma.index().index, 42);
        // context() and pd() should not panic.
        let _ = rdma.context();
        let _ = rdma.pd();
    }

    #[test]
    fn test_bandwidth_limiter_is_shared_per_port() {
        let rdma = RdmaDevice::new(crate::rdma::test_utils::open_rdma_device());
        let config = RdmaSocketPoolConfig::default();
        rdma.configure_bandwidth_limit(&config).unwrap();
        let port_num = rdma.info().ports[0].port_num;
        let first = rdma.bandwidth_limiter(port_num).unwrap();
        let second = rdma.bandwidth_limiter(port_num).unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        let mut different = config;
        different.remote_memory.bandwidth_limit_ratio = 0.5;
        assert!(rdma.configure_bandwidth_limit(&different).is_err());
    }
}
