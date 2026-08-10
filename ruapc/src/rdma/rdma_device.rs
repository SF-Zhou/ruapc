use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ruapc_bufpool::DeviceIndex;
use ruapc_rdma::{ActiveDevice, Context, DeviceInfo, ProtectionDomain};

use super::path::gid_ip;

type GidZones = HashMap<(u8, u8), Vec<String>>;

pub struct RdmaDevice {
    index: DeviceIndex,
    inner: ActiveDevice,
    snapshot: ArcSwap<RdmaDeviceSnapshot>,
    zone_config: Vec<crate::RdmaZoneConfig>,
}

struct RdmaDeviceSnapshot {
    info: Arc<DeviceInfo>,
    gid_zones: Arc<GidZones>,
}

impl RdmaDevice {
    pub fn new(inner: ActiveDevice, zone_config: Vec<crate::RdmaZoneConfig>) -> Self {
        let info = Arc::new(inner.info().clone());
        let gid_zones = Arc::new(discover_gid_zones(&info, &zone_config));
        Self {
            index: DeviceIndex::default(),
            inner,
            snapshot: ArcSwap::from_pointee(RdmaDeviceSnapshot { info, gid_zones }),
            zone_config,
        }
    }

    pub fn context(&self) -> &Arc<Context> {
        self.inner.context()
    }

    pub fn pd(&self) -> &Arc<ProtectionDomain> {
        self.inner.pd()
    }

    pub fn info(&self) -> Arc<DeviceInfo> {
        self.snapshot.load().info.clone()
    }

    pub(crate) fn info_with_zones(&self) -> (Arc<DeviceInfo>, Arc<GidZones>) {
        let snapshot = self.snapshot.load();
        (snapshot.info.clone(), snapshot.gid_zones.clone())
    }

    /// Refreshes the cached device info snapshot from the hardware.
    ///
    /// GID filtering (RoCE v2 loopback / link-local) happens at collection
    /// time inside [`ActiveDevice::query_device_info`].
    pub fn refresh_port_attrs(&self) -> ruapc_rdma::Result<()> {
        let info = Arc::new(self.inner.query_device_info()?);
        let gid_zones = Arc::new(discover_gid_zones(&info, &self.zone_config));
        self.snapshot
            .store(Arc::new(RdmaDeviceSnapshot { info, gid_zones }));
        Ok(())
    }
}

fn discover_gid_zones(info: &DeviceInfo, config: &[crate::RdmaZoneConfig]) -> GidZones {
    if config.is_empty() {
        return HashMap::new();
    }
    let mut result = HashMap::new();
    for port in &info.ports {
        for gid in &port.gids {
            let Some(ip) = gid_ip(&gid.gid) else {
                continue;
            };
            let zones = labels_for_ips(&[ip], config);
            if !zones.is_empty() {
                result.insert((port.port_num, gid.index), zones);
            }
        }
    }
    result
}

fn labels_for_ips(ips: &[IpAddr], config: &[crate::RdmaZoneConfig]) -> Vec<String> {
    config
        .iter()
        .filter(|subnet| {
            ips.iter()
                .any(|ip| subnet.cidrs.iter().any(|network| network.contains(ip)))
        })
        .map(|subnet| subnet.name.clone())
        .collect()
}

impl ruapc_bufpool::Device for RdmaDevice {
    fn index(&self) -> DeviceIndex {
        self.index
    }

    fn set_index(&mut self, idx: DeviceIndex) {
        self.index = idx;
    }

    fn register(
        &self,
        mem: &Arc<ruapc_bufpool::AlignedMemory>,
    ) -> std::io::Result<Box<dyn ruapc_bufpool::Registration>> {
        let mr = self
            .inner
            .register(mem)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(Box::new(mr))
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
        let mut rdma = RdmaDevice::new(crate::rdma::test_utils::open_rdma_device(), Vec::new());
        rdma.set_index(DeviceIndex { magic: 0, index: 3 });
        let debug = format!("{rdma:?}");
        assert!(debug.contains("RdmaDevice"));
    }

    #[test]
    fn test_rdma_device_index_and_inner() {
        let mut rdma = RdmaDevice::new(crate::rdma::test_utils::open_rdma_device(), Vec::new());
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
    fn test_labels_for_ips() {
        let config = vec![
            crate::RdmaZoneConfig {
                name: "a".into(),
                cidrs: vec!["10.1.0.0/16".parse().unwrap()],
            },
            crate::RdmaZoneConfig {
                name: "v6".into(),
                cidrs: vec!["2001:db8::/32".parse().unwrap()],
            },
        ];
        let ips = ["10.1.2.3".parse().unwrap(), "192.0.2.1".parse().unwrap()];
        assert_eq!(labels_for_ips(&ips, &config), ["a"]);
    }
}
