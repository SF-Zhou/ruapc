use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use arc_swap::ArcSwap;
use ruapc_bufpool::DeviceIndex;
use ruapc_rdma::{ActiveDevice, DeviceInfo, Gid, ProtectionDomain};

pub struct RdmaDevice {
    index: DeviceIndex,
    inner: ActiveDevice,
    info: ArcSwap<DeviceInfo>,
}

impl RdmaDevice {
    pub fn new(inner: ActiveDevice) -> Self {
        let info = Arc::new(inner.info().clone());
        Self {
            index: DeviceIndex::default(),
            inner,
            info: ArcSwap::from(info),
        }
    }

    pub fn inner(&self) -> &ActiveDevice {
        &self.inner
    }

    pub fn pd(&self) -> &Arc<ProtectionDomain> {
        self.inner.pd()
    }

    pub fn info(&self) -> Arc<DeviceInfo> {
        self.info.load_full()
    }

    pub fn refresh_port_attrs(&self) -> ruapc_rdma::Result<()> {
        let mut info = (*self.info()).clone();
        let device_attr = self.inner.context().query_device()?;

        let mut ports = Vec::with_capacity(device_attr.phys_port_cnt as usize);
        for port_num in 1..=device_attr.phys_port_cnt {
            let port_attr = self.inner.context().query_port(port_num)?;
            let gids = self.collect_port_gids(port_num, &port_attr, &info);
            ports.push(ruapc_rdma::Port {
                port_num,
                port_attr,
                gids,
            });
        }

        info.device_attr = device_attr;
        info.ports = ports;
        self.info.store(Arc::new(info));
        Ok(())
    }

    fn collect_port_gids(
        &self,
        port_num: u8,
        port_attr: &ruapc_rdma::ibv_port_attr,
        info: &DeviceInfo,
    ) -> Vec<Gid> {
        let mut gids = Vec::with_capacity(port_attr.gid_tbl_len as usize);
        for gid_index in 0..port_attr.gid_tbl_len as u16 {
            let Ok(gid) = self.inner.context().query_gid(port_num, gid_index) else {
                continue;
            };
            if let Ok(gid_type) = self.inner.context().query_gid_type(
                port_num,
                gid_index,
                &info.ibdev_path,
                port_attr,
            ) {
                gids.push(Gid {
                    index: gid_index,
                    gid,
                    gid_type,
                });
            }
        }
        gids
    }
}

struct RefreshSignal {
    stop: AtomicBool,
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl RefreshSignal {
    fn new() -> Self {
        Self {
            stop: AtomicBool::new(false),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }
}

pub(crate) struct RdmaDeviceRefresher {
    signal: Arc<RefreshSignal>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RdmaDeviceRefresher {
    const REFRESH_INTERVAL: Duration = Duration::from_secs(15);

    pub(crate) fn start(devices: Arc<crate::Devices>) -> crate::Result<Self> {
        let signal = Arc::new(RefreshSignal::new());
        let thread_signal = signal.clone();
        let handle = thread::Builder::new()
            .name("ruapc-rdma-port-refresh".into())
            .spawn(move || {
                while !thread_signal.stop.load(Ordering::Relaxed) {
                    Self::refresh_all(&devices);

                    let guard = thread_signal.mutex.lock().expect("RDMA refresher poisoned");
                    let _ = thread_signal
                        .condvar
                        .wait_timeout_while(guard, Self::REFRESH_INTERVAL, |_| {
                            !thread_signal.stop.load(Ordering::Relaxed)
                        })
                        .expect("RDMA refresher poisoned");
                }
            })
            .map_err(|e| {
                crate::Error::new(
                    crate::ErrorKind::Unknown("RdmaDeviceRefresher".into()),
                    e.to_string(),
                )
            })?;

        Ok(Self {
            signal,
            handle: Mutex::new(Some(handle)),
        })
    }

    pub(crate) fn stop(&self) {
        self.signal.stop.store(true, Ordering::Relaxed);
        self.signal.condvar.notify_all();

        let Some(handle) = self
            .handle
            .lock()
            .expect("RDMA refresher handle poisoned")
            .take()
        else {
            return;
        };

        if handle.join().is_err() {
            tracing::warn!("RDMA port attribute refresher thread panicked");
        }
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
        self.stop();
    }
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
            .inner()
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

    fn open_rdma_device() -> ActiveDevice {
        let active_devices =
            ruapc_rdma::ActiveDevice::available().expect("RDMA devices should be available");
        let prefer_rxe = std::env::var("RUAPC_PREFER_RXE").is_ok();
        active_devices
            .into_iter()
            .find(|d| !prefer_rxe || d.info().name.starts_with("rxe"))
            .expect("no RDMA device matching filter found")
    }

    #[test]
    fn test_rdma_device_debug_format() {
        let mut rdma = RdmaDevice::new(open_rdma_device());
        rdma.set_index(DeviceIndex { magic: 0, index: 3 });
        let debug = format!("{rdma:?}");
        assert!(debug.contains("RdmaDevice"));
    }

    #[test]
    fn test_rdma_device_index_and_inner() {
        let mut rdma = RdmaDevice::new(open_rdma_device());
        rdma.set_index(DeviceIndex {
            magic: 0,
            index: 42,
        });
        assert_eq!(rdma.index().index, 42);
        // inner() and pd() should not panic.
        let _ = rdma.inner();
        let _ = rdma.pd();
    }
}
