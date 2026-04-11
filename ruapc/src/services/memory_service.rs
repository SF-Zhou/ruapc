use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::device::Devices;
use crate::memory::MemoryKey;
use crate::{Context, Result};

/// Request to read from a remote memory region.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryReadReq {
    pub key: MemoryKey,
    pub addr: u64,
    pub len: u64,
}

/// Request to write to a remote memory region.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MemoryWriteReq {
    pub key: MemoryKey,
    pub addr: u64,
    pub data: Vec<u8>,
}

/// Built-in service for Remote Read/Write operations over TCP.
///
/// When a peer wants to read from or write to registered memory on this
/// side, it sends a reverse RPC to this service. The service validates
/// the access against the device's registry and performs the operation.
#[ruapc_macro::service]
pub trait MemoryService {
    /// Reads data from a registered memory region.
    async fn read(&self, ctx: &Context, req: &MemoryReadReq) -> Result<Vec<u8>>;

    /// Writes data to a registered memory region.
    async fn write(&self, ctx: &Context, req: &MemoryWriteReq) -> Result<()>;
}

/// Implementation of MemoryService backed by a Devices collection.
pub struct MemoryServiceImpl {
    pub devices: Arc<Devices>,
}

impl MemoryService for MemoryServiceImpl {
    async fn read(&self, _ctx: &Context, req: &MemoryReadReq) -> Result<Vec<u8>> {
        let device = self.device_for_key(&req.key)?;
        device.read_memory(self.tcp_id(&req.key)?, req.addr, req.len)
    }

    async fn write(&self, _ctx: &Context, req: &MemoryWriteReq) -> Result<()> {
        let device = self.device_for_key(&req.key)?;
        device.write_memory(self.tcp_id(&req.key)?, req.addr, &req.data)
    }
}

impl MemoryServiceImpl {
    fn device_for_key(&self, key: &MemoryKey) -> Result<&Arc<crate::device::Device>> {
        match key {
            MemoryKey::Tcp { .. } => {
                // Find the first TCP device.
                for dev in self.devices.iter() {
                    if matches!(dev.as_ref(), crate::device::Device::Tcp(_)) {
                        return Ok(dev);
                    }
                }
                Err(crate::Error::new(
                    crate::ErrorKind::InvalidArgument,
                    "no TCP device available".into(),
                ))
            }
            #[cfg(feature = "rdma")]
            MemoryKey::Rdma { .. } => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "RDMA remote read/write via MemoryService not supported".into(),
            )),
        }
    }

    fn tcp_id(&self, key: &MemoryKey) -> Result<u32> {
        match key {
            MemoryKey::Tcp { id } => Ok(*id),
            #[cfg(feature = "rdma")]
            _ => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "expected TCP memory key".into(),
            )),
        }
    }
}
