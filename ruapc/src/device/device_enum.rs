use std::sync::Arc;

use crate::Result;
use crate::device::TcpDevice;
use crate::memory::{MemoryKey, MemoryRegistration, RemoteBufferInfo};

#[cfg(feature = "rdma")]
use crate::device::RdmaDevice;
use crate::services::{MemoryReadReq, MemoryWriteReq};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Device {
    Tcp(TcpDevice),
    #[cfg(feature = "rdma")]
    Rdma(RdmaDevice),
}

impl Device {
    pub fn read_memory(&self, req: &MemoryReadReq) -> Result<Vec<u8>> {
        let id = match req.key {
            MemoryKey::Tcp { id } => id,
            #[cfg(feature = "rdma")]
            MemoryKey::Rdma { .. } => {
                return Err(crate::Error::new(
                    crate::ErrorKind::InvalidArgument,
                    "RDMA remote read/write via MemoryService not supported".into(),
                ));
            }
        };

        match self {
            Device::Tcp(d) => d.read_memory(id, req.addr, req.len),
            #[cfg(feature = "rdma")]
            Device::Rdma(_) => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "read_memory not supported on RDMA device".into(),
            )),
        }
    }

    pub fn write_memory(&self, req: &MemoryWriteReq) -> Result<()> {
        let id = match req.key {
            MemoryKey::Tcp { id } => id,
            #[cfg(feature = "rdma")]
            MemoryKey::Rdma { .. } => {
                return Err(crate::Error::new(
                    crate::ErrorKind::InvalidArgument,
                    "RDMA remote read/write via MemoryService not supported".into(),
                ));
            }
        };

        match self {
            Device::Tcp(d) => d.write_memory(id, req.addr, &req.data),
            #[cfg(feature = "rdma")]
            Device::Rdma(_) => Err(crate::Error::new(
                crate::ErrorKind::InvalidArgument,
                "write_memory not supported on RDMA device".into(),
            )),
        }
    }

    pub fn memory_key(
        &self,
        buffer: &ruapc_bufpool::Buffer<crate::device::Devices>,
    ) -> Result<MemoryKey> {
        let reg = buffer
            .registration(self)
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;
        Ok(reg.memory_key())
    }

    pub fn remote_buffer_info(
        &self,
        buffer: &ruapc_bufpool::Buffer<crate::device::Devices>,
    ) -> Result<RemoteBufferInfo> {
        let key = self.memory_key(buffer)?;
        Ok(RemoteBufferInfo {
            key,
            addr: buffer.as_ptr() as u64,
            len: buffer.capacity() as u64,
        })
    }
}

impl ruapc_bufpool::Device for Device {
    type Registration = MemoryRegistration;

    fn index(&self) -> usize {
        match self {
            Device::Tcp(d) => d.index(),
            #[cfg(feature = "rdma")]
            Device::Rdma(d) => d.index(),
        }
    }

    fn set_index(&mut self, idx: usize) {
        match self {
            Device::Tcp(d) => d.set_index(idx),
            #[cfg(feature = "rdma")]
            Device::Rdma(d) => d.set_index(idx),
        }
    }

    #[allow(unsafe_code)]
    fn register(
        self: &Arc<Self>,
        mem: &mut ruapc_bufpool::RegisteredMemory<Self::Registration>,
    ) -> std::io::Result<()> {
        match self.as_ref() {
            Device::Tcp(tcp) => {
                let id = tcp.register(mem.aligned_memory().as_ptr(), mem.aligned_memory().size());
                mem.add_registration(MemoryRegistration::Tcp {
                    device: self.clone(),
                    id,
                });
                Ok(())
            }
            #[cfg(feature = "rdma")]
            Device::Rdma(rdma) => {
                let ptr = mem.aligned_memory().as_ptr();
                let size = mem.aligned_memory().size();
                let access = ruapc_rdma_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
                    | ruapc_rdma_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
                    | ruapc_rdma_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
                    | ruapc_rdma_sys::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING.0;
                let mr = unsafe {
                    ruapc_rdma_sys::MemoryRegion::register(
                        rdma.pd(),
                        ptr as *mut _,
                        size,
                        access as _,
                    )
                }
                .map_err(|e| std::io::Error::other(e.to_string()))?;
                mem.add_registration(MemoryRegistration::Rdma {
                    device: self.clone(),
                    mr,
                });
                Ok(())
            }
        }
    }
}
