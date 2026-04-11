use std::sync::Arc;

use crate::device::{Device, Devices};
use crate::{Error, ErrorKind, Result};

use super::aligned_memory::AlignedMemory;
use super::memory_key::MemoryKey;
use super::memory_registration::MemoryRegistration;

/// A registered memory block.
///
/// Contains an `AlignedMemory` together with its registrations on a set
/// of devices. On drop, all registrations are undone before the underlying
/// memory is freed.
pub struct Memory {
    aligned_memory: AlignedMemory,
    registrations: Vec<Option<MemoryRegistration>>,
}

impl Memory {
    /// Creates a new `Memory` by allocating aligned memory and registering
    /// it on every device in `devices`.
    pub fn new(size: usize, devices: &Devices) -> Result<Self> {
        let aligned_memory = AlignedMemory::new(size)?;
        let mut registrations: Vec<Option<MemoryRegistration>> =
            (0..devices.len()).map(|_| None).collect();

        for device in devices.iter() {
            let reg = Self::register_on_device(&aligned_memory, device)?;
            registrations[device.index()] = Some(reg);
        }

        Ok(Self {
            aligned_memory,
            registrations,
        })
    }

    /// Registers the aligned memory on a single device.
    #[allow(unsafe_code)]
    fn register_on_device(mem: &AlignedMemory, device: &Arc<Device>) -> Result<MemoryRegistration> {
        match device.as_ref() {
            Device::Tcp(tcp) => {
                let id = tcp.register(mem.as_ptr() as usize, mem.size());
                Ok(MemoryRegistration::Tcp {
                    device: device.clone(),
                    id,
                })
            }
            #[cfg(feature = "rdma")]
            Device::Rdma(rdma) => {
                let mr = unsafe {
                    ruapc_rdma::verbs::ibv_reg_mr(
                        rdma.pd_ptr(),
                        mem.as_ptr() as *mut _,
                        mem.size(),
                        ruapc_rdma::verbs::ACCESS_FLAGS as _,
                    )
                };
                if mr.is_null() {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "ibv_reg_mr failed".into(),
                    ));
                }
                let raw_mr = unsafe { ruapc_rdma::RawMemoryRegion::from_raw(mr) };
                Ok(MemoryRegistration::Rdma {
                    device: device.clone(),
                    mr: raw_mr,
                })
            }
        }
    }

    /// Returns the `MemoryKey` for the given device, looked up by device index.
    pub fn get_memory_key(&self, device: &Device) -> Result<MemoryKey> {
        let index = device.index();
        let reg = self
            .registrations
            .get(index)
            .and_then(|r| r.as_ref())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("memory not registered on device index {index}"),
                )
            })?;
        Ok(reg.memory_key())
    }

    /// Returns a reference to the underlying `AlignedMemory`.
    pub fn aligned_memory(&self) -> &AlignedMemory {
        &self.aligned_memory
    }

    /// Returns a mutable reference to the underlying `AlignedMemory`.
    pub fn aligned_memory_mut(&mut self) -> &mut AlignedMemory {
        &mut self.aligned_memory
    }
}

impl Drop for Memory {
    fn drop(&mut self) {
        for reg in self.registrations.iter_mut().flatten() {
            reg.unregister(&self.aligned_memory);
        }
    }
}

impl std::fmt::Debug for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memory")
            .field("aligned_memory", &self.aligned_memory)
            .field("registrations", &self.registrations.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_register_and_get_key() {
        let mut devices = Devices::new();
        let d0 = devices.add_tcp_device();
        let d1 = devices.add_tcp_device();

        let mem = Memory::new(4096, &devices).unwrap();

        let key0 = mem.get_memory_key(&d0).unwrap();
        let key1 = mem.get_memory_key(&d1).unwrap();

        // Both should be Tcp keys.
        assert!(matches!(key0, MemoryKey::Tcp { .. }));
        assert!(matches!(key1, MemoryKey::Tcp { .. }));
    }

    #[test]
    fn test_memory_drop_unregisters() {
        let mut devices = Devices::new();
        let d0 = devices.add_tcp_device();

        let mem = Memory::new(4096, &devices).unwrap();
        let key = mem.get_memory_key(&d0).unwrap();
        let id = match key {
            MemoryKey::Tcp { id } => id,
            #[cfg(feature = "rdma")]
            _ => panic!("expected Tcp key"),
        };

        // Verify the TCP device has this ID registered.
        let tcp = match d0.as_ref() {
            Device::Tcp(tcp) => tcp,
            #[cfg(feature = "rdma")]
            _ => panic!("expected Tcp device"),
        };
        assert!(tcp.validate_access(id, 0, 1).is_ok());

        // Drop memory — should unregister.
        drop(mem);

        assert!(tcp.validate_access(id, 0, 1).is_err());
    }
}
