use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use crate::{AlignedMemory, AsDeviceIndex, Devices, Registration};

pub struct RegisteredMemory {
    aligned_memory: Arc<AlignedMemory>,
    registrations: Vec<Box<dyn Registration>>,
}

impl RegisteredMemory {
    pub fn new(size: usize, devices: &dyn Devices) -> Result<Self> {
        let aligned_memory = Arc::new(AlignedMemory::new(size)?);
        let registrations = devices.register(&aligned_memory)?;
        Ok(Self {
            aligned_memory,
            registrations,
        })
    }

    pub fn new_unregistered(size: usize) -> Result<Self> {
        let aligned_memory = Arc::new(AlignedMemory::new(size)?);
        Ok(Self {
            aligned_memory,
            registrations: Vec::new(),
        })
    }

    pub fn add_registration(&mut self, reg: Box<dyn Registration>) {
        self.registrations.push(reg);
    }

    pub fn aligned_memory(&self) -> &Arc<AlignedMemory> {
        &self.aligned_memory
    }

    pub fn registration(&self, device_index: &impl AsDeviceIndex) -> Result<&dyn Registration> {
        let idx = device_index.as_device_index();
        self.registrations
            .get(idx.index as usize)
            .map(|r| r.as_ref())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("memory not registered on device index {}", idx.index),
                )
            })
    }
}

impl std::fmt::Debug for RegisteredMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredMemory")
            .field("aligned_memory", &self.aligned_memory)
            .field("registrations", &self.registrations.len())
            .finish()
    }
}
