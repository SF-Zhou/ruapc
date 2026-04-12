use std::io::{Error, ErrorKind, Result};

use crate::AlignedMemory;
use crate::device::{Device, Devices, Registration};

/// A registered memory block.
///
/// Contains an `AlignedMemory` together with its registrations on a set
/// of devices. On drop, all registrations are undone before the underlying
/// memory is freed.
///
/// A `RegisteredMemory` can be created with no registrations via
/// [`new_unregistered`](Self::new_unregistered), then registered on
/// devices one at a time via [`Device::register`]. If a device's
/// registration fails, the `RegisteredMemory` still holds registrations
/// from previously successful devices, ensuring correct cleanup on drop.
pub struct RegisteredMemory<R: Registration> {
    aligned_memory: AlignedMemory,
    registrations: Vec<R>,
}

impl<R: Registration> RegisteredMemory<R> {
    /// Creates a new `RegisteredMemory` by allocating aligned memory and
    /// registering it on every device in `devices`.
    ///
    /// If any device registration fails, the `RegisteredMemory` is dropped,
    /// which unregisters all previously successful registrations.
    pub fn new<D: Device<Registration = R>>(size: usize, devices: &Devices<D>) -> Result<Self> {
        let mut mem = Self::new_unregistered(size)?;

        for device in devices.iter() {
            device.register(&mut mem)?;
        }

        Ok(mem)
    }

    /// Creates a new `RegisteredMemory` with no registrations.
    ///
    /// The underlying `AlignedMemory` is allocated immediately.
    /// Use [`Device::register`] to register on individual devices
    /// afterwards.
    pub fn new_unregistered(size: usize) -> Result<Self> {
        let aligned_memory = AlignedMemory::new(size)?;
        Ok(Self {
            aligned_memory,
            registrations: Vec::new(),
        })
    }

    /// Pushes a registration into this memory.
    ///
    /// Called by [`Device::register`] implementations after they have
    /// successfully created the device-side registration handle.
    pub fn add_registration(&mut self, reg: R) {
        self.registrations.push(reg);
    }

    /// Returns a reference to the underlying `AlignedMemory`.
    pub fn aligned_memory(&self) -> &AlignedMemory {
        &self.aligned_memory
    }

    /// Returns a mutable reference to the underlying `AlignedMemory`.
    pub fn aligned_memory_mut(&mut self) -> &mut AlignedMemory {
        &mut self.aligned_memory
    }

    /// Returns a reference to the registration for the given device index.
    pub fn registration(&self, device_index: usize) -> Result<&R> {
        self.registrations.get(device_index).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("memory not registered on device index {device_index}"),
            )
        })
    }

    /// Returns a reference to all registrations.
    pub fn registrations(&self) -> &[R] {
        &self.registrations
    }
}

impl<R: Registration> Drop for RegisteredMemory<R> {
    fn drop(&mut self) {
        let buf = self.aligned_memory.as_slice();
        for reg in &self.registrations {
            reg.unregister(buf);
        }
    }
}

impl<R: Registration> std::fmt::Debug for RegisteredMemory<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredMemory")
            .field("aligned_memory", &self.aligned_memory)
            .field("registrations", &self.registrations.len())
            .finish()
    }
}
