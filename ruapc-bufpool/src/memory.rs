use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use crate::AlignedMemory;
use crate::device::{Device, Devices};

/// A registered memory block.
///
/// Contains an `Arc<AlignedMemory>` together with its registrations on a
/// set of devices. The `Arc` allows devices (e.g. `TcpDevice`) to hold a
/// reference to the underlying memory for safe read/write access.
///
/// On drop, all registrations are undone before the `Arc` is released.
///
/// A `RegisteredMemory` can be created with no registrations via
/// [`new_unregistered`](Self::new_unregistered), then registered on
/// devices one at a time via [`Device::register`]. If a device's
/// registration fails, the `RegisteredMemory` still holds registrations
/// from previously successful devices, ensuring correct cleanup on drop.
pub struct RegisteredMemory<R: Send + Sync + std::fmt::Debug> {
    aligned_memory: Arc<AlignedMemory>,
    registrations: Vec<R>,
}

pub type Reg<DS> = <<DS as Devices>::Device as Device>::Registration;

impl<R: Send + Sync + std::fmt::Debug> RegisteredMemory<R> {
    /// Creates a new `RegisteredMemory` by allocating aligned memory and
    /// registering it on every device in `devices`.
    ///
    /// If any device registration fails, the `RegisteredMemory` is dropped,
    /// which unregisters all previously successful registrations.
    pub fn new<T, DS>(size: usize, devices: &T) -> Result<Self>
    where
        T: AsRef<DS>,
        DS: Devices + ?Sized,
        DS::Device: Device<Registration = R>,
    {
        let devices = devices.as_ref();
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
        let aligned_memory = Arc::new(AlignedMemory::new(size)?);
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

    /// Returns a shared reference to the underlying aligned memory.
    pub fn aligned_memory(&self) -> &Arc<AlignedMemory> {
        &self.aligned_memory
    }

    /// Returns a reference to the registration for the given device.
    pub fn registration<D: Device<Registration = R>>(&self, device: &D) -> Result<&R> {
        let idx = device.index();
        self.registration_by_index(idx)
    }

    /// Returns a reference to the registration for the given device index.
    pub fn registration_by_index(&self, device_index: usize) -> Result<&R> {
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

impl<R: Send + Sync + std::fmt::Debug> std::fmt::Debug for RegisteredMemory<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredMemory")
            .field("aligned_memory", &self.aligned_memory)
            .field("registrations", &self.registrations.len())
            .finish()
    }
}
