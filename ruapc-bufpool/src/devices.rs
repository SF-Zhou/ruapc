//! Device collection trait for registering memory on multiple devices.

use std::io::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::{AlignedMemory, Device, DeviceIndex, MemoryRegistrar, Registration, TcpDevice};

static NEXT_MAGIC: AtomicU32 = AtomicU32::new(1);

/// A TCP device followed by a statically dispatched collection of other devices.
///
/// Devices receive stable indices when inserted. Only their audited
/// [`MemoryRegistrar`] implementations receive backing memory; safe application
/// wrappers can add metadata and policy without implementing a memory-safety
/// contract themselves.
#[derive(Debug)]
pub struct DeviceSet<D: Device = TcpDevice> {
    tcp_device: TcpDevice,
    devices: Vec<D>,
    magic: u32,
}

impl<D: Device> Default for DeviceSet<D> {
    fn default() -> Self {
        let magic = NEXT_MAGIC
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .expect("device collection identifiers exhausted");
        let mut tcp_device = TcpDevice::default();
        tcp_device.set_index(DeviceIndex { magic, index: 0 });
        Self {
            tcp_device,
            devices: Vec::new(),
            magic,
        }
    }
}

impl<D: Device> DeviceSet<D> {
    /// Returns the TCP device, always at registration index zero.
    pub fn tcp_device(&self) -> &TcpDevice {
        &self.tcp_device
    }

    /// Returns the additional devices in registration order.
    pub fn devices(&self) -> &[D] {
        &self.devices
    }

    /// Adds a device and assigns its index within this collection.
    pub fn push(&mut self, mut device: D) {
        let index = u32::try_from(self.devices.len())
            .ok()
            .and_then(|index| index.checked_add(1))
            .expect("too many devices in collection");
        device.set_index(DeviceIndex {
            magic: self.magic,
            index,
        });
        self.devices.push(device);
    }
}

// SAFETY: safe Device implementations never receive memory. Registration calls
// go directly to their MemoryRegistrar, whose contract enforces allocation
// borrowing and lifetime rules. Returned handles remain owned by the pool; on
// partial failure the local vector drops completed registrations before return.
unsafe impl<D: Device> Devices for DeviceSet<D> {
    fn len(&self) -> usize {
        1 + self.devices.len()
    }

    fn register(&self, mem: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>> {
        let mut registrations = Vec::with_capacity(self.len());
        registrations.push(self.tcp_device.registrar().register(mem)?);
        for device in &self.devices {
            registrations.push(device.registrar().register(mem)?);
        }
        Ok(registrations)
    }
}

/// Trait for a collection of devices that can register memory.
///
/// # Safety
///
/// The pool grants exclusive access to individual allocations through
/// [`crate::Buffer`]. Implementations may retain the supplied `Arc` to keep a
/// registration alive, but must not expose it to callers or access its bytes
/// independently of those allocations. Any CPU or device access must preserve
/// the allocation's lifetime and obey its shared/exclusive borrowing rules.
/// In particular, retaining the `Arc` does not authorize reading its slice while
/// a buffer can be mutated or returned to the pool.
///
/// Registration handle destruction must end any use of its region before it
/// returns; the pool releases backing memory after dropping all handles.
pub unsafe trait Devices: Send + Sync + std::fmt::Debug {
    /// Returns the number of devices.
    fn len(&self) -> usize;

    /// Returns `true` if there are no devices.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Registers the given aligned memory on all devices.
    fn register(&self, mem: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>>;
}

/// An empty device collection that performs no registrations.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyDevices;

// SAFETY: this collection neither retains nor accesses the supplied memory.
unsafe impl Devices for EmptyDevices {
    fn len(&self) -> usize {
        0
    }

    fn register(&self, _mem: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BufferPoolBuilder, MemoryKey};

    #[derive(Debug)]
    struct TrackedRegistrar {
        key: u32,
        fail: bool,
        dropped: Arc<AtomicU32>,
    }

    #[derive(Debug)]
    struct TrackedRegistration {
        key: u32,
        memory: Arc<AlignedMemory>,
        dropped: Arc<AtomicU32>,
    }

    impl Registration for TrackedRegistration {
        fn memory_key(&self) -> MemoryKey {
            MemoryKey {
                lkey: self.key,
                rkey: self.key,
            }
        }
    }

    impl Drop for TrackedRegistration {
        fn drop(&mut self) {
            assert!(self.memory.size() >= 4096);
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    // SAFETY: these registrations only retain memory; neither the registrar nor
    // its handle exposes or accesses bytes, including during destruction.
    unsafe impl MemoryRegistrar for TrackedRegistrar {
        fn register(&self, memory: &Arc<AlignedMemory>) -> Result<Box<dyn Registration>> {
            if self.fail {
                return Err(std::io::Error::other("registration failed"));
            }
            Ok(Box::new(TrackedRegistration {
                key: self.key,
                memory: Arc::clone(memory),
                dropped: Arc::clone(&self.dropped),
            }))
        }
    }

    // An application wrapper receives only its own metadata and registrar.
    #[derive(Debug)]
    struct ApplicationDevice {
        index: DeviceIndex,
        registrar: TrackedRegistrar,
    }

    impl ApplicationDevice {
        fn new(key: u32, fail: bool, dropped: &Arc<AtomicU32>) -> Self {
            Self {
                index: DeviceIndex::default(),
                registrar: TrackedRegistrar {
                    key,
                    fail,
                    dropped: Arc::clone(dropped),
                },
            }
        }
    }

    impl Device for ApplicationDevice {
        type Registrar = TrackedRegistrar;

        fn registrar(&self) -> &Self::Registrar {
            &self.registrar
        }

        fn index(&self) -> DeviceIndex {
            self.index
        }

        fn set_index(&mut self, index: DeviceIndex) {
            self.index = index;
        }
    }

    #[test]
    fn device_set_assigns_stable_indices_after_its_single_tcp_device() {
        let mut devices = DeviceSet::<ApplicationDevice>::default();
        assert_eq!(devices.len(), 1);
        assert!(!devices.is_empty());
        assert!(devices.devices().is_empty());
        let tcp_index = devices.tcp_device().index();
        assert_eq!(tcp_index.index, 0);
        let other = DeviceSet::<ApplicationDevice>::default();
        assert_ne!(tcp_index.magic, other.tcp_device().index().magic);

        let dropped = Arc::default();
        devices.push(ApplicationDevice::new(41, false, &dropped));
        devices.push(ApplicationDevice::new(73, false, &dropped));
        assert_eq!(devices.len(), 3);
        assert_eq!(devices.tcp_device().index(), tcp_index);
        for (offset, device) in devices.devices().iter().enumerate() {
            assert_eq!(device.index().magic, tcp_index.magic);
            assert_eq!(device.index().index, offset as u32 + 1);
        }
    }

    #[test]
    fn safe_device_wrappers_preserve_registration_order_and_buffer_lifetime() {
        let dropped = Arc::default();
        let mut devices = DeviceSet::<ApplicationDevice>::default();
        devices.push(ApplicationDevice::new(41, false, &dropped));
        devices.push(ApplicationDevice::new(73, false, &dropped));
        let devices = Arc::new(devices);
        let pool = BufferPoolBuilder::new(devices.clone()).build();
        let buffer = pool.allocate(1024 * 1024).unwrap();
        assert_eq!(buffer.memory_key(devices.tcp_device()).unwrap().lkey, 0);
        for device in devices.devices() {
            assert_eq!(
                buffer.memory_key(device).unwrap().lkey,
                device.registrar.key
            );
        }
        drop(pool);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(buffer);
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn failed_registration_releases_all_completed_handles_and_memory_holds() {
        let dropped = Arc::default();
        let mut devices = DeviceSet::<ApplicationDevice>::default();
        devices.push(ApplicationDevice::new(41, false, &dropped));
        devices.push(ApplicationDevice::new(73, true, &dropped));
        let memory = Arc::new(AlignedMemory::new(4096).unwrap());
        assert!(devices.register(&memory).is_err());
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        // Both the successful custom handle and the initial TCP registry entry
        // must have released their memory holds before the error is returned.
        assert_eq!(Arc::strong_count(&memory), 1);
    }
}
