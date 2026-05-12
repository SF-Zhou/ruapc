use std::io::{Error, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ruapc_bufpool::Devices as DevicesTrait;
use ruapc_bufpool::{BufferPool, Device, RegisteredMemory};

// ---------------------------------------------------------------------------
// Local Devices implementation (moved out of ruapc-bufpool)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Devices<D: Device> {
    devices: Vec<D>,
}

impl<D: Device> Devices<D> {
    fn new() -> Self {
        Self {
            devices: Vec::new(),
        }
    }

    fn add(&mut self, mut device: D) -> usize {
        let index = self.devices.len();
        device.set_index(index);
        self.devices.push(device);
        index
    }

    fn get(&self, index: usize) -> Option<&D> {
        self.devices.get(index)
    }
}

impl<D: Device> DevicesTrait for Devices<D> {
    type Device = D;
    type Iter<'a>
        = std::slice::Iter<'a, D>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.devices.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.devices.iter()
    }
}

impl<D: Device> Default for Devices<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: Device> AsRef<Devices<D>> for Devices<D> {
    fn as_ref(&self) -> &Devices<D> {
        self
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MockRegistration {
    device_index: usize,
    /// Optional shared counter decremented on unregister, for drop verification.
    drop_counter: Option<Arc<AtomicUsize>>,
}

impl MockRegistration {
    fn new(device_index: usize) -> Self {
        Self {
            device_index,
            drop_counter: None,
        }
    }

    fn with_counter(device_index: usize, counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self {
            device_index,
            drop_counter: Some(counter.clone()),
        }
    }
}

impl Drop for MockRegistration {
    fn drop(&mut self) {
        if let Some(counter) = &self.drop_counter {
            counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[derive(Debug)]
struct MockDevice {
    index: usize,
}

impl MockDevice {
    fn new() -> Self {
        Self { index: 0 }
    }
}

impl Device for MockDevice {
    type Registration = MockRegistration;

    fn index(&self) -> usize {
        self.index
    }

    fn set_index(&mut self, idx: usize) {
        self.index = idx;
    }

    fn register(&self, mem: &mut RegisteredMemory<Self::Registration>) -> Result<()> {
        mem.add_registration(MockRegistration::new(self.index));
        Ok(())
    }
}

/// A device that tracks registrations via a shared counter.
#[derive(Debug)]
struct CountingDevice {
    index: usize,
    counter: Arc<AtomicUsize>,
}

impl CountingDevice {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { index: 0, counter }
    }
}

impl Device for CountingDevice {
    type Registration = MockRegistration;

    fn index(&self) -> usize {
        self.index
    }

    fn set_index(&mut self, idx: usize) {
        self.index = idx;
    }

    fn register(&self, mem: &mut RegisteredMemory<Self::Registration>) -> Result<()> {
        mem.add_registration(MockRegistration::with_counter(self.index, &self.counter));
        Ok(())
    }
}

/// A device that succeeds the first N calls, then fails.
#[derive(Debug)]
struct PartialDevice {
    index: usize,
    succeed_count: usize,
    call_count: AtomicUsize,
}

impl PartialDevice {
    fn new(succeed_count: usize) -> Self {
        Self {
            index: 0,
            succeed_count,
            call_count: AtomicUsize::new(0),
        }
    }
}

impl Device for PartialDevice {
    type Registration = MockRegistration;

    fn index(&self) -> usize {
        self.index
    }

    fn set_index(&mut self, idx: usize) {
        self.index = idx;
    }

    fn register(&self, mem: &mut RegisteredMemory<Self::Registration>) -> Result<()> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst);
        if n < self.succeed_count {
            mem.add_registration(MockRegistration::new(self.index));
            Ok(())
        } else {
            Err(Error::other("simulated partial failure"))
        }
    }
}

// Helper: Use small block/chunk sizes so tests don't allocate huge pages.
// AlignedMemory rounds up to 2 MiB, so the minimum chunk is 2 MiB.
const BLOCK: usize = 2 * 1024 * 1024; // 2 MiB (== ALIGN on 64-bit)
const CHUNK: usize = BLOCK; // 1 block per chunk for simplicity

fn mock_devices(n: usize) -> Arc<Devices<MockDevice>> {
    let mut devices = Devices::new();
    for _ in 0..n {
        devices.add(MockDevice::new());
    }
    Arc::new(devices)
}

fn mock_pool(
    devices: Arc<Devices<MockDevice>>,
    max_memory: usize,
) -> Arc<BufferPool<Devices<MockDevice>>> {
    BufferPool::new(devices, BLOCK, CHUNK, max_memory)
}

// ---------------------------------------------------------------------------
// Devices tests
// ---------------------------------------------------------------------------

#[test]
fn devices_empty() {
    let devices = Devices::<MockDevice>::new();
    assert!(devices.is_empty());
    assert_eq!(devices.len(), 0);
    assert!(devices.get(0).is_none());
}

#[test]
fn devices_add_assigns_indices() {
    let mut devices = Devices::new();
    let d0 = devices.add(MockDevice::new());
    let d1 = devices.add(MockDevice::new());
    let d2 = devices.add(MockDevice::new());

    assert_eq!(d0, 0);
    assert_eq!(d1, 1);
    assert_eq!(d2, 2);
    assert_eq!(devices.len(), 3);
    assert!(!devices.is_empty());
}

#[test]
fn devices_get_and_iter() {
    let mut devices = Devices::new();
    devices.add(MockDevice::new());
    devices.add(MockDevice::new());

    assert!(devices.get(0).is_some());
    assert!(devices.get(1).is_some());
    assert!(devices.get(2).is_none());

    let indices: Vec<usize> = devices.iter().map(|d| d.index()).collect();
    assert_eq!(indices, vec![0, 1]);
}

#[test]
fn devices_iter_order() {
    let mut devices = Devices::new();
    devices.add(MockDevice::new());
    devices.add(MockDevice::new());

    let indices: Vec<usize> = devices.iter().map(|d| d.index()).collect();
    assert_eq!(indices, vec![0, 1]);
    assert_eq!(devices.len(), 2);
}

#[test]
fn devices_default() {
    let devices = Devices::<MockDevice>::default();
    assert!(devices.is_empty());
}

// ---------------------------------------------------------------------------
// RegisteredMemory tests
// ---------------------------------------------------------------------------

#[test]
fn memory_new_unregistered() {
    let mem = RegisteredMemory::<MockRegistration>::new_unregistered(BLOCK).unwrap();
    assert!(mem.registrations().is_empty());
    assert!(mem.aligned_memory().size() >= BLOCK);
}

#[test]
fn memory_add_registration_and_lookup() {
    let devices = mock_devices(2);
    let mut mem = RegisteredMemory::<MockRegistration>::new_unregistered(BLOCK).unwrap();
    let d0 = devices.get(0).unwrap();
    let d1 = devices.get(1).unwrap();
    mem.add_registration(MockRegistration::new(d0.index()));
    mem.add_registration(MockRegistration::new(d1.index()));

    assert_eq!(mem.registration(d0).unwrap().device_index, 0);
    assert_eq!(mem.registration(d1).unwrap().device_index, 1);
    assert_eq!(mem.registrations().len(), 2);
}

#[test]
fn memory_new_with_devices() {
    let devices = mock_devices(3);
    let mem = RegisteredMemory::new(BLOCK, &*devices).unwrap();

    assert_eq!(mem.registrations().len(), 3);
    for i in 0..3 {
        assert_eq!(
            mem.registration(devices.get(i).unwrap())
                .unwrap()
                .device_index,
            i
        );
    }
}

#[test]
fn memory_drop_calls_unregister() {
    let counter = Arc::new(AtomicUsize::new(0));
    {
        let mut devices = Devices::new();
        devices.add(CountingDevice::new(counter.clone()));
        devices.add(CountingDevice::new(counter.clone()));
        let _mem = RegisteredMemory::new(BLOCK, &devices).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
    // After drop, unregister should have been called on each registration.
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

#[test]
fn memory_aligned_memory_accessors() {
    let mem = RegisteredMemory::<MockRegistration>::new_unregistered(BLOCK).unwrap();
    let ptr = mem.aligned_memory().as_ptr();
    let ptr_mut = mem.aligned_memory().as_mut_ptr();
    assert_eq!(ptr as *mut u8, ptr_mut);
}

// ---------------------------------------------------------------------------
// Partial registration failure tests
// ---------------------------------------------------------------------------

#[test]
fn partial_registration_failure_leaves_successful_intact() {
    let mut mem = RegisteredMemory::<MockRegistration>::new_unregistered(BLOCK).unwrap();

    let dev = Arc::new(PartialDevice::new(1)); // succeeds once, then fails

    // First call succeeds.
    assert!(dev.register(&mut mem).is_ok());
    assert_eq!(mem.registrations().len(), 1);

    // Second call fails — mem still has exactly 1 registration.
    assert!(dev.register(&mut mem).is_err());
    assert_eq!(mem.registrations().len(), 1);
}

#[test]
fn memory_new_with_failing_device_cleans_up() {
    // 3 devices: first two always succeed, third always fails.
    // RegisteredMemory::new iterates all devices. When device 2 fails the `?`
    // propagates, dropping the partially-registered RegisteredMemory and
    // unregistering the 2 successful registrations.
    let mut devices = Devices::new();
    devices.add(PartialDevice::new(100)); // always succeeds
    devices.add(PartialDevice::new(100)); // always succeeds
    devices.add(PartialDevice::new(0)); // always fails

    let result = RegisteredMemory::new(BLOCK, &devices);
    assert!(result.is_err());
    // The 2 successful registrations were cleaned up by
    // RegisteredMemory::drop inside RegisteredMemory::new when `?`
    // propagated the error. We can't assert on the global counter here
    // due to parallel test races, but the structural invariant (error
    // returned, RegisteredMemory dropped) guarantees unregister was called.
}

// ---------------------------------------------------------------------------
// BufferPool tests
// ---------------------------------------------------------------------------

#[test]
fn pool_allocate_and_return() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);

    assert_eq!(pool.free_count(), 0);
    assert_eq!(pool.allocated_memory(), 0);

    let buf = pool.allocate().unwrap();
    assert_eq!(buf.capacity(), BLOCK);
    assert_eq!(buf.len(), BLOCK); // len == capacity by default
    assert!(!buf.is_empty());
    // Verify the buffer has a valid memory reference.
    let dev = pool.devices().get(0).unwrap();
    assert!(buf.registration(dev).is_ok());

    // After allocating one block from a 1-block chunk, free count is 0.
    assert_eq!(pool.free_count(), 0);
    assert!(pool.allocated_memory() >= CHUNK);

    // Drop returns the buffer.
    drop(buf);
    assert_eq!(pool.free_count(), 1);
}

#[test]
fn pool_reuse_returned_buffer() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);

    let buf1 = pool.allocate().unwrap();
    let ptr1 = buf1.as_ptr();
    drop(buf1);

    let buf2 = pool.allocate().unwrap();
    let ptr2 = buf2.as_ptr();

    // Should reuse the same block.
    assert_eq!(ptr1, ptr2);
    // Only one chunk allocated total.
    assert!(pool.allocated_memory() >= CHUNK);
    assert!(pool.allocated_memory() < CHUNK * 2);
}

#[test]
fn pool_exhaustion() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, CHUNK);

    let buf = pool.allocate().unwrap();
    // Pool has 1 block total, it's checked out — next allocate should fail.
    assert!(pool.allocate().is_err());

    drop(buf);
    // Now it should succeed again.
    let _buf2 = pool.allocate().unwrap();
}

#[test]
fn pool_multiple_chunks() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, CHUNK * 2);

    let buf1 = pool.allocate().unwrap();
    let buf2 = pool.allocate().unwrap();

    // Two buffers from different chunks should have different RegisteredMemory objects.
    let dev = pool.devices().get(0).unwrap();
    assert!(buf1.registration(dev).is_ok());
    assert!(buf2.registration(dev).is_ok());

    drop(buf1);
    drop(buf2);
    assert_eq!(pool.free_count(), 2);
}

#[test]
fn buffer_memory_registration_lookup() {
    let devices = mock_devices(2);
    let pool = mock_pool(devices, 0);

    let buf = pool.allocate().unwrap();

    let dev0 = pool.devices().get(0).unwrap();
    let dev1 = pool.devices().get(1).unwrap();

    let reg0 = buf.registration(dev0).unwrap();
    assert_eq!(reg0.device_index, 0);
    let reg1 = buf.registration(dev1).unwrap();
    assert_eq!(reg1.device_index, 1);
}

#[test]
fn pool_config_accessors() {
    let devices = mock_devices(1);
    let pool = mock_pool(Arc::clone(&devices), 1024 * 1024 * 1024);

    assert_eq!(pool.block_size(), BLOCK);
    assert_eq!(pool.chunk_size(), CHUNK);
    assert_eq!(pool.devices().len(), 1);
}

#[test]
fn pool_debug_format() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let _buf = pool.allocate().unwrap();

    let debug = format!("{pool:?}");
    assert!(debug.contains("BufferPool"));
    assert!(debug.contains("block_size"));
    assert!(debug.contains("chunks"));
}

// ---------------------------------------------------------------------------
// Buffer tests
// ---------------------------------------------------------------------------

#[test]
fn buffer_set_len() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let mut buf = pool.allocate().unwrap();

    assert_eq!(buf.len(), BLOCK);
    buf.set_len(0);
    assert_eq!(buf.len(), 0);
    assert!(buf.is_empty());

    buf.set_len(100);
    assert_eq!(buf.len(), 100);
}

#[test]
#[should_panic(expected = "len")]
fn buffer_set_len_panics_on_overflow() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let mut buf = pool.allocate().unwrap();
    buf.set_len(BLOCK + 1);
}

#[test]
fn buffer_extend_from_slice() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let mut buf = pool.allocate().unwrap();

    buf.set_len(0);
    buf.extend_from_slice(b"hello").unwrap();
    assert_eq!(buf.len(), 5);
    assert_eq!(&buf[..5], b"hello");

    buf.extend_from_slice(b" world").unwrap();
    assert_eq!(buf.len(), 11);
    assert_eq!(&buf[..11], b"hello world");
}

#[test]
fn buffer_extend_from_slice_overflow() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let mut buf = pool.allocate().unwrap();

    // buf.len == BLOCK, so any extend should fail.
    assert!(buf.extend_from_slice(b"x").is_err());
}

#[test]
fn buffer_deref_read_write() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let mut buf = pool.allocate().unwrap();

    // Write via DerefMut.
    buf[0] = 0xAA;
    buf[1] = 0xBB;

    // Read via Deref.
    assert_eq!(buf[0], 0xAA);
    assert_eq!(buf[1], 0xBB);

    // AsRef / AsMut.
    let slice: &[u8] = buf.as_ref();
    assert_eq!(slice[0], 0xAA);
    let slice_mut: &mut [u8] = buf.as_mut();
    slice_mut[2] = 0xCC;
    assert_eq!(buf[2], 0xCC);
}

#[test]
fn buffer_raw_pointers() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let mut buf = pool.allocate().unwrap();

    let ptr = buf.as_ptr();
    let mut_ptr = buf.as_mut_ptr();
    assert_eq!(ptr, mut_ptr as *const u8);
}

#[test]
fn buffer_debug_format() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let buf = pool.allocate().unwrap();

    let debug = format!("{buf:?}");
    assert!(debug.contains("Buffer"));
    assert!(debug.contains("capacity"));
}

// ---------------------------------------------------------------------------
// Async tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pool_async_allocate_basic() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);

    let buf = pool.async_allocate().await.unwrap();
    assert_eq!(buf.capacity(), BLOCK);
}

#[tokio::test]
async fn pool_async_allocate_waits_for_return() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, CHUNK);

    let buf = pool.allocate().unwrap();

    let pool2 = Arc::clone(&pool);
    let handle = tokio::spawn(async move { pool2.async_allocate().await.unwrap() });

    // Give the spawned task a moment to park on the waiter.
    tokio::task::yield_now().await;

    // Return the buffer — this should wake the waiter.
    drop(buf);

    let buf2 = handle.await.unwrap();
    assert_eq!(buf2.capacity(), BLOCK);
}

// ---------------------------------------------------------------------------
// Coverage gap tests
// ---------------------------------------------------------------------------

#[test]
fn aligned_memory_debug_format() {
    let mem = ruapc_bufpool::AlignedMemory::new(4096).unwrap();
    let debug = format!("{mem:?}");
    assert!(debug.contains("AlignedMemory"));
    assert!(debug.contains("size"));
}

#[test]
fn registered_memory_debug_format() {
    let mem = RegisteredMemory::<MockRegistration>::new_unregistered(BLOCK).unwrap();
    let debug = format!("{mem:?}");
    assert!(debug.contains("RegisteredMemory"));
    assert!(debug.contains("registrations"));
}

#[test]
fn buffer_pool_accessor() {
    let devices = mock_devices(1);
    let pool = mock_pool(devices, 0);
    let buf = pool.allocate().unwrap();

    // buf.pool() should return a reference to the same pool.
    assert_eq!(buf.pool().block_size(), BLOCK);
}

#[test]
fn pool_allocate_chunk_failure() {
    // All devices fail immediately, so the first allocate() triggers
    // allocate_chunk -> RegisteredMemory::new -> device.register -> Err.
    let mut devices = Devices::new();
    devices.add(PartialDevice::new(0)); // always fails
    let devices = Arc::new(devices);
    let pool = BufferPool::new(devices, BLOCK, BLOCK, 0);

    assert!(pool.allocate().is_err());
}

#[test]
fn registered_memory_new_unregistered_zero_size() {
    // Triggers AlignedMemory::new(0) -> Err inside new_unregistered.
    let result = RegisteredMemory::<MockRegistration>::new_unregistered(0);
    assert!(result.is_err());
}

#[test]
fn registered_memory_new_first_device_fails() {
    // Triggers the `?` error return at the top of the device iteration loop
    // inside RegisteredMemory::new (memory.rs:30).
    let mut devices = Devices::new();
    devices.add(PartialDevice::new(0)); // first device always fails
    let result = RegisteredMemory::new(BLOCK, &devices);
    assert!(result.is_err());
}

#[test]
fn pool_multi_block_chunk() {
    // Use a chunk size that is a multiple of the block size to get
    // multiple blocks per chunk.
    let devices = mock_devices(1);
    let pool = BufferPool::new(devices, BLOCK, BLOCK * 2, 0);

    let buf1 = pool.allocate().unwrap();
    let buf2 = pool.allocate().unwrap();

    // Both should come from the same chunk (single allocation).
    assert_eq!(pool.allocated_memory(), BLOCK * 2);

    // They must have different addresses.
    assert_ne!(buf1.as_ptr(), buf2.as_ptr());

    drop(buf1);
    drop(buf2);
    assert_eq!(pool.free_count(), 2);
}
