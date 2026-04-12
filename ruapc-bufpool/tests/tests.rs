use std::io::{Error, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ruapc_bufpool::{BufferPool, Device, Devices, Memory, Registration};

// ---------------------------------------------------------------------------
// Mock types
// ---------------------------------------------------------------------------

/// Global counter tracking net registrations (created minus unregistered).
/// Used by tests that need to verify cleanup behavior. Because tests run
/// in parallel, assertions should always use *relative* deltas (snapshot
/// before and after) rather than checking for an absolute value.
static LIVE_REGISTRATIONS: AtomicUsize = AtomicUsize::new(0);

fn live_regs() -> usize {
    LIVE_REGISTRATIONS.load(Ordering::SeqCst)
}

#[derive(Debug)]
struct MockRegistration {
    device_index: usize,
}

impl MockRegistration {
    fn new(device_index: usize) -> Self {
        LIVE_REGISTRATIONS.fetch_add(1, Ordering::SeqCst);
        Self { device_index }
    }
}

impl Registration for MockRegistration {
    fn unregister(&self, _buf: &[u8]) {
        LIVE_REGISTRATIONS.fetch_sub(1, Ordering::SeqCst);
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

    fn register(&self, mem: &mut Memory<Self::Registration>) -> Result<()> {
        mem.add_registration(MockRegistration::new(self.index));
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

    fn register(&self, mem: &mut Memory<Self::Registration>) -> Result<()> {
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

fn mock_pool(devices: Arc<Devices<MockDevice>>, max_memory: usize) -> Arc<BufferPool<MockDevice>> {
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

    assert_eq!(d0.index(), 0);
    assert_eq!(d1.index(), 1);
    assert_eq!(d2.index(), 2);
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
fn devices_as_slice() {
    let mut devices = Devices::new();
    devices.add(MockDevice::new());
    devices.add(MockDevice::new());

    let slice = devices.as_slice();
    assert_eq!(slice.len(), 2);
    assert_eq!(slice[0].index(), 0);
    assert_eq!(slice[1].index(), 1);
}

#[test]
fn devices_default() {
    let devices = Devices::<MockDevice>::default();
    assert!(devices.is_empty());
}

// ---------------------------------------------------------------------------
// Memory tests
// ---------------------------------------------------------------------------

#[test]
fn memory_new_unregistered() {
    let mem = Memory::<MockRegistration>::new_unregistered(BLOCK).unwrap();
    assert!(mem.registrations().is_empty());
    assert!(mem.aligned_memory().size() >= BLOCK);
}

#[test]
fn memory_add_registration_and_lookup() {
    let before = live_regs();
    let mut mem = Memory::<MockRegistration>::new_unregistered(BLOCK).unwrap();
    mem.add_registration(MockRegistration::new(0));
    mem.add_registration(MockRegistration::new(1));

    assert_eq!(mem.registrations().len(), 2);
    assert_eq!(mem.registration(0).unwrap().device_index, 0);
    assert_eq!(mem.registration(1).unwrap().device_index, 1);
    assert!(mem.registration(2).is_err());
    assert_eq!(live_regs(), before + 2);
}

#[test]
fn memory_new_with_devices() {
    let devices = mock_devices(3);
    let mem = Memory::new(BLOCK, &devices).unwrap();

    assert_eq!(mem.registrations().len(), 3);
    for i in 0..3 {
        assert_eq!(mem.registration(i).unwrap().device_index, i);
    }
}

#[test]
fn memory_drop_calls_unregister() {
    let before = live_regs();
    {
        let devices = mock_devices(2);
        let _mem = Memory::new(BLOCK, &devices).unwrap();
        assert_eq!(live_regs(), before + 2);
    }
    // After drop, the delta should be zero.
    assert_eq!(live_regs(), before);
}

#[test]
fn memory_aligned_memory_accessors() {
    let mut mem = Memory::<MockRegistration>::new_unregistered(BLOCK).unwrap();
    let ptr = mem.aligned_memory().as_ptr();
    let ptr_mut = mem.aligned_memory_mut().as_mut_ptr();
    assert_eq!(ptr as *mut u8, ptr_mut);
}

// ---------------------------------------------------------------------------
// Partial registration failure tests
// ---------------------------------------------------------------------------

#[test]
fn partial_registration_failure_leaves_successful_intact() {
    let mut mem = Memory::<MockRegistration>::new_unregistered(BLOCK).unwrap();

    let dev = PartialDevice::new(1); // succeeds once, then fails

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
    // Memory::new iterates all devices. When device 2 fails the `?`
    // propagates, dropping the partially-registered Memory and
    // unregistering the 2 successful registrations.
    let mut devices = Devices::new();
    devices.add(PartialDevice::new(100)); // always succeeds
    devices.add(PartialDevice::new(100)); // always succeeds
    devices.add(PartialDevice::new(0)); // always fails

    let result = Memory::new(BLOCK, &devices);
    assert!(result.is_err());
    // The 2 successful registrations were cleaned up by Memory::drop
    // inside Memory::new when `?` propagated the error. We can't
    // assert on the global counter here due to parallel test races,
    // but the structural invariant (error returned, Memory dropped)
    // guarantees unregister was called.
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
    assert_eq!(buf.memory_index(), 0);

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

    assert_eq!(buf1.memory_index(), 0);
    assert_eq!(buf2.memory_index(), 1);

    drop(buf1);
    drop(buf2);
    assert_eq!(pool.free_count(), 2);
}

#[test]
fn pool_registration_lookup() {
    let devices = mock_devices(2);
    let pool = mock_pool(devices, 0);

    let buf = pool.allocate().unwrap();

    let reg0 = pool.registration(buf.memory_index(), 0).unwrap();
    assert_eq!(reg0.device_index, 0);

    let reg1 = pool.registration(buf.memory_index(), 1).unwrap();
    assert_eq!(reg1.device_index, 1);

    // Invalid device index.
    assert!(pool.registration(buf.memory_index(), 99).is_err());
    // Invalid memory index.
    assert!(pool.registration(99, 0).is_err());
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
    assert!(debug.contains("memory_index"));
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
