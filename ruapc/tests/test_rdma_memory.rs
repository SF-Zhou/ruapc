#![cfg(feature = "rdma")]
#![feature(return_type_notation)]

use std::sync::Arc;

use ruapc::*;

fn make_rdma_devices() -> (Devices, Arc<Device>) {
    let rdma_devices = ruapc_rdma::Devices::availables().unwrap();
    let mut devices = Devices::new();
    let device = devices.add_rdma_device(rdma_devices[0].clone());
    (devices, device)
}

#[test]
fn test_rdma_memory_registration() {
    let (devices, device) = make_rdma_devices();

    let mem = Memory::new(4096, &devices).unwrap();
    let key = mem.get_memory_key(&device).unwrap();

    // Should be an RDMA key with non-zero lkey and rkey.
    match key {
        MemoryKey::Rdma { lkey, rkey } => {
            assert_ne!(lkey, 0, "lkey should be non-zero");
            assert_ne!(rkey, 0, "rkey should be non-zero");
        }
        _ => panic!("expected MemoryKey::Rdma, got {:?}", key),
    }
}

#[test]
fn test_rdma_memory_drop_cleanup() {
    let (devices, device) = make_rdma_devices();

    // Create and drop memory — should not panic.
    let mem = Memory::new(4096, &devices).unwrap();
    let _key = mem.get_memory_key(&device).unwrap();
    drop(mem);

    // Allocate again to verify the device is still functional.
    let mem2 = Memory::new(8192, &devices).unwrap();
    let key2 = mem2.get_memory_key(&device).unwrap();
    assert!(matches!(key2, MemoryKey::Rdma { .. }));
}

#[test]
fn test_rdma_memory_mixed_devices() {
    // Create both TCP and RDMA devices.
    let rdma_devices = ruapc_rdma::Devices::availables().unwrap();
    let mut devices = Devices::new();
    let tcp_device = devices.add_tcp_device();
    let rdma_device = devices.add_rdma_device(rdma_devices[0].clone());

    let mem = Memory::new(4096, &devices).unwrap();

    // TCP key should be Tcp variant.
    let tcp_key = mem.get_memory_key(&tcp_device).unwrap();
    assert!(matches!(tcp_key, MemoryKey::Tcp { .. }));

    // RDMA key should be Rdma variant.
    let rdma_key = mem.get_memory_key(&rdma_device).unwrap();
    match rdma_key {
        MemoryKey::Rdma { lkey, rkey } => {
            assert_ne!(lkey, 0);
            assert_ne!(rkey, 0);
        }
        _ => panic!("expected MemoryKey::Rdma"),
    }
}

#[test]
fn test_rdma_buffer_pool_allocate() {
    let (devices, device) = make_rdma_devices();
    let devices = Arc::new(devices);

    let pool = BufferPool::new(devices, 2 * 1024 * 1024, 2 * 1024 * 1024, 0);
    let buf = pool.allocate().unwrap();

    assert_eq!(buf.len(), 2 * 1024 * 1024);

    let info = buf.remote_buffer_info(&device).unwrap();
    assert_eq!(info.len, 2 * 1024 * 1024);
    assert_eq!(info.addr, buf.as_ptr() as u64);

    match info.key {
        MemoryKey::Rdma { lkey, rkey } => {
            assert_ne!(lkey, 0);
            assert_ne!(rkey, 0);
        }
        _ => panic!("expected MemoryKey::Rdma in RemoteBufferInfo"),
    }
}

#[test]
fn test_rdma_buffer_pool_multiple_buffers() {
    let (devices, device) = make_rdma_devices();
    let devices = Arc::new(devices);

    let pool = BufferPool::new(devices, 2 * 1024 * 1024, 4 * 1024 * 1024, 0);

    let buf1 = pool.allocate().unwrap();
    let buf2 = pool.allocate().unwrap();

    // Both buffers should have valid RDMA keys.
    let info1 = buf1.remote_buffer_info(&device).unwrap();
    let info2 = buf2.remote_buffer_info(&device).unwrap();

    assert!(matches!(info1.key, MemoryKey::Rdma { .. }));
    assert!(matches!(info2.key, MemoryKey::Rdma { .. }));

    // Different buffers should have different addresses.
    assert_ne!(info1.addr, info2.addr);

    // Same memory chunk, so same keys.
    assert_eq!(info1.key, info2.key);
}
