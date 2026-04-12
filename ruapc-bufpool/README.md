# ruapc-bufpool

Generic, high-performance buffer pool with device registration support.

This crate is the memory management foundation for [ruapc](../ruapc/) ("Rua! Procedure Call"). It provides aligned memory allocation, device-agnostic memory registration, and a pool that hands out fixed-size buffers from large registered chunks. The crate is **transport-independent** — it knows nothing about TCP, RDMA, or any specific device; concrete device types are defined in downstream crates and wired in via generics (static dispatch).

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                     BufferPool<D>                          │
│  ┌───────────────────────────────────────────────────┐    │
│  │ chunk 0: RegisteredMemory<R>  (in AliasableBox)   │    │
│  │  ┌──────────────┐  ┌───────────────────────────┐  │    │
│  │  │AlignedMemory │  │ registrations: Vec<R>      │  │    │
│  │  │ (2 MiB align)│  │  [R_dev0, R_dev1, …]      │  │    │
│  │  └──────────────┘  └───────────────────────────┘  │    │
│  │  ├─block 0─┤─block 1─┤─ … ─┤─block N─┤           │    │
│  └───────────────────────────────────────────────────┘    │
│  ┌───────────────────────────────────────────────────┐    │
│  │ chunk 1: RegisteredMemory<R>  (in AliasableBox)   │    │
│  │  …                                                │    │
│  └───────────────────────────────────────────────────┘    │
│                                                           │
│  free_list: [slot, slot, …]                               │
│  waiters:   [oneshot::Sender, …]                          │
└───────────────────────────────────────────────────────────┘
         │
         ▼
    Buffer<D>  ← returned to caller
    (Arc<BufferPool>, ptr, capacity, len,
     NonNull<RegisteredMemory>, memory_index, block_index)
```

Each `RegisteredMemory` is stored in an `AliasableBox` inside an append-only `Vec`, so its heap address is stable even when the `Vec` grows. `Buffer` holds a `NonNull<RegisteredMemory>` pointer, allowing lock-free access to device registrations via `buf.memory().registration(device_index)`.

## Core Types

### `AlignedMemory`
Owned, 2 MiB-aligned memory block (4 KiB on 32-bit). Freed on drop via `std::alloc::dealloc`. This is the smallest unit of OS memory allocation in the pool.

### `trait Registration`
A device registration handle. Implementations track whatever state the device needs (e.g. RDMA memory region keys). `unregister(&self, buf: &[u8])` is called on drop to clean up device-side state.

### `trait Device`
A device that can register memory. Key method:

```rust
fn register(self: &Arc<Self>, mem: &mut RegisteredMemory<Self::Registration>) -> io::Result<()>;
```

The `self: &Arc<Self>` receiver lets implementations clone the `Arc` when they need to store a reference to the device in the registration handle. The device performs its registration work and calls `mem.add_registration(reg)` on success. If registration fails, `mem` is left unchanged — any registrations from previously successful devices remain intact for safe cleanup when `RegisteredMemory` is dropped.

Each device has an `index` assigned by `Devices::add()`.

### `Devices<D>`
An ordered collection of `Arc<D>`. Devices are assigned monotonically increasing indices when added. Finalized before creating a `BufferPool`.

### `RegisteredMemory<R>`
An `AlignedMemory` plus a `Vec<R>` of device registrations. Can be created in two ways:

- `RegisteredMemory::new(size, &devices)` — allocates and registers on all devices in one call. Partial failure is safe: if the 3rd device fails, the first 2 registrations are cleaned up on drop.
- `RegisteredMemory::new_unregistered(size)` + manual `device.register(&mut mem)` calls — for fine-grained control.

On drop, all registrations are unregistered before the underlying memory is freed.

### `Buffer<D>`
A fixed-size buffer carved from a pool chunk. Holds `Arc<BufferPool<D>>` to keep the pool alive. Returned to the pool's free list on drop. Supports `Deref<Target=[u8]>`, `DerefMut`, `set_len`, and `extend_from_slice`.

Key method:

```rust
fn memory(&self) -> &RegisteredMemory<D::Registration>
```

Returns a reference to the `RegisteredMemory` this buffer belongs to, allowing direct lock-free access to device registrations. Consumers can call `buf.memory().registration(device.index())` without going through the pool's lock.

### `BufferPool<D>`
Manages large registered memory chunks and hands out `Buffer`s from them.

- `allocate()` — synchronous, returns error if pool is exhausted.
- `async_allocate()` — waits via `tokio::sync::oneshot` if pool is exhausted.
- Automatic chunk growth up to a configurable `max_memory` limit.

## Dependencies

- `aliasable` — for `AliasableBox`, providing stable heap addresses for `RegisteredMemory` inside the pool's `Vec`.
- `tokio` (sync feature only) — for `async_allocate` waiter channels.

No dependency on `ruapc`, `ruapc-rdma`, or any specific transport.

## Usage in ruapc

In `ruapc`, concrete device types (`TcpDevice`, `RdmaDevice`) are wrapped in an enum `Device` that implements `ruapc_bufpool::Device`. Type aliases make this ergonomic:

```rust
// ruapc/src/memory/mod.rs
pub type Memory = ruapc_bufpool::RegisteredMemory<MemoryRegistration>;
pub type Buffer = ruapc_bufpool::Buffer<Device>;
pub type BufferPool = ruapc_bufpool::BufferPool<Device>;

// ruapc/src/device/mod.rs
pub type Devices = ruapc_bufpool::Devices<Device>;
```

Extension traits (`BufferExt`, `MemoryExt`, `DevicesExt`) add transport-specific methods like `memory_key()`, `remote_buffer_info()`, and `add_tcp_device()` / `add_rdma_device()` that depend on types from `ruapc`'s wire protocol.

## Testing

```bash
cargo test -p ruapc-bufpool
```

The test suite covers:
- `AlignedMemory` allocation, alignment, read/write
- `Devices` collection management and index assignment
- `RegisteredMemory` registration, lookup, drop cleanup
- Partial registration failure safety (key design invariant)
- `BufferPool` allocation, reuse, exhaustion, multi-chunk growth
- `Buffer` operations: `memory()`, `set_len`, `extend_from_slice`, `Deref`, raw pointers
- Async allocation with waiter wakeup
