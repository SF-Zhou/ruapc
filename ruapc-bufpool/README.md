# ruapc-bufpool

Generic, high-performance buffer pool with device registration support.

This crate is the memory management foundation for [ruapc](../ruapc/) ("Rua! Procedure Call"). It provides aligned memory allocation, device-agnostic memory registration, and a pool that hands out fixed-size buffers from large registered chunks. The crate is **transport-independent** — it knows nothing about TCP, RDMA, or any specific device; concrete device types are defined in downstream crates and wired in via generics (static dispatch).

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    BufferPool<D>                      │
│  ┌──────────────────────────────────────────────┐    │
│  │ chunk 0: Memory<R>                           │    │
│  │  ┌──────────────┐  ┌──────────────────────┐  │    │
│  │  │AlignedMemory │  │ registrations: Vec<R> │  │    │
│  │  │ (2 MiB align)│  │  [R_dev0, R_dev1, …]  │  │    │
│  │  └──────────────┘  └──────────────────────┘  │    │
│  │  ├─block 0─┤─block 1─┤─ … ─┤─block N─┤      │    │
│  └──────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────┐    │
│  │ chunk 1: Memory<R>                           │    │
│  │  …                                           │    │
│  └──────────────────────────────────────────────┘    │
│                                                      │
│  free_list: [slot, slot, …]                          │
│  waiters:   [oneshot::Sender, …]                     │
└──────────────────────────────────────────────────────┘
         │
         ▼
    Buffer<D>  ← returned to caller
    (Arc<BufferPool>, ptr, capacity, len, memory_index, block_index)
```

## Core Types

### `AlignedMemory`
Owned, 2 MiB-aligned memory block (4 KiB on 32-bit). Freed on drop via `std::alloc::dealloc`. This is the smallest unit of OS memory allocation in the pool.

### `trait Registration`
A device registration handle. Implementations track whatever state the device needs (e.g. RDMA memory region keys). `unregister(&self, buf: &[u8])` is called on drop to clean up device-side state.

### `trait Device`
A device that can register memory. Key method:

```rust
fn register(&self, mem: &mut Memory<Self::Registration>) -> io::Result<()>;
```

The device performs its registration work and calls `mem.add_registration(reg)` on success. If registration fails, `mem` is left unchanged — any registrations from previously successful devices remain intact for safe cleanup when `Memory` is dropped.

Each device has an `index` assigned by `Devices::add()`.

### `Devices<D>`
An ordered collection of `Arc<D>`. Devices are assigned monotonically increasing indices when added. Finalized before creating a `BufferPool`.

### `Memory<R>`
An `AlignedMemory` plus a `Vec<R>` of device registrations. Can be created in two ways:

- `Memory::new(size, &devices)` — allocates and registers on all devices in one call. Partial failure is safe: if the 3rd device fails, the first 2 registrations are cleaned up on drop.
- `Memory::new_unregistered(size)` + manual `device.register(&mut mem)` calls — for fine-grained control.

On drop, all registrations are unregistered before the underlying memory is freed.

### `Buffer<D>`
A fixed-size buffer carved from a pool chunk. Holds `Arc<BufferPool<D>>` to keep the pool alive. Returned to the pool's free list on drop. Supports `Deref<Target=[u8]>`, `DerefMut`, `set_len`, and `extend_from_slice`.

### `BufferPool<D>`
Manages large registered memory chunks and hands out `Buffer`s from them.

- `allocate()` — synchronous, returns error if pool is exhausted.
- `async_allocate()` — waits via `tokio::sync::oneshot` if pool is exhausted.
- Automatic chunk growth up to a configurable `max_memory` limit.
- `registration(memory_index, device_index)` — look up a registration for consumer crate use (e.g. to extract device-specific keys).

## Dependencies

- `tokio` (sync feature only) — for `async_allocate` waiter channels.

No dependency on `ruapc`, `ruapc-rdma`, or any specific transport.

## Usage in ruapc

In `ruapc`, concrete device types (`TcpDevice`, `RdmaDevice`) are wrapped in an enum `Device` that implements `ruapc_bufpool::Device`. Type aliases make this ergonomic:

```rust
// ruapc/src/memory/mod.rs
pub type Memory = ruapc_bufpool::Memory<MemoryRegistration>;
pub type Buffer = ruapc_bufpool::Buffer<Device>;
pub type BufferPool = ruapc_bufpool::BufferPool<Device>;
pub type Devices = ruapc_bufpool::Devices<Device>;
```

Extension traits (`BufferExt`, `MemoryExt`) add transport-specific methods like `memory_key()` and `remote_buffer_info()` that depend on types from `ruapc`'s wire protocol.

## Testing

```bash
cargo test -p ruapc-bufpool
```

The test suite includes 32 tests covering:
- `AlignedMemory` allocation, alignment, read/write
- `Devices` collection management and index assignment
- `Memory` registration, lookup, drop cleanup
- Partial registration failure safety (key design invariant)
- `BufferPool` allocation, reuse, exhaustion, multi-chunk growth
- `Buffer` operations: set_len, extend_from_slice, Deref, raw pointers
- Async allocation with waiter wakeup
