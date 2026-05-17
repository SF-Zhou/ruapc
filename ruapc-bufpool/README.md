# ruapc-bufpool

A high-performance memory pool using buddy memory allocation algorithm for efficient
fixed-size buffer management. This crate is part of the [ruapc](../ruapc/) project.

## Features

- **Buddy Memory Allocation**: Supports allocation of 1MiB, 4MiB, 16MiB, and 64MiB buffers
- **Both Sync and Async APIs**: Designed for tokio environments with async-first design
- **Automatic Memory Reclamation**: Buffers are automatically returned to the pool on drop
- **Memory Limits**: Configurable maximum memory usage with async waiting when limits are reached
- **Custom Allocators**: Pluggable allocator trait for memory allocation backend
- **O(1) Buddy Merging**: Intrusive doubly-linked list with O(1) free/merge operations
- **Device Registration**: Optional device registration support for RDMA and TCP transports

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       BufferPool                             │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ BuddyBlock 0:  64MiB memory region                 │    │
│  │  State tree: 85 nodes (2-bit packed, 22 bytes)      │    │
│  │  Free nodes: inline intrusive list nodes            │    │
│  │  Registrations: [reg_dev0, reg_dev1, ...]           │    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ BuddyBlock 1:  64MiB memory region                 │    │
│  │  ...                                                │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                             │
│  free_lists: [IntrusiveList; 4]  (one per level)            │
│  waiting_lists: [VecDeque<Sender>; 4]  (async waiters)      │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
    Buffer  ← returned to caller
    (ptr, level, index, block_ptr, pool: Arc<BufferPool>)
```

Each 64MiB block is a **4-level quad-tree**:
- Level 0: 64 nodes × 1MiB
- Level 1: 16 nodes × 4MiB
- Level 2: 4 nodes × 16MiB
- Level 3: 1 node × 64MiB (root)

## Core Types

### `Allocator` / `DefaultAllocator`
Pluggable memory allocation backend. Default uses `std::alloc` with 2MiB alignment for huge page support.

### `BufferPoolBuilder`
Builder pattern for configuring max memory, custom allocator, and device registration.

### `BufferPool`
Manages 64MiB buddy blocks and supports allocation at four size levels.
- `allocate(size)` — synchronous, returns error if pool exhausted
- `async_allocate(size)` — waits via `tokio::sync::oneshot` if pool exhausted

### `Buffer`
A buffer allocated from the pool. Supports `Deref<[u8]>`, `DerefMut`, `set_len`, `extend_from_slice`, and automatic return on drop.

### Device Registration
- `trait Device` — register memory with a device
- `trait Devices` — collection of devices
- `trait Registration` — handle for a registered memory region
- `TcpDevice` — TCP transport device (simulates RDMA-style registration)

## Usage

```rust
use ruapc_bufpool::BufferPoolBuilder;

let pool = BufferPoolBuilder::new()
    .max_memory(256 * 1024 * 1024)
    .build();

// Allocate a 1MiB buffer
let mut buffer = pool.allocate(1024 * 1024)?;
buffer[0] = 42;

// Buffer is returned to the pool when dropped
drop(buffer);
```

## Testing

```bash
cargo test -p ruapc-bufpool
```
