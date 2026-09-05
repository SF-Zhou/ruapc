# ruapc-bufpool

A high-performance memory pool using buddy memory allocation algorithm for efficient
fixed-size buffer management. This crate is part of the [ruapc](../ruapc/) project.

## Features

- **Buddy Memory Allocation**: Supports allocation of 1MiB, 4MiB, 16MiB, and 64MiB buffers
- **Slab Layer for Small Buffers**: 16KiB, 64KiB and 256KiB allocations are served from slabs
  (1MiB buddy leaves carved into fixed-size chunks) behind per-class mutexes, keeping
  small-buffer traffic off the buddy pool's global mutex; empty slabs are cached up to a
  watermark and returned to the buddy pool on demand
- **Both Sync and Async APIs**: Designed for tokio environments with async-first design
- **Automatic Memory Reclamation**: Buffers are automatically returned to the pool on drop
- **Memory Limits**: Configurable maximum memory usage with async waiting when limits are reached
- **Aligned Memory**: Zero-initialized blocks with 2MiB alignment on 64-bit targets
- **O(1) Buddy Merging**: Intrusive doubly-linked list with O(1) free/merge operations
- **Lazy Buddy Merging**: Per-level watermarks defer merging on free to avoid split/merge
  thrashing; complete-but-unmerged quads are tracked on intrusive pending-merge lists and
  coalesced on demand in O(merges performed), independent of pool size
- **Starvation Protection**: Async waiters are served smallest-request-first; a large
  waiter queued past a configurable timeout gets a reserved subtree whose capacity is
  drained toward it and protected from smaller allocations until it is satisfied
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
│  pending_lists: [IntrusiveList; 4]  (deferred merge quads)  │
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

## Implementation boundaries

| Module | Responsibility |
| --- | --- |
| `pool.rs` | Public API and shared sync/async allocation transitions |
| `pool/builder.rs` | Configuration and construction |
| `pool/allocator.rs` | Budget, buddy splitting, free lists and lazy merging |
| `pool/allocator/waiters.rs` | Direct handoff, cancellation and starvation reservations |
| `pool/small.rs` | Slab/cache coordination and memory-pressure reclamation |
| `buddy.rs` | Immutable region ownership and mutable allocation metadata |
| `slab.rs` | Chunk ownership, free bitmaps and backing tokens |
| `thread_cache.rs` | Thread magazines and their pool registry |

Cache locks are released before touching slab classes; slab locks are released
before returning backing to the buddy allocator. Public buffers retain the pool.
Internal slab backing tokens do not, preventing a strong-reference cycle. Device
registrations are destroyed before their backing memory.

Thread-cache hits borrow the shard for one push or pop, avoiding temporary Arc
reference-count updates. That TLS borrow ends before buffer construction, slab
refill, or reclamation can call back into the pool.

Buddy allocation metadata uses interior mutability under the pool mutex. Tree
updates borrow only that metadata, allowing buffers to read immutable registration
keys concurrently and preserving the raw pointers held by intrusive free lists.

## Core Types

### `AlignedMemory`
Owns initialized memory with 2MiB alignment on 64-bit targets (4KiB otherwise).
Linux 64-bit builds use anonymous mappings whose pages are initialized lazily by
the OS. Other targets use the system zeroed allocator, which can make initial
block allocation more expensive. Reusing a pooled buffer does not clear its bytes.

### `BufferPoolBuilder`
Builder pattern for configuring memory limits, merge policy, caching, starvation protection, and device registration.

### `BufferPool`
Manages 64MiB buddy blocks and supports three slab sizes and four buddy sizes.
- `allocate(size)` — synchronous, returns error if pool exhausted
- `async_allocate(size)` — waits via `tokio::sync::oneshot` if pool exhausted

### `Buffer`
A buffer allocated from the pool. Supports `Deref<[u8]>`, `DerefMut`, `set_len`, `extend_from_slice`, and automatic return on drop.

### Device Registration
- `trait Device` — device identity and access to an audited registrar; safe wrappers never receive pool memory
- `unsafe trait MemoryRegistrar` — register memory while preserving each allocation's access and lifetime rules
- `DeviceSet<D>` — safely combine a TCP device with statically dispatched additional devices
- `unsafe trait Devices` — collection that preserves pooled allocations' access rules
- `trait Registration` — handle for a registered memory region
- `TcpDevice` — TCP transport device (simulates RDMA-style registration)

Registration may retain backing memory, but does not authorize independent byte
access. `Devices` implementations must preserve the lifetime and shared/exclusive
borrows of each allocation. `TcpDevice::read_memory` is unsafe: callers must hold
the requested allocation alive and prevent concurrent writes throughout the copy.

## Usage

```rust
use std::sync::Arc;
use ruapc_bufpool::{BufferPoolBuilder, EmptyDevices};

let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
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
cargo bench -p ruapc-bufpool --bench lazy_merge
cargo bench -p ruapc-bufpool --bench contention
cargo bench -p ruapc-bufpool --bench initialization
```

For reproducible Linux contention measurements, set `RUAPC_BENCH_CPU_BASE` after
checking the machine's CPU topology. Worker `i` is pinned to `base + i`, and the
coordinator to `base + 16`, before warmup. Choose available physical cores and
bind memory to their NUMA node. For example, on a machine with cores 96–112 on
NUMA node 1:

```bash
RUAPC_BENCH_CPU_BASE=96 numactl --membind=1 cargo bench -p ruapc-bufpool --bench contention
```

Without this variable the operating system places the workers. Restricting the
whole process to a CPU mask still allows workers to migrate between cache groups.

Compare individual cases in fresh processes when evaluating a refactor. Pool
lifetimes and allocation history can change the addresses used by later cases in
the full matrix. These filters preserve the workload while selecting one case:

```bash
RUAPC_BENCH_CPU_BASE=96 RUAPC_BENCH_SIZE=65536 RUAPC_BENCH_THREADS=4 \
    numactl --membind=1 cargo bench -p ruapc-bufpool --bench contention
```

Supported sizes are 65536 and 1048576 bytes; thread counts are 1, 2, 4, 8, and 16.
Use the same harness, CPU and NUMA placement, and pool lifecycle for both versions.
