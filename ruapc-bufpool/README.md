# ruapc-bufpool

A reusable buffer pool with buddy allocation, small-buffer slabs and optional
device registration. Part of [RuaPC](../README.md), usable without RDMA.

## Allocation

The pool grows in registered 64 MiB blocks. Requests are rounded up to these classes:

| Layer | Buffer capacities |
| --- | --- |
| Slabs, carved from 1 MiB buddy leaves | 16 KiB, 64 KiB, 256 KiB |
| Four-way buddy tree | 1 MiB, 4 MiB, 16 MiB, 64 MiB |

`allocate(size)` returns an error when capacity is unavailable;
`async_allocate(size)` waits for capacity to return. Both reject zero and sizes
above 64 MiB. Growth and device registration run outside allocator locks but
still execute on the calling thread, including in the async API.

`BufferPoolBuilder` controls the memory budget (256 MiB by default), lazy merging,
empty-slab caching, thread caches and starvation protection. Choose a budget in
64 MiB multiples: a remainder cannot back another block. Per-thread caches are
enabled by default and reclaimed under memory pressure. Large async waiters can
reserve a subtree after the starvation timeout; otherwise waiters are served
smallest-request-first.

Buffers initially have `len() == capacity()`. Use `set_len` to select the logical
data length; `extend_from_slice` appends at that length. Dropping a buffer returns
its allocation to the pool. Reused bytes are **not cleared**. Newly allocated
backing blocks are initialized, with 2 MiB alignment on 64-bit targets and 4 KiB
otherwise; Linux 64-bit builds use demand-paged anonymous mappings.

## Usage

```rust
use std::sync::Arc;
use ruapc_bufpool::{BufferPoolBuilder, EmptyDevices};

fn main() -> std::io::Result<()> {
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(256 * 1024 * 1024)
        .build();

    let mut buffer = pool.allocate(1024)?; // 16 KiB capacity
    buffer.set_len(0);
    buffer.extend_from_slice(b"Rua!")?;
    assert_eq!(&*buffer, b"Rua!");
    Ok(()) // buffer returns to the pool
}
```

## Registration and ownership

`DeviceSet<D>` combines a TCP device at index zero with additional devices.
`EmptyDevices` disables registration. A safe `Device` wrapper supplies identity
and an audited `MemoryRegistrar`; it never receives pool backing memory itself.
Custom `MemoryRegistrar` and `Devices` implementations are unsafe contracts:
registration may retain backing memory but must preserve each allocation's
lifetime and shared/exclusive byte access. It grants no independent permission
to read or write those bytes.

Public buffers retain the pool. Internal slab backing tokens do not, avoiding
an ownership cycle. Device registrations are destroyed before backing memory.
`TcpDevice::read_memory` is unsafe: the caller must keep the requested allocation
alive and prevent concurrent writes throughout the copy.

## Implementation

| Module | Responsibility |
| --- | --- |
| `pool.rs`, `pool/builder.rs` | Public API, allocation transitions and configuration |
| `pool/allocator.rs` | Budget, buddy splitting, free lists and lazy merging |
| `pool/allocator/waiters.rs` | Direct handoff, cancellation and starvation reservations |
| `pool/small.rs`, `slab.rs` | Slab classes, chunk ownership and reclamation |
| `thread_cache.rs` | Thread magazines and their pool registry |
| `buddy.rs` | Backing regions and allocation metadata |

Cache locks are released before acquiring slab or buddy locks. Slab locks may
precede the buddy mutex; the reverse order is forbidden. Buddy metadata changes
stay under that mutex, while buffers can read immutable registration keys.
See [DESIGN.md](../DESIGN.md) for workspace ownership boundaries.

## Tests and benchmarks

```bash
cargo test -p ruapc-bufpool
cargo bench -p ruapc-bufpool --bench lazy_merge
cargo bench -p ruapc-bufpool --bench contention
cargo bench -p ruapc-bufpool --bench initialization
```

On Linux, `RUAPC_BENCH_CPU_BASE=N` pins contention worker `i` to CPU `N+i`
and the coordinator to `N+16`. Select available physical cores and bind memory
to their NUMA node. To compare one case in fresh processes, set
`RUAPC_BENCH_SIZE` (65536 or 1048576 bytes) and `RUAPC_BENCH_THREADS`
(1, 2, 4, 8 or 16). Keep placement, harness and pool lifecycle identical between
versions. See [benchmark instructions](../docs/benchmark.md).
