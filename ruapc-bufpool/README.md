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
Allocation futures are runtime independent; Tokio is used only by tests.
`BufferPool::allocation_capacity(size)` reports the size class without
allocating or reserving capacity.

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
Custom `MemoryRegistrar` and `Devices` implementations must uphold the
[registration and byte-access contracts](../docs/safe-boundaries.md#registration-and-byte-access).
Device registrations are destroyed before backing memory.
`TcpDevice::read_memory` is unsafe: the caller must keep the requested allocation
alive and prevent concurrent writes throughout the copy.

## Implementation

| Module | Responsibility |
| --- | --- |
| [`pool.rs`](src/pool.rs), [`pool/builder.rs`](src/pool/builder.rs) | Public API, allocation transitions and configuration |
| [`pool/allocator.rs`](src/pool/allocator.rs), [`waiters.rs`](src/pool/allocator/waiters.rs) | Budget, merging, direct handoff and starvation reservations |
| [`completion.rs`](src/completion.rs) | Safe runtime-independent single-value notification shared with RDMA |
| [`pool/small.rs`](src/pool/small.rs), [`slab.rs`](src/slab.rs), [`thread_cache.rs`](src/thread_cache.rs) | Slab classes, chunk ownership, thread caches and reclamation |
| [`buddy.rs`](src/buddy.rs), [`aligned.rs`](src/aligned.rs) | Backing regions, allocation metadata and initialized memory |

Cache locks are released before acquiring slab or buddy locks. Slab locks may
precede the buddy mutex; the reverse order is forbidden. `BuddyState` keeps
mutable tree metadata and intrusive nodes behind `UnsafeCell`, accessed only
under the buddy mutex. Registrations stay immutable; the allocator never borrows
an entire published block mutably. Self-referencing nodes are initialized after
the block has aliasable ownership so moves cannot invalidate their links.

Public buffers retain the pool; internal slab backing tokens do not, avoiding
a pool/slab/buffer ownership cycle. Chunk ownership moves across cache
boundaries. Growth failure restores reserved budget; cancelling a waiter hands
reserved capacity to another waiter or releases it to the allocator.
Capacity assignment and waiter registration share the allocator mutex.
Channel delivery, cancelled sender destruction and wakers run after unlocking;
a cancelled delivery can therefore return its buffer without recursively
locking the allocator. Assigned buffers remain reserved for their waiters
while delivery runs outside the lock.

Pools that have never installed a slab skip slab and thread-cache reclaim
sweeps. A monotonic hint is published before the first installation; once a
slab has been used, the normal sweeps remain enabled. Its sequentially
consistent ordering with waiter demand prevents a first installation racing
with waiter registration from stranding capacity. The budget check constructs
an out-of-memory error only when returning failure to a synchronous caller.

<a id="tests-and-benchmarks"></a>

## Build and validation

This crate needs no RDMA libraries or device, including for its tests:

```bash
cargo test -p ruapc-bufpool
cargo doc -p ruapc-bufpool --no-deps
cargo bench -p ruapc-bufpool --bench lazy_merge
cargo bench -p ruapc-bufpool --bench contention
cargo bench -p ruapc-bufpool --bench initialization
cargo bench -p ruapc-bufpool --bench async_handoff
```

See [allocator benchmark controls](../docs/benchmark.md#allocator-harnesses)
for case selection, CPU placement and measurement boundaries, and
[workspace validation](../CONTRIBUTING.md) for contribution checks.
