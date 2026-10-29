# RuaPC memory spaces

`ruapc-memory` provides logical memory spaces, copy validation and planning,
synchronous CPU gather/scatter, and owned buffers with cached layouts. It depends
on `ruapc-bufpool`, serde and schemars, and works without the RPC runtime, an async
executor or verbs.

RPC applications use the types re-exported by `ruapc` and its `Context` methods.
The [remote-memory guide](https://github.com/SF-Zhou/ruapc/blob/main/docs/remote-memory.md)
covers attaching buffers and executing remote transfers.

## Validate a batch and check its spaces

`CopyOp` addresses the concatenated source and destination spaces using byte
offsets and a length. `CopyBatch::new` borrows immutable operations and checks
their structure once: operation count, offset arithmetic, total copied bytes
and destination overlap. Source ranges may overlap. Zero-length operations
perform no access regardless of their offsets.

A batch does not bind itself to a particular space length. `check_source`,
`check_destination` and `check_bounds` check actual space bounds using the
cached maximum range ends, without sorting operations again. `byte_count`
returns the sum of operation lengths, which can differ from either logical
space's length. Callers preparing transport work check both spaces before
submitting it; each receiving peer independently validates incoming operations.

`SpaceLayout::from_buffers` constructs a layout from buffers' logical lengths;
`from_lens` accepts lengths directly. `CopyBatch::plan_chunks` checks the supplied
layouts and splits the batch into source-contiguous chunks with a bounded list
of destination slices. Planning performs no I/O; it allocates only plan metadata.

```rust
use ruapc_memory::{CopyBatch, CopyOp, SpaceLayout};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = SpaceLayout::from_lens([4, 4])?;
    let destination = SpaceLayout::from_lens([3, 5])?;
    let operations = [CopyOp::new(2, 1, 6)];
    let batch = CopyBatch::new(&operations)?;
    batch.check_bounds(source.total(), destination.total())?;
    let chunks = batch.plan_chunks(&source, &destination, 2)?;
    assert_eq!(batch.byte_count(), 6);
    assert_eq!(chunks.iter().map(|chunk| chunk.len).sum::<u64>(), 6);
    assert!(chunks.iter().all(|chunk| chunk.dst.len() <= 2));
    Ok(())
}
```

`SpaceLayout::for_each_slice` checks a complete range before invoking its
callback. `RemoteSpace` validates advertised region counts, address arithmetic
and logical lengths. It borrows the region descriptions; it does not own remote
memory or establish access permission, request validity or DMA completion.

## Gather and scatter CPU bytes

`CopyBatch::gather` packs the selected source ranges in operation order. It
builds the actual source layout and checks source bounds; destination offsets
do not index the packed result. `CopyBatch::scatter` copies packed bytes to the
operations' destination offsets. It checks the actual destination layout and
requires exactly `byte_count` packed bytes before changing any destination.
Source offsets do not index the packed input. Gaps remain unchanged.

`BufferSet::try_new` takes owned buffers and caches their checked layout. Its
`gather` and `scatter` methods reuse this layout. Buffer lengths cannot be changed
through a set: `buffers()` exposes immutable buffers and `into_buffers()`
consumes the set to return ownership. Public copy methods never accept a
caller-supplied promise that a separate layout matches actual buffers.

```rust
use std::sync::Arc;
use ruapc_bufpool::{BufferPool, EmptyDevices};
use ruapc_memory::{BufferSet, CopyBatch, CopyOp};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = BufferPool::new(Arc::new(EmptyDevices));
    let mut source = pool.allocate(4)?;
    source.set_len(4);
    source.copy_from_slice(b"Rua!");
    let mut destination = pool.allocate(4)?;
    destination.set_len(4);
    destination.fill(0);
    let source = BufferSet::try_new(vec![source])?;
    let mut destination = BufferSet::try_new(vec![destination])?;

    let operations = [CopyOp::new(1, 0, 3)];
    let batch = CopyBatch::new(&operations)?;
    let packed = source.gather(&batch)?;
    assert_eq!(packed, b"ua!");
    destination.scatter(&batch, &packed)?;
    assert_eq!(&destination.buffers()[0][..], b"ua!\0");
    Ok(())
}
```

A batch can be reused with different buffer segmentations and space lengths as
long as its ranges fit the actual accessed space. Even an empty batch checks
actual buffer layouts; scatter still requires an empty packed payload.

If `BufferSet::try_new` fails, `BufferSetError::into_parts()` returns its
`MemoryError` and every original buffer in original order. Dropping that error
releases its buffers normally. These are local ownership operations, not
transfer-completion evidence.

## Immutable sources and runtime integration

`ReadSource::try_new` wraps a checked buffer set with an immutable interface.
`gather(&CopyBatch)` checks source bounds and reuses the cached layout;
`buffers()` borrows the source and `into_buffers()` consumes it. Construction
failure returns the same `BufferSetError` recovery contract. Sources can be
shared through `Arc<ReadSource>`; recovering owned buffers requires uniqueness.

RuaPC keeps write-target checkout, RPC attachments and waiter registration,
reverse RPCs, deadlines, READ execution and completion witnesses in `ruapc`.
An inline write holds the target's mutex for the complete scatter batch. The
runtime converts `MemoryError` into its RPC error categories. Posted RDMA
buffers and completion authority remain in `ruapc-rdma`. See
[safety boundaries](https://github.com/SF-Zhou/ruapc/blob/main/docs/safe-boundaries.md)
for ownership, recovery and source-validation contracts.

## Tests and benchmarks

```bash
cargo test -p ruapc-memory
```

Tests cover structural and actual-space checks, overflow, overlap, planning,
packed copies, immutable layouts and construction-failure recovery without RDMA
libraries or a device. See
[Contributing](https://github.com/SF-Zhou/ruapc/blob/main/CONTRIBUTING.md) for
workspace checks and transport tests.

The [planning benchmark](https://github.com/SF-Zhou/ruapc/blob/main/docs/benchmark.md#memory-spaces)
measures validation and chunk planning. The
[CPU-copy benchmark](https://github.com/SF-Zhou/ruapc/blob/main/docs/benchmark.md#cpu-copies)
compares raw buffer slices and cached layouts, with reused batches or validation
inside each measured iteration.
