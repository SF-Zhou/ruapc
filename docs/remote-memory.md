# Remote memory

Clients borrow read sources and attach owned write destinations; handlers access them through
`Context::remote_read` and `remote_write`. TCP, WebSocket and HTTP/2 use reverse
RPCs to copy bytes. RDMA uses READ, including a client-side READ for
`remote_write`. Ordinary unary HTTP calls do not support these reverse RPCs.

For standalone logical-space validation, copy planning and immutable buffer
sources, see the [memory crate](../ruapc-memory/README.md). RPC applications keep
using the `ruapc` types and `Context` methods shown here.

## Spaces and copy operations

A buffer list forms a logical space by concatenating its buffers' `len()` values.
`CopyOp { src_offset, dst_offset, len }` addresses that space, so local and remote
buffer boundaries may differ.

| Operation | Source space | Destination space |
|---|---|---|
| `remote_read(ops, local)` | Client read attachments | Handler's `local` buffers |
| `remote_write(ops, local)` | Handler's `local` buffers | Client write attachments |
| `remote_read_all()` | Entire client read space | Newly allocated handler buffers |
| `remote_write_all(local)` | Entire `local` space | Beginning of client write space |

Set each buffer's logical length before attaching or transferring it: allocations
initially expose their full size class, and reused bytes are not cleared.
Stream transports accept unregistered buffers, including buffers from a separate
pool created with `BufferPool::new(Vec::new())`. Their metadata preserves each
buffer's length with zero addresses and keys. RDMA requires registration on the
selected connection's device; allocating from the context's pool supplies those
registrations.
Transfers require a connected context and the corresponding advertised remote
space. They validate operation structure, overflow, region/op limits, destination
overlap and bounds against the actual spaces. Empty or all-zero batches follow
these same checks, then return without destination checkout or I/O. Use
`sent_nothing()` for an intentional no-transfer reply that does not access remote
memory. Only the requested destination ranges are filled; separate calls require
caller coordination.

## Copy a client's read space into its write space

The handler returns a `BufferReply<T>` by completing a write and pairing its
`WriteReceipt` with the response value. The service macro implements the same
`BlobService` trait for the clients, preserving the declared return type:

```rust
use ruapc::{BufferReply, Context, Result};

#[ruapc::service]
trait BlobService {
    async fn copy(&self, ctx: &Context, req: &()) -> Result<BufferReply<u64>>;
}

struct Blob;

impl BlobService for Blob {
    async fn copy(&self, ctx: &Context, _req: &()) -> Result<BufferReply<u64>> {
        let data = ctx.remote_read_all().await?;
        let total = data.iter().map(|buffer| buffer.len() as u64).sum();
        let receipt = ctx.remote_write_all(data).await?;
        Ok(receipt.into_reply(total))
    }
}
```

Register `Blob` and create a client context as in the [quick start](../README.md#example).
Allocate source and destination buffers from that context's pool:

```rust,ignore
let mut src = ctx.state.buffer_pool.allocate(4)?;
src.set_len(4);
src.copy_from_slice(b"Rua!");
let mut dst = ctx.state.buffer_pool.allocate(4)?;
dst.set_len(4);

let client = ruapc::Client::default();
let transfer = client
    .with_read_buffer(&src)
    .with_write_buffers(vec![dst]);
let reply: BufferReply<u64> = transfer.copy(&ctx, &()).await?;
let (total, destinations) = reply.into_parts();
assert_eq!(total, 4);
let ruapc::BufferAvailability::Available(buffers) = destinations else {
    panic!("copy completed but destinations are unavailable");
};
assert_eq!(&*buffers[0], b"Rua!");
drop(transfer);
src.copy_from_slice(b"next"); // The caller still owns the source.
```

Import `BlobService` wherever the client call is made. The handler, `Client` and
`ClientWithBuffers` all implement that trait, and both sides return
`Result<BufferReply<T>, E>`. A service can also declare the return alias
`ResultBufferReply<T>`. The response payload on the wire remains the application
value `T`; local buffer availability is never serialized.

A reply constructed with `WriteReceipt::into_reply` has `NotAttached` destination
availability because the handler does not own the client's attached destinations.
The client populates the same reply type with its local destination recovery
state after receiving the response. `response()` borrows the application value;
`destinations()` reports that local state, and `into_parts()` returns both.

Use `ctx.sent_nothing().into_reply(value)` when a handler intentionally performs
no write. A receipt records completion, independently of whether its local
sources can be recovered; `take_sources()` returns their `BufferAvailability`
and leaves the receipt usable for `into_reply`. It does not claim that every
destination byte was filled or bind the write to a particular response value.

`with_read_buffer(&Buffer)` and `with_read_buffers(&[Buffer])` return a
`ClientWithBuffers` directly. They borrow the buffers and cache their logical
layout; replacing an attachment consumes the wrapper and replaces its borrowed
source list. The RPC call checks attachment region limits before acquiring a
connection and reports `InvalidCopyOp` through the method's existing error
result. The caller still owns its read sources. Attachment-validation failures
leave owned write attachments recoverable through `take_write_buffers()`.

The standalone memory crate likewise constructs `ReadSource::new(&[Buffer])`,
`BufferSet::new(Vec<Buffer>)` and `SpaceLayout::from_buffers(&[Buffer])` without
a construction error. Copying, planning and slice traversal check region limits
and ranges before accessing bytes, including empty batches. Remote-memory
transfer preparation performs these checks before I/O and returns supplied
local buffers in `RemoteIoError` on failure. See the
[memory crate guide](../ruapc-memory/README.md) for the standalone APIs.

## Timeouts

Incoming RPCs give the handler an absolute request deadline. `remote_read` and
`remote_write` calls that transfer bytes use that deadline throughout their
asynchronous waits; transferring control between admission, posting and
completion does not restart the budget.

RDMA READs also have an independent configured budget. Disabling that budget
does not disable the handler's request deadline. Admission can time out before
the first WR or between WRs; posted READs can time out without receiving a CQE.
See [READ admission and failure](rdma-capacity.md#read-admission-and-failure)
for configuration, error kinds and the periodic completion checks.

Reverse RPCs use the default client's response budget, capped by the handler's
remaining deadline. This covers TCP/WebSocket/HTTP/2 inline copies, RDMA
`remote_write`, and the source validation after an RDMA `remote_read` completes.
Send-queue admission and response waiting both honor this effective budget.
The response and READ expiry sweeps can delay notification beyond the nominal
deadline; they do not require a response or completion to arrive first.

Deadline expiry during a transfer of bytes returns an error to the handler;
it does not cancel the handler itself or authorize early recovery of DMA
buffers. A validated zero-byte batch performs no I/O or deadline check. The
handler decides how to handle the error. Recovery follows the ownership rules
below.

## Ownership and recovery

| Resource | Lifetime and recovery |
|---|---|
| Read attachments | Borrowed and immutable; reusable across calls. Attaching a new list replaces the old one. `read_buffers()` exposes the borrowed list. The caller retains ownership and can mutate or release sources once their borrow ends. |
| Write attachments | Consumed by the next call. A successful `BufferReply` reports the set's availability through `destinations()`, and `into_parts()` recovers the response and that state. After an error or a successful plain result, `take_write_buffers()` reports it instead; an available set left in the wrapper is attached again by that wrapper's next call. |
| Handler buffers | `remote_read` returns its destinations; `remote_write` returns a `WriteReceipt` with separate source availability. `RemoteIoError::recovered_buffers()` and `take_recovered_buffers()` expose any recoverable buffers on failure; converting the error with `?` to `Error` drops them. |

`BufferAvailability` distinguishes `NotAttached`, `Available(Vec<Buffer>)` and
`Unavailable`. An explicitly attached empty set is `Available(vec![])`.
`Unavailable` describes local recovery only: it does not prove that DMA is
still running, since the pool may already have recycled the buffers. An
unavailable response, write-attachment state or receipt is not a handle that
can later recover buffers. Read sources are borrowed separately and never enter
this recovery state machine.

When a call ends, with any result, the client seals its write target: a
reverse handler that still holds the target can no longer write, and a set
that no READ owns is returned as `Available`. Posted READ destinations remain
owned until completion or successful QP destruction. If a READ still owns the
set at that point, the call reports `Unavailable` and the QP later recycles
the memory into the pool. Recovered error buffers are reusable, but their
readable contents must not be treated as a successful transfer result.

### Wait for write destinations

Enable `ClientWithBuffers::wait_for_write_buffers(grace)` when a call must
also wait for a READ that still owns its write destinations:

```rust,ignore
let transfer = client
    .with_write_buffers(destinations)
    .wait_for_write_buffers(std::time::Duration::from_millis(100));
match transfer.copy(&ctx, &()).await {
    Ok(reply) => {
        let (response, destinations) = reply.into_parts();
        let buffers = destinations.into_available().expect("attached destinations returned");
        // Use the response and completed destination buffers.
    }
    Err(error) => {
        let buffers = transfer.take_write_buffers().into_available()
            .expect("attached destinations returned even on error");
        // Reuse these buffers; their contents are not a successful transfer result.
    }
}
```

This option only adds waiting; sealing and the return paths are those of every
call. It covers every RPC result, including success, application errors,
timeouts and connection failures. The READ returns the complete destination
set once it settles, whether it succeeded or failed. If the set remains
unavailable after `grace`, the client closes the connection (and its QP) that
is executing that READ and continues waiting without a time limit. Zero grace
requests closure immediately when buffers remain unavailable. Closing a shared
connection also fails its other pending requests. Existing connection failures
and the READ watchdog may close it sooner.

The original RPC result is preserved, and an attached set, including an empty
one, is always `Available` when the call returns. Without an attachment,
availability remains `NotAttached`.

The grace period starts during recovery after the RPC ends; it does not extend
the request's execution or source-validation deadline. Recovery can therefore
make the call outlive its RPC deadline indefinitely. QP closure alone does not
release buffers: all posted READs must settle through CQ-issued completions,
or provider QP destruction must succeed. Dropping or forgetting the request
future does not promise buffers back to the wrapper, and still cannot release
posted memory early.

This option applies to owned write destinations. Borrowed read attachments
retain their separate source-validation contract below.

### Read-source validation

Application RDMA reads validate the source request after READ completion. The
original borrowing future must be polled to answer the probe or service an
inline read; a forgotten future cannot authorize a new source operation.
Source reuse after failure still does not prove remote DMA has stopped.
The [safety contract](safe-boundaries.md#application-read-source-validation-and-recovery)
explains successful-result validation, its assumptions and the failed-read
boundary.

For runnable workloads and data verification, see the [demo guide](../ruapc-demo/README.md#remote-readwrite).
