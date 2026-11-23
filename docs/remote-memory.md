# Remote memory

Clients borrow read sources and attach write destinations, either owned or
[lent from caller-owned slots](#lend-write-destinations). Handlers access them
through `Context::remote_read` and `remote_write`. TCP, WebSocket and HTTP/2
move bulk bytes in [data frames](#stream-transports-data-frames). A stream
read requests source ranges and places the returned bytes using the handler's
local destination plan. RDMA uses READ,
including a client-side READ for `remote_write`. Ordinary unary HTTP calls do
not support these reverse RPCs or data frames.

For standalone logical-space validation, copy planning and immutable buffer
sources, see the [memory crate](../ruapc-memory/README.md). RPC applications keep
using the `ruapc` types and `Context` methods shown here.

The [project example](../README.md#example) is a complete runnable service with
lent write destinations. The snippets below focus on whole-space helpers,
owned destinations and recovery.

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

Set each buffer's logical length with `Buffer::set_len` before attaching or
transferring it: allocations initially expose their full size class
(`len() == capacity()`), and reused bytes are not cleared.
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

On TCP, WebSocket and HTTP/2 the bytes travel as
[data frames](#stream-transports-data-frames) of at most 1 MiB each, so no
message size limit bounds a batch; RDMA uses READs.

## Stream transports: data frames

A data frame carries up to 1 MiB of raw bytes and names a request pending on
the receiving connection. The receiver uses the frame's placement to interpret
the bytes.

- `remote_write` sends explicit destination ranges for the original call's
  attached write space. Adjacent destinations in operation order can share a
  range. Each frame holds at most 1024 ranges whose lengths add up to its raw
  byte count. The client's receive loop applies the bytes and answers the
  final frame on the same connection. A successful acknowledgement completes
  the write.
- `remote_read` keeps its destination buffers and validated copy plan with a
  local waiter. Its reverse call to `MemoryService/read_inline` sends the
  source ranges in operation order. The original borrowing future gathers
  the requested source bytes, and the client sends packed frames followed by
  the RPC response. As bytes arrive, the reader scatters them through the next
  destinations of its own plan, including across frame boundaries. Success
  requires both a successful RPC response and complete delivery.

An explicit frame sent to a packed target, or a packed frame sent to an
explicit target, fails the transfer. An out-of-bounds frame also causes the
target to refuse subsequent frames, so a write's final acknowledgement reports
an earlier refusal. Returning or dropping the owning call seals the target,
including for a frame already routed to it. The receive loop consumes bytes
for a gone or sealed request to preserve framing.

For writes, TCP and HTTP/2 can share the source buffers with the send queue.
TCP uses vectored writes, and HTTP/2 yields each segment as a body chunk. A
write frame spanning more than 64 source slices is gathered into contiguous
storage. For HTTP/2 frames of at most 64 KiB of raw bytes the threshold is 16
slices. WebSocket always packs the write into one binary message. On receive,
TCP can read directly into destination slices. WebSocket and HTTP/2 copy from
incoming chunks.

Every connection has an 8 MiB budget for data frames in preparation, in the
queue and being written. A producer reserves the encoded header and raw byte
count before allocating or gathering the payload, then takes a queue entry
when the frame is ready. Producers waiting for bytes keep their place and
leave the queue available for other messages. A frame releases its reservation
after its last segment is written or the frame is dropped.

A receive loop that cannot queue an acknowledgement immediately hands it to a
task and continues reading. A connection permits up to 1024 pending tasks
for these replies and closes if another reply needs deferral. See
[connection-bound routes](safe-boundaries.md#routes-belong-to-the-requests-connection).

The source buffers of a stream `remote_write` return once the connection has
dropped every frame that shares them, usually as soon as they are written to
the socket. A successful write waits for that within its deadline; a set that
is still shared then is reported `Unavailable`. A failed write does not wait:
its queued frames can outlive it, so its error recovers the buffers only when
no frame holds them, and otherwise each buffer returns to its pool as soon as
its own last frame is dropped.

The `ruapc_data_frames_total` and `ruapc_data_bytes_total` counters, labelled
`direction` (`sent` or `received`), count the frames a connection accepted and
the frames a receiver consumed. `ruapc_data_frames_discarded_total` counts
received frames whose bytes did not all reach a write space: the request was
gone or on another connection, or its write space was sealed or refused the
frame. Stream writes run no reverse RPC, so these counters, not per-method
metrics, show them.

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

Register `Blob` and create a client context as in the [quick start](../ruapc/README.md#quick-start).
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
leave owned write attachments recoverable through `take_write_buffers()` and
lent ones in their slots.

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
remaining deadline. This covers TCP/WebSocket/HTTP/2 inline reads, the wait
for a stream `remote_write`'s acknowledgement, RDMA `remote_write`, and the
source validation after an RDMA `remote_read` completes. Send-queue admission
of every data frame and response waiting both honor this effective budget.
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
| Owned write attachments | Consumed by the next call. A successful `BufferReply` reports the set's availability through `destinations()`, and `into_parts()` recovers the response and that state. After an error or a successful plain result, `take_write_buffers()` reports it instead; an available set left in the wrapper is attached again by that wrapper's next call. |
| Lent write attachments | Moved out of the caller's slots by each call and moved back before it returns, so the same slots serve the wrapper's next call. Only a call that is not polled to completion can leave them empty; see [lending](#lend-write-destinations). |
| Handler buffers | `remote_read` returns its destinations; `remote_write` returns a `WriteReceipt` with separate source availability. `RemoteIoError::recovered_buffers()` and `take_recovered_buffers()` expose any recoverable buffers on failure; converting the error with `?` to `Error` drops them. |

`BufferAvailability` distinguishes `NotAttached`, `Available(Vec<Buffer>)` and
`Unavailable`. An explicitly attached empty set is `Available(vec![])`.
`Unavailable` describes local recovery only: it does not prove that DMA is
still running, since the pool may already have recycled the buffers. An
unavailable response, write-attachment state or receipt is not a handle that
can later recover buffers. Read sources are borrowed separately and never enter
this recovery state machine.

Returning or dropping a call seals its write target, revoking every existing
writer. A returning call reports a set that no READ owns as `Available`.
Posted READ destinations remain
owned until completion or successful QP destruction. If a READ still owns the
set at that point, the call reports `Unavailable` and the QP later recycles
the memory into the pool. A call with [lent destinations](#lend-write-destinations)
waits for that READ instead. Recovered error buffers are reusable, but their
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
future does not promise buffers back to the wrapper or to lent slots, and still
cannot release posted memory early.

This option applies to write destinations: it is opt-in for owned sets and
only sets the grace period for lent ones, which always wait. Borrowed read
attachments retain their separate source-validation contract below.

### Lend write destinations

`with_write_buffer_mut(&mut Buffer)` and `with_write_buffers_mut(&mut [Buffer])`
lend caller-owned slots instead of transferring a `Vec<Buffer>`. The call fills
the buffers in place:

```rust,ignore
let reply = client
    .with_read_buffer(&src)
    .with_write_buffer_mut(&mut dst)
    .copy(&ctx, &())
    .await?;
assert_eq!(*reply.response(), 4);
assert_eq!(&*dst, b"Rua!"); // The original buffer, back in its slot.
```

The slots form the same write space as an owned set. Each call moves the
buffers out of their slots into its write target, leaving empty buffers behind,
and moves the originals back before it returns. The slots are then attached to
the wrapper's next call. They are the only return path:
`BufferReply::destinations()` and `take_write_buffers()` report `NotAttached`.
While one call holds the slots, concurrent calls on the same wrapper carry no
write attachment.

| How the call ends | Slots afterwards |
|---|---|
| Returns, with any result | The original buffers in their original positions and with their lengths. A READ that still owns them is awaited first, without a time limit. |
| Dropped, including cancellation by `select!` or a timeout wrapper | The original buffers if restoring them needs no waiting; otherwise empty buffers. |
| Forgotten with `mem::forget` | Empty buffers. |

A call takes the buffers at its first poll; one that is dropped or forgotten
before that leaves the slots untouched.

A returned call waits as described under
[wait for write destinations](#wait-for-write-destinations). Its grace period
is `Client::timeout` unless `wait_for_write_buffers(grace)` sets one. The call
can therefore outlive its RPC deadline, and closes the connection executing the
READ when the grace period expires. After an error, the restored contents are
not a successful transfer result.

A dropped call cannot wait. It leaves the slots empty if a READ owns the
buffers at that moment, or if the call was already waiting for one; that READ
later returns the buffers to the pool. TCP, WebSocket and HTTP/2 use no READ,
so there a dropped call always restores the slots. A forgotten call restores
nothing, and its buffers never return to the pool.

Slots that were not restored hold empty buffers, whose `capacity()` is 0, and
are not attached to later calls on that wrapper. In every case the slots never
expose memory that a READ can still write.

### Read-source validation

Application RDMA reads validate the source request after READ completion. The
original borrowing future must be polled to answer the probe or service an
inline read; a forgotten future cannot authorize a new source operation.
Source reuse after failure still does not prove remote DMA has stopped.
The [safety contract](safe-boundaries.md#application-read-source-validation-and-recovery)
explains successful-result validation, its assumptions and the failed-read
boundary.

For runnable workloads and data verification, see the [demo guide](../ruapc-demo/README.md#remote-readwrite).
