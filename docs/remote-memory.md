# Remote memory

Clients attach owned buffers to a request; handlers access them through
`Context::remote_read` and `remote_write`. TCP, WebSocket and HTTP/2 use reverse
RPCs to copy bytes. RDMA uses READ, including a client-side READ for
`remote_write`. Ordinary unary HTTP calls do not support these reverse RPCs.

For standalone logical-space validation, copy planning and immutable buffer
sources, see the [memory crate](../ruapc-memory/README.md). RPC applications keep
using the `ruapc` types and `Context` methods shown here.

TCP and HTTP/2 send immutable source slices with a small control prefix,
without gathering or serializing the bulk bytes into a temporary buffer.
WebSocket coalesces them once into its binary message. Receivers scatter directly
from the received payload into the destination buffers, without decoding bulk
bytes into a separate vector. Socket/kernel buffering, WebSocket masking and
HTTP/2 frame assembly can still copy data; this is not kernel zero-copy I/O.
See the [inline protocol](builtin-services.md#remote-memory-memoryservice).

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
let mut transfer = client
    .with_read_buffer(src)
    .expect("valid source layout")
    .with_write_buffers(vec![dst]);
let reply: BufferReply<u64> = transfer.copy(&ctx, &()).await?;
let (total, destinations) = reply.into_parts();
assert_eq!(total, 4);
let ruapc::BufferAvailability::Available(buffers) = destinations else {
    panic!("copy completed but destinations are unavailable");
};
assert_eq!(&*buffers[0], b"Rua!");
let reusable_sources = transfer.take_read_buffers();
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

Read attachments validate their layout before being installed. The fallible
`with_read_buffer(s)` methods return `RemoteIoError` on invalid input, retaining
the supplied buffers. `into_parts()` returns the RPC error and an optional
recovered buffer vector. Handle the error explicitly when those buffers must be
reused: converting it with `?` to the core `Error` drops them. The standalone
memory crate's constructors keep their own `BufferSetError` type. Replacing read
attachments consumes the wrapper: on failure, the error retains only the newly
supplied buffers, and the wrapper releases its previous attachments.

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
| Read attachments | Immutable and reusable across calls. Attaching a new list replaces the old one. `read_buffers()` borrows it; `take_read_buffers()` returns `Available` only when no pending request or local reader shares ownership. `Unavailable` preserves the source for another attempt; `NotAttached` means no source remains. |
| Write attachments | Consumed by the next call. `BufferReply::destinations()` reports the set's availability; `into_parts()` recovers the response and that state. Plain results do not return destinations. After failure, `take_write_buffers()` reports any recoverable set. |
| Handler buffers | `remote_read` returns its destinations; `remote_write` returns a `WriteReceipt` with separate source availability. `RemoteIoError::recovered_buffers()` and `take_recovered_buffers()` expose any recoverable buffers on failure; converting the error with `?` to `Error` drops them. |

`BufferAvailability` distinguishes `NotAttached`, `Available(Vec<Buffer>)` and
`Unavailable`. An explicitly attached empty set is `Available(vec![])`.
`Unavailable` describes local recovery only: it does not prove that DMA is
still running, since the pool may already have recycled the buffers. An
unavailable response, write-attachment state or receipt is not a handle that
can later recover buffers. Only the read-attachment wrapper retains a shared
source after an unsuccessful `take_read_buffers()` attempt, allowing that
specific recovery attempt to be repeated.

Stream send queues and in-progress socket writes retain immutable source slices.
Cancelling or timing out the RPC does not release those owners while a send can
still access the bytes. Response admission waits for queue capacity or connection
closure, with no independent timeout. A peer that stops reading can therefore
retain sources indefinitely and keep handlers waiting to enqueue responses in
their server admission slots.
`remote_write` recovers its sources only if they are
unique when it returns; even a successful receipt can report `Unavailable` if
the transport still holds a source. Read-attachment recovery can be retried once
those owners finish or the connection drops its queue.

Received `Payload` values and their `Bytes` conversions share the original
allocation; cloning or slicing `Bytes` does not release that owner. Keeping even
a small slice in a cache can therefore retain an entire registered buffer or
RDMA receive allocation. Large RDMA payloads also retain their receive-byte
budget until the last owner is dropped. For long-lived storage that should not
occupy transport capacity, explicitly copy the needed bytes, for example with
`Bytes::copy_from_slice(payload.as_slice())`, then release the original payload
and any shared views. See [large-payload admission](rdma-capacity.md#large-payload-admission-and-cleanup).

Posted READ destinations remain owned until completion or successful QP
destruction. Cancellation or failure can make recovery unavailable even after
the QP eventually recycles the memory. Recovered error buffers are reusable,
but their readable contents must not be treated as a successful transfer result.

Application RDMA reads validate the source request after READ completion.
Source recovery after failure still does not prove remote DMA has stopped.
The [safety contract](safe-boundaries.md#application-read-source-validation-and-recovery)
explains successful-result validation, its assumptions and the failed-read
boundary.

For runnable workloads and data verification, see the [demo guide](../ruapc-demo/README.md#remote-readwrite).
