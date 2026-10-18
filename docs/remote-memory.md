# Remote memory

Clients attach owned buffers to a request; handlers access them through
`Context::remote_read` and `remote_write`. TCP, WebSocket and HTTP/2 use reverse
RPCs to copy bytes. RDMA uses READ, including a client-side READ for
`remote_write`. Ordinary unary HTTP calls do not support these reverse RPCs.

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
Nonzero transfers validate bounds, overflow, region/op limits and overlapping
destination ranges. `remote_read` and `remote_write` return without validation
or I/O for empty or all-zero `CopyOp` batches. Only the requested destination
ranges are filled; separate calls require caller coordination.

## Copy a client's read space into its write space

The handler returns a `WithBuffers<T>` response by completing a write and pairing
its `SentBuffers` witness with the response value:

```rust
use ruapc::{Context, Result, WithBuffers};

#[ruapc::service]
trait BlobService {
    async fn copy(&self, ctx: &Context, req: &()) -> Result<WithBuffers<u64>>;
}

struct Blob;

impl BlobService for Blob {
    async fn copy(&self, ctx: &Context, _req: &()) -> Result<WithBuffers<u64>> {
        let data = ctx.remote_read_all().await?;
        let total = data.iter().map(|buffer| buffer.len() as u64).sum();
        let sent = ctx.remote_write_all(data).await?;
        Ok(sent.reply(total))
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
let mut transfer = client.with_read_buffer(src).with_write_buffers(vec![dst]);
let (total, buffers) = transfer.copy(&ctx, &()).await?.into_parts();
assert_eq!(total, 4);
assert_eq!(&*buffers[0], b"Rua!");
let reusable_sources = transfer.take_read_buffers();
```

Use `ctx.sent_nothing().reply(value)` when a handler intentionally performs no
write. `SentBuffers::merge` combines completed writes. Generated clients select
the buffer-returning contract by type, including aliases such as
`ResultWithBuffers<T>`.

## Timeouts

Incoming RPCs give the handler an absolute request deadline. Nonempty
`remote_read` and `remote_write` calls use that deadline throughout their
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

An expired remote-memory call returns an error to the handler; it does not
cancel the handler itself or authorize early recovery of DMA buffers. The
handler decides how to handle the error. Recovery follows the ownership rules
below.

## Ownership and recovery

| Resource | Lifetime and recovery |
|---|---|
| Read attachments | Immutable and reusable across calls. Attaching a new list replaces the old one. `read_buffers()` borrows it; `take_read_buffers()` returns it only when no pending request or local reader shares ownership. `None` preserves the source for another attempt. |
| Write attachments | Consumed by the next call. Successful `WithBuffers` responses return available, uniquely held destinations; plain results do not return them. After failure, `take_write_buffers()` recovers any available set. |
| Handler buffers | `remote_read` returns its destinations; `remote_write` returns a `SentBuffers` witness owning its sources. `RemoteIoError` can carry recoverable buffers on failure; converting it with `?` to `Error` drops them. |

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
