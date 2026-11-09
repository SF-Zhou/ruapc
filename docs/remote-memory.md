# Remote memory

Ordinary callers use safe source references and owned destinations in `ruapc`;
advanced callers can select direct source validation or borrow destinations
through `ruapc-unsafe`. These APIs execute the same logical copy
plans and use the same TCP, WebSocket, HTTP/2 and RDMA dispatch. No business
feature branch is required. RDMA configuration types and fields remain behind
the `rdma` Cargo feature. Ordinary unary HTTP does not support reverse RPCs.

## Spaces and copy operations

Buffer lengths concatenate into a logical space. `CopyOp {src_offset,
dst_offset,len}` addresses that space independently of buffer boundaries.

| Operation | Source | Destination |
|---|---|---|
| `remote_read(ops)` | Client read attachments | Handler buffers |
| `ctx.remote_write(ops, &sources)` | Handler buffers | Client write attachments |
| `remote_read_all()` | Entire client read space | Handler buffers |
| `ctx.remote_write_all(&sources)` | Entire handler source | Beginning of client write space |
| `ctx.remote_read_allocated()` | Entire client read space | Newly allocated handler buffers |

Set logical lengths before transferring. Allocations initially expose their size
class and reused bytes are not cleared. Streams accept unregistered buffers;
RDMA requires registration for the selected connection's device. Connection,
spaces, bounds, overflow, region/op limits and destination overlap are validated.
Zero-byte batches validate and then skip I/O. Only requested ranges are filled.

## Borrow or transfer buffer ownership

| Client builder | Source validation | Destinations | Cancellation contract |
|---|---|---|---|
| Safe `with_buffers(&sources, destinations)` | Original future confirms its `&[Buffer]` borrow | Owned `Vec<Buffer>` | Drop and forget are safe |
| Unsafe `with_buffers_unchecked(&sources, destinations)` | Direct msgid check under the caller's source contract | Owned `Vec<Buffer>` | Drop is permitted; do not forget started, unfinished calls |
| Unsafe `borrow_buffers(&sources, &mut destinations)` | Direct msgid check under the caller's source contract | Borrowed `&mut [Buffer]`; original handles stay in place | Drive every started call to completion; no drop, forget, task abortion, cancelling timeout or unwinding |

All builders execute through consuming `run` and use the same copy and transport
executor. They add no per-buffer
reference counting, allocation lease or access guard. Service methods keep their
ordinary result types, and buffered client handles are confined to `run`.

The safe client API borrows its source slice and owns its destination vector.
`Completed<R>::buffers` is `Option<Vec<Buffer>>`: `Some` contains every original
destination, while `None` means recovery was abandoned after
the cleanup grace. Custom error types and ordinary service declarations remain
unchanged:

```rust,ignore
let completed = client.with_buffers(&read, write)
    .run(async |rpc| rpc.copy(&ctx, &()).await).await;
let (result, buffers) = completed.into_parts();
if let Some(buffers) = buffers {
    let write = buffers;
    // Original destinations are available, including when result is Err.
}
let copied_bytes = result?;
```

Buffered client requests run only inside the builder's consuming `run` callback.
Its `rpc` argument is a temporary `&ClientWithBuffers`, the handle implementing
typed service methods and raw calls. Builders do not expose those request
methods, and the handle cannot be constructed, cloned or returned from the
callback. This makes the outer recovery step part of every buffered client call
scope.

Handlers read into owned destinations with
`ctx.own_buffers(local).remote_read(ops)`; `remote_read_all()` selects the full
remote source range. These return
`Completed<Result<()>, Vec<Buffer>>`, whose `buffers` field is
`Option<Vec<Buffer>>`, on both success and failure. `Some` always preserves the
entire original vector and its order; `None` is distinct from an empty vector.
`remote_read_allocated()` returns `Result<Vec<Buffer>>` for convenience allocation.
Source writes use ordinary safe references and return `Result<()>`:

```rust,ignore
ctx.remote_write(&ops, &sources).await?;
ctx.remote_write_all(&sources).await?;
```

The original source handles never move. Streams gather into owned request bytes
before sending; RDMA exports region metadata and asks the peer to READ it.
No background task retains a dereferenceable source snapshot. Cancellation and
forgetting need no local DMA settlement; an abandoned write does not establish
successful destination contents. See the
[source-write contract](safe-boundaries.md#safe-source-writes).

Cancelling an owned operation retains destinations used by locally posted READs
until those READs settle. With safe `with_buffers`, client sources stay in their
original safe borrow.
Only the calling future serves CPU copies and source-validation commands;
waiters carry a bounded command channel, never the source reference. Dropping
the call closes that channel; forgetting it prevents further positive validation.
Neither case waits for remote READ completion.
Forgotten futures may retain owned resources indefinitely. See the
[source recovery contract](safe-boundaries.md#application-read-source-validation-and-recovery).

Advanced users explicitly import the separate unsafe facade:

```rust,ignore
use ruapc_unsafe::{ClientBuffersExt, ContextBuffersExt};

// SAFETY: sources stay valid and immutable while requests are pending.
// No started method future or enclosing run is forgotten or leaked.
let completed = unsafe { client.with_buffers_unchecked(&read, write) }
    .run(async |rpc| rpc.copy(&ctx, &()).await).await;
let (result, recovered_write) = completed.into_parts();
```

This returns the same `Completed<R>` as the safe builder and uses the same owned
destination recovery grace. Ordinary cancellation remains supported, but
forgetting a started call violates the source contract. The unsafe path skips
the source future's confirmation, not the post-READ network probe. CPU copies
still require the original future to be polled. See
[caller-validated sources](safe-boundaries.md#caller-validated-sources).

Borrow destinations when the complete-the-future contract is suitable:

```rust,ignore
use ruapc_unsafe::{ClientBuffersExt, ContextBuffersExt};

// SAFETY: buffers remain valid and correctly excluded from other access;
// the operation is driven to completion, including its error cleanup.
let result = unsafe {
    client.borrow_buffers(&read, &mut write)
        .run(async |rpc| rpc.copy(&ctx, &()).await).await
};
// The original handles, addresses and lengths never moved or became empty.
let copied_bytes = result?;

unsafe { ctx.remote_read(&ops, &mut destinations).await?; }
ctx.remote_write(&ops, &sources).await?;
```

Unsafe borrowed reads return ordinary `Result<()>`; their `_all` variants also
accept references. The caller must keep destination allocations and registrations
alive and exclude all other access. `borrow_buffers` also selects direct source
validation under the caller's contract. Once polled, unsafe destination futures must complete: cancellation,
task abortion, forget or unwinding violates
the contract. There is no dynamic busy check or per-buffer keepalive mechanism
protecting a violated contract. See [the unsafe facade](../ruapc-unsafe/README.md)
and [safety boundaries](safe-boundaries.md#borrowed-and-owned-call-adapters).
`borrow_buffers` returns a `BorrowedBuffers` builder; its only execution entry
point is `run`, with the same scoped client handle used by owned calls.

## Copy a client's read space into its write space

The ordinary handler returns its application value without receipts or special
reply types, on every transport:

```rust
use ruapc::{Context, Result};

#[ruapc::service]
trait BlobService {
    async fn copy(&self, ctx: &Context, req: &()) -> Result<u64>;
}
struct Blob;
impl BlobService for Blob {
    async fn copy(&self, ctx: &Context, _req: &()) -> Result<u64> {
        let data = ctx.remote_read_allocated().await?;
        let total = data.iter().map(|buffer| buffer.len() as u64).sum();
        ctx.remote_write_all(&data).await?;
        Ok(total)
    }
}
```

Calls within one `run` scope are serialized. After cancelling an owned inner
call, the next call in that scope recovers its target; the outer `run` also
performs recovery before returning. Both use the same cleanup grace. Retrying
internal recovery preserves destination ownership. Once an owned scope
abandons recovery, later typed calls in that scope fail with `BuffersInUse`;
they cannot silently send empty attachments. There is no public `recover`
method or buffered request handle outside `run`. Source access ends with its
calling future without waiting for remote READs. None of these owned recovery paths waive the unsafe
borrowed caller's obligations.

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
The response and READ expiry sweeps can detect expiry after the nominal
deadline. Returning original buffers can require further cleanup time.

Deadline expiry records an error; it does not cancel the handler itself or
authorize early recovery of DMA buffers. `rdma.remote_memory.settle_grace_ms`
defaults to 100 ms. When an owned operation is waiting for recovery after its
request finishes or a READ fails, work still pending after this grace causes
connection closure and a return with `buffers: None`. The framework keeps the
posted allocations and reclaims them after actual settlement. This abandons
delivery of the entire original destination container; it does not return
partial vectors. An existing
application error is preserved. If the service responded successfully but the
local destination did not settle, the typed call returns `BufferRecoveryTimeout`.

Borrowed destination calls, including the hidden destination-storage bridge,
continue waiting for safe recovery on both `Ok` and `Err`. The runtime drains
actual completion/flush CQEs and
destroys the provider QP before completing connection teardown in either mode.
Cleanup does not restart the business deadline or permit another request
attempt. Owned return no longer waits for the entire background drain after
grace expiry, but expiry observation and task scheduling can delay the return;
neither a stalled provider call nor a stalled lock is interrupted. Permanent CQE
loss still
triggers the [teardown failure policy](qp-registry.md#closing-a-connection), even
if an owned call already returned without buffers; eventual reclamation is not
guaranteed after a fatal device failure.
A validated zero-byte batch performs no I/O or deadline check.

## Result validity

Local availability and successful transfer are separate. Failed reads leave
readable destinations after settlement, but their bytes are not valid results.
Client sources remain in a borrow held by the calling future. After its READs
complete, the reader obtains positive validation from that same future for safe
sources, or a direct msgid check for caller-validated sources. The unsafe caller
must prevent a forgotten call from leaving a valid msgid after source reuse.
Cancelling an owned call removes its waiter without waiting for remote READ
completion. See the
[source contract](safe-boundaries.md#application-read-source-validation-and-recovery).

Standalone planning and synchronous copies live in
[ruapc-memory](../ruapc-memory/README.md); runnable workloads are in the
[demo guide](../ruapc-demo/README.md#remote-readwrite).
