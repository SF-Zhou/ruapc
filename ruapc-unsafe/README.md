# RuaPC unsafe borrowing

This optional facade adds direct source validation and borrowed destinations
to the same transport executor used by `ruapc`'s safe source references and
owned destinations. The core crate still forbids all unsafe code.
This crate depends on core, never the other way around; it works
with TCP, WebSocket, HTTP/2 and optional RDMA without business feature branches.
RDMA configuration remains visible only with the core's `rdma` feature.

```rust,ignore
use ruapc_unsafe::{ClientBuffersExt, ContextBuffersExt};

// SAFETY: read stays valid and immutable while requests remain pending.
// No started method future or enclosing run is forgotten or leaked.
let completed = unsafe { client.with_buffers_unchecked(&read, write) }
    .run(async |rpc| rpc.exchange(&ctx, &request).await)
    .await;
let (response, recovered_write) = completed.into_parts();
```

`with_buffers_unchecked` borrows sources and owns destinations. It returns the
same `OwnedBuffers` builder and `Completed<R>` result as safe
`Client::with_buffers`, while skipping the source future's confirmation of each
post-READ probe. Ordinary cancellation remains supported; forgetting a started
call does not. Source-only callers pass an empty destination vector.

Borrowed destinations use a stricter contract:

```rust,ignore
use ruapc_unsafe::{ClientBuffersExt, ContextBuffersExt};

// SAFETY: write stays alive and exclusive; this future is driven to completion,
// including failure cleanup. read stays valid and immutable throughout.
let response = unsafe {
    client.borrow_buffers(&read, &mut write)
        .run(async |rpc| rpc.exchange(&ctx, &request).await)
        .await
};

// In a handler: keep local destinations valid and exclusive until completion.
unsafe { ctx.remote_read(&ops, &mut local).await?; }
// Source-only writes are safe core methods and need no extension trait.
ctx.remote_write(&ops, &local).await?;
```

With `borrow_buffers`, sources use `&[Buffer]` and destinations `&mut [Buffer]`; use `slice::from_ref`
or `slice::from_mut` for a single buffer. Handles, pointers and lengths remain
in the caller. There are no per-buffer allocation owners, access counters,
leases or busy checks. Only destination descriptors use unsafe metadata
snapshots; sources remain real Rust references in the executing future.
`borrow_buffers` returns a `BorrowedBuffers` builder exposing consuming `run`.
Typed and raw requests are available only on the temporary `&ClientWithBuffers`
passed to that callback. It cannot be constructed, cloned or returned from the
scope, and has no public `recover` method.

`Context::remote_write(ops, sources)` and `remote_write_all(sources)` accept
`&[Buffer]` directly in `ruapc` and return ordinary `Result<()>`. Their futures
can be cancelled or dropped safely. For fully safe client sources, use
`client.with_buffers(&read, destinations).run(...)`, which borrows sources and
owns destinations; use an empty destination vector for source-only requests. Its
future answers validation commands while holding the original source borrow.

## Safety contract

Both unsafe client builders use direct msgid validation. Keep the sources and
registrations valid and the source bytes unchanged while published requests
remain pending. Do not forget or otherwise leak a started, unfinished method
future or its enclosing `run`: a leaked waiter can still validate a source
after its Rust borrow ends. Ordinary cancellation of `with_buffers_unchecked`
is permitted because it revokes the waiter before releasing the source borrow.
The [source contract](../docs/safe-boundaries.md#caller-validated-sources) owns
the complete validation and cancellation rules.

Once an operation using borrowed destinations has been polled, drive it to
completion. Do not drop or forget its future, abort its task, wrap it in a
cancelling timeout/select branch, or unwind through it. Keep destination
allocations and registrations alive and exclude all other CPU and DMA access.
This obligation covers both the enclosing `run` future and every method it
starts, including calls that also have source references. A `run` callback may
make several serialized calls: keep its destinations valid until the whole
scope has completed. Dropping an unpolled operation or an unused builder submits
no work. This destination contract remains strict even when all sources are
read-only.

Borrowed-destination calls returning `Ok` or `Err` wait for local DMA settlement and revoke
old request access. The scoped client retains its destination descriptors between
calls; the enclosing `run` or handler transfer drops them before caller reuse.
CPU source servicing still runs inside the future holding its references on
every transport. Direct validation skips only the local confirmation exchange;
the reader still probes the original msgid after READ completion. Source reuse
after cancellation does not wait for remote DMA: failed bytes are not a valid
transfer result. See the authoritative [safety contract](../docs/safe-boundaries.md).

For cancellation freedom with destinations, use `client.with_buffers(&read, write).run(...)`
or `ctx.own_buffers(...).remote_read(...)` from `ruapc`. They return recovered
destinations as `Some(Vec<Buffer>)` in `Completed<R>::buffers` outside either
result branch. Sources remain with the caller. If
recovery exceeds cleanup grace, they can return `None` while the framework
retains posted destinations and reclaims them after settlement. The same local
DMA retention applies when an owned future is cancelled.
All modes use the same copy validation, transport dispatch, CQE settlement and
QP teardown for local destinations. Borrowed calls do not use the owned early
return policy. The hidden core destination-storage bridge accepts already
established capabilities and likewise waits for recovery; it does not add
lifetime tracking or cancellation protection to an unsafe borrowed descriptor.
