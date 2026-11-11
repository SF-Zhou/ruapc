# Safety boundaries

`ruapc`, `ruapc-memory` and `ruapc-wire` forbid unsafe Rust. Allocation,
registration and verbs operations live in `ruapc-bufpool` and `ruapc-rdma`.
Their safe interfaces retain posted local buffers until completion evidence
or successful QP destruction permits reclamation. Application remote sources
use the separate [validation and recovery contract](#application-read-source-validation-and-recovery)
below. See [remote-memory usage](remote-memory.md) for the public API.

## Registration and byte access

`BufferPool` owns an immutable ordered list of `Arc<dyn MemoryRegistrar>` and
registers each new backing block in that order, outside the allocator lock.
Buffer key lookups and QPs use zero-based positions in this list. These positions
do not identify a pool or validate that a buffer belongs to a QP's protection
domain; the runtime constructs its RDMA device list and registrar list together
to keep their order aligned.

Custom `MemoryRegistrar` implementations have an unsafe contract: registration
grants no independent access to bytes owned by pool buffers, and deregistration
must finish before backing memory is released. The pool drops registrations
already created on partial failure and restores the allocation budget. It keeps
registrars alive until all backing blocks have been released; each block drops
its registrations before its memory.

TCP, WebSocket and HTTP/2 need no memory registration. Their attachment metadata
contains logical buffer lengths with zero addresses and keys; reverse RPCs
route source-copy requests to the original borrowing future and resolve write
targets through its waiter. RDMA exports
registered addresses and keys for the connection's actual NIC.

New backing memory is initialized before safe slices are exposed. Reused
buffers are not cleared. Mutable slices require exclusive access, and logical
buffer lengths determine which bytes a transfer exposes.

RDMA registration requires ordinary virtual addresses. QP creation requires
an RC QP and matching PD/CQ contexts, and rejects an external SRQ or raw context
pointer. Failure to destroy a QP or deregister an MR aborts the process before
device-visible memory can be freed.

## Completion and destination ownership

Posted destinations remain owned until all posted READs have completion
evidence or provider QP destruction succeeds. Timeout, cancellation, a dropped
result receiver and moving a QP to ERR do not authorize early release.
Forgetting an owner can retain resources indefinitely.

`ruapc-rdma` takes ownership of destination buffers, validates the concrete
READ plan and derives local addresses and keys itself. CQ-issued tokens are
the only completion authority; copied WRIDs and raw CQE metadata cannot
release buffers. See [WRID ownership](wrid.md) for token validation, posting
cursors and reclamation, and [capacity](rdma-capacity.md) for permits and CQ
retirement.

Inline CPU writes hold the `WriteTarget` mutex for the entire validated batch.
Under that same mutex, `WriteTarget` moves its entire destination vector into
an owned READ plan and keeps the plan's recovery receiver in its place; no
await separates the two. CPU copies and competing transfers that access bytes
fail with `BuffersInUse` while the READ owns the vector. Empty and all-zero
batches still validate the request, spaces and operations, then succeed
without checkout.

Only that recovery receiver returns the vector. It resolves after all posted
work settles or provider QP destruction succeeds, including when the transfer
failed or its handler was cancelled, and for a plan that never posted. Early
READ failure notification does not resolve it. The next access to the target
restores a settled vector.

The original RPC seals the target when it ends, with any result, and takes
whatever the target holds. A reverse handler holding an older target reference
cannot start another READ or CPU write afterwards, so the final destination
set can be returned even if such references still exist. A vector that a READ
still owns is not returned: by default the call drops the recovery receiver
and reports the set unavailable, and the QP releases the memory once the READ
settles.

With [`wait_for_write_buffers`](remote-memory.md#wait-for-write-destinations),
the call awaits that receiver instead. If destinations remain unavailable
after the configured grace period, it closes the connection (and its QP)
executing the READ and waits without a further deadline. Waiting uses only a
weak connection reference, so it does not itself prevent QP destruction. This
guarantees local availability on a normally awaited return, not successful
transfer contents or a remote completion lease for borrowed read sources. See
the [API guide](remote-memory.md#wait-for-write-destinations) for return paths
and cancellation behavior.

[Lent write destinations](remote-memory.md#lend-write-destinations) use the
same owned target. A call moves the buffers out of the caller's `&mut` slots
and leaves empty buffers that own no memory, so the caller's borrow never
covers bytes that a READ can write. Only buffers returned by the target or its
recovery receiver are moved back. A call polled to completion always awaits
that receiver, as above. A dropped call restores the slots only when that
needs no wait, and a forgotten call leaves them empty while retaining the
original buffers indefinitely.

## Large-payload source ownership

Large RDMA requests and responses use transport-owned serialized source
buffers and the built-in `RdmaService/offer` and `finish` RPCs.
Publishing an offer installs the
immutable source in its connection's transfer table in the same synchronous
step that accepts the offer into the send queue. A send failure before that
step is safe to retry. Once accepted, cancellation, request expiry and
connection failure cannot trigger an automatic retry or return the source.
Offer acknowledgements are supervised independently of the business call.
An acknowledgement failure, expiry or supervision shutdown closes the
connection without releasing its unconfirmed sources. These tasks hold only
weak socket references so they cannot delay provider QP destruction.

The receiving connection validates the advertised length, region bounds and
allocation budget before posting an owned READ plan. It calls `finish` only
after every READ has completed successfully, or after deciding to post none
for an unwanted request or response. The source accepts `finish` on the same
established RDMA connection.
It releases the corresponding
source and replies whether the message remains valid. Request validity checks
the pending waiter on that exact connection and its deadline. The receiver
dispatches only after a positive reply and another request-deadline check.

The public RDMA send API rejects application-generated requests for these two
reserved methods. Only transport code can send them, after source publication
or READ completion; a raw RPC call cannot fabricate completion evidence.

Transfer IDs are connection-scoped and never wrap. Receiver replay tracking
persists for the connection's lifetime, including after completed transfers
leave the active table. The two methods share ordinary RPC framing, routing,
response correlation and discovery. Their handlers reject non-RDMA transports;
incoming message dispatch is internal to the runtime. They share the same
`max_inflight_requests` cap as other RPCs; see
[admission and cleanup](rdma-capacity.md#large-payload-admission-and-cleanup)
for capacity requirements during nested calls.

Cancelling a receive task, abandoning a posted READ, or failing confirmation
moves the connection to ERR. Unconfirmed sources remain owned until provider
QP destruction succeeds; `RdmaSocket` destroys its QP before its transfer
table. Transfer cleanup deadlines close a connection rather than releasing
memory on a timer. A successful offer response acknowledges the receive task,
not READ completion. Peers must honor the advertised-memory protocol: a finish
RPC asserts that the peer has settled its READs and is not itself a local
CQ-issued completion token. Access to
registration keys is a trust boundary, even with this source-retention
protocol.

<a id="application-read-source-limitation"></a>

## Application read-source validation and recovery

`ReadSource<'a>` borrows an immutable `&'a [Buffer]` and caches its logical
layout. Constructing a source or attachment is infallible; the RPC checks read
and write attachment region limits before connection acquisition and source
publication. Local copy operations and transfer preparation validate their
layouts before accessing bytes, including empty batches. The client wrapper
borrows these buffers; ownership remains with the caller. The original request
future handles source operations while it is polled. The waiter stores only a
control-message route, never a source reference
or address that a background CPU reader can dereference.

`read_inline` accepts a request ID and logical operations, not peer-provided
addresses. It sends that operation to the original request future, which
synchronously gathers the bytes during its poll and replies with an owned byte
vector. No borrowed source escapes that poll into an independent task.

### Read first, then validate

For an RDMA `remote_read` that transfers bytes, the reader waits for all READ
completions, then calls `MemoryService/request_is_pending` on the source peer.
It returns `Ok(buffers)` only if that check succeeds with `true`. Empty or
all-zero-length batches validate the connected request, advertised space and
copy ranges, then return the local buffers without I/O or a source-validation
probe.

The validation handler routes the probe to the original request future. A
positive answer is produced only while that future is polled with its immutable
source borrow. Background waiter membership alone cannot answer positively.
Assuming no request-ID reuse during the READ and validation window, a positive
answer after READ completion proves that the original source remained immutable
through that READ. Later source reuse cannot change the completed copy. Each
completed READ batch needs its own subsequent probe; a previous answer cannot
validate later READs.

Source operations honor the earlier of the original request's deadline and the
reverse RPC's deadline, and reject expiry before accessing the source or
validating it. Connection failure and waiter removal prevent new route lookups;
already-routed operations still require the original future to be polled.
The validity proof still depends on the live source borrow, not the clock or
waiter membership alone. A positive probe is not a local CQ-issued completion
token or a remote DMA completion lease.

Request IDs use a wrapping `u64` counter local to each `State`'s waiter. The
protocol assumes that an ID is not reused while an earlier request or probe
with that ID can still affect the result. At one million ID allocations per
second, a full counter cycle takes about 585,000 years. Wraparound is therefore
treated as unreachable in practice, without a runtime exhaustion guard.
Deadlines further limit stale probes in normal operation, but do not themselves
prevent ID reuse. This is an engineering assumption, not an enforced
non-wrapping guarantee.

Planned follow-up: once Rust stabilizes `!Forget` with the required destruction
guarantees, make borrowing request futures non-forgettable and simplify
post-READ `msgid` validation to a direct check of the original source's waiter,
avoiding the extra handoff to the source future. The constraint must survive
public future wrappers and cover safe ways to skip cleanup. Every completion,
cancellation and error path must
synchronously revoke validation eligibility before releasing the source borrow,
with validation ordered against revocation. READ completion must still precede
the probe, and the same request-ID non-reuse assumption applies. This plan
concerns `msgid` validation; inline copies still require protected source access.
Until then, the future-driven protocol above remains required.

### Cancellation, forgetting and source reuse

The source borrow is **not a remote DMA completion lease**. Timeout,
cancellation or failure can end the original request before an already posted
remote READ finishes. Once no wrapper or future borrows the source, the caller
can mutate or release its buffers without waiting for that READ. For example,
while the backing storage remains registered:

| Step | Source peer | Reader |
|---|---|---|
| 1 | Original request future borrows the immutable source. | Posts a READ. |
| 2 | Cancels the request and reuses the source storage after its borrow ends. | READ may still be in flight. |
| 3 | Original future can no longer service a probe. | READ completes, possibly with bytes from reused storage. |
| 4 | Probe cannot return a new positive answer. | Returns an error instead of a successful read result. |

Forgetting a future with safe `mem::forget` can also end the caller's borrow
without running its cleanup. A stale waiter or control route does not grant
source access: the forgotten future is no longer polled, so it cannot perform
an inline copy or produce a new positive validation answer. Probe requests may
instead wait until their deadlines, and forgotten resources can remain leaked.
An answer produced before forgetting remains valid only for the READ batch that
had already completed before that probe.

An unsuccessful probe, including a probe RPC error, can return completed
destinations in `RemoteIoError`. Its `recovered_buffers()`,
`take_recovered_buffers()` and `into_parts()` expose their bytes without clearing
them. Recovery permits reuse, **not acceptance as a valid transfer result**.

This protocol assumes cooperating peers. It rejects invalid read results but
does not prevent a failed READ from fetching bytes written after source reuse
or isolate those bytes from the reader. The
[destination ownership guarantee](#completion-and-destination-ownership) still
applies on failure.

### `remote_write` and transport payloads

RDMA `remote_write` publishes an immutable server source in a reverse
`read_into_target` RPC. Its successful response follows the client's READ
completions; the server owns the source and borrows it into the reverse request while awaiting
that response.
This path does not use the `remote_read` pending probe. After reverse-RPC
failure or cancellation, source recovery still does not prove that remote
DMA has stopped. Recovered write destinations likewise carry no successful
transfer guarantee.

The [large-payload protocol](#large-payload-source-ownership) retains separate
serialized sources until finish or successful QP destruction. It does not
extend that source-retention contract to application read attachments or
`remote_write` sources.

## Validation

Logical spaces and immutable read sources live in
[`ruapc-memory`](../ruapc-memory/README.md). The application paths are implemented
in [client attachments](../ruapc/src/client/with_buffers.rs),
[write targets](../ruapc/src/remote_memory/write_target.rs) and
[RDMA transfers](../ruapc/src/rdma/rdma_socket/read.rs).

Run the checks in [CONTRIBUTING.md](../CONTRIBUTING.md). Relevant tests cover
registration rollback, overlap and overflow, partial posting, cancelled READs,
completion identity, exclusive write targets, large-payload framing and
confirmation, and buffer recovery. Compile-fail
rustdoc examples check that safe callers cannot construct completion proofs or
implement an unaudited registrar. Tests exercise these contracts; they do not
constitute a proof of soundness or a performance guarantee.
