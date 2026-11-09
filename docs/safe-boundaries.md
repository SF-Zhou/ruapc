# Safety boundaries

`ruapc`, `ruapc-memory` and `ruapc-wire` forbid unsafe Rust. Allocation,
registration and verbs operations live in `ruapc-bufpool` and `ruapc-rdma`.
Their safe owned interfaces retain posted local buffers until completion evidence
or successful QP destruction permits reclamation. The separate `ruapc-unsafe`
leaf crate exposes explicitly unsafe reference APIs; their callers supply the
lifetime and access guarantees that owned buffers otherwise enforce. Application remote sources
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
resolve buffer storage through the original request's waiter. RDMA exports
registered addresses and keys for the connection's actual NIC.

New backing memory is initialized before safe slices are exposed. Reused
buffers are not cleared. Each non-Clone `Buffer` uniquely owns its buddy node or
slab chunk and retains the pool. Dropping it returns that allocation. Ordinary
allocation, slice access and release have no per-buffer shared control block,
access counter or busy check.

`WriteBuffer` is a non-Clone destination capability implemented in bufpool.
Safe `From<Buffer>` construction moves ownership into the capability; `into_owned`
returns the same Buffer. Its hidden unsafe borrowed constructor snapshots the
payload pointer, logical length, capacity and registration metadata without
moving the original handle, cloning its pool, allocating a shared owner or
retaining its allocation. `into_owned` returns a borrowed capability unchanged
as `Err`; raw metadata can never be converted into ownership.

Borrowed construction requires the original allocation and registrations to
remain valid for every consumer. A write capability excludes every other byte accessor. Copying raw metadata grants
no new permission. Dropping a borrowed capability releases no allocation and
proves no DMA completion. These obligations belong to the unsafe constructor's
caller, including across threads and after a consumer future is abandoned.
The public unsafe facade imposes the stricter complete-the-future contract
[below](#borrowed-and-owned-call-adapters).

RDMA registration requires ordinary virtual addresses. QP creation requires
an RC QP and matching PD/CQ contexts, and rejects an external SRQ or raw context
pointer. Failure to destroy a QP or deregister an MR aborts the process before
device-visible memory can be freed.

## Completion and destination ownership

Posted destinations remain owned until all posted READs have completion
evidence or provider QP destruction succeeds. Timeout, cancellation, a dropped
result receiver and moving a QP to ERR do not authorize early release.
`QueuePair::prepare_reads` accepts `Vec<WriteBuffer>` and returns a posting cursor
and `ReadCompletion`. Its `ReadOutcome` returns that original capability vector on
both success and failure, after posted work and the unposted suffix have
settled. `WriteBuffer::from(Buffer)` supplies owned storage; callers can recover
it with `into_owned` after completion. The same preparation and completion
types handle unsafe borrowed capabilities. A failure records a reason; it does
not publish an early low-level result without the buffers. Forgetting an owner
can retain resources indefinitely.

`ruapc-rdma` takes ownership of exclusive destination access, validates the concrete
READ plan and derives local addresses and keys itself. CQ-issued tokens are
the only completion authority; copied WRIDs and raw CQE metadata cannot
release buffers. See [WRID ownership](wrid.md) for token validation, posting
cursors and reclamation, and [capacity](rdma-capacity.md) for permits and CQ
retirement.

Inline CPU writes hold the `WriteTarget` mutex for the entire validated batch.
`WriteTarget` removes its entire destination storage vector before handing it to a
READ plan. CPU copies and competing transfers that access bytes fail
with `BuffersInUse` while the target is empty. Empty and all-zero batches still
validate the request, spaces and operations, then succeed without checkout.
Only the original storage returned by a settled READ can be restored.

Before a buffered RPC exposes its result, recovery closes the
target to new access. A retained target
handle cannot start another byte access after closure; recovery does not depend
on `Arc` uniqueness. An existing CPU copy finishes under the same mutex, while
an outstanding READ restores its buffers and wakes recovery. Failed transfers
return available buffers without establishing valid contents. Borrowed calls
wait for the original destination vector. Owned calls may instead abandon
delivery after the configured grace, returning `None` while the completion
machinery retains the original storage. If work does not settle within that
grace, the runtime closes its connection, drains real completion/flush CQEs and
destroys the provider QP; see
[READ admission and failure](rdma-capacity.md#read-admission-and-failure).

### Borrowed and owned call adapters

The safe core API `Client::with_buffers(&sources, destinations).run(...)`
borrows the source slice and moves the destination vector into the common
executor. It returns `Completed<R>` with `buffers: Option<Vec<Buffer>>` outside
the unchanged closure result. `Context::own_buffers` does the same for direct
read destinations. `Some` contains every original destination; `None` means the
owning adapter abandoned recovery after cleanup grace. Missing destinations
remain owned by completion machinery while local DMA is pending, and are
reclaimed after settlement rather than delivered later. Sources never leave
the caller. No per-buffer lifetime tracking is needed. Service methods retain
ordinary `Result<T, E>` declarations without RDMA feature branches.

Both client adapters expose only a consuming `run` builder. The callback receives
`&ClientWithBuffers`, the only handle implementing buffered typed and raw calls.
It has no public constructor, `Clone`, `Default`, `run` or `recover`, and its
reference cannot escape the callback. The hidden `with_buffer_storage` bridge
returns a `BufferScope` builder, re-exported by `ruapc-unsafe` as
`BorrowedBuffers`; it does not expose an execution handle directly. This keeps
scope recovery on every public buffered-client execution path.

The `ruapc-unsafe` leaf crate depends on core and bufpool. `ClientBuffersExt`
provides caller-validated sources with owned destinations through
`with_buffers_unchecked`, or with destination `&mut [Buffer]` attachments through
`borrow_buffers`. Both skip the source future's validation reply under the
[caller-validated source contract](#caller-validated-sources).
`ContextBuffersExt` borrows local read destinations through **unsafe** methods.
The original destination handles, pointers and lengths never move or become
empty. Their unsafe contract keeps
allocations and registrations alive and excludes every conflicting access until
local DMA settles. Once polled, these destination-borrowing futures must not be
dropped, forgotten, aborted, timed out or unwound. This includes the enclosing
borrowed `run` and each method it starts. Dropping an unused builder starts no
work. A borrowed `run` may make several serialized calls, all of which must
complete before the scope returns.

Every normal borrowed typed-method return, including `Err`, settles local
destination transfers and closes request access before exposing its
result. The hidden destination-storage bridge uses the same strict policy
because its capabilities can represent borrowed memory. Owned typed calls also revoke stale
access, but may return without recovered buffers after cleanup grace. If an
otherwise successful response still has an unsettled local destination, the
typed call returns `BufferRecoveryTimeout`; an existing application error is
preserved. The enclosing `run` does not rewrite an arbitrary closure result.
An owned scope that abandons attachments enters a terminal state: later typed
calls within that callback return `BuffersInUse` instead of issuing requests
without those buffers.
Cleanup uses the same target, completion and QP teardown machinery in both
modes. Recovered error bytes remain available but do not establish a successful
transfer. Safe source access follows
[read-then-validate](#application-read-source-validation-and-recovery),
which does not acknowledge remote READ completion.

Cancellation of locally posted DMA requires owned destinations. Dropping an
owned method removes its waiter but may leave local DMA in flight; posted READs
still own their destinations. The next call within an owned `run` and its final
recovery wait for a cancelled method's destinations, subject to the same grace
and missing-buffer result. A forgotten method may retain its call mutex and
owned destination storage indefinitely; recovery cannot bypass that gate.
Source references are never held by an independent runtime task. A forgotten
safe source future cannot service copies or positively validate another READ.
Caller-validated sources must satisfy their separate no-forgetting contract;
owning the destinations does not waive it.

Returning without owned buffers does not relax the
[fatal teardown policy](qp-registry.md#closing-a-connection), including the
30-second missing-CQE deadline. Safe sources do not make cancellation of an
unsafe destination borrow valid. See the
[API examples](remote-memory.md#borrow-or-transfer-buffer-ownership).

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

The calling client future retains the actual source `&[Buffer]` and its checked
layout. Each request attempt with a nonempty source has its own bounded
source-command channel; the waiter stores only a sender and the validation mode.
`read_inline` sends owned logical operations through that channel. The original
future gathers bytes synchronously under its safe
borrow and replies with an owned byte vector. No handler can obtain a source
pointer or independently access the allocation. Response delivery removes the
waiter and closes access before the calling scope recovers its destinations.

### Read first, then validate

After a nonempty RDMA READ completes, the reader calls
`MemoryService/request_is_pending` on the source peer. For safe sources, that
handler finds the original request's command channel and asks the source future
to validate it. The source future checks that the original msgid is still pending and replies
while it holds the original immutable borrow. Only a positive reply establishes
a successful read result. Empty batches validate inputs without a probe.

The source future's check is the validation point: this READ completed before
the check, and the source borrow still exists at the check. Later cancellation
or source reuse cannot change that completed copy. Request IDs must not be
reused, and channels must never be shared with a subsequent request. A previous
positive probe cannot validate a later READ.

This checks original waiter membership, not its deadline clock. The probe's own
RPC deadline bounds command delivery and reply waiting. A dropped source future
closes the channel; a forgotten one cannot service commands and the probe fails
by its deadline. Thus a leaked waiter alone cannot cause a false positive.
Request IDs come from a `u64` counter; the proof assumes no ID reuse, unlike the
enforced [non-wrapping WRID sequence](wrid.md#why-the-sequence-does-not-wrap).

Validation requires the original future to be polled. The existing reverse RPC
now hands a command to that future and receives its reply before sending the
network response. This adds local task scheduling and channel overhead, without
another network round trip. Executor load or a caller that stops polling can
delay validation until the probe times out; a live waiter alone cannot bypass
this confirmation. Stream source copies have the same polling dependency.

### Caller-validated sources

The unsafe facade's `with_buffers_unchecked(&sources, destinations)` and
`borrow_buffers(&sources, &mut destinations)` select direct waiter validation.
`request_is_pending` checks the original msgid without sending a validation
command or waking the source future. The check is performed for every probe:
neither a retained source handle nor an earlier positive reply authorizes a
later READ. The network probe still occurs after the reader's completed READ.

The caller must keep the original sources and registrations valid and the
source bytes unchanged while a published request remains pending. Do not
forget or otherwise leak a started, unfinished method future or its enclosing
`run` future: a leaked waiter can still pass direct validation after the Rust
borrow ends. This obligation covers every typed and raw call in the scope.
An unused builder or an unpolled future has not published a request.

With owned destinations, ordinary cancellation by dropping the future, task
abortion, a cancelling timeout or unwinding is permitted. Cancellation removes
the original waiter before releasing the borrow; source reuse needs no remote
DMA wait. Locally posted destinations remain owned by the completion machinery.
With borrowed destinations, the stricter complete-the-future contract still
applies. Choosing direct source validation does not relax destination recovery.

Only validation takes this fast path. CPU copies still run synchronously in the
original future through the same bounded channel on every transport. No source
pointer escapes to an independent task. Bufpool's hidden `ReadSources` capability
holds the real source slice and records the unsafe caller's validation contract;
core accepts that established capability, never a public boolean or an arbitrary
lifetime promise. This adds no allocation owner, access lease or counter to
`Buffer`.

### Failure and buffer recovery

A source borrow is **not a remote DMA completion lease**. Timeout,
cancellation or failure can remove the source waiter before an already posted
remote READ finishes. Dropping the call removes the original message ID and
closes its command receiver. CPU copies execute synchronously within that same
future, so none can outlive its borrow. No remote completion acknowledgement or
QP teardown is required. For example, while the backing storage remains registered:

| Step | Source peer | Reader |
|---|---|---|
| 1 | Original future holds an immutable source borrow. | Posts a READ. |
| 2 | Cancels the request and reuses the released source storage. | READ may still be in flight. |
| 3 | Original waiter is absent. | READ completes, possibly with bytes from the reused storage. |
| 4 | Pending probe fails. | Returns an error instead of a successful read result. |

An unsuccessful probe, including a probe RPC error, returns an ordinary error
after local destination access has settled. The caller's original destinations
remain readable without clearing their bytes. Availability permits reuse,
**not acceptance as a valid transfer result**.

This protocol assumes cooperating peers. It rejects invalid read results but
does not prevent a failed READ from fetching bytes written after source reuse
or isolate those bytes from the reader. The
[destination ownership guarantee](#completion-and-destination-ownership) still
applies on failure.

### Safe source writes

`Context::remote_write(ops, &[Buffer])` and `remote_write_all(&[Buffer])`
borrow their sources safely. The reference remains in the calling future;
no independent runtime task or waiter retains a dereferenceable source snapshot.
Streams synchronously gather the requested bytes into an owned RPC payload.
RDMA exports only address, key and length metadata for the reverse
`read_into_target` RPC. A successful response follows the client's READ
completions, while the calling future still holds the original source borrow.

This path does not use the `remote_read` pending probe. Drop or cancellation
removes its response waiter, and neither cancellation nor `mem::forget` leaves
background CPU access to the source. They need no local source DMA settlement,
but do not establish successful destination contents or prove remote DMA has
stopped. An abandoned future cannot return a successful write result. This
differs from client read attachments, whose bytes and validity can be queried
independently by another request handler.

The [large-payload protocol](#large-payload-source-ownership) retains separate
serialized sources until finish or successful QP destruction. It does not
extend that source-retention contract to application read attachments or
`remote_write` sources.

## Validation

Logical spaces and immutable read sources live in
[`ruapc-memory`](../ruapc-memory/README.md). The application paths are implemented
in [client attachments](../ruapc/src/client/with_buffers.rs),
[call adapters](../ruapc/src/client/buffers.rs),
[source command channels](../ruapc/src/remote_memory/read_source.rs),
[write targets](../ruapc/src/remote_memory/write_target.rs) and
[RDMA transfers](../ruapc/src/rdma/rdma_socket/read.rs).

Run the checks in [CONTRIBUTING.md](../CONTRIBUTING.md). Relevant tests cover
registration rollback, overlap and overflow, partial posting, cancelled READs,
completion identity, exclusive write targets, large-payload framing and
confirmation, and buffer recovery. Compile-fail
rustdoc examples check that safe callers cannot construct completion proofs or
implement an unaudited registrar, and that untracked borrowed capabilities
require an explicit unsafe call. Tests exercise these contracts; they do not
constitute a proof of soundness or a performance guarantee.
