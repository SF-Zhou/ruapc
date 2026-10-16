# Safety boundaries

`ruapc` forbids unsafe Rust in its library, tests and benchmarks. Allocation,
registration and verbs operations live in `ruapc-bufpool` and `ruapc-rdma`.
Their safe interfaces retain posted local buffers until completion evidence
or successful QP destruction permits reclamation. Application remote sources
use the separate [validation and recovery contract](#application-read-source-validation-and-recovery)
below. See [remote-memory usage](remote-memory.md) for the public API.

## Registration and byte access

`DeviceSet<D>` assigns stable indices, with TCP at index zero. A safe `Device`
provides an associated `MemoryRegistrar`; only the registrar receives backing
memory. Custom `MemoryRegistrar` and `Devices` implementations have unsafe
contracts: registration grants no independent access to bytes owned by pool
buffers, and deregistration must finish before backing memory is released.
Partial registration failure drops registrations already created.

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

`WriteTarget` removes its entire destination vector before handing it to an
owned READ plan. CPU copies and competing transfers fail with `BuffersInUse`
while the target is empty. Only buffers returned by the dependency can be
restored; failed or cancelled transfers can leave the target empty even after
the QP eventually recycles those buffers.

## Large-payload source ownership

Large RDMA requests and responses use transport-owned serialized source
buffers and a private offer/finish protocol. Publishing an offer installs the
immutable source in its connection's transfer table in the same synchronous
step that accepts the offer into the send queue. A send failure before that
step is safe to retry. Once accepted, cancellation, request expiry and
connection failure cannot trigger an automatic retry or return the source.

The receiving connection validates the advertised length, region bounds and
allocation budget before posting an owned READ plan. It sends finish only
after every READ has completed successfully, or after deciding to post none
for an unwanted request or response. The source accepts finish only through
that connection's private RDMA dispatcher. It releases the corresponding
source and replies whether the message remains valid. Request validity checks
the pending waiter on that exact connection and its deadline. The receiver
dispatches only after a positive reply and another request-deadline check.

Transfer IDs are connection-scoped and never wrap. Receiver replay tracking
persists for the connection's lifetime, including after completed transfers
leave the active table. Controls cannot be invoked through application RPCs,
HTTP endpoints or `State::handle_recv`.

Cancelling a receive task, abandoning a posted READ, or failing confirmation
moves the connection to ERR. Unconfirmed sources remain owned until provider
QP destruction succeeds; `RdmaSocket` destroys its QP before its transfer
table. Transfer cleanup deadlines close a connection rather than releasing
memory on a timer. Peers must honor the advertised-memory protocol; access to
registration keys is a trust boundary, even with this source-retention
protocol.

<a id="application-read-source-limitation"></a>

## Application read-source validation and recovery

An immutable `ReadSource` is shared by the client wrapper, pending requests and
inline reverse-RPC readers. This keeps memory alive during local CPU copies.
`read_inline` accepts a request ID and logical operations, not peer-provided
addresses.

### Read first, then validate

For an RDMA `remote_read` that transfers bytes, the reader waits for all READ
completions, then calls `_ruapc.memory/request_is_pending` on the source peer.
It returns `Ok(buffers)` only if that check succeeds with `true`. Empty or
all-zero-length operation batches return the local buffers without I/O or
validation.

Success depends on the original waiter retaining the immutable source and
its request ID not being reused during the operation. A positive check after
READ completion then proves that the source remained held throughout the
READ. Later source recovery cannot change the completed copy.

The probe checks waiter-table membership, not the deadline clock. An entry
can retain its source past its nominal deadline until the expiry sweep removes
it. Request IDs come from a `u64` counter; the proof assumes no ID reuse,
unlike the enforced [non-wrapping WRID sequence](wrid.md#why-the-sequence-does-not-wrap).

### Failure and buffer recovery

Read-source ownership is **not a remote DMA completion lease**. Timeout,
cancellation or failure can remove the source waiter before an already posted
remote READ finishes. Local owners can then release or recover the source
without waiting for that READ. For example, while the backing storage remains
registered:

| Step | Source peer | Reader |
|---|---|---|
| 1 | Original waiter holds the immutable source. | Posts a READ. |
| 2 | Cancels the request and reuses the released source storage. | READ may still be in flight. |
| 3 | Original waiter is absent. | READ completes, possibly with bytes from the reused storage. |
| 4 | Pending probe returns `false`. | Returns an error instead of a successful read result. |

An unsuccessful probe, including a probe RPC error, can return completed
destinations in `RemoteIoError`. Its `buffers()`, `take_buffers()` and
`into_parts()` expose their bytes without clearing them. Recovery permits
reuse, **not acceptance as a valid transfer result**.

This protocol assumes cooperating peers. It rejects invalid read results but
does not prevent a failed READ from fetching bytes written after source reuse
or isolate those bytes from the reader. The
[destination ownership guarantee](#completion-and-destination-ownership) still
applies on failure.

### `remote_write` and transport payloads

RDMA `remote_write` publishes an immutable server source in a reverse
`read_into_target` RPC. Its successful response follows the client's READ
completions; the server retains the source while awaiting that response.
This path does not use the `remote_read` pending probe. After reverse-RPC
failure or cancellation, source recovery still does not prove that remote
DMA has stopped. Recovered write destinations likewise carry no successful
transfer guarantee.

The [large-payload protocol](#large-payload-source-ownership) retains separate
serialized sources until finish or successful QP destruction. It does not
extend that source-retention contract to application read attachments or
`remote_write` sources.

## Validation

The application paths are implemented in [client attachments](../ruapc/src/client/with_buffers.rs),
[write targets](../ruapc/src/remote_memory/write_target.rs) and
[RDMA transfers](../ruapc/src/rdma/rdma_socket/read.rs).

Run the checks in [CONTRIBUTING.md](../CONTRIBUTING.md). Relevant tests cover
registration rollback, overlap and overflow, partial posting, cancelled READs,
completion identity, exclusive write targets, large-payload framing and
confirmation, and buffer recovery. Compile-fail
rustdoc examples check that safe callers cannot construct completion proofs or
implement an unaudited registrar. Tests exercise these contracts; they do not
constitute a proof of soundness or a performance guarantee.
