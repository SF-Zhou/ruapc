# Safety boundaries

`ruapc` forbids unsafe Rust in its library, tests and benchmarks. Allocation,
registration and verbs operations live in `ruapc-bufpool` and `ruapc-rdma`.
Their safe interfaces enforce ownership and completion evidence; a caller's
promise to keep a raw address alive is insufficient.

## Registration and byte access

`DeviceSet<D>` assigns stable indices, with TCP at index zero. A safe `Device`
provides an associated `MemoryRegistrar`; only the registrar receives backing
memory. Custom `MemoryRegistrar` and `Devices` implementations have unsafe
contracts: registration grants no independent access to bytes owned by pool
buffers, and deregistration must finish before backing memory is released.
Partial registration failure drops the registrations already created.

New backing memory is initialized before safe slices are exposed. Reused
buffers are not cleared. Mutable slices require exclusive access, and logical
buffer lengths determine which bytes a transfer exposes.

RDMA registration requires ordinary virtual addresses. QP creation requires
an RC QP and matching PD/CQ contexts, and rejects an external SRQ or raw context
pointer. Failure to destroy a QP or deregister an MR aborts the process before
device-visible memory can be freed.

## Completion and destination ownership

- `CompletionQueue::poll_batch` lends non-cloneable tokens from private storage.
  `QueuePair::complete` consumes a token and validates its CQ, QPN and immutable
  sequence floor. Raw CQE metadata cannot authorize buffer recovery.
- `QueuePair::prepare_reads` owns destination buffers, validates bounds and
  overlap across work requests, and resolves local addresses and keys itself.
  Its posting cursor submits each request once to the preparing QP.
- Dropping the cursor accounts for requests not posted. Posted destinations
  remain owned until all their completions arrive or QP destruction succeeds,
  even after timeout, cancellation or a dropped result receiver. Forgetting an
  owner can retain resources indefinitely.
- `WriteTarget` removes its entire destination vector before handing it to an
  owned READ plan. CPU copies and competing transfers fail with `BuffersInUse`
  while the target is empty. Only buffers returned by the dependency can be
  restored; failed or cancelled transfers can leave the target empty even
  after the QP eventually recycles those buffers.

See [completion identity](wrid.md) for QPN reuse and sequence exhaustion, and
[capacity](rdma-capacity.md) for READ permits and CQ retirement. Moving a QP to
ERR or failing a waiter does not by itself release DMA memory or posted permits.

## Read-source limitation

An immutable `ReadSource` is shared by the client wrapper, pending requests and
inline reverse-RPC readers. This keeps memory alive during local CPU copies.
`read_inline` accepts a request ID and logical operations, not peer-provided
addresses.

This ownership is **not a remote DMA completion lease**. After source-side
timeout or cancellation, the source cannot observe when the peer's already
posted one-sided READ finishes. The post-READ `request_is_pending` check rejects
expired results but does not delay source recovery until remote completion.
Destination ownership does not resolve this source-side limitation.

## Validation

Run the checks in [CONTRIBUTING.md](../CONTRIBUTING.md). Relevant tests cover
registration rollback, overlap and overflow, partial posting, cancelled READs,
completion identity, exclusive write targets and buffer recovery. Compile-fail
rustdoc examples check that safe callers cannot construct completion proofs or
implement an unaudited registrar. Tests exercise these contracts; they do not
constitute a proof of soundness or a performance guarantee.
