# WRID allocation and completion ownership

A work completion is identified by `(CQ, hardware QPN, WRType, sequence)`.
The CQ issues the completion token, the CQE supplies the QPN, and the WRID
encodes the type and sequence. Numeric WRIDs may repeat across different QPs
or CQs; the full identity must not repeat.

## Fixed layout

```text
| per-direction QP sequence: bits 63..2 | type: bits 1..0 |
```

`WRID::{TYPE_BITS, SEQUENCE_BITS, MAX_SEQUENCE}` define 2 type bits and 62
sequence bits, independent of CQ capacity. Encoding is `(sequence << 2) | type`.
The four types are `Recv`, `SendData`, `SendImm` and `Read`. RQ has its own
sequence stream; data SEND, standalone ACK and READ share SQ's stream.
SQ and RQ may allocate the same sequence, so type remains part of the identity.
Error completions need not contain a valid opcode; WRID type selects the
ownership ledger even on that path. WRIDs are local verbs metadata and never
appear in the RPC wire format.

## Live QPN leases and the retired floor

Each CQ owns a registry of live QPNs and their immutable `first_sequence`,
plus one `retired_floor`, initially zero. `QueuePair::create` first creates
the provider QP to obtain its QPN, then leases that QPN before exposing the
QP or posting work. A duplicate live QPN is rejected. Shared send/receive
CQs use one lease; separate CQs lease the QPN independently.

The non-cloneable `IdentityLease` retains its CQ and owns independent
`send_next` and `recv_next` counters, starting at the leased floor. SQ
allocation holds the posting mutex through ownership registration, provider
posting and rollback; RQ uses a bounded atomic increment. Failed posts consume
a sequence. Neither path acquires the CQ registry mutex.

Only after successful provider destruction does lease retirement perform
both updates under the registry mutex:

```text
retired_floor = max(retired_floor, send_next, recv_next, first_sequence + 1)
remove this QPN from the live registry
```

The new floor exceeds every sequence issued by the retiring QP, including
failed posts. Even an unused lease advances it once. Retirement order cannot
lower the floor, and existing live QPs retain their original floors. Newly
registered QPs inherit the current floor, including when their QPN has never
been used before.

The registry keeps no historical QPN entries. Its allocation can retain the
peak live occupancy, so storage is O(peak live QPs), independent of cumulative
QP churn, CQ capacity and the largest QPN. Only creation, destruction and
explicit introspection take its mutex.

## Creation, destruction and completion authority

`CreatingQueuePair` owns the provider QP and any acquired leases during setup.
Partial setup failure destroys the provider QP before releasing acquired
leases. An occupied-QPN failure never removes the existing lease. This also
handles the interval in which a provider reuses a destroyed QPN before its old
Rust lease has retired.

Final QP destruction precedes release of identity leases, SEND/RECV buffers
and READ destinations. If provider destruction fails, the process aborts
before releasing memory still accessible to DMA. Removing a core poller entry
does not destroy a QP retained by another socket owner.

Memory reclamation requires all of the following:

1. `CompletionQueue::poll_batch` lends a non-cloneable `Completion` from
   private batch storage. Neither that storage nor the originating CQ can
   be destroyed while the token is borrowed.
2. `QueuePair::complete` uses WR type to select SQ or RQ and checks CQ
   identity, hardware QPN and the lease's immutable sequence floor.
3. The QP's private ownership tables identify the buffers or READ batch to
   settle. Core flow accounting runs only after these checks succeed.

A raw CQE, copied WRID or `CompletionIdentity` cannot authorize reclamation.
The floor check rejects earlier QPN incarnations; it intentionally has no
upper bound. CQ-issued evidence and ownership-table matching establish which
work completed. A future incarnation cannot obtain the QPN's lease while
this QP is still live.

## SEND/RECV slots and selective signaling

`WrSlots` stores owned SEND/RECV buffers in power-of-two arrays indexed by
sequence modulo capacity. Each occupied slot retains the full sequence.
Insertion makes one claim attempt; an occupied or changing slot returns the
untouched buffer with `WorkRequestSlotsExhausted`. It never overwrites a live
buffer or waits for a completion that the posting thread may itself need to
process. Removal requires an exact sequence match.

Selective SEND signaling relies on RC SQ ordering. The SQ posting mutex
keeps allocation and provider posting in the same order, so a later completion
cannot sweep a still-unposted earlier SEND. Reclamation starts at this QP's
immutable floor. Signaling cadence uses the SQ offset from that floor;
READs, ACKs and failed posts also advance the shared SQ counter. A send that
exhausts the data window is forced signaled to avoid stranded credits.
READ destinations have separate batch ownership and are not reclaimed by the
SEND-buffer sweep.

## Why the sequence does not wrap

The last valid sequence is `2^62 - 1`; `2^62` is an exhausted sentinel.
Allocation never wraps. Exhaustion returns `WorkRequestIdsExhausted`; an
exhausted retired floor also rejects future QP registrations on that CQ.
An empty registry does not reset the floor. Existing QPs retain their own
remaining sequence ranges.

A completion token may remain borrowed while other batches are polled.
For example, a later SQ completion can reclaim an earlier SEND buffer while
its token remains unused. Reusing that SEND's full identity would let the
retained token reclaim a new buffer before DMA finishes. Polling the CQ
empty, checking a free buffer slot or bounding outstanding WRs cannot revoke
such a token. Failed posts also consume sequence numbers without occupying
hardware queue capacity.

Safe identity reuse would need a protocol covering live WRs, unpolled CQEs
and borrowed tokens. No sequence-reset or automatic generation-switching
protocol is implemented.

## READ ownership and source lifetime

`QueuePair::prepare_reads` takes owned destination buffers and logical
buffer-index/offset/length descriptors. It validates bounds, registration,
scatter limits, overflow and destination overlap, then resolves local
addresses and keys internally. A non-cloneable `ReadPosting` cursor submits
each request at most once to the preparing QP. The QP records a private
`Arc<ReadBatch>` before the provider post; post failure removes that record.
Dropping the cursor accounts for unposted requests without releasing posted
memory.

Destinations remain owned through success/error/flush completions or
successful QP destruction. Dropping, cancelling or forgetting the waiting
future never authorizes early release. Failure notification can return to
the caller before ownership is recoverable; it does not return DMA buffers.
READ admission, timeouts and teardown are described in
[RDMA capacity](rdma-capacity.md#read-admission-and-failure).

This local destination guarantee is distinct from remote source lifetime.
`ReadSource` shares ownership with pending requests and CPU inline readers,
but is not a lease for unobservable one-sided READ completions. The server's
post-READ `_ruapc.memory/request_is_pending` check rejects results from expired
requests; it does not make source recovery wait for remote DMA. The same
limitation applies to server sources advertised by `remote_write`, which is
implemented as a client-side READ.

## Implementation and validation

- [WRID encoding](../ruapc-rdma/src/types/wrid.rs) defines the fixed layout.
- [CQ identities](../ruapc-rdma/src/verbs/completion_queue/identities.rs)
  implements live leases, retirement and bounded allocation.
- [QueuePair](../ruapc-rdma/src/verbs/queue_pair.rs) owns setup rollback,
  completion validation and provider-before-memory destruction.
- [READ plans](../ruapc-rdma/src/verbs/queue_pair/read.rs) and
  [buffer slots](../ruapc-rdma/src/verbs/wr_slots.rs) enforce buffer ownership.

Tests alongside these modules cover QPN reuse, retained tokens, separate CQs,
partial setup rollback, failed-post floors, exhaustion, overlap rejection and
slot collisions. The completion API includes a compile-fail test against token
fabrication. See [QP registry](qp-registry.md) for core routing and publication,
and [RDMA capacity](rdma-capacity.md) for the separate CQ credit lifetime.
