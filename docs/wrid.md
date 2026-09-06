# WRID allocation and completion routing

The CQ owns the identifier space used to route completions back to QPs. Each
QP leases a slot at creation and keeps it until successful destruction. Reusing
a slot advances its sequence floor above every sequence allocated by its
previous occupants. This combines direct array routing with stale-completion
rejection, without a separate connection-generation field.

## Previous design and the ownership change

The previous WRID layout used 2 type bits, 22 opaque connection-tag bits and
40 per-direction sequence bits. The core poller divided the tag into a 14-bit
slot and an 8-bit generation. The poller allocated the tag, then assigned it
through `QueuePair::set_wr_tag`; the dependency's CQ permanently marked that
tag in a 512 KiB bitmap. Each poller slot retired after 256 generations.

That design supported direct array lookup and rejected old tags, but split
identifier ownership between the poller and the verbs layer. Connection churn
consumed generations independently of the number of work requests, while a
long-lived QP had only the separate 40-bit sequence budget. A poller reservation
also needed to manage tag identity before connection registration.

The replacement makes the verbs layer responsible for allocation, posting
identity and reuse. The poller reads the QP's immutable `CompletionRoute` and
installs its connection in that slot. Its reservation now accounts for CQ work
capacity; it does not allocate QP identity.

| Property | Previous design | CQ-owned routes |
|---|---|---|
| WRID layout | 2 type + 22 tag + 40 sequence | 2 type + 14 slot + 48 sequence |
| Slot owner | Core poller | Completion queue |
| QP identity setup | Caller assigns a tag | QP creation acquires a route lease |
| Reuse protection | 8-bit generation plus permanent tag bitmap | Sequence watermark preserved across QPs |
| Slot lifetime limit | 256 generations | Exhaustion of the 48-bit sequence space |
| Live slot limit per CQ/poller | 16384 | 16384 |
| Completion lookup | Array index plus generation check | Array index plus sequence-floor check |

This is an API change: `set_wr_tag`, `get_tag` and the tag constants are removed.
`send_route()` and `recv_route()` expose the CQ-assigned route; `WRID::get_slot()`
returns an array index. WRID constructors remain value constructors and do not
confer authority to recover buffers. WRIDs are local verbs metadata and are not
part of the RPC message framing exchanged with peers.

## Bit layout and direction

```text
63     62 61                    48 47                              0
+--------+------------------------+--------------------------------+
| type:2 | CQ route slot:14       | per-direction sequence:48      |
+--------+------------------------+--------------------------------+
```

The type values remain `Recv`, `SendData`, `SendImm` and `Read`. A receive queue
has its own sequence stream. SEND, SEND-with-immediate and READ all consume the
send queue's stream. Two directions can use the same numeric sequence because
their types distinguish them; SEND and READ cannot reuse an SQ sequence.

Keeping the type in WRID is necessary for failed and flushed completions. The
rdma-core `ibv_poll_cq` contract guarantees only `wr_id`, `status`, `qp_num` and
`vendor_err` when status is not success. In particular, `opcode` cannot be used
to recover the direction or READ bookkeeping on that path.
See the [rdma-core `ibv_poll_cq(3)` manual](https://www.man7.org/linux/man-pages/man3/ibv_poll_cq.3.html).

The 14 slot bits retain the old 16384 simultaneous-slot ceiling. Actual usable
QP counts also depend on CQ capacity, queue-depth reservations and device
resources. Reclaiming the 8 generation bits expands the sequence range by
256 times, from 40 to 48 bits. At a continuous one million allocations per
second in the limiting direction, one fresh slot has roughly 8.9 years of
sequence space. This is a capacity illustration, not a lifetime guarantee:
failed posts consume sequences, and every lease consumes at least one step.

## Route leases and watermarks

`CompletionRoute` is a copyable descriptor with private fields:

```text
slot:           CQ-local array index
first_sequence: lowest sequence allowed for this occupant
```

Only the CQ allocator creates these descriptors. `RouteLease` is private to
the dependency and is not cloneable. It retains the CQ and holds independent
`send_next` and `recv_next` counters, initially set to `first_sequence`. The SQ
counter lives inside the existing posting mutex: `lock_send` returns a sequence
guard held through ownership registration, provider posting and any rollback.
The RQ counter uses a bounded atomic update. The CQ allocator stores a watermark
per slot and a free list behind a separate mutex that is used only to acquire
and return leases.

A returned slot records:

```text
next_floor = max(send_next, recv_next, first_sequence + 1)
```

The counters hold the next sequence to allocate, so this floor is strictly
greater than every sequence already issued. Taking the maximum of SQ and RQ
is essential when both directions share a CQ. They start together and advance
independently; their allocations are not added together.

For example, QP A leases slot 7 with floor 100. It allocates SQ sequences
100–109 and RQ sequences 100–139. SQ 109 fails to post. On destruction its
next counters are 110 and 140, so slot 7 returns with floor 140. QP B can then
use SQ 140 and RQ 140. Every completion from A has a sequence below 140 and is
rejected, even if the provider assigns B the same QP number. If B never
allocates any work request, its destruction still advances the next floor to
141, giving the next lease a distinct identity.

Posting failure rolls back buffer or READ-batch registration, never the
sequence counter. Attempting to reclaim a sequence would require coordinating
all concurrent allocators and would make identity depend on provider failure
handling. Gaps are permitted throughout the ownership tables.

## Creation, destruction and completion authority

`QueuePair::create` validates its resources and acquires route leases before
calling the provider to create a QP. Shared send and receive CQs use one lease.
With distinct CQs, each CQ leases a slot from its own namespace. Failure to
acquire the second lease or create the provider QP drops the acquired leases;
even these unused leases advance their floors.

The QP retains its leases independently of core poller registration. Removing
a connection from the poller's array does not make its route reusable if some
other owner still holds the QP. On final QP drop, successful provider
destruction precedes dropping leases and work-request memory. If destruction
fails, the process aborts before releasing resources still accessible to DMA.

Completion routing and completion authority have separate checks:

1. The core indexes its connection array using `wrid.get_slot()` and verifies
   `sequence >= route.first_sequence()` before changing flow accounting.
2. `CompletionQueue::poll_batch` lends a non-cloneable token from its private
   batch storage. The storage cannot change while that token is borrowed.
3. `QueuePair::complete` consumes the token and verifies its originating CQ,
   provider QP number, slot and sequence floor before consulting the owned
   buffer or READ tables.

A copied WRID, `CompletionRoute` or raw CQE is not a completion token. The
route's floor check deliberately has no upper bound: it rejects earlier slot
occupants, while CQ-issued evidence and the QP's private ownership tables
establish what can be reclaimed. A replacement QP cannot exist in the same
slot until the previous lease has been returned after QP destruction. Thus a
future occupant cannot produce a valid completion while the previous QP is
still available to accept it.

The same reasoning prevents ABA when both a slot and a provider QP number are
reused. Every CQ-local slot incarnation has a higher floor; every allocated
sequence fits in 48 bits; exhaustion never resets a counter or reintroduces an
old identity. CQ identity additionally separates identical numeric routes from
different completion queues.

Selective SEND signaling retains RC SQ ordering. The existing SQ posting mutex
serializes sequence allocation with posting so a later completion cannot sweep
a still-unposted earlier ID. The signaling cadence uses the offset from this
QP's first sequence, so its first SEND remains signaled independently of prior
occupants. Reclamation starts at the current route's floor,
avoiding a scan through the sequence space used by previous QPs in that slot.
READ destinations retain their independent batch ownership through completion
or successful QP destruction.

## Registration and early completions

Assigning identity during QP creation permits valid receive WRIDs before the
connection appears in the poller's array. Initial receives must be posted
before the peer can send. A completion can therefore arrive while registration
is still being published.

Core registration allocates receive buffers before taking the shared inbox
mutex. It then holds that mutex across initial receive posting and insertion
of the connection into the inbox, setting `has_incoming` before unlocking.
The poller checks the CQ origin of the QP and the reservation before starting
this transaction.

If completion lookup misses, the poller takes that same mutex, drains the
inbox and retries routing. It bypasses the `has_incoming` hint on this path:
the hint can still be false while the registrar is posting receives. The mutex
therefore acts as the publication barrier for an early CQE. A failed setup can
leave no registered owner, in which case the completion is discarded and QP
ownership handles cleanup. Both ordinary CQ draining and the poll-after-arming
event-race check use this dispatch path. Established-route hits do not acquire
the inbox mutex or the CQ allocator mutex.

## Bounded failures and buffer-slot collisions

Sequence allocation is bounded, under the SQ posting guard or with an atomic
update for RQ. The last valid sequence is `2^48 - 1`; the next counter value is
the exhausted sentinel `2^48`. Concurrent allocation cannot increment beyond
the sentinel or wrap to zero.

| Failure | Behavior |
|---|---|
| `WorkRequestIdsExhausted` | The direction has no remaining sequence; no new WRID is issued. |
| `CompletionRoutesExhausted` | No reusable slot remains and all 16384 slot positions have been issued. |
| `WorkRequestSlotsExhausted` | A SEND/RECV buffer's bounded tracking position is still occupied; posting fails without replacing that buffer. |
| `InvalidCompletion` | CQ, QPN, slot or sequence floor does not belong to the QP; ownership tables are not accessed. |

A lease whose next watermark exceeds `2^48 - 1` permanently retires its slot
on release. Another slot may be allocated if capacity remains. Exhaustion does
not recycle identifiers, and creating a new QP on the same exhausted slot is
not a reset mechanism.

`WrSlots` is distinct from the CQ route table. It stores a QP's SEND or RECV
buffers in a power-of-two array indexed by the sequence modulo capacity; each
occupied position carries the full sequence for exact matching. Failed posts
and bufferless work requests leave gaps, so a queue's in-flight count alone
cannot prove that a selected position is empty.

Insertion makes one claim attempt. If the position is occupied or changing,
it immediately returns the untouched buffer to the submission path, which
reports `WorkRequestSlotsExhausted`. It neither overwrites the existing owner
nor spins waiting for a completion. This matters when the poll thread itself
posts a replacement receive or a data send: waiting inside insertion could prevent
that same thread from processing the completion needed to free the position.
Taking a buffer requires an exact sequence match, so an old completion cannot
take a newer buffer that happens to use the same array position.

## Memory and capacity

The old claimed-tag bitmap allocated 512 KiB per CQ. The replacement's payload
is 8 bytes for every slot ever issued, plus 2 bytes for each reusable slot on
the free list, excluding vector capacity slack, vector headers and the mutex.
At 16384 issued slots these payloads are at most 128 KiB and 32 KiB respectively.
The CQ's allocator starts empty and grows dynamically.

In normal operation, slot reuse makes growth follow the peak number of
simultaneously leased routes, including QPs retained outside the poller. At
sequence exhaustion, retired slots remain in the watermark table, so further
QP creation can grow the table even without an increase in peak live QPs.
Connection churn is therefore bounded by consumed sequence space rather than
an independent 256-generation limit; the table is not an unbounded connection
history.

Each lease keeps an SQ posting mutex and counter, an atomic RQ counter, one
route descriptor and a CQ reference. On the measured x86-64 build, `QueuePair`
grows from 184 to 232 bytes (+48 bytes), and `CompletionQueue` from 40 to 80
bytes (+40 bytes), excluding their heap storage. `CompletionCursor` remains
8 bytes. See [the type-size measurements](wrid-data/type-sizes.json). The core
connection array grows to the highest installed route slot. The per-QP
`WrSlots` arrays and READ bookkeeping remain separate ownership costs.

## Validation and performance evidence

Tests cover cross-direction watermarks, empty-lease reuse, slot and sequence
exhaustion, concurrent allocation, retained completion tokens, provider QPN
reuse checks, CQ separation, WR-slot collisions and registration races. The
completion-token API also retains its compile-fail proof against fabrication.

Structurally, established completion routing remains an array lookup and a
sequence comparison. It adds no hash table, allocation, reference-count update
or allocator lock per CQE. SQ sequence allocation uses a plain increment inside
the posting mutex already required by selective signaling, eliminating the
previous additional atomic increment. RQ uses a bounded atomic increment.
End-to-end QPS must still be measured rather than inferred from those
properties. Reuse work is confined to QP construction and destruction, and
the registration mutex is only on setup and routing misses.

The WRID-specific measurements and validation records belong in
[wrid-data](wrid-data/). The older [safety-boundary report](safe-boundaries.md)
measured the preceding tag-bitmap implementation; its test count and throughput
tables are historical evidence for that change, not measurements of these
route leases.

The final workspace validation passed **521 tests, zero failures**, with 13
ignored documentation examples. [Validation records](wrid-data/checks.json)
include the source fingerprint, [test output](wrid-data/tests.log) and
[Clippy output](wrid-data/clippy.log):

```text
cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo fmt --all -- --check
```

The registration regression uses an actual device-issued receive flush CQE:
the registrar pauses after posting and before inbox publication; dispatch must
wait for publication and then settle the receive ledger and release setup
accounting. Other regression cases cover 10000 reuses of one route, an SQ
completion starting at sequence `2^47`, and SQ posting-guard exclusion through
the entire transaction, including poisoned-lock cleanup.

### Final performance comparison

The baseline is commit `642a7d9efd6c51c298a541b42e144ff96591ae50`. Both versions
use the same lockfile and benchmark harness, with 2560000 requests per
concurrent case. Nine pairs alternate execution order on physical mlx5_0
(200 Gb/s Ethernet, local loopback), with fixed worker/poller CPU placement
and NUMA node 0 memory. No builds or project tests run during sampling.
See [the reproduction notes](wrid-data/README.md) and
[all final echo samples](wrid-data/echo-final-long.json).

| Echo case | Baseline median | Final median | Change |
|---|---:|---:|---:|
| 1 endpoint, 64 tasks | 717.4 kQPS | 723.3 kQPS | +0.82% |
| 1 endpoint, 1024 tasks | 1366.6 kQPS | 1406.8 kQPS | +2.94% |
| 2 endpoints, 64 tasks | 678.1 kQPS | 680.5 kQPS | +0.35% |
| 2 endpoints, 1024 tasks | 1420.4 kQPS | 1451.1 kQPS | +2.16% |
| Serial, 16 bytes | 26.57 µs | 26.25 µs | -1.20% |
| Serial, 4096 bytes | 28.37 µs | 28.11 µs | -0.92% |

These final echo measurements show no throughput regression in the tested
cases. They establish performance for this placement and workload, not for
every device or traffic pattern. The raw data includes ranges and paired
ratios as well as the medians above.

The [initial short samples](wrid-data/echo.json) showed up to a 2.32% lower
throughput median. Repeating that same initial implementation with
[longer samples](wrid-data/echo-initial-long.json) produced changes from
−0.72% to +1.36%; the short-sample decrease did not persist. The final version
also removes the SQ atomic increment already covered by the posting mutex.
All stages are retained rather than selecting only favorable measurements.

Three paired [final remote-memory runs](wrid-data/remote-memory-final.json)
use 1000 warmups and 5000 measured operations per case, including complete
payload comparison. All data checks passed. Values below are microseconds
per operation; negative changes are faster.

| Operation | Baseline median | Final median | Change |
|---|---:|---:|---:|
| Read, 64 KiB | 63.13 | 62.02 | -1.76% |
| Write, 64 KiB | 64.50 | 64.94 | +0.68% |
| Read, 1 MiB | 183.66 | 168.22 | -8.41% |
| Write, 1 MiB | 182.93 | 183.09 | +0.09% |

Remote-memory sample ranges overlap. In particular, the 1 MiB results vary
enough that neither the initial nor final three-pair run establishes a
general bulk-transfer speedup; the initial run is also preserved in
[remote-memory.json](wrid-data/remote-memory.json).
