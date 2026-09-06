# WRID allocation and completion routing

The CQ owns the identifier space used to route completions back to QPs. Each
QP leases a slot at creation and keeps it until successful destruction. Reusing
a slot advances its sequence floor above every sequence allocated by its
previous occupants. This combines direct array routing with stale-completion
rejection, without a separate connection-generation field.

## CQ-sized layout

At CQ creation, the provider's actual positive `cqe` sets the route capacity
`C`. The CQ fixes `s = ceil(log2(C))` slot bits and `q = 62 - s` sequence bits
for its entire lifetime. Slot indices are `u32`; only slots `0..C` may be
allocated, even when the encoding has spare positions. The route table grows
on demand rather than allocating C entries at startup.

The provider reports `cqe` as a positive `c_int`, so `1 <= C <= 2^31 - 1`.
Consequently `0 <= s <= 31` and `31 <= q <= 62`; a capacity-one CQ needs no
slot bits.

```text
| type: 2 bits | CQ-local slot: s bits | per-direction sequence: q bits |
```

`CompletionQueue::{capacity, route_capacity, sequence_bits}` expose these
bounds. `Completion::{slot, sequence}` decodes through the token's originating
CQ. A raw WRID exposes only `raw()` and `get_type()` because decoding its slot
without the CQ layout is ambiguous. `CompletionRoute` precomputes the slot
prefix and masks, so posting still combines a prefix and sequence with OR.

The previous CQ-owned layout fixed 14 slot bits and 48 sequence bits, limiting
each CQ to 16384 QPs regardless of available CQ capacity. Before CQ-owned
leases, poller-assigned tags also had a separate 8-bit generation counter and
a permanently claimed tag bitmap. Both limits are removed. Completion lookup
remains a direct array index with a sequence-floor check; no QPN hash table or
per-WR routing allocation is introduced.

| Actual CQ capacity C | Slot bits s | Sequence bits q |
|---:|---:|---:|
| 16384 | 14 | 48 |
| 65536 | 16 | 46 |
| 262144 | 18 | 44 |
| 1048576 | 20 | 42 |
| 2147483647 | 31 | 31 |

Larger CQs trade sequence space for simultaneous route capacity. At one million
allocations per second in one direction, 46 bits last about 2.23 years, while
42 bits last about 50.9 days. The extreme 31-bit sequence space lasts only
about 35.8 minutes at that rate. These are fresh-slot capacity illustrations,
not lifetime guarantees: both failed posts and earlier occupants consume
sequence space. Exhaustion fails explicitly and retires a slot; it never wraps.
Use multiple CQ shards when more connection capacity and a longer sequence
range are both needed. A CQ's layout cannot change, including while old tokens
are retained; resizing and reinterpreting existing WRIDs is not supported.

CQE capacity is not a verbs-imposed limit on the number of associated QPs;
using C route slots is this library's admission policy. Hardware QP resources,
registered buffers and the core's completion-credit budget impose additional
limits. The provider may round the requested CQ size up; the actual value,
not the request, controls both routing and admission. See the
[rdma-core CQ creation contract](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_create_cq.3).

The type values remain `Recv`, `SendData`, `SendImm` and `Read`. RQ has its own
sequence stream; all SEND forms and READ share SQ's stream. On failed CQEs,
only `wr_id`, `status`, `qp_num` and `vendor_err` are guaranteed, so opcode
cannot recover direction or READ bookkeeping. See the
[rdma-core CQ polling contract](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_poll_cq.3).

WRIDs are local verbs metadata, not part of the RPC wire format.

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

1. The core indexes its connection array using `completion.slot()` and verifies
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
sequence fits in the CQ's sequence bits; exhaustion never resets a counter or reintroduces an
old identity. CQ identity additionally separates identical numeric routes from
different completion queues.

Selective SEND signaling retains RC SQ ordering. The existing SQ posting mutex
serializes sequence allocation with posting so a later completion cannot sweep
a still-unposted earlier ID. The signaling cadence uses the offset from this
QP's first sequence, so a data SEND at SQ offset zero is signaled independently
of prior occupants. Earlier READs, ACKs and failed posts in the same QP still
advance that shared SQ offset. Reclamation starts at the current route's floor,
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

## Connection maintenance at large route counts

CQE routing and inbox registration mark a connection dirty once per drain.
The poller maintains that compact list immediately after draining completions,
including the arm/re-poll race path, so credit publication and pending sends
do not wait for a periodic scan. The list and its membership bits belong only
to the poll thread; no per-CQE shared atomic or notification allocation is
needed. Removal and full scans clear or rebuild the list before slot reuse.

Every 100 ms a full pass handles keepalive deadlines, READ timeouts and
teardown that became ready without a CQE. Existing pending-send, activation
and error wakeups set a shared hint before writing the wake pipe. The poller
consumes that hint before maintenance, so a concurrent wake schedules another
pass. These undirected wakeups still require a full connection scan; they are
not claimed to be O(active). The normal CQE path no longer scans idle QPs.

Only receive-buffer deficits remain on the dirty list for timed recovery:
100 microseconds while busy, up to a 1 ms idle poll timeout. Window-blocked
pending sends wait for a CQE, a wake or periodic housekeeping; they do not
force busy spinning. `poll_spin_us = 0` retains its event-driven behavior.

An ACK-of-ACK delta independently triggers a send only after at least two
received ACKs, including when the send window is one. DATA acknowledgments and
keepalives can still carry a smaller pending ACK delta. An activation ACK
therefore cannot cause an unending pure-ACK exchange on the minimum receive
ring. DATA acknowledgment remains immediate at window one, and the ACK
admission limit used by the CQ budget is unchanged. The default ring/window
already used this two-ACK threshold.

## Bounded failures and buffer-slot collisions

Sequence allocation is bounded, under the SQ posting guard or with an atomic
update for RQ. The last valid sequence is `2^q - 1`; the next counter value is
the exhausted sentinel `2^q`. Concurrent allocation cannot increment beyond
the sentinel or wrap to zero.

| Failure | Behavior |
|---|---|
| `WorkRequestIdsExhausted` | The direction has no remaining sequence; no new WRID is issued. |
| `CompletionRoutesExhausted` | No reusable slot remains and all C slot positions have been issued. |
| `WorkRequestSlotsExhausted` | A SEND/RECV buffer's bounded tracking position is still occupied; posting fails without replacing that buffer. |
| `InvalidCompletion` | CQ, QPN, slot or sequence floor does not belong to the QP; ownership tables are not accessed. |

A lease whose next watermark exceeds `2^q - 1` permanently retires its slot
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

## CQ admission and sharding

For a connection, let R be the posted receive-ring length, W = max(1, R / 2)
its data SEND window, A = max(2, W) its standalone ACK limit, and
K = max(1, negotiated max_send_wr / 2) its per-QP READ admission limit. H is
the shared per-NIC `max_inflight_read_wrs`. Each CQ admits connections only if:

```text
sum(R + W + A) + min(H, sum(K)) <= actual CQ capacity
```

This replaces the old `2 * (max_send_wr + max_recv_wr)` reservation per QP.
With default R8/W4/A4/K32/H32, the budget is `16*N + 32` for N > 0, instead
of `256*N`. At actual capacity 65536, that supports 4094 connections instead
of 256. A provider that rounds the requested size up can admit more. The
registered receive ring is still R buffers per connection: reducing the CQ
budget does not eliminate memory or hardware QP limits.

This bound includes error and flush CQEs for unsignaled SENDs; selective
signaling does not discount the budget. Data and ACK credits are returned
only after local completion and peer confirmation. Every receive replacement
follows consumption of an earlier CQE. READ permits return after completion
processing. Thus each unpolled CQE still consumes a software credit, even if
the provider has already recycled its queue entry. H is reserved once per CQ,
up to its combined K, because all per-NIC READs could concentrate on that CQ.
Activation SENDs consume the same capped ACK credits.

`poll_threads_per_device` bounds CQ shards and their dedicated OS threads.
Shards start lazily and share the fixed dispatcher worker pool. Before QP
creation, setup tries shards in increasing reserved
fraction, rechecking admission on each; equal loads rotate fairly. A failed
admission on one shard does not prevent another from accepting the connection.
Both incoming and outgoing connections use this path. No CQ-wide lock or
counter is added to WR posting or normal CQE dispatch.

The reservation is stored with its QP in `ReservedQueuePair`, preserving
QP-before-reservation destruction order through setup errors, handoff and
socket teardown. Removing a poller entry does not release credits while a
socket owner can still submit work. On destruction, the guard marks its
budget retired and wakes the poller. The poller takes a retirement snapshot
**before polling**, then releases only that snapshot after observing an empty
CQ. Later retirements require a later drain. This also covers providers that
leave CQEs behind on QP destruction, such as the
[RXE userspace provider](https://github.com/linux-rdma/rdma-core/blob/master/providers/rxe/rxe.c)
and [kernel QP cleanup](https://github.com/torvalds/linux/blob/master/drivers/infiniband/sw/rxe/rxe_qp.c).

Connection failure closes per-QP READ admission and wakes tasks waiting for
SQ or NIC permits. Normal connection removal waits until all SQ READ permits
have returned, covering the race between the last state check and posting.
The NIC semaphore stays open for other connections. Poller shutdown instead
fails outstanding waiters and releases its connection entries while the QPs
retain any DMA holds until destruction. A stopped poller admits no new
connections; its remaining reservations cannot be reused without completion
evidence.

`rdma_path_report().completion_queues` exposes each shard's actual capacity,
reserved entries, reserved connections, route capacity and sequence width.
Reserved connections include setup and retired QPs awaiting a drain, so this
count can temporarily exceed the established paths count.

## Memory and capacity

The route allocator stores 8 bytes per issued slot and a 4-byte index per free
slot, excluding vector slack and headers. It grows with peak route occupancy;
retired sequence-exhausted slots also remain in the watermark table. It has
no eager allocation proportional to the full CQ capacity. CompletionRoute
stores precomputed encoding masks; QP buffer slots and READ batch ownership
remain separate costs. The poller's connection array grows to the highest
installed slot. Each QP's READ ownership map uses four lock shards rather
than scaling empty tables with the host CPU count. The periodic timeout sweep
skips QPs whose READ semaphore has all permits available; a concurrently
starting READ is checked on the next scheduled sweep.

## Validation and performance evidence

The current capacity redesign is validated in [RDMA capacity](rdma-capacity.md).
The measurements below are historical evidence for the earlier CQ-owned lease
change at `601f404`, before dynamic bit widths and completion-credit admission.

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

The earlier lease redesign's validation passed **521 tests, zero failures**,
with 13 ignored documentation examples. [Validation records](wrid-data/checks.json)
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

### Historical lease-design performance comparison

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
