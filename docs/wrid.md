# WRID allocation and completion routing

Completion identity is the tuple `(CQ, hardware QPN, WRType, sequence)`.
The WRID carries only the type and sequence; the CQE already supplies the QPN,
and its non-cloneable completion token retains the originating CQ. Each QP
registers its hardware QPN with that CQ and holds the registration through
successful provider destruction. A CQ-wide retired sequence floor prevents
old completions from matching a reused QPN without storing historical QPNs.

## Fixed layout and scope

```text
| type: 2 bits | per-direction QP sequence: 62 bits |
```

`WRID::{SEQUENCE_BITS, MAX_SEQUENCE}` define the fixed bounds; `get_type()` and
`sequence()` decode without consulting a CQ layout. `Completion::{qp_num,
sequence}` exposes the QPN and sequence of an authentic CQ-issued token.
No WRID bits are reserved for routing slots or connection generations.

WRIDs need not be unique across QPs. Two live QPs may issue identical numeric
WRIDs because their QPNs distinguish their completions. The same QPN on
different CQs is also a different identity. Only reuse of the same QPN on the
same CQ requires a floor above all sequences allocated by its old occupant.

CQ capacity no longer changes the sequence width or limits the registry's
number of entries. Hardware QP resources, registered buffers and the core's
completion-credit budget still limit admission. `CompletionQueue::capacity()`
returns the provider's actual positive `cqe`, which is at most `2^31 - 1`.
The provider may round the requested size up; the returned capacity controls
completion-credit admission. See the
[rdma-core CQ creation contract](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_create_cq.3).

The type values are `Recv`, `SendData`, `SendImm` and `Read`. RQ has its own
sequence stream; all SEND forms and READ share SQ's stream. SQ and RQ can
allocate the same numeric sequence, so the type remains part of the identity.
On failed CQEs, only `wr_id`, `status`, `qp_num` and `vendor_err` are guaranteed,
so opcode cannot recover direction or READ bookkeeping. See the
[rdma-core CQ polling contract](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_poll_cq.3).

WRIDs are local verbs metadata, not part of the RPC wire format.

## Live QPN leases and the retired floor

`CompletionIdentity` is a copyable descriptor with private fields:

```text
qp_num:         provider-assigned hardware QPN
first_sequence: lowest sequence allowed for this QPN incarnation
```

Only the CQ registry creates these descriptors. The registry holds a map of
live QPNs to their immutable floors, plus one `retired_floor` for future
registrations. A new registration uses the current `retired_floor`, initially
zero. It rejects a QPN that is already leased in this CQ.

`IdentityLease` is private to the dependency and is not cloneable. It retains
the CQ and holds independent `send_next` and `recv_next` counters, initially
set to `first_sequence`. The SQ counter lives inside the existing posting
mutex: `lock_send` returns a sequence guard held through ownership registration,
provider posting and any rollback. The RQ counter uses a bounded atomic update.
Neither counter is shared with other QPs. The CQ registry mutex is used only
for registration, retirement and explicit introspection, never for per-WR
allocation or normal completion dispatch.

After successful provider destruction, returning a lease performs both steps
under the registry mutex:

```text
retired_floor = max(retired_floor, send_next, recv_next, first_sequence + 1)
remove this QPN from the live registry
```

The counters hold the next sequence to allocate, so the returned floor is
strictly greater than every sequence that lease issued. Taking the maximum
of SQ and RQ is essential when both directions share a CQ; their allocations
are not added together. Including the previous `retired_floor` makes retirement
order irrelevant. Even an unused lease advances at least once.

For example, QPs A and B register with floor 100 and different QPNs. A issues
SQ 100–109 and RQ 100–139, including a failed SQ 109 post. Its destruction
raises `retired_floor` to at least 140. B remains valid at its original floor
100 and can continue issuing work; completion validation never compares B
against the CQ's new retired floor. A later QP C starts at least at 140,
whether or not it reuses A's QPN. If it does, every old A completion is below
C's floor. B's eventual retirement cannot lower the CQ watermark.

The single retired floor is conservative for newly registered, unrelated
QPNs, which do not require distinct sequence ranges. It avoids both a global
per-WR allocator and a history map that grows with every QPN ever observed.
Each live QP keeps dense local SQ and RQ sequence streams irrespective of
other QPs' traffic.

Posting failure rolls back buffer or READ-batch registration, never the
sequence counter. Attempting to reclaim a sequence would require coordinating
all concurrent allocators and would make identity depend on provider failure
handling. Gaps are permitted throughout the ownership tables.

## Creation, destruction and completion authority

`QueuePair::create` validates its resources and creates the provider QP first
to obtain its hardware QPN. It registers that QPN before exposing the QP or
posting any work. Shared send and receive CQs use one lease. With distinct
CQs, each CQ registers that QPN independently and supplies its own floor.

`CreatingQueuePair` owns the provider QP and any acquired leases during setup.
If either registration fails, its destructor destroys the new provider QP
before releasing the successful registrations. Failure to register an occupied
QPN does not own or remove the old lease. This handles the narrow interval in
which the provider has recycled a destroyed QPN but the old Rust lease has
not yet retired. The new setup fails cleanly; it cannot inherit the old floor.

The QP retains its leases independently of core poller registration. Removing
a connection from the poller's QPN map does not make its identity reusable if
some other owner still holds the QP. On final QP drop, successful provider
destruction precedes dropping leases and work-request memory. If destruction
fails, the process aborts before releasing resources still accessible to DMA.

Completion routing and completion authority have separate checks:

1. Each core poller looks up `completion.qp_num()` in its CQ-local connection
   hash map and verifies the immutable identity's sequence floor before
   dispatching. Registration verifies that the QP belongs to that poller's CQ.
2. `CompletionQueue::poll_batch` lends a non-cloneable token from its private
   batch storage. The storage cannot change while that token is borrowed.
3. `QueuePair::complete` consumes the token, uses its WR type to select SQ or
   RQ, and verifies its originating CQ, hardware QPN and sequence floor before
   consulting the owned buffer or READ tables. The core changes flow accounting
   only after this validation succeeds.

A copied WRID, `CompletionIdentity` or raw CQE is not a completion token. The
identity's floor check deliberately has no upper bound: it rejects earlier
QPN occupants, while CQ-issued evidence and the QP's private ownership tables
establish what can be reclaimed. A replacement cannot obtain that QPN's
registration until the previous lease has retired after provider destruction.
It cannot produce a future-incarnation completion while the previous QP is
still available to accept one.

This prevents ABA when the provider reuses a QPN, including tokens retained
across destruction. The reused `(CQ, QPN)` starts above all its old allocations,
and sequence exhaustion never resets a counter or reintroduces an old identity.
CQ identity separates otherwise identical metadata from different queues;
the lease retains its CQ, and a borrowed completion token also prevents the
originating CQ from being destroyed while that token is live.

Selective SEND signaling retains RC SQ ordering. The existing SQ posting mutex
serializes sequence allocation with posting so a later completion cannot sweep
a still-unposted earlier ID. The signaling cadence uses the offset from this
QP's first sequence, so a data SEND at SQ offset zero is signaled independently
of prior occupants. Earlier READs, ACKs and failed posts in the same QP still
advance that shared SQ offset. Reclamation starts at the current QP's immutable
floor, avoiding a scan through the sequence space preceding its registration.
READ destinations retain their independent batch ownership through completion
or successful QP destruction.

## Registration and early completions

Assigning identity during QP creation permits valid receive WRIDs before the
connection appears in the poller's QPN map. Initial receives must be posted
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
event-race check use this dispatch path. Established-QPN hits do not acquire
the inbox mutex or the CQ registry mutex.

## Connection maintenance at large QP counts

CQE routing and inbox registration mark a connection dirty once per drain.
The poller maintains that compact list immediately after draining completions,
including the arm/re-poll race path, so credit publication and pending sends
do not wait for a periodic scan. The list stores hardware QPNs, and each
connection has a membership bit. Both belong only to the poll thread; no
per-CQE shared atomic or notification allocation is needed. Removal and full
scans clear or rebuild the list before QPN reuse.

Every 100 ms a full pass handles keepalive deadlines, READ timeouts and
teardown that became ready without a CQE. Existing pending-send, activation
and error wakeups set a shared hint before writing the wake pipe. The poller
consumes that hint before maintenance, so a concurrent wake schedules another
pass. These undirected wakeups still require a full connection scan. Such
hash-map passes can scale with retained capacity after a former connection
peak; they are not claimed to be O(active). The normal CQE path no longer
scans idle QPs.

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
update for RQ. The last valid sequence is `2^62 - 1`; the next counter value is
the exhausted sentinel `2^62`. Concurrent allocation cannot increment beyond
the sentinel or wrap to zero.

| Failure | Behavior |
|---|---|
| `WorkRequestIdsExhausted` | This direction has no remaining sequence, or the CQ's retired floor is exhausted during QP registration. |
| `CompletionIdentityInUse` | This CQ still has a lease for the provider QPN; creating the new QP rolls back without changing the old registration. |
| `WorkRequestSlotsExhausted` | A SEND/RECV buffer's bounded tracking position is still occupied; posting fails without replacing that buffer. |
| `InvalidCompletion` | The type-selected CQ, QPN or sequence floor does not belong to the QP; ownership tables are not accessed. |

When a retired lease advances the CQ floor to `2^62`, every future QP
registration on that CQ fails explicitly. Existing QPs retain their own
floors and may use the remaining sequences in their local streams. The CQ
never resets its floor, even after its live registry becomes empty. At one
million allocations per second, a fresh 62-bit stream spans about 146000 years;
this illustrates the width, not a lifetime guarantee. Failed posts consume
sequences, and other QPs' retirements can raise a new QP's starting floor.

`WrSlots` is distinct from the CQ QPN registry. It stores a QP's SEND or RECV
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
creation, setup tries shards in increasing reserved fraction, rechecking
admission on each; equal loads rotate fairly. A failed
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

The identity registry and completion-credit budget have different retirement
boundaries. Successful QP destruction followed by lease retirement allows
the QPN to register again, with its higher floor rejecting old CQEs. The CQ's
capacity reservation remains occupied until the snapshot-and-empty-poll
condition also holds. A stale CQE still uses physical CQ space even when its
old identity can no longer authorize completion processing.

Connection failure closes per-QP READ admission and wakes tasks waiting for
SQ or NIC permits. Normal connection removal waits until all SQ READ permits
have returned, covering the race between the last state check and posting.
The NIC semaphore stays open for other connections. Poller shutdown instead
fails outstanding waiters and releases its connection entries while the QPs
retain any DMA holds until destruction. A stopped poller admits no new
connections; its remaining reservations cannot be reused without completion
evidence.

`rdma_path_report().completion_queues` exposes each shard's actual capacity,
reserved entries, reserved connections, live `registered_qps`,
`next_sequence_floor` and the fixed `sequence_bits = 62`.
Reserved connections include setup and retired QPs awaiting a drain, so this
count can temporarily exceed the established paths count. The live registry
count includes setup and externally retained QPs, but excludes leases already
retired after destruction; it can therefore differ from reserved connections.

## Memory and capacity

The CQ registry stores one QPN-to-floor hash-map entry per live lease and one
`u64` retired floor. Removing a lease removes its entry; no historical QPN
tombstones or per-QPN retired watermarks accumulate. Hash-map storage can retain
capacity from a previous occupancy peak, so its space bound is O(peak live QPs),
independent of cumulative QP churn, CQ entry capacity and the largest numeric
QPN. The core poller's QPN map has the same peak-occupancy storage bound.
QP buffer slots and READ batch ownership remain separate costs.
Each QP's READ ownership map uses four lock shards rather
than scaling empty tables with the host CPU count. The periodic timeout sweep
skips QPs whose READ semaphore has all permits available; a concurrently
starting READ is checked on the next scheduled sweep.

## Validation and performance evidence

Current QPN-registry validation and measurements are recorded in
[QP registry](qp-registry.md). The registry regressions cover cross-direction
and failed-post watermarks, unused-lease reuse, occupied-QPN rejection,
unchanged live-QP floors, sequence exhaustion, sparse QPNs and registration
counts beyond the old slot bound. Completion tests cover retained tokens,
reused QPNs and CQ separation. WR-slot collisions, SQ posting order and the
early-completion registration barrier remain separate ownership regressions.
The completion-token API also retains its compile-fail proof against fabrication.

Established completion routing now performs a QPN hash lookup and an immutable
sequence-floor comparison. It does not take the CQ identity-registry mutex or
allocate a routing entry per CQE. SQ allocation uses a plain increment inside
the posting mutex already required by selective signaling; RQ uses a bounded
QP-local atomic increment. Only lease retirement changes the shared
retired floor. This preserves local dense sequences and bounded selective
sweeps, while replacing the prior route-slot array lookup. The hash lookup's
end-to-end cost must be measured rather than inferred from these properties.

### Historical dynamic-width implementation: 36682ae

[RDMA capacity](rdma-capacity.md) and [its raw evidence](rdma-capacity-data/)
describe commit `36682ae`, the baseline before the QPN registry. That version
encoded `2` type bits, `s = ceil(log2(actual_cqe))` CQ-local slot bits and
`62 - s` sequence bits. Depending on CQ capacity, its sequence width ranged
from 31 to 62 bits. It used a slot-indexed poller array and per-slot retired
watermarks. Those routing and width limits are historical; the completion-credit
budget, CQ sharding and active-connection maintenance described above continue
to apply to the QPN-registry implementation.

### Historical CQ-owned lease implementation: 601f404

The measurements below and [wrid-data](wrid-data/) belong to the earlier
`601f404` CQ-owned lease change, which used fixed 14-bit slots and 48-bit
sequences before the dynamic-width design. They are not measurements of
`36682ae` or the current QPN registry. The older
[safety-boundary report](safe-boundaries.md) measured the preceding tag-bitmap
implementation; its test count and throughput tables belong to that stage.

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
accounting. Other historical regression cases covered 10000 reuses of one
route, an SQ completion starting at sequence `2^47`, and SQ posting-guard
exclusion through the entire transaction, including poisoned-lock cleanup.

### Historical lease-design performance comparison

The baseline is commit `642a7d9efd6c51c298a541b42e144ff96591ae50`; the final
version is `601f404`. Both versions use the same lockfile and benchmark harness,
with 2560000 requests per
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
