# RDMA connection capacity

Connection admission reserves software completion credits before creating a
QP. It uses the CQ capacity returned by the provider, which may exceed the
requested `rdma.polling.device_cq_len`. This page owns resource budgets,
admission, and credit retirement. See [connection setup](rdma-connection.md)
for negotiated limits, [poller scheduling](qp-registry.md) for progress, and
[completion identities](wrid.md) for QPN/WRID reuse.

## Completion-credit budget

For each connection, define:

| Symbol | Resource |
|---|---|
| `R` | Negotiated receive-ring length (`recv_queue_len`). |
| `W` | Data SEND window: `max(1, R / 2)`, with integer division. |
| `A` | Standalone ACK limit: `max(2, W)`. |
| `K` | Per-connection READ admission: `max(1, negotiated max_send_wr / 2)`. |
| `H` | Per-local-NIC READ limit: `rdma.remote_memory.max_inflight_read_wrs`. |

A CQ admits the connection only if the resulting reservation satisfies:

```text
sum(R + W + A) + min(H, sum(K)) <= actual CQ capacity
```

With default `R=8`, `W=4`, `A=4`, `K=32`, `H=32`, this is `16*N + 32`
for `N > 0`. A CQ with actual capacity 65536 therefore admits at most 4094
such reservations. This is an arithmetic credit bound, not a measured limit
on hardware QPs or application throughput. Receive-ring memory alone costs
`R * max_msg_size` per connection.

Queue depths cannot bound unpolled CQEs: a provider can reuse a WQE before
software consumes its completion. The budget therefore follows software
credits, including error/flush CQEs from unsignaled SENDs:

- Data and standalone ACK credits return only after local completion and
  peer confirmation. Activation SENDs consume the same capped ACK credits.
- Receive replacement follows consumption of the previous receive CQE.
- Posted READ permits return after completion processing while the CQ runs.
- Each CQ reserves up to `H` READ credits because all per-NIC READs may
  concentrate on any one shard. `sum(K)` caps the demand of its connections.

A standalone ACK-of-ACK requires at least two received ACKs, including at
window one. DATA acknowledgments and keepalives may carry a smaller ACK
delta. This prevents an activation ACK from starting an endless pure-ACK
exchange on the minimum receive ring.

## Pending business sends

Each connection has 1024 ingress slots and a poller FIFO limited to another
1024 messages. The combined bound is **2048 reserved or accepted, unposted
messages**, including payload-transfer RPC requests and replies. Reserved ingress slots
count toward the channel limit. Moving a message into the FIFO frees an
ingress slot only while there is space in the bounded FIFO; repeated poller
maintenance cannot move an unlimited backlog out of the channel.

A batch being aggregated or synchronously posted still fits this bound: only
the poller refills the FIFO, after the preceding batch's posting returns.
Full queues wait within the request's absolute deadline and wake on connection
closure. Accepted messages are not checked for expiry at dequeue time. See
[poller scheduling](qp-registry.md#bounded-drains-and-maintenance) for FIFO
refill and priority.

This is a message-count limit on the send queue, not a process-memory limit.
Caller-owned serialization buffers waiting for admission are outside it.
Posted WRs use the separate SEND window. Large serialized sources retain their
byte budget independently of send-queue slots. Draining a queue slot does not
release that storage budget. DMA ownership follows
[safety boundaries](safe-boundaries.md#completion-and-destination-ownership).

## Large-payload admission and cleanup

The encoder serializes each message once into registered storage, growing it
as needed. Messages within `max_msg_size` keep the inline format; larger ones
use a [payload offer RPC](builtin-services.md#payload-transfer).
`max_msg_size` still sizes each posted receive buffer, so large messages do
not enlarge the receive ring. The complete serialized size includes metadata
and the twelve-byte frame and metadata-length prefix; negotiation and version
rules are in [connection limits](rdma-connection.md#parameter-negotiation-and-validation).

`rdma.connection.max_inflight_large_bytes` defaults to 128 MiB per connection
and applies independently to outgoing source storage and incoming destination
storage. It is local rather than negotiated, must be nonzero, and must cover
the configured large-message limit. Accounting uses allocation capacity,
including buffers retained after READ completion until their payload owner
is dropped. If a READ fails or its receive task is cancelled before recovering
the destination, its byte reservation remains quarantined until QP destruction;
failing a waiter does not return that budget. Growth reserves the replacement
allocation while the old one remains charged. A budget equal to the message
limit can therefore reject growth before that limit; the default allows one
maximum-size message with both allocations present. Payload READs use the
existing READ permits and bandwidth admission.

Each connection admits at most 128 active outgoing and 128 active incoming
transfers, plus at most 128 pending offer acknowledgements. An outgoing send
returns at queue acceptance; a supervised task handles its offer reply without
holding the sending handler's capacity. Its acknowledgement permit remains held
even if `finish` has already released the source. Offers start supervised receive
tasks and acknowledge them immediately; the incoming transfer limit covers those
tasks after their offer handlers return.

`RdmaService/offer` and `finish` use the same `max_inflight_requests` handler cap
as every other RPC, with ordinary router dispatch, deadline checks, panic
containment and per-method metrics. A finite cap must cover handlers still
running while they await nested or reverse calls, plus those calls and their
payload-transfer RPCs. Insufficient handler capacity rejects these requests
with `Overloaded`; transfer failure then follows the cleanup rules below.
The transfer, acknowledgement-task and byte limits above account for their own
resources and do not reserve additional handler capacity.
All payload RPCs and replies use the normal send queue and data SEND credits.

Receiver replay tracking retains compressed transfer-ID intervals for the
connection's lifetime; more than 128 disjoint intervals closes the connection.
The source keeps at most 256 finished-transfer results for replying to duplicate
finishes.

An independent 30-second transfer cleanup deadline bounds retained offers and
READ/confirmation tasks, including when the business request has expired.
READs also remain subject to `rdma.remote_memory.read_timeout_ms`. Exceeding a
transfer deadline or failing a receiving transfer closes the connection.
Source release and dispatch validity follow the
[large-payload ownership protocol](safe-boundaries.md#large-payload-source-ownership).

## CQ shards and admission

`rdma.polling.poll_threads_per_device` bounds CQ shards, each with one OS
poll thread. Shards start lazily and share the pool's dispatcher workers.
Admission tries shards in increasing reserved fraction, rotates equal loads,
and rechecks the budget before creating the QP. Failure to fit on one shard,
or to create another shard, does not prevent admission on another fitting
shard. Incoming and outgoing connections use the same path.

`ReservedQueuePair` destroys the provider QP before retiring its reservation,
including setup errors and handoff to the socket. Its posting gate permits
explicit destruction while other owners retain the closed socket. Retaining an
`Arc<RdmaSocket>` therefore does not delay normal QP destruction or reservation
retirement. Posting and ordinary completion dispatch never acquire the CQ
budget lock.

## Retirement and credit reuse

QP identity retirement and CQ capacity reclamation have different boundaries:

1. Successful provider QP destruction permits
   [identity retirement](wrid.md); it does not yet return CQ capacity.
2. The reservation guard marks its credits retired and wakes the poller.
   They remain charged because a provider may leave CQEs behind.
3. The poller takes a retirement snapshot **before polling**. Only a later
   poll returning zero releases the credits included in that snapshot.

Snapshots survive the 16-batch drain limit and short nonempty batches across
poll-loop rounds. A retirement occurring after a snapshot requires a new
snapshot and a subsequent empty poll. An earlier empty CQ or a retired QPN
alone cannot authorize capacity reuse: a stale CQE still occupies physical
CQ space even when it can no longer release memory.

## READ admission and failure

Every posted READ holds both a per-connection SQ permit (`K`) and a shared
per-local-NIC permit (`H`, default 32). The SQ permit is acquired first so a
congested connection does not reserve NIC capacity while waiting for its own
queue. Connection closure wakes blocked admission tasks without closing the
NIC semaphore used by other connections.

`rdma.remote_memory.read_timeout_ms` defaults to 10000. Its budget starts
before bandwidth admission and covers bandwidth, SQ/NIC permit waits and
completion. The request's remaining budget independently limits the same
stages; zero disables only the configured READ budget, not request deadlines.
Normal server RPC contexts always carry a request deadline. A live internal
READ with neither deadline has no time-based expiry; abandonment or another
recorded failure still starts cleanup.
Permit waits use the earlier absolute deadline, including before the first WR
is posted and between later WRs. Uncontended permit acquisition creates no
timer; the posting path checks expiry before handing a WR to the provider.

The poll thread checks posted READ deadlines and recorded failures during its
100 ms housekeeping sweep, skipping QPs with all READ permits available.
The core socket owns the pending registry and both deadlines; the verbs crate
provides weak observation/failure handles and retains completion authority.
The registry prunes on a bounded registration cadence, including when idle
sweeps are skipped. Temporary strong observations are dropped outside the
registry lock, so a final producer release cannot invoke a waker under it.
Only provider-accepted, unsettled WRs count: a batch enters the registry after
its first accepted post. A suffix waiting for admission does not itself prove
a connection fault, while the caller's deadline still bounds that wait.

`rdma.remote_memory.settle_grace_ms` defaults to 100. A deadline or the first
observed failure, including a dropped completion receiver, starts this cleanup
grace. On first observation, it uses the earliest already expired request/READ
deadline, or the observation time when neither deadline has expired. Zero
requests closure immediately on observation. If accepted work remains after the
grace, the poller closes the connection and requests ERR. Large-payload failures can close their connection
earlier under their separate
[source-ownership protocol](safe-boundaries.md#large-payload-source-ownership).
Closing an attached write target also gives an outstanding transfer this grace
before requesting connection teardown.

The first failure reason is preserved. Request expiry reports `Timeout`; the
independent READ deadline reports `RdmaReadTimeout` when it expires first.
Recording either error does not finish `ReadCompletion`: its `ReadOutcome`
returns the same original vector only after all posted READs and the unposted
suffix settle. A partial-posting failure cancels that suffix and waits for the
posted work. Before any WR is posted, failure returns the complete vector
without a DMA drain. No failure notification returns posted permits early.

Connection closure eagerly fails ordinary RPC waiters and records READ
failure, closes admission, and wakes the poller. A READ published around the
closure rechecks socket health before awaiting its batch. Normal teardown
closes the posting gate, requests provider ERR, consumes the real READ and
receive/ACK completion or flush CQEs, then explicitly destroys the QP.
Successfully completed unsignaled SEND tails can remain owned until that
destruction. The same drain runs during shutdown. A rejected ERR transition,
failed CQ draining, teardown deadline expiry or poller panic aborts the process
rather than fabricate completion evidence. See
[connection closing](qp-registry.md#closing-a-connection) for the lifecycle.

Owned RPC and remote-memory adapters may stop waiting at cleanup-grace expiry
and return without buffers. The QP ledger or supervised target-restoration task
continues holding posted allocations through settlement; abandoning the result
does not return READ permits or allocations early. Borrowed calls and hidden
storage bridges retain the strict recovery wait. See
[call adapters](safe-boundaries.md#borrowed-and-owned-call-adapters) for the
optional-buffer result and error semantics.

The grace does not bound CQ draining, borrowed return latency, or a provider call
or lock that itself never returns. An owned return with missing buffers also does
not disable the fatal teardown policy. Moving to ERR, a timer firing, or an empty
result channel cannot authorize memory reuse.
Normal READ permits return through completion processing. The final socket-drop
fallback still captures actual unpolled READs and returns their NIC permits
only after provider destruction, excluding unposted work and previously
processed completions. A stopped CQ admits no new connections and cannot reuse
remaining reservations without the same retirement-snapshot/empty-poll proof.

For destination ownership and the separate application source contract, see
[safety boundaries](safe-boundaries.md#completion-and-destination-ownership)
and [read validation and recovery](safe-boundaries.md#application-read-source-validation-and-recovery).

## Introspection and implementation

`State::rdma_path_report().completion_queues` exposes actual CQ capacity,
reserved entries/connections, live `registered_qps`, `next_sequence_floor`
and fixed `sequence_bits = 62`. Reserved connections include setup and retired
QPs awaiting a drain. Registry counts include setup and externally retained
QPs, but exclude destroyed QPs whose leases have retired; these counts need
not match established paths.

The implementation and adjacent tests are in
[poller/budget.rs](../ruapc/src/rdma/poller/budget.rs),
[poller routing and shard selection](../ruapc/src/rdma/poller/mod.rs),
[READ execution](../ruapc/src/rdma/rdma_socket/read.rs) and
[READ admission and credits](../ruapc/src/rdma/rdma_socket/read/credits.rs), with
[deadline tracking](../ruapc/src/rdma/rdma_socket/read/pending.rs) in the core.
Tests cover concurrent admission, retirement races, short/full drain bounds,
shard fallback and cancelled permit waits. Routing and maintenance are
covered in [QP registry](qp-registry.md).
