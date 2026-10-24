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
business messages**, including large-payload offers. Reserved ingress slots
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
byte budget, and finish controls have their own queue below; neither budget
is released by draining a business queue slot. DMA ownership follows
[safety boundaries](safe-boundaries.md#completion-and-destination-ownership).

## Large-payload admission and cleanup

The encoder serializes each message once into registered storage, growing it
as needed. Messages within `max_msg_size` keep the inline format; larger ones
use a [payload offer](builtin-services.md#private-rdma-payload-controls).
`max_msg_size` still sizes each posted receive buffer, so large messages do
not enlarge the receive ring. The complete serialized size includes metadata
and the ordinary eight-byte RDMA frame prefix; negotiation and compatibility
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
transfers. Finish and finish-reply controls use a separate bounded queue of
256 messages; the poller services it before queued business messages. Offers
and finish/reply controls consume normal data SEND credits, not a separate
CQ-credit class. Confirmation bypasses application handler admission so a
handler waiting for a large callback cannot block its own completion. Receiver replay
tracking retains compressed transfer-ID intervals for the connection's
lifetime; more than 128 disjoint intervals closes the connection. The source
keeps at most 256 finished-transfer results for replying to duplicate finishes.

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

`ReservedQueuePair` drops the QP before its reservation, including setup errors
and handoff to the socket. Removing a connection from the poller's map does
not release its reservation while another owner retains the QP. Posting and
ordinary completion dispatch never acquire the CQ budget lock.

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
Normal server RPC contexts always carry a request deadline. An internal or
low-level READ with neither deadline has no automatic expiry and must rely on
completion or connection closure.
Permit waits use the earlier absolute deadline, including before the first WR
is posted and between later WRs. Uncontended permit acquisition creates no
timer; the posting path checks expiry before handing a WR to the provider.

The poll thread checks posted READ deadlines during its 100 ms housekeeping
sweep, skipping QPs with all READ permits available; completion waits do not
create per-operation timers. The core socket owns the pending registry and
both deadlines; the verbs crate provides weak observation/failure handles
and retains exclusive completion authority. The registry prunes on a bounded
registration cadence, including when idle sweeps are skipped. Temporary strong
observations are dropped and failures notified outside the registry lock, so
closing a result producer cannot invoke a waker under that lock.
Only provider-accepted, unsettled WRs count: a
batch is registered for the sweep after its first accepted post, so a suffix
still waiting for admission neither arms the READ watchdog nor requests
closure, while the caller's own deadline still bounds that wait. Request
expiry in application remote-memory operations reports `Timeout` without
itself closing the connection. Large-payload cancellation still closes its
connection under the separate
[source-ownership protocol](safe-boundaries.md#large-payload-source-ownership).
The independent READ watchdog remains armed after request expiry or receiver
cancellation: when accepted work exceeds that budget, the sweep moves the QP
to ERR. A waiting caller receives `RdmaReadTimeout` unless an earlier request
deadline applies. Admission expiry alone does not prove a connection failure.

Connection closure immediately fails READ waiters even with the watchdog
disabled or missing flush completions. Expiry and closure never release
posted DMA holds or return their permits. If no WR was posted, admission
failure can restore the complete destination vector; after partial posting,
the unposted suffix is cancelled and the QP retains the posted destinations.

The [poller](qp-registry.md#closing-a-connection) owns normal removal and fatal
shutdown. Shutdown closes all connection admission before releasing socket
owners. A READ published after the shutdown sweep rechecks socket health
before awaiting its batch and repeats failure notification; that notification
leaves posted permits and memory held.

When the final socket owner drops, it captures the QP's actual outstanding
READ count, destroys the QP, then returns those NIC permits. Unposted requests
and already processed completions are excluded. External socket owners delay
this recovery. A stopped CQ admits no new connections and cannot reuse its
remaining reservations without an empty-poll proof.

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
