# RDMA connection capacity

Connection admission reserves software completion credits before creating a
QP. It uses the CQ capacity returned by the provider, which may exceed the
requested `rdma.polling.device_cq_len`. CQ capacity does not change the fixed
62-bit WRID sequence or limit the number of QPN registry entries. Hardware
resources, registered buffers and completion credits still bound usable
connections.

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

## Large-payload admission and cleanup

`rdma.connection.max_msg_size` continues to size each posted receive buffer;
large payload support does not enlarge the receive ring. The negotiated
`max_large_msg_size` bounds a complete serialized message, including metadata
and the ordinary eight bytes of RDMA framing. It defaults to 64 MiB, the
maximum single pool allocation. Zero disables large messages; nonzero values
must cover the inline limit. Both peers take the smaller advertised limit,
and a peer omitting the field negotiates zero.

`rdma.connection.max_inflight_large_bytes` defaults to 128 MiB per connection
and applies independently to outgoing source storage and incoming destination
storage. It is local rather than negotiated, must be nonzero, and must cover
the configured large-message limit. Accounting uses allocation capacity,
including buffers retained after READ completion until their payload owner
is dropped. If a READ fails or its receive task is cancelled before recovering
the destination, its byte reservation remains quarantined until QP destruction;
failing a waiter does not return that budget. Serialization growth reserves its
replacement allocation while the previous large allocation is still charged, so a budget equal to the
message-size limit can reject a growing message before that limit is reached.
The default budget allows one maximum-size message to grow with both
allocations present. Payload READs use the existing per-connection and NIC READ
permits and bandwidth admission. Descriptors and confirmations consume normal
data SEND credits, so they do not add a separate CQ-credit class.

Each connection admits at most 128 active outgoing and 128 active incoming
transfers. Finish and finish-reply controls use a separate bounded queue of
256 messages; the poller services it before queued business messages.
Confirmation bypasses application handler admission so a handler waiting
for a large callback cannot block its own completion. Receiver replay
tracking retains compressed transfer-ID intervals for the connection's
lifetime; more than 128 disjoint intervals closes the connection.

An independent 30-second transfer cleanup deadline bounds retained offers and
READ/confirmation tasks, including when the business request has expired.
READs also remain subject to `rdma.remote_memory.read_timeout_ms`. Exceeding a
transfer deadline or failing a receiving transfer closes the connection;
unconfirmed sources remain held until QP destruction. A confirmed expired
request can release its serialized source while declining dispatch. See
[large-payload ownership](safe-boundaries.md#large-payload-source-ownership).

## CQ shards and admission

`rdma.polling.poll_threads_per_device` bounds CQ shards, each with one OS
poll thread. Shards start lazily and share the pool's dispatcher workers.
Admission tries shards in increasing reserved fraction, rotates equal loads,
and rechecks the budget before creating the QP. Failure to fit on one shard,
or to create another shard, does not prevent admission on another fitting
shard. Incoming and outgoing connections use the same path.

`ReservedQueuePair` keeps the QP before its reservation in field destruction
order, including setup errors and handoff to the socket. Removing a connection
from the poller's map does not release its reservation while another owner
retains the QP. Posting and ordinary completion dispatch never acquire the
CQ budget lock.

## Retirement and credit reuse

QP identity retirement and CQ capacity reclamation have different boundaries:

1. Successful provider QP destruction permits its identity lease to retire.
   A reused QPN receives a higher sequence floor, rejecting old CQEs.
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

`rdma.remote_memory.read_timeout_ms` defaults to 10000; zero disables it.
The poll thread checks READ deadlines during its 100 ms housekeeping sweep,
skipping QPs with all READ permits available. Timeout reports
`RdmaReadTimeout` and moves the QP to ERR. It does not release posted DMA
holds or return their permits. Normal removal requires closed READ admission,
all SQ permits returned and no pending READs, as well as settled flow state.
This also covers a posting task paused between its last health check and the
provider post.

Fatal poller shutdown closes admission and fails waiters on every incoming
and live socket before releasing any socket owner. A task that publishes a
READ after the shutdown sweep rechecks socket health before awaiting its
batch and repeats failure notification. Notification leaves the QP's records,
permits and destination memory intact.

When the final socket owner drops, it captures the QP's actual outstanding
READ count, destroys the QP, then returns those NIC permits. Unposted requests
and already processed completions are excluded. External socket owners delay
this recovery. A stopped CQ admits no new connections and cannot reuse its
remaining reservations without an empty-poll proof.

Destination ownership and the separate application remote-source lifetime limitation are
documented in [WRID ownership](wrid.md#read-ownership-and-source-lifetime).

## Introspection and implementation

`State::rdma_path_report().completion_queues` exposes actual CQ capacity,
reserved entries/connections, live `registered_qps`, `next_sequence_floor`
and fixed `sequence_bits = 62`. Reserved connections include setup and retired
QPs awaiting a drain. Registry counts include setup and externally retained
QPs, but exclude destroyed QPs whose leases have retired; these counts need
not match established paths.

The implementation and adjacent tests are in
[poller/budget.rs](../ruapc/src/rdma/poller/budget.rs),
[poller routing and shard selection](../ruapc/src/rdma/poller/mod.rs) and
[READ admission](../ruapc/src/rdma/rdma_socket/read.rs).
Tests cover concurrent admission, retirement races, short/full drain bounds,
shard fallback and cancelled permit waits. Routing and maintenance are
covered in [QP registry](qp-registry.md).
