# QP routing and poller maintenance

Each CQ poll thread owns a `HashMap<u32, ConnState>` keyed by the provider's
local QP number. This connection map is separate from the verbs CQ's live
identity registry: the former routes completions and maintains flow state;
the latter prevents identity reuse while QPs or borrowed completions can
still matter. See [WRID ownership](wrid.md) for identity and reclamation rules.

## Completion routing

The poller looks up `completion.qp_num()` and checks the connection's immutable
sequence floor before passing the CQ-issued token to `QueuePair::complete`.
The QP validates the CQ, QPN and type-selected identity before releasing
buffers. Only then does the poller update flow credits. Established routing
requires a hash lookup and floor comparison; it does not lock the CQ identity
registry or allocate an entry per completion.

CQ capacity does not bound registry entries or alter WRID width. The connection
map and CQ identity registry retain at most their peak live allocation, with
no history map indexed by every QPN ever seen. Full hash-map scans can still
scale with retained capacity after a connection peak.

## Initial-receive publication

A QP can receive a completion before its connection reaches the poller's map:
initial receives must be posted before the peer can send. Registration uses
the shared inbox mutex as a publication barrier:

1. Allocate receive buffers before taking the inbox mutex; verify the QP and
   reservation belong to this poller's CQ.
2. Hold the mutex across initial receive posting and inbox insertion, setting
   `has_incoming` before unlocking.
3. On a routing miss, take that same mutex, drain the inbox and retry routing.
   Bypass the hint because it may still be false while receives are posted.

A failed setup can leave no registered owner; its completion is discarded and
QP ownership handles cleanup. Both normal drains and the poll-after-arming
race check use the same routing path. Established-QPN hits avoid the inbox
mutex. Removing a connection never retires an identity still held by a QP.

## Bounded drains and maintenance

A drain processes at most 16 batches of 64 CQEs (1024 completions); a short
batch returns earlier. This bounds work before maintenance and shutdown
checks even under continuous traffic. A short nonempty batch is not an empty
CQ and cannot release retired [capacity reservations](rdma-capacity.md#retirement-and-credit-reuse).

CQE handling and registration add touched QPNs to a deduplicated dirty list.
The poller maintains those connections after draining, including completions
found after notification arming, so credit publication and pending sends do
not wait for a periodic scan. List membership belongs to the poll thread and
needs no shared atomic per CQE.

Every 100 ms a full pass checks keepalives, READ deadlines and teardown that
became ready without a CQE. Pending-send, activation and error wakeups also
request a full scan because the wake pipe does not identify a connection.
The shared wake hint is consumed before maintenance so a concurrent wake
requests another pass.

Only receive-buffer deficits remain dirty for timed recovery: retry every
100 microseconds while busy and use a 1 ms idle wait. Window-blocked sends
wait for a completion, wake or housekeeping pass and do not force spinning.
After the configured `poll_spin_us` window, the poller arms CQ notifications,
re-polls to close the event race and sleeps on the completion channel and
wake pipe. Zero disables the spin window.

## Receive dispatch

The poll thread never parses RPC frames. One receive completion consumes one
flow credit regardless of how many frames it contains. Received buffers are
batched and handed to `rdma.polling.dispatch_workers` Tokio tasks (default 32)
that walk the frames and dispatch messages.

Each worker has a Tokio mpsc queue shared by the pollers. Each poller clone
gets a home worker in round-robin order and returns to that home after a
spill. A batch goes to the first worker below 16 outstanding batches, scanning
from home; otherwise it goes to the least loaded worker below 32. If all are
at that threshold, a one-shot Tokio task handles the batch. These are routing
thresholds, not hard queue limits: concurrent pollers may observe the same
backlog before enqueueing. A stopped worker causes its queued-send attempt
to fail and the batch to be dropped.

Small received frame batches (up to 1 KiB total) are copied out so registered
receive buffers can be reused promptly; larger batches retain the zero-copy path.
This reduces receive-pool pressure without moving parsing onto the poller.

## Implementation and regression coverage

[Poller routing](../ruapc/src/rdma/poller/mod.rs),
[per-connection maintenance](../ruapc/src/rdma/poller/conn.rs),
[flow control](../ruapc/src/rdma/poller/flow.rs) and
[dispatch](../ruapc/src/rdma/poller/dispatch.rs) own these paths.
Their tests cover early receive publication, stale identity rejection,
bounded drains, dirty-list maintenance, minimum-window ACK behavior and
shutdown. The verbs tests additionally verify same numeric WRIDs on distinct
QPs, retained tokens after QPN reuse and partial setup rollback.

For setup and activation, see [RDMA connection establishment](rdma-connection.md).
For admission and shutdown ownership, see [RDMA capacity](rdma-capacity.md).
