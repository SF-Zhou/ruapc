# QP routing and poller maintenance

Each CQ poll thread owns a `HashMap<u32, ConnState>` keyed by the provider's
local QP number. This connection map is separate from the verbs CQ's live
identity registry: the former routes completions and maintains flow state;
the latter prevents identity reuse while QPs or borrowed completions can
still matter. This page owns routing and poll-loop scheduling; see
[completion identities](wrid.md), [capacity](rdma-capacity.md), and
[connection establishment](rdma-connection.md) for the adjacent contracts.

## Completion routing

The poller looks up `completion.qp_num()` and checks the connection's immutable
sequence floor before passing the CQ-issued token to `QueuePair::complete`.
Only accepted completions update flow credits; the QP owns the
[identity and completion checks](wrid.md). Established routing requires a
hash lookup and floor comparison, without locking the CQ identity registry
or allocating an entry per completion.

The connection map retains its peak allocation rather than a history of every
QPN. Full hash-map scans can therefore scale with retained capacity after a
connection peak, even when few connections remain.

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
QP ownership handles cleanup. Normal drains and the poll after notification
arming use this same path. Established-QPN hits avoid the inbox mutex.

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
After `rdma.polling.poll_spin_us` (default 50 microseconds), the poller arms CQ
notifications, re-polls to close the event race, and sleeps on the completion
channel and wake pipe. Zero disables spinning.

While flushing, the poller refills an empty business FIFO so remaining SEND
credits can serve ingress messages in the same pass. Queued messages retain
priority over direct senders. Payload-transfer RPCs use the same queue. Queue
bounds and deadline admission are in
[pending business sends](rdma-capacity.md#pending-business-sends).

## Receive dispatch

The poll thread never parses RPC frames. One receive completion consumes one
flow credit regardless of how many frames it contains. Received buffers are
batched and handed to `rdma.polling.dispatch_workers` Tokio tasks (default 32)
that walk the frames and dispatch messages. The flush threshold is 256 receive
buffers; a CQ drain also flushes its remaining batch before returning.

Each worker has an unbounded Tokio mpsc queue shared by the pollers. Each
poller clone gets a home worker in round-robin order and returns there after
a spill. A batch goes to the first worker below 16 outstanding batches, scanning
from home; otherwise it goes to the least loaded worker below 32. If all are
at that threshold, a one-shot Tokio task handles the batch. These are routing
thresholds, not hard queue limits: concurrent pollers may observe the same
backlog before enqueueing. A stopped worker causes its queued-send attempt
to fail and the batch to be dropped.

Small received frame batches (up to 1 KiB total) are copied out so registered
receive buffers can be reused promptly; larger batches retain the zero-copy path.
This reduces receive-pool pressure without moving parsing onto the poller.

## Closing a connection

Closing a socket marks it failed, closes READ admission, records failure on
posted READ batches and wakes the poller. Ordinary RPC waiters are removed
eagerly; a low-level READ completion still waits for its original buffers.
An owning RPC adapter may abandon that completion and return without buffers
after cleanup grace. The QP ledger and target-restoration tasks continue to hold
the posted storage, independently of the business future; see
[owned recovery](safe-boundaries.md#borrowed-and-owned-call-adapters).
The poll thread closes the QP posting gate before requesting provider ERR.
The gate waits for synchronous SEND, READ and receive posting already in
progress and rejects later posts, even through retained socket handles. A
repost already in progress therefore precedes the ERR flush. Posting a receive
after that flush can otherwise leave it without a completion on providers such
as Soft-RoCE, preventing teardown.

Successful receive completions queued before closure still settle their
receive counts, but do not dispatch messages or replenish the receive ring.
Removal requires a successful provider ERR transition, settled receive/ACK
flow, empty pending sends, closed READ admission and no posted READs awaiting
completion. A poster paused after acquiring permits cannot submit through the
closed gate; it accounts for its unposted work when it resumes or is cancelled.
The poller then explicitly destroys the provider QP and retires its reservation
before removing the connection. Successfully completed unsignaled SEND tails
are released by destruction, without inventing CQEs for them. Retained socket
handles now refer to a closed shell and cannot delay destruction. See
[READ admission and failure](rdma-capacity.md#read-admission-and-failure)
for grace timing and permit recovery, and
[credit retirement](rdma-capacity.md#retirement-and-credit-reuse) for the later
empty-poll requirement.

If the provider rejects the ERR transition, the process aborts immediately,
matching failed CQ draining or QP destruction. An attempted transition is not a
successful flush. Once teardown starts, each connection has a fixed internal
30-second deadline for receiving the required completions. Repeated closure,
CQE progress and entry into shutdown do not restart it. A connection whose
completion accounting is already settled can still be destroyed; otherwise
expiry aborts the process without releasing unsettled buffers or substituting
destruction for the missing CQEs. This policy also applies when an owned call
already returned with missing buffers; moving recovery into the background does
not guarantee reclamation after permanent completion loss.

An ordinary provider error leaving the main loop enters shutdown cleanup,
which still requires every actual completion. Shutdown stops registration under
the inbox mutex, installs any already
published connections in the map, closes admission and requests ERR for all of
them. The poller continues consuming real completion/flush CQEs and maintaining
connections until each provider QP has been destroyed. It does not release the
map and defer teardown to the last external socket owner. Shutdown also has a
30-second total drain deadline and preserves earlier connection deadlines.
If CQ polling fails or the drain deadline expires with connections still pending,
the process aborts: a timeout or fabricated completion cannot satisfy the
recovery contract. These deadlines are checked by the poller; they detect an
empty CQ or missing CQEs, but cannot interrupt a provider call or lock that itself
never returns. ERR and failure recording do not
themselves release DMA memory; see
[destination ownership](safe-boundaries.md#completion-and-destination-ownership).

Both the poll loop and shutdown cleanup run inside a panic boundary. A panic
aborts before the poll-loop owner releases its connection registry; cleanup is
not retried against potentially poisoned state. This prevents a lost completion
thread from leaving retained sockets waiting indefinitely for destruction.

## Implementation and regression coverage

[Poller routing](../ruapc/src/rdma/poller/mod.rs),
[per-connection maintenance](../ruapc/src/rdma/poller/conn.rs),
[revocable QP ownership](../ruapc/src/rdma/poller/queue_pair.rs),
[flow control](../ruapc/src/rdma/poller/flow.rs) and
[dispatch](../ruapc/src/rdma/poller/dispatch.rs) own these paths.
Adjacent tests cover early receive publication, stale routing, bounded drains,
dirty-list maintenance, minimum-window ACK behavior, shutdown, and subprocess
checks for fatal ERR/flush/panic failures. Verbs
identity tests are indexed in [WRIDs](wrid.md).
