# RDMA connection establishment

<a id="rdma-连接建立"></a>

RuaPC discovers devices and exchanges QP parameters through `_ruapc.rdma`
RPCs over TCP, then activates the connection through the RDMA data plane.
A peer is identified by its complete normalized group of bootstrap TCP
addresses. Each connection stripe chooses its own local and remote device,
port, and GID. Discovery caches, acquisition locks, path failures and stripes
belong to that full group, independently of which address answered a bootstrap
RPC. Different or overlapping groups are not automatically merged.

Callers declare all group addresses equivalent: they must reach one server
`State`, including shared prepare/commit/cancel lease records. Bootstrap does
not negotiate an instance ID to verify this. A later RPC or rollback can use
another group address. See [endpoint identity and strings](endpoints.md).

This page owns bootstrap, path selection, negotiation, peer maintenance, and
connection health probes.
See the [built-in method catalog](builtin-services.md#internal-rdma-bootstrap-_ruapcrdma),
[capacity budgets](rdma-capacity.md), and [poller routing](qp-registry.md) for
the adjacent contracts.

<a id="模块职责"></a>

## Implementation map

Paths are relative to the repository root.

| Module | Responsibility |
|---|---|
| `ruapc/src/rdma/rdma_service.rs` | Bootstrap methods, protocol version, connectable-device advertisements. |
| `ruapc/src/rdma/endpoint.rs` | Wire types, endpoint validation, directional negotiation, prepare-response validation. |
| `ruapc/src/rdma/rdma_socket_pool/placement.rs` | Candidate paths, static policy, load ranking, failure preferences. |
| `ruapc/src/rdma/rdma_socket_pool/connect.rs` | Discovery cache, path failover, stripe-set publication. |
| `ruapc/src/rdma/rdma_socket_pool/handshake.rs` | One outbound handshake and rollback until publication. |
| `ruapc/src/rdma/rdma_socket_pool/setup.rs` | Shared negotiation, poller/QP creation, endpoint generation, connection and registration. |
| `ruapc/src/rdma/rdma_socket_pool/accept.rs` | Acceptor prepare/commit/cancel, leases, expiry. |
| `ruapc/src/rdma/probe.rs` | Internal health RPC, remote READ validation, per-probe deadline. |
| `ruapc/src/rdma/rdma_socket_pool/probe.rs` | Periodic checks of individual connections, timeout counts, reconnect scheduling. |
| `ruapc/src/rdma/rdma_socket_pool/reconcile.rs` | Shared first-use, periodic and recovery replenishment, single-flight scheduling and backoff. |
| `ruapc/src/rdma/rdma_socket_pool/peer.rs` | Group state, connection identity checks and recovery intent synchronized with publication. |
| `ruapc/src/rdma/rdma_socket_pool/maintenance.rs` | Periodic dead-path pruning, idle-peer eviction, rebalancing and draining. |
| `ruapc-rdma/src/verbs/qp_connection.rs`, `ruapc-rdma/src/verbs/queue_pair.rs` | Verbs parameter validation and RESET → INIT → RTR → RTS transitions. |
| `ruapc/src/rdma/poller/conn.rs` | Activation SEND, first successful receive, completion failures. |

<a id="完整时序"></a>

## Establishment and activation

The first connection to a peer follows this sequence. Healthy stripes are
reused; replenishment and migration reuse the same single-stripe handshake.
Discovery must return bootstrap protocol version **2**.

```mermaid
sequenceDiagram
    participant I as Initiator pool
    participant IP as Initiator poller
    participant A as Acceptor bootstrap
    participant AP as Acceptor poller
    I->>A: TCP discover()
    A-->>I: version, devices/ports/GIDs, load, limits
    I->>I: Validate version and select paths
    loop Each initial stripe
        I->>I: Negotiate limits; create local QP and endpoint
        I->>A: TCP prepare_connection(attempt_id, endpoint, target, limits)
        A->>A: Validate; create QP and connect to RTS
        A->>AP: Post receives and register socket
        A->>A: Create Pending lease
        A-->>I: Endpoint, lease, actual limits
        I->>I: Hold rollback guard; validate response
        I->>I: Connect local QP to RTS
        I->>IP: Post receives and register socket
        I->>A: TCP commit_connection(lease)
        A->>A: Confirmed; start activation deadline
        A-->>I: Success
        I->>I: Retain EstablishedSocket and rollback guard
    end
    I->>I: Publish all initial stripes together
    I->>IP: Request activation and wake poller
    Note over I,IP: Acquire may return before activation completes
    IP->>AP: Empty RDMA SEND_WITH_IMM
    AP->>A: First successful receive completion
    A->>A: Confirmed → Active; retain idempotency record
```

RTS means the NIC accepted local configuration, not that the remote path is
reachable. TCP commit confirms initiator QP ownership; routing failures may
first appear in the activation SEND's completion. Publication and acquire do
not wait for activation. Either the activation packet or ordinary traffic can
provide the acceptor's first successful receive.

A failed handshake can try another candidate. Local QP connection failures
mark the peer's NIC pair failed for 30 seconds; selection prefers alternatives
but can fall back to marked paths when none remain. Advertisements are cached
for 30 seconds. Prepare RPC failure, invalid prepare responses, and path
enumeration failure invalidate that cache for subsequent discovery.

<a id="路径策略与维护"></a>

## Path policy and maintenance

These settings are under `SocketPoolConfig.rdma`. A context fixes its device
set at creation; use separate contexts for different device policies.

| `path` setting | Behavior |
|---|---|
| `device_filter` | A nonempty list restricts local devices to those names. |
| `device_exclude` | Excludes names, taking precedence over the filter. |
| `allow_down_ports` | Default `false`. Retains devices with DOWN ports for later recovery; a DOWN port itself remains ineligible. |
| `subnets` | A list of connectivity domains, each a list of CIDRs. A pair matches when each NIC address belongs to any CIDR in the same domain. |
| `subnet_policy` | Default `prefer`: prefer matching paths, otherwise fall back. `require`: reject nonmatching paths; an empty domain list matches nothing. |

Only the initiator applies connectivity-domain policy. After that filter,
selection prefers InfiniBand, then RoCE v2, then other RoCE, before comparing
load. It draws two remote NIC candidates and chooses the lighter one using
advertised connection count plus locally known healthy stripes. Among local
NICs able to reach that candidate, it chooses the lowest connection count,
including both inbound and outbound connections. These policies rank paths;
they do not establish reachability.

Ethernet paths require a matching GID type on both ends. RoCE v1 and RoCE v2
do not match each other; a NIC with no compatible local GID pair is skipped.

After the initial foreground stripes are published, the pool immediately
schedules background preconnection for that address group. The first request
does not wait for this work. Each remote NIC is a coverage target only if at
least one local device/port/GID pair passes the connection constraints,
including limit negotiation and a required connectivity domain. A remote NIC
without such a pair is skipped. Subnet `prefer`, link-class preferences and
load ranking choose paths within each target NIC; they do not exclude other
compatible NICs from coverage.

First-use preconnection, periodic replenishment and health-triggered recovery
share one queued or running reconciliation pass per group. It shares the peer's
connection lock with foreground acquisition and periodic maintenance. Discovery and each
connection action release the lock before rejoining its FIFO queue, so an
already waiting foreground acquisition can proceed before the next action.
Each action recomputes placement from published healthy connections after
reacquiring the lock, and holds it through handshake and publication. A
foreground acquisition can still wait for the current discovery or connection
action, including path failover; a handshake already in progress is not
preempted. The background pass continues toward its coverage targets even
when periodic maintenance is
disabled. Failed NICs enter backoff without reserving connection slots or
preventing other eligible NICs from being attempted. Shutdown cancels the task
and rolls back unpublished connections through the normal handshake cleanup.

Periodic maintenance starts on use and runs every `maintenance.interval_ms`
(default 5000), with 0.5–1.5× jitter. Zero disables maintenance retries and
rebalancing; first-use background preconnection and the independent
[health probes](#periodic-connection-probes) still run. An independent
task refreshes device properties every 15 seconds. Maintenance fails connections
on unusable local ports and prunes dead stripes. For peers with active stripes
or use within the last 60 seconds, and peers awaiting probe-triggered recovery,
it replenishes remote-NIC coverage first, then total connections:

| Setting | Default | Purpose |
|---|---:|---|
| `peers.min_connections_per_remote_nic` | 1 | Coverage target per compatible remote NIC; zero disables per-NIC preconnection. |
| `peers.connections_per_peer` | 1 | Initial foreground stripe count and total healthy-stripe target. |
| `peers.preconnect_max_per_peer` | 16 | Upper bound on active connections for replenishment/additional connections. |
| `maintenance.rebalance_threshold` | 2 | Required load improvement before migration. |
| `maintenance.drain_timeout_ms` | 10000 | Grace period before closing a replaced stripe. |

Coverage takes priority over extra connections to an already covered NIC.
At the active-connection cap, a missing NIC can replace a redundant connection;
the replacement is published before the old stripe drains. If the cap cannot
hold the configured per-NIC targets, coverage is distributed across eligible
NICs within that cap. Set the cap high enough to cover every desired NIC.

Failed replenishment backs off. Each maintenance pass considers one peer for
rebalancing. A migration establishes and publishes a replacement before moving
the old connection to draining. `State::rdma_path_report()` reports each NIC pair,
direction, QP, health, active/draining phase, and device connection counts;
servers access it through `Server::state()`. CQ fields are described in
[capacity introspection](rdma-capacity.md#introspection-and-implementation).
Implementations are `rdma_socket_pool/placement.rs`, `reconcile.rs`,
`maintenance.rs`, and `report.rs`.

## Periodic connection probes

Both clients and servers periodically probe each published outbound connection
and each activated inbound connection. A probe uses that exact RDMA socket;
it does not acquire another stripe or retry the request. The internal
`_ruapc.rdma/probe` RPC carries a nonce and eight bytes of immutable read space.
The receiver performs `Context::remote_read` on the same connection and returns
both the RPC nonce and the value read. Success requires both values to match,
including the normal post-READ source-request validation. Probes from both ends
therefore exercise RPC and RDMA READ in both directions.

These settings are under `SocketPoolConfig.rdma.probe` and apply locally;
the peers may configure different schedules and thresholds.

| Setting | Default | Purpose |
|---|---:|---|
| `interval_ms` | 60000 | Base delay before the first round and after each completed round, with 0.5–1.5× jitter; zero disables this side's probes. |
| `timeout_ms` | 1000 | Total budget for one probe's RPC and remote READ checks; must be nonzero when enabled. |
| `max_consecutive_timeouts` | 3 | Consecutive timed-out probes before closing the connection; must be nonzero when enabled. |

Each pool draws a new randomized delay for the first round and every later
round, spreading clients that start together. Rounds do not overlap or catch
up after a slow check. Connections are probed concurrently within a round,
so one unresponsive connection does not serialize checks of the others. A
connection has at most one probe in flight. Timeout counts belong to its process-unique `conn_id` and
never carry over to a replacement. `Timeout` and `RdmaReadTimeout` count toward
the threshold. Success clears the count; a non-timeout error also interrupts
the consecutive-timeout sequence, without establishing a successful health
check. In particular, `Overloaded` and local allocation failures do not count
as connection timeouts. Internal RPCs use ordinary handler admission, so load
can reject a probe or its reverse source-validation RPC.

Reaching the threshold marks that socket failed and immediately fails its
bound RPC waiters. The poller performs normal QP teardown. The initiator queues
background replenishment for the original address group under its connection
lock, preserving compatible-NIC coverage and the total connection target.
`peers.min_connections_per_remote_nic = 0` disables coverage, but does not
disable replacement of the configured `peers.connections_per_peer` target.
A failed reconnect remains pending and is retried under backoff by later
maintenance or probe rounds, including when `maintenance.interval_ms = 0`.

The acceptor does not have the initiator's bootstrap address group. When it
closes an inbound connection, the original initiator detects that closure
through its probes or transport errors and rebuilds the connection. It does
not establish a new peer identity from the inbound NIC address. If the
initiator disables probes, recovery follows ordinary maintenance and subsequent
acquisition instead; an idle peer may need another application call.

Timeouts accumulate across scheduled rounds, not three immediate attempts.
With defaults, persistent probe timeouts normally close a connection after
roughly three minutes. Existing verbs failures and the independent
`remote_memory.read_timeout_ms` can close it sooner. Probe failure never
replays an application RPC already accepted by the transport.

The probe timer retains only a weak `State` reference while sleeping. Pool
shutdown cancels in-flight probes and reconnect tasks. Probe timeout or
cancellation does not release posted READ memory or permits early; the usual
[completion and source-lifetime rules](safe-boundaries.md) still apply.

<a id="参数协商与验证"></a>

## Parameter negotiation and validation

Let `L` be the initiator's configured limits clamped to device capabilities,
and `R` the acceptor advertisement. From the initiator's perspective:

```text
send = min(L.max_send_wr, R.max_recv_wr)
recv = min(L.max_recv_wr, R.max_send_wr)
ring = min(L.recv_queue_len, R.recv_queue_len, send, recv)
message = min(L.max_msg_size, R.max_msg_size)
large_message = min(L.max_large_msg_size, R.max_large_msg_size)
```

`PrepareConnectionRequest.limits` carries these resolved values. The acceptor
negotiates again against current capabilities and returns its actual limits.
The initiator requires reversed send/receive limits and identical ring,
inline-message, and large-message limits. Capabilities changing after discovery
therefore fail and roll back setup rather than produce incompatible peers.

| Field | Validation and meaning |
|---|---|
| `recv_queue_len` | At least 2 and no greater than either negotiated WR direction. The derived SEND window is defined in [capacity](rdma-capacity.md#completion-credit-budget). |
| `max_msg_size` | Inline serialized-message limit and each posted receive-buffer size. Minimum 16 KiB; local default 256 KiB. |
| `max_large_msg_size` | Complete serialized-message limit, including metadata and framing. Local default 64 MiB, the maximum single pool allocation. Zero disables large payloads; otherwise it must cover `max_msg_size` and be at most 64 MiB. |
| `qp_num`, `psn` | 24 bits; QPN must be nonzero. PSN is randomized per QP and may be zero. |
| `port_num`, `gid_index` | Ports start at 1; the response must match the requested port and GID index. |
| `link_layer`, `lid`, `gid` | Link layers must match. IB uses a unicast LID and may omit GID; RoCE requires a nonzero unicast GID. |
| `active_mtu` | One of the verbs MTUs, 256–4096 bytes; use the smaller peer value. |
| `rd_atomic_cap` | 1–16; the smaller peer value configures both `max_rd_atomic` and `max_dest_rd_atomic`. |
| `traffic_class` | Chosen by the initiator and reused by the acceptor. |
| `attempt_id`, `accepted_connection_id` | Nonzero; the response attempt must match. Together they identify an accepted connection. |

`max_large_msg_size` is an optional named MessagePack field: missing defaults
to zero, and zero is omitted. An older peer omitting it negotiates inline-only
operation without changing bootstrap version 2. The local
[`max_inflight_large_bytes` budget](rdma-capacity.md#large-payload-admission-and-cleanup)
is not negotiated. SGE limits, P_Key index, selective signaling, and CQ settings
also remain local; CQs are shared by device shards.

Lease IDs identify lifecycle operations, not authentication credentials.
Bootstrap assumes a trusted control plane.

<a id="lease-状态与期限"></a>

## Lease states and deadlines

`peers.connect_lease_ms` defaults to 30000 ms and must be at least 15000 ms.
Preparation, activation, and Active idempotency retention each use this duration.

| State | Observed events | Expiry |
|---|---|---|
| `Pending` | Prepare complete | From prepare completion; close inactive connection. |
| `ReceiveObserved` | Successful receive before commit | Keep preparation deadline; close on expiry. |
| `Confirmed` | Commit before successful receive | From first commit; close if not activated. |
| `Active` | Commit and successful receive, in either order | From activation; forget only the idempotency record, leaving the connection open. |

Repeated commit/receive events do not extend deadlines. Commit is idempotent
while a valid lease or Active record remains; missing records, mismatched
identity, expiry, or closed sockets return errors. A matching cancel closes
the socket even during Active record retention; duplicate or unmatched cancels
do nothing. A live duplicate prepare attempt ID is rejected.

A separate sweeper runs every `clamp(connect_lease_ms / 4, 100ms, 1s)`.
Commit, first receive, and repeated prepare also check expiry. Every Active
expiry path only removes the record.

<a id="期限回滚与发布"></a>

## Deadlines, rollback, and publication

The overall connection deadline covers discovery, path attempts, and all
stripe handshakes. Bootstrap connect and response timeouts are each 5 seconds,
capped by the remaining deadline. Commit retries one response timeout if
the overall budget remains; this relies on commit's method-specific idempotency.

Once prepare returns a valid lease for the attempt, `BootstrapRollback` owns
remote cleanup and, after local registration, failure closure of that socket.
Handshake failure, cancellation, or an unpublished completed stripe drops the
guard: it marks the local socket failed and schedules TCP cancel through a
fresh cleanup context. The [poller](qp-registry.md#closing-a-connection) performs
the QP transition to ERR. If cancel fails, the supervisor has stopped, or a
lost prepare response prevents learning the lease, acceptor expiry reclaims
the incomplete connection.

Initial `connections_per_peer` stripes retain their guards until all handshakes
and publication health checks pass. Any failure rolls back the unpublished
set. Under the publication lock, each stripe disarms rollback and enters the
selectable active set. After unlocking, the pool requests activation and enables
health probes. Thus automatic activation cannot precede publication.
Migration likewise publishes a replacement before draining the old connection.

<a id="日志定位"></a>

## Diagnostics

With an `EnvFilter` tracing subscriber, use `RUST_LOG=ruapc::rdma=debug`.
Correlate bootstrap by `attempt_id`, then poller events by each side's `conn_id`
and `local_qp`. Connection IDs differ between peers; prepare's
`accepted_connection_id` belongs to the acceptor.

| Span or message | Useful evidence |
|---|---|
| `rdma_connect` | Peer, attempt, both device/port/GID selections, QPs, local connection ID. |
| `rdma.accept.prepare` | Same attempt ID, acceptor device, remote QP. |
| `RDMA local endpoint prepared` | Complete endpoint and negotiated config: MTU, PSN, READ cap, queue sizes. |
| `RDMA connection attempt failed` | `error_kind`, stage context, `elapsed_ms`; discovery errors include peer and stage. |
| `ibv_modify_qp failed` | RESET→INIT / INIT→RTR / RTR→RTS stage, target state, attribute mask, provider errno, connection parameters. |
| `RDMA connection confirmed; awaiting publication` | Control-plane confirmation only. |
| `RDMA ... published; activation requested` | Selectable connection; activation remains asynchronous. |
| `RDMA replacement discarded; previous stripe is no longer active` | Peer and old/new connection IDs; a concurrent failure removed the victim, so the unpublished replacement rolls back. |
| `RDMA accept lease expired before activation` | Attempt, connection, state, QP, path; distinguishes missing commit from missing receive. |
| `RDMA work completion failed; closing connection` | Connection, QP, path, WR ID, status, `vendor_err`; activation may expose reachability failure. |
| `RDMA connection probe failed` | Connection, NIC pair, error and consecutive-timeout count. Non-timeout errors interrupt the count. |
| `RDMA connection probe timeout threshold reached; closing connection` | Connection, NIC pair and timeout count that triggered closure. |
| `rdma_bootstrap_cancel` | Unpublished-connection rollback; failed peer cleanup falls back to lease expiry. |

Stage wrappers preserve `ErrorKind`; verbs errors retain their `RdmaError`
category, and insufficient CQ capacity reports `Overloaded`. Find the first
stage/completion failure before examining later closure and lease logs;
subsequent flush completions do not repeat the error. Completion identity and
release authority are documented in [WRIDs](wrid.md).
