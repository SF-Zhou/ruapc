# AGENTS.md

## Project

RuaPC provides TCP, WebSocket, HTTP and optional RDMA through one Rust RPC API.
Use [DESIGN.md](DESIGN.md) for crate/module ownership and
[the documentation index](docs/README.md) for the authoritative contract guides.

## Implementation constraints

- Use enum dispatch for transport selection (`Socket`, `SocketPool`,
  `HttpSocket`) and statically dispatched futures. Application service closures
  and device registration are explicit trait-object extension boundaries.
- `ruapc` forbids unsafe code. Allocation, registration and verbs operations
  belong in `ruapc-bufpool` / `ruapc-rdma`. Safe APIs must enforce memory
  ownership and completion evidence, not accept caller-supplied lifetime promises.
- `DeviceSet` owns TCP at index zero and assigns additional device indices.
  Safe `Device` wrappers expose an associated `MemoryRegistrar`; custom
  registrars and `Devices` collections must uphold their unsafe contracts.
- Keep metadata in named-field MessagePack. Add compatible fields with
  `#[serde(default)]` and `skip_serializing_if`. `UseMessagePack` selects the
  payload decoder only; default clients use MessagePack, otherwise JSON.
- TCP and HTTP/2 streams share framing in `msg/frame.rs`; TCP/WS/HTTP stream
  sends share connection binding and queue preparation in `sockets/channel.rs`.
  RDMA framing belongs in `rdma/frame.rs`, outside the poll loop.
- Each connection has a process-unique `conn_id`. Closing it eagerly fails its
  waiters; eviction must check identity so it cannot remove a replacement.
- An `Endpoint` is one transport plus a nonempty normalized group of equivalent
  server addresses. Cache and coalesce by the complete group; never merge
  different or overlapping groups. Callers guarantee shared server `State`
  and RDMA lease records across all group addresses.
- Propagate deadlines and cap nested calls at the parent's remaining budget.
  Allocate response waiters after connection acquisition. Retry only failures
  before transport acceptance; never retry ambiguous in-flight requests.
- Server dispatch reserves capacity atomically, drops expired requests before
  execution and contains handler panics. Deadline expiry does not cancel running
  handlers; long-running handlers can poll `Context::is_expired()`.
- Emit metrics through the `metrics` facade. Keep per-method handles cached per
  `State`; users install a recorder before traffic. Only `server_inflight` has
  a locally tracked value, for admission control.

## Remote-memory invariants

- Read/write spaces concatenate buffer lengths. Validate bounds, overflow,
  region/op limits and destination overlap before transferring `CopyOp`s.
- Read attachments own immutable buffers and replace the wrapper's source list.
  Pending requests and inline readers share that ownership. Recovery requires
  uniqueness; a failed recovery attempt preserves the source.
- Preserve [read-then-validate](docs/safe-boundaries.md#application-read-source-validation-and-recovery):
  success depends on the original waiter's immutable source and no request ID reuse.
  The probe checks membership, not a strict deadline or remote DMA completion.
  Failure can permit source reuse before remote completion; recovered error
  bytes remain readable but are not valid transfer results.
- RDMA transfers use READ, including client-side READ for `remote_write`.
  A write target transfers its entire destination vector into the owned READ
  plan and excludes CPU copies or competing READs until buffers are restored.
  Cancellation or failure can leave the target empty after the QP recycles it.
- `SentBuffers` witnesses a completed write or explicit `sent_nothing()`;
  successful calls return available destinations only when uniquely held.
- Dropping a posting cursor accounts for unposted work. Posted buffers and
  READ permits remain held until completions settle or QP destruction succeeds.
  Timeout, ERR state and waiter failure do not authorize early recovery.
- Completion authority comes from non-cloneable CQ-issued tokens checked
  against CQ, QPN and immutable sequence identity. WR sequences never wrap.
  Provider QP destruction precedes release of identity leases or DMA holds.
- CQ capacity retirement is separate from identity retirement: return its
  reservation only after QP destruction and an empty CQ poll following a
  retirement snapshot. Preserve snapshots across bounded drains.

See [safety boundaries](docs/safe-boundaries.md), [WRIDs](docs/wrid.md),
[capacity](docs/rdma-capacity.md) and [poller routing](docs/qp-registry.md) before
changing ownership or completion handling. See [connection lifecycle](docs/rdma-connection.md)
for bootstrap, path selection and peer maintenance; peer identity is the full
user-declared address group, while each connection owns its NIC pair. See
[endpoints](docs/endpoints.md) for URI syntax and group identity; no negotiated
server-instance ID or automatic group merging establishes equivalence.

## Build and review

Follow [CONTRIBUTING.md](CONTRIBUTING.md) for prerequisites, workspace checks,
feature/rustdoc checks and Soft-RoCE setup. Core tests and RPC benchmarks enable
RDMA through a self dev-dependency even though downstream RDMA is opt-in.
Always run formatting and Clippy before committing. PRs target `main`; all CI
checks must pass before merging. Release order is bufpool, macro, RDMA, core.

Follow the [documentation ownership rules](docs/README.md#maintaining-the-docs):
keep each contract in its owning guide, link to it from summaries, and keep
historical benchmark outputs and rejected experiments outside `docs/`.
