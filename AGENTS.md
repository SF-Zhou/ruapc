# AGENTS.md

## Project

RuaPC provides TCP, WebSocket, HTTP and optional RDMA through one Rust RPC API.
Use [DESIGN.md](DESIGN.md) for crate/module ownership and
[the documentation index](docs/README.md) for the authoritative contract guides.

## Implementation constraints

- Use enum dispatch for transport selection (`Socket`, `SocketPool`,
  `HttpSocket`) and statically dispatched futures. Application service closures
  and device registration are explicit trait-object extension boundaries.
- `ruapc`, `ruapc-memory` and `ruapc-wire` forbid unsafe code. Allocation,
  registration and verbs operations belong in `ruapc-bufpool` / `ruapc-rdma`.
  Safe APIs must enforce memory ownership and completion evidence, not accept
  caller-supplied lifetime promises.
- `BufferPool` owns an immutable ordered list of `MemoryRegistrar` trait objects;
  registration indices are pool-local positions. The runtime builds its typed
  RDMA device list in the same order. Stream transports need no registration.
  Custom registrars must uphold their unsafe registration contract.
- Keep metadata in named-field MessagePack. Add compatible fields with
  `#[serde(default)]` and `skip_serializing_if`. `UseMessagePack` selects the
  payload decoder only; default clients use MessagePack, otherwise JSON.
- Message metadata, payload codecs, direct serialization and framing belong in
  `ruapc-wire`, which reports codec errors and does not depend on verbs. Keep
  RPC error types and owned `Message` / `Payload` APIs in `ruapc`, converting
  codec failures at that boundary. TCP, WebSocket, HTTP/2 and RDMA share the
  wire `frame` module; frame parsing stays outside the RDMA poll loop.
  Keep connection binding and queue preparation in
  `ruapc/src/sockets/channel.rs`, and transfer ownership and completion
  validation in the runtime.
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
- Schema generation and OpenAPI projection belong in `ruapc-schema`, which has
  no workspace runtime dependencies. Keep handler dispatch in `ruapc`; built-in
  and application services share one method directory and exposure rules.
- Handlers and typed clients share the declared service trait and method return
  types. Generated client implementations must preserve that public contract.
- Emit metrics through the `metrics` facade. Keep per-method handles cached per
  `State`; users install a recorder before traffic. Only `server_inflight` has
  a locally tracked value, for admission control.

## Remote-memory invariants

- Logical spaces, copy validation/planning, synchronous CPU gather/scatter and
  immutable read sources belong in `ruapc-memory`, with no RPC runtime, async
  executor or verbs dependency.
  Keep RPC errors, write-target checkout, completion witnesses, waiter binding
  and transport execution in `ruapc`; convert memory errors at that boundary.
- Read/write spaces concatenate buffer lengths. Validate bounds, overflow,
  region/op limits and destination overlap before transferring `CopyOp`s.
- Read attachments borrow immutable buffers and replace the wrapper's source list.
  The original request future services inline copies and validation while polled;
  waiters only route control messages and never retain borrowed source pointers.
  Forgetting a future must not allow background source access or new validation.
- Reverse memory RPCs reach a request's source route or write target only on
  the connection carrying that request; a request ID alone selects nothing.
  Bound peer-supplied inline read work before queueing it to the source future.
- Preserve [read-then-validate](docs/safe-boundaries.md#application-read-source-validation-and-recovery):
  success depends on the original borrowing future answering after READ completion
  and the documented assumption that request IDs are not reused within the
  validation window. Source operations reject the earlier original-call or
  probe deadline. Waiter membership alone is insufficient, and validation is
  not a remote DMA completion lease.
  Failure can permit source reuse before remote completion; recovered error
  bytes remain readable but are not valid transfer results.
- RDMA transfers use READ, including client-side READ for `remote_write`.
  A write target transfers its entire destination vector into the owned READ
  plan and excludes CPU copies or competing READs until the plan's recovery
  receiver returns it after settlement, whatever the transfer's result.
  The original call seals the target when it ends, revoking stale handlers.
- `WriteReceipt` records a completed write or explicit `sent_nothing()`.
  Keep transfer success separate from local source/destination availability;
  calls return destinations unless a READ still owns them, and only
  `wait_for_write_buffers` and lent destinations wait for that READ.
- Lent write destinations (`with_write_buffers_mut`) move out of the caller's
  slots into the owned target; the slots hold empty buffers meanwhile and never
  alias DMA memory. A returned call has restored them, waiting without a
  deadline for any READ. Only a dropped or forgotten call may leave them empty.
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
checks must pass before merging. Release order is bufpool, memory, macro,
schema, RDMA, wire, core.

Follow the [documentation ownership rules](docs/README.md#maintaining-the-docs):
keep each contract in its owning guide, link to it from summaries, and keep
historical benchmark outputs and rejected experiments outside `docs/`.
