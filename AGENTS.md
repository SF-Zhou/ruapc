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
  Safe APIs accept owned buffers or already established access capabilities;
  they do not accept arbitrary caller-supplied lifetime promises. The separate
  `ruapc-unsafe` leaf crate is the explicit exception: its unsafe reference APIs
  delegate lifetime and access obligations to the advanced caller.
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
- `Buffer` stays non-Clone with ordinary ownership, slice access and pool return;
  do not add per-allocation reference counts, access leases or busy checks.
  Bufpool's `WriteBuffer` holds either a moved Buffer or an unsafe
  raw metadata snapshot. Borrowed snapshots neither move the original handle nor
  keep its allocation or registration alive.
- `Context::remote_write` safely borrows source `&[Buffer]`: streams synchronously
  gather owned request bytes, and RDMA exports metadata without retaining a
  dereferenceable source snapshot in the waiter. No local READ uses this source;
  cancellation needs no source settlement. Normal successful completion keeps
  the borrow valid through the peer's completed READ.
- Only the explicit unsafe facade may borrow destination `&mut [Buffer]` without
  runtime lifetime tracking. Its caller must retain the
  originals, exclude conflicting access and drive started futures to completion;
  dropping, forgetting, aborting or unwinding from them violates that contract.
- Client sources are real `&[Buffer]` borrows held only by the calling future.
  Each request has a separate bounded source-command channel; waiters retain
  only its sender and validation mode. The calling future synchronously gathers
  CPU bytes and confirms the original msgid for safe sources. Explicit unsafe
  sources may skip that confirmation and directly check waiter membership under
  their established `ReadSources` capability. No independent task retains source
  references. Drop closes the channel; forget prevents positive validation for
  safe sources. Caller-validated sources must not forget or leak started,
  unfinished method or enclosing `run` futures. Owned destinations still permit
  ordinary cancellation; borrowed destinations retain their strict contract.
  Reverse-RPC deadlines bound command delivery and reply waiting. Response
  delivery releases waiter holds before waking its receiver; the calling scope
  retains its target and owns recovery. Never bypass a forgotten call's gate.
- Preserve [read-then-validate](docs/safe-boundaries.md#application-read-source-validation-and-recovery):
  success requires the original source future to confirm waiter membership while
  holding its immutable borrow, or the explicit unsafe caller's source guarantee,
  after the reader's READ completion. No request
  ID reuse is allowed. The check is not a strict original-request deadline check.
  Failure can permit source reuse before remote completion; error bytes remain
  readable but are not valid transfer results.
- RDMA transfers use READ, including client-side READ for `remote_write`.
  A write target transfers its entire storage vector into the READ plan and
  excludes CPU copies or competing READs until the original storage is restored.
  Seal targets against new access before recovery. Borrowed calls return only
  after their original destinations are locally available. Owned calls may stop
  waiting after cleanup grace and return without buffers; posted allocations
  remain held by completion machinery until settlement.
- Public transfers use ordinary `Result`; service signatures do not carry
  ownership-recovery wrappers. Owning call adapters return `Completed<R>` with
  `Option` buffers outside either result branch, around the same storage executor.
  Buffered client builders expose only consuming `run`; typed and raw buffered
  requests use the `&ClientWithBuffers` supplied to its callback. The execution
  handle cannot be constructed, cloned or retained outside that scope.
  `Some` contains every original destination; `None` abandons recovery of the whole
  container. An owned scope with abandoned attachments cannot issue another
  call. A successful response cannot hide an unsettled destination:
  recovery timeout becomes an error; an existing application error is preserved.
  Owned cancellation retains posted allocations through settlement; unsafe
  borrowed calls require the caller to prevent cancellation and premature reuse.
- Dropping a posting cursor accounts for unposted work. Posted buffers and
  READ permits remain held until completions settle or QP destruction succeeds.
  Use one `prepare_reads(Vec<WriteBuffer>)` path and one `ReadCompletion` /
  `ReadOutcome` for both input modes; convert owned buffers at the boundary.
  READ failures record a reason; settled completion returns the original storage
  on both success and failure. Timeout, ERR state and waiter failure do not
  authorize early recovery. Cleanup grace triggers ERR and real CQE draining,
  then explicit provider destruction; retained socket handles cannot delay it.
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
schema, RDMA, wire, core, unsafe facade.

Follow the [documentation ownership rules](docs/README.md#maintaining-the-docs):
keep each contract in its owning guide, link to it from summaries, and keep
historical benchmark outputs and rejected experiments outside `docs/`.
