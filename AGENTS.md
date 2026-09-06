# AGENTS.md

## Project Overview

RuaPC ("Rua! Procedure Call") is a high-performance Rust RPC library supporting multiple transport protocols with a unified API.

## Architecture

### Workspace Structure
- `ruapc/` — Core library: server, client, router, socket abstractions, message format
- `ruapc-bufpool/` — Buddy allocator + slab buffer pool with device registration (transport-independent)
- `ruapc-macro/` — Proc macro `#[service]` for service definition and code generation
- `ruapc-rdma/` — Low-level ibverbs FFI bindings (optional, behind `ruapc`'s `rdma` feature flag)
- `ruapc-demo/` — Example server/client applications (not published)

The `rdma` feature is NOT in `ruapc`'s default features (it requires libibverbs at build time, which e.g. docs.rs doesn't have). `ruapc`'s own tests and benches always enable it via a self dev-dependency (`ruapc = { path = ".", features = ["rdma"] }`), so `cargo test` in this repo requires libibverbs-dev.

### Core Module Boundaries
- `client/` — Configuration, generated-call contracts (`call.rs`), request execution (`request.rs`), retry policy (`attempt.rs`), buffer attachments
- `core/` — Context lifetime, state, listener/server, endpoint health and handler dispatch; `router/schema.rs` owns OpenAPI projection
- `msg/` — Metadata, direct serialization, body decoding, and shared TCP/HTTP stream framing
- `remote_memory/` — Logical spaces and copy validation, inline reverse-RPC fallback, owned read sources, pinned write targets and completion witnesses
- `sockets/` — Enum transport dispatch, connection maps and lifecycle; `channel.rs` owns the common TCP/WS/HTTP send queue
- `rdma/` — RDMA framing, connection/path policy and CQ polling; `rdma_socket/read.rs` owns logical READ planning and admission, `poller/flow.rs` owns credits. The dependency's `ruapc-rdma/src/verbs/queue_pair/read.rs` owns validated READ plans and completion lifetime.

See `DESIGN.md` for ownership invariants, `docs/refactoring.md` for refactor validation,
and `docs/safe-boundaries.md` for the core safety boundary and performance evidence.

### Transport Protocols
- **TCP**: Custom binary protocol with magic number `RUA!`, length-prefixed framing
- **WebSocket**: Over HTTP upgrade, using tokio-tungstenite
- **HTTP**: HTTP/1.1 and HTTP/2 (h2c) via hyper, supports bidirectional streaming for reverse RPC
- **RDMA**: High-performance RDMA via ibverbs (optional)
- **UNIFIED**: Multiplexes all protocols on a single port (peeks first 4 bytes to detect TCP magic)

### Key Abstractions
- `SocketTrait` — Per-connection send interface
- `SocketPoolTrait` — Connection pool management (create, acquire, handle_new_stream)
- `Router` — Method registry mapping "ServiceName/method_name" to handler functions
- `Waiter` — Request/response correlation via unique message IDs and oneshot channels
- `State` — Shared state holding Router, Waiter, and SocketPool

### Design Principles

- **Enum dispatch for transports**: Runtime transport selection uses enum variants (`Socket`, `SocketPool`, `HttpSocket`) and statically dispatched futures. Add variants for new transports rather than boxing async calls. Application service closures and buffer-pool device registrations remain explicit trait-object extension boundaries.
- **Core safety boundary**: `ruapc` uses `#![forbid(unsafe_code)]`. Allocation, registration and verbs operations stay in `ruapc-bufpool` / `ruapc-rdma`, whose safe interfaces enforce actual memory ownership and completion evidence. Moving a raw-pointer operation behind a safe function with caller-supplied lifetime promises is insufficient.
- **Device registration**: `ruapc::Devices` aliases `DeviceSet<RdmaDevice>` with RDMA, or `DeviceSet` otherwise. `DeviceSet` owns the TCP device at index zero and assigns indices to additional devices. Safe `Device` wrappers supply identity and an associated `MemoryRegistrar`; only that audited capability receives backing memory. `TcpDevice` implements it in `ruapc-bufpool`, and `ActiveDevice` in `ruapc-rdma`. Custom `MemoryRegistrar` / `Devices` implementations must uphold their unsafe registration and borrowing contracts outside the core crate.

### Remote Read/Write (vectored, multi-buffer)

- **Logical spaces**: both sides of a transfer are *logical contiguous spaces* — ordered buffer/region lists, each segment contributing its logical `len()`. Clients attach an owned immutable **read space** (`with_read_buffer(Buffer)` / `with_read_buffers(Vec<Buffer>)`) and/or an owned pinned **write space** (`with_write_buffers(Vec<Buffer>)`); the regions travel in `MsgMeta.read_regions` / `write_regions`. Read attachment methods replace the wrapper's source list. The wrapper can reuse the same source across calls and expose shared views with `read_buffers()`; `take_read_buffers(&mut self)` returns ownership only when uniquely held, otherwise returns `None` while preserving it for later recovery.
- **CPU read lifetime**: pending requests and `read_inline` handlers share ownership of the source, so cancellation or `mem::forget` of a request future cannot free memory still being copied. `read_inline` sends only the original request ID and logical ops; it reads the request's owned source rather than accepting peer-provided addresses. This ownership is not a completion lease for remote one-sided DMA: the existing post-READ pending check validates the result, but does not make source recovery wait for unobservable remote completions.
- **Vectored ops**: servers issue `CopyOp { src_offset, dst_offset, len }` batches via `Context::remote_read/remote_write` (`*_all` convenience for 1:1). Validation on both ends (bounds, overflow, `MAX_COPY_OPS`/`MAX_REGIONS`, non-overlapping dst) → `InvalidCopyOp`. RDMA fragments ops at remote-region boundaries into RDMA READ WRs with local SG lists (`remote_memory/scatter.rs`); TCP/WS/HTTP fall back to the internal `_ruapc.memory/read_inline` / `write_inline` reverse RPCs with inline data.
- **Write = client-side READ**: no RDMA WRITE verb; `remote_write` sends an internal `_ruapc.memory/read_into_target` reverse RPC advertising the server's source buffers as read regions, and the *client* RDMA-READs into its pinned buffers. The waiter and writers share `Arc<WriteTarget>`. Its `Mutex<Option<Vec<Buffer>>>` moves the entire destination into the QP-owned READ plan; while absent, CPU copies and competing READs fail with `BuffersInUse`. Completed buffers are restored only when the dependency returns ownership. Successful responses return available write buffers through `Result<WithBuffers<T>, E>` when their target is uniquely held (witness: `SentBuffers` from a completed `remote_write` or `ctx.sent_nothing()`). Failed calls can recover available buffers through `take_write_buffers()`; cancelled or failed DMA can leave the target empty even after the QP eventually recycles its buffers.
- **Read-batch machinery**: `QueuePair::prepare_reads` takes `Vec<Buffer>` and buffer-index/offset/length descriptors, validates local bounds and cross-WR destination overlap, and resolves local addresses/keys internally. Its non-cloneable `ReadPosting` cursor posts each request once to the preparing QP; that QP records a private `Arc<ReadBatch>` before posting. Dropping the cursor accounts the unposted suffix without releasing posted memory. In-flight READ WRs are bounded by a *per-local-NIC* semaphore (`rdma.remote_memory.max_inflight_read_wrs`, default 32, shared by all connections on the device across server `remote_read` and client-side `read_into_target`), plus a per-connection SQ-overflow guard (`qp.max_send_wr / 2`, not a policy knob). `max_rd_atomic`/`max_dest_rd_atomic` are negotiated via `RdmaQpEndpoint.rd_atomic_cap` (min of device caps, ≤16).
- **Completion evidence**: `CompletionQueue::poll_batch` lends non-cloneable CQ-issued tokens from private batch storage. `QueuePair::complete` checks the originating CQ, QP number and CQ-owned `CompletionRoute` (slot plus sequence floor) before recovering SEND/RECV buffers or settling READ ownership. WRIDs encode 2 type bits, `s = ceil(log2(actual_cqe))` CQ-local route-slot bits and `62 - s` sequence bits; the positive provider CQ capacity is at most `2^31 - 1`, giving 31–62 sequence bits. The layout is fixed at CQ creation and CQ-issued tokens decode it. A QP owns its route lease from creation through successful destruction; reuse starts above every sequence allocated in either direction, including failed posts, so old completions cannot match a replacement QP. Sequence allocation never wraps; exhausted slots retire. Core pollers use array lookup by route slot and reject older sequences before flow accounting. CQEs schedule only touched connections for immediate maintenance; full housekeeping runs every 100 ms or on an undirected external wake. Raw CQE metadata cannot authorize recovery. If the provider cannot destroy a QP, the process aborts rather than releasing memory still accessible to DMA. Core CQ admission uses `sum(R + W + A) + min(H, sum(K))` software completion credits, selects a fitting CQ shard before QP creation, and retains the reservation until QP destruction followed by a retirement snapshot and a poll that observes an empty CQ. See `docs/wrid.md` for route ownership, registration ordering and bounds.
- **READ timeout**: `rdma.remote_memory.read_timeout_ms` (default 10s, 0 disables), enforced by the poll thread's periodic sweep (no per-op timers). Timeout fails the waiter with `RdmaReadTimeout` and moves the QP to ERR; destination memory remains held through all posted success/error/flush completions or successful QP destruction. `ready_to_remove` waits for pending READs and all per-QP READ permits to return after admission closes; poller shutdown instead fails their receivers without releasing the QP's holds. Dropping or forgetting a request future cannot authorize early recovery; forgetting ownership can retain resources indefinitely. The server-read path still uses `_ruapc.memory/request_is_pending` to reject results from expired source requests, subject to the source-lifetime limitation above.

### Request Lifecycle & Dispatch Policies

- **Connection tracking**: every connection (TCP/WS/HTTP-stream/RDMA) has a process-unique `conn_id`. Requests bind to it on send (`Waiter::bind_connection`); when a connection dies, `State::connection_closed` eagerly fails its pending waiters (`ErrorKind::ConnectionClosed`). Transport pools evict dead sockets exactly once (`mark_closed`) with identity checks against replacements.
- **Deadline propagation**: `Client.timeout` (min'd with the context's remaining budget for nested RPCs) travels as `MsgMeta.timeout_ms`; the server derives `Context::deadline()` / `remaining_time()` / `is_expired()` on arrival and drops requests that expire before execution.
- **No mid-flight cancellation**: once a handler starts it runs to completion (aborting user code at await points risks broken invariants, and cancel signals are inherently unreliable). Long-running handlers should poll `Context::is_expired()` to stop wasted work; undeliverable responses are simply discarded.
- **Server dispatch** (`spawn_handler`, called by macro-generated code): atomic capacity reservation (`SocketPoolConfig.max_inflight_requests`, rejects with `Overloaded`), expired-request drop, panic containment (`catch_handler_panic` → `HandlerPanic` error response), per-method metrics.
- **Client retries**: `Client.max_retries` (default 2) retries only pre-wire failures (acquire/send); the waiter entry is allocated *after* connect so slow connection setup doesn't consume the response budget. `Context::with_endpoints` accepts equivalent transport-bearing endpoints, prefers healthy established connections, preconnects alternatives, and fails over across addresses or transports.
- **Metrics**: emitted through the `metrics` facade crate (`ruapc_server_*` / `ruapc_client_*` per-method counters/gauges/latency histograms, `ruapc_connections`, shed/expired counters, `ruapc_waiter_pending`); see `ruapc/src/metrics.rs` for the full table. RuaPC never renders or exports — users install their own `metrics::Recorder`/exporter; without one, emissions are no-ops. Per-method handles are interned per `State` (install the recorder before serving traffic). The only locally-tracked value is `Metrics.server_inflight` (an `AtomicI64`), because load shedding needs a readable count and the facade is write-only.

### RDMA Multi-NIC Path Awareness

- **Peer identity vs path**: peers are identified by their bootstrap TCP address (the socket_map key); the *path* — the (local NIC, remote NIC) pair, `RdmaPathInfo` — is a per-connection property carried on every `RdmaSocket`. Each stripe of a peer picks its own path.
- **Static policy**: `rdma.path.device_filter` defines the allowed local NICs and `rdma.path.device_exclude` rejects listed NICs when a context is created. `rdma.path.allow_down_ports` retains devices with a currently DOWN port during discovery so the refresher can activate them after recovery; DOWN ports remain ineligible for paths. On the connecting client, each inner list in `rdma.path.subnets` is a connectivity domain made of one or more CIDRs; a path matches when both NIC addresses belong to the same domain. `rdma.path.subnet_policy` controls whether such paths are preferred or required before link class and load. The server does not use local subnet configuration for matching. Applications needing different policies create independent contexts.
- **Placement**: within the statically allowed, preferred-subnet candidates, local NIC is selected by least-connections over live per-device counters (`ConnCountGuard`, outbound + inbound); remote NIC uses power-of-two-choices over the peer's advertised per-NIC load (`RdmaDeviceInfo.active_connections` from `_ruapc.rdma/discover`).
- **Reachability**: device matching cannot verify routability; QP setup failures (e.g. no route between subnets) blacklist the NIC pair per peer for 30s and placement falls over to the next candidate (`connect_with_failover`).
- **Maintenance task** (per pool, jittered `rdma.maintenance.interval_ms`, default 5s): fails connections on downed local ports, prunes dead stripes, maintains `rdma.peers.min_connections_per_remote_nic` coverage, replenishes desired peers, and gradually rebalances connections with make-before-break migration.
- **Introspection**: `State::rdma_path_report()` / `Server::state()` report paths, direction, health, and per-device load.

### Wire Format
- TCP / HTTP-2 stream: `[4B magic "RUA!"][4B total_len][4B meta_len][meta bytes][payload bytes]`
- RDMA send: a sequence of self-delimiting frames `[4B frame_len][4B meta_len][meta][payload]` (usually one). Window-blocked sends are aggregated by plain frame concatenation; one RDMA send consumes one flow-control credit (= one peer receive buffer) regardless of frame count. The poll thread never parses messages — received buffers are batched per CQ drain and routed (sticky, spilling on pressure) to a fixed pool of dispatch worker tasks (`rdma.polling.dispatch_workers`, default 32), each owning one SPSC queue, that walk and parse the frames; when every worker is saturated the poll thread falls back to a one-shot `tokio::spawn` per batch.

### Serialization
- Meta (`MsgMeta`): always MessagePack with named fields — the encoding cannot depend on a flag stored inside itself; new fields are added with `#[serde(default)]` + `skip_serializing_if` for compatible evolution
- Payload: MessagePack by default for `Client`, JSON when `use_msgpack` is false; `MsgFlags::UseMessagePack` selects the wire payload decoder

## Development

### Build & Test
```bash
cargo build --all-features
cargo test --all-features
cargo fmt
cargo clippy --all-features
cargo bench -p ruapc --bench echo  # end-to-end echo RPC benchmark
```

### CI
- `rust.yml`: rustfmt check, clippy (`--all-features -D warnings`), tests with coverage via cargo-llvm-cov (`--all-features`), Codecov upload
- `release.yml` (on `v*` tags): publishes to crates.io in dependency order (ruapc-bufpool → ruapc-macro → ruapc-rdma → ruapc)
- RDMA tests use `rxe_0` virtual device in CI (env var `RUAPC_PREFER_RXE=1`)

### Conventions
- Always run `cargo fmt` and `cargo clippy` before committing
- PRs target `main` branch
- All CI checks must pass before merging
