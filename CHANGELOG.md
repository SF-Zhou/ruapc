# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Owned RDMA READ submission through `QueuePair::prepare_reads` / `post_read`.
  Plans take destination buffers and buffer-index/offset/length descriptors,
  validate bounds and cross-request overlap, and return a non-cloneable posting
  cursor. The dependency retains posted memory through cancellation, timeout
  and error/flush completion.
- Structured built-in reflection through
  `_ruapc.meta/describe`, including the RuaPC version, public service/method
  schemas, and resolvable OpenAPI components.
- `#[service(name = "...", internal)]` support for stable wire names and
  dispatchable control-plane methods that stay out of public discovery and
  OpenAPI.
- `rdma.path.allow_down_ports` (default `false`) can retain devices whose ports
  are currently DOWN during discovery, allowing the port refresher to make them
  available after the link becomes active.
- Per-port, per-direction RDMA remote read/write bandwidth shaping based on a lock-free GCRA.
  `rdma.remote_memory.bandwidth_limit_ratio` defaults to 95% of the port's reported link
  bandwidth, `bandwidth_limit_burst_ms` controls burst tolerance, and
  `bandwidth_limit_max_wait_ms` bounds admission delay and rejects
  immediately when set to zero.

### Changed
- **BREAKING**: `ruapc_bufpool::Device::register` is replaced by an associated
  `Registrar: MemoryRegistrar` and `registrar()` accessor. Safe device wrappers
  no longer receive pool backing memory; custom registration implementations
  require the unsafe `MemoryRegistrar` contract. `ruapc::Devices` now aliases
  `DeviceSet<RdmaDevice>` with RDMA enabled, or `DeviceSet` otherwise; device
  access uses `tcp_device()` / `devices()`, and insertion uses `push()`.
- **BREAKING**: Raw-ID buffer reclamation methods `QueuePair::take_buffer`,
  `take_send_buffer` and `reclaim_send_buffers` are replaced by `complete`,
  which consumes a non-cloneable `Completion` from
  `CompletionQueue::poll_batch` and checks CQ/QP/tag identity. `set_wr_tag` now
  returns a result and permits one assignment, with no tag reuse on either CQ.
  QP creation requires an RC QP, matching PD/CQ contexts, and no external SRQ
  or raw context pointer.
- **BREAKING**: `MemoryRegion::register` rejects offset-based addressing;
  owned buffer operations require ordinary virtual-address registration.
- The `ruapc` core now enforces `#![forbid(unsafe_code)]`. Registration,
  raw verbs and DMA ownership enforcement reside in `ruapc-bufpool` and
  `ruapc-rdma`; core operations use their safe ownership and completion APIs.
- **BREAKING**: `AlignedMemory::as_mut_slice` requires a mutable receiver;
  implementing `Devices` and calling raw TCP/RDMA memory operations now require
  explicit unsafe contracts. `QueuePair::poll_send` / `poll_recv` and unchecked
  public task registration were removed; unsupported service declarations now
  produce explicit macro diagnostics. See the
  [API migration notes](docs/refactoring.md#api-changes).
- **BREAKING**: Read attachments take ownership through
  `with_read_buffer(Buffer)` / `with_read_buffers(Vec<Buffer>)`; setting an
  attachment replaces the source list. Wrappers reuse immutable sources across
  calls, expose shared `read_buffers()` views, and support conditional ownership
  recovery through `take_read_buffers(&mut self)`. Local reverse-RPC readers
  retain the source through cancellation; `read_inline` now sends logical ops
  and a request ID without duplicating region metadata.
- **BREAKING**: Built-in services now use the reserved `_ruapc.*` wire
  namespace. Remote-memory methods are named `read_inline`, `write_inline`,
  `read_into_target`, and `request_is_pending`; RDMA bootstrap methods are
  named `discover`, `prepare_connection`, `commit_connection`, and
  `cancel_connection`. Internal methods are hidden from OpenAPI and unary HTTP.
- **BREAKING**: RDMA bootstrap types now separate peer-advertised directional
  limits from local QP settings. Per-connection CQ and wire SGE fields were
  removed, endpoint leases are returned separately from QP endpoints, and
  asymmetric send/receive limits are negotiated in the correct direction.
- **BREAKING**: Router registration rejects duplicate wire names. `MethodInfo` is replaced
  by `MethodSchema` with explicit `request_schema` / `response_schema` fields;
  `method_names()` and `method_schemas()` expose public methods only.
- **BREAKING**: `RdmaSocketPoolConfig` is grouped into `connection`, `polling`,
  `path`, `peers`, `maintenance`, and `remote_memory` sub-configurations.
- `SocketPoolConfig::buffer_pool_memory` now defaults explicitly to
  `DEFAULT_BUFFER_POOL_MEMORY` (256 MiB); zero is rejected instead of acting as
  an implicit default.
- When built with the `rdma` feature, `SocketPoolConfig::default()` now enables
  RDMA resources with `RdmaSocketPoolConfig::default()`; set `rdma` to `None`
  to disable them explicitly.

### Fixed
- Write targets move their buffers into owned RDMA READ plans for the entire
  transfer. CPU copies and competing READs now return `BuffersInUse` while the
  target is checked out, preventing concurrent CPU/NIC access to the same
  destination. Cancelled or failed transfers can leave the target empty while
  the QP retains and eventually recycles its buffers.
- Completion tags and WR sequence numbers no longer wrap and allow stale CQEs
  to reclaim newer work. If the provider fails to destroy a QP or deregister
  memory, the process aborts rather than releasing memory that DMA may still access.

### Removed
- **BREAKING**: Removed the dead `Metadata` type and the redundant
  `MetaService/list_methods` API. The public raw message-waiter probe was also
  removed; its safety-critical use is now an internal remote-memory method.

## [0.2.0-alpha.5] - 2026-08-23

### Changed
- RDMA device discovery is sorted by device name and supports an explicit
  `rdma.device_exclude` list. `rdma.subnets` now groups CIDRs into multiple
  connectivity domains for path matching.

## [0.2.0-alpha.4] - 2026-08-20

### Changed
- **BREAKING**: RDMA subnet matching is now client-only. `rdma.subnets` is a
  plain CIDR list and `rdma.subnet_policy` prefers or requires paths whose two
  NIC addresses belong to the same configured CIDR; named virtual zones and
  server-side zone matching were removed.

## [0.2.0-alpha.3] - 2026-08-19

### Added
- Transport-aware endpoint failover across equivalent addresses and protocols,
  with healthy established connections preferred and alternatives preconnected
  (#96)
- HTTP base path support for serving RPC, OpenAPI, and RapiDoc endpoints below a
  configurable URL prefix (#99)
- Client-selected RDMA GRH traffic class configuration for RoCE connections
  (`rdma.traffic_class`) (#89)
- CRC32C-verified remote read/write throughput benchmarks in `ruapc-demo` (#90)
- `ruapc-rdma` README with feature overview, C-shim rationale, and crate layout

### Changed
- **BREAKING**: RDMA NIC policy is now static per `Context`. Removed request-level
  path selectors/modes and remote-name filters. `rdma.device_filter` and
  CIDR-backed virtual zones now define path policy at initialization; automatic
  failure avoidance, NIC coverage, and connection rebalancing remain internal
  (#97, #100)
- **BREAKING** (`ruapc-rdma`): removed the unused `RdmaBuffer` trait, a
  leftover from before `QueuePair` took `ruapc_bufpool::Buffer` directly
- **BREAKING** (`ruapc-rdma`): `ibv_device_cap_flags`, `ibv_port_cap_flags`,
  and `ibv_port_cap_flags2` are generated as `enumflags2` enums instead of
  integer newtypes. Combining variants yields a serializable `BitFlags` value,
  which is also used directly by capability fields in device/port attributes
- `ruapc-rdma`: extensions on generated FFI types moved into `src/ffi/`
  (`gid`, `wc`, `flags`, `pthread`); lint allowances are now scoped to the
  generated bindings instead of crate-wide; the build script reruns when the
  bound C headers change (#94)
- Client, socket, RDMA poller, and RDMA socket pool internals were split into
  focused modules without changing their public API (#98)

### Fixed
- `ruapc-rdma`: `ibv_port_cap_flags2` now uses the bound struct field's `u16`
  width, so reading `ibv_port_attr.port_cap_flags2` no longer includes adjacent
  padding. `query_port` now uses a zeroed `MaybeUninit` out buffer and constructs
  `ibv_port_attr` only after a successful FFI call, avoiding invalid zero
  discriminants in Rust enum fields such as `ibv_mtu` (#94)
- Link `libibverbs` after the C shim so static-link symbol resolution succeeds
  (#91)

## [0.2.0-alpha.2] - 2026-07-26

### Changed
- **BREAKING**: remote read/write redesigned around vectored, multi-buffer
  *logical contiguous spaces* (#85). Clients attach a read space
  (`with_read_buffers`, borrowed) and/or a write space (`with_write_buffers`,
  ownership moved and pinned until the call resolves); the regions travel in
  `MsgMeta.read_regions` / `write_regions` (replacing `buffer_info`). Servers
  issue validated `CopyOp` batches with explicit offsets through
  `Context::remote_read` / `remote_write` (`remote_read_all` /
  `remote_write_all` for whole-space transfers); on RDMA the ops fragment into
  concurrent one-sided READ work requests with scatter-gather lists, on
  TCP/WS/HTTP into reverse-RPC copies. `WithBuffer` / `SentBuffer` /
  `ClientWithBuffer` / `ResultWithBuffer` become plural (`WithBuffers` etc.)
  and carry every attached buffer back; `remote_read_request` /
  `request_buffer_info` are replaced by `remote_read_all` /
  `remote_read_space`
- **BREAKING**: `remote_write` requires the client to pre-provide pinned
  destination buffers; the transfer runs as client-initiated RDMA READs
  (reverse `pull`), with an `Arc`-pinned write target guaranteeing the NIC
  can never DMA into recycled memory even across request timeouts (#85)
- RDMA config structs use inline serde defaults throughout; partial `rdma`
  config objects now deserialize with documented defaults for the omitted
  fields (#87)

### Added
- Software timeout for RDMA READ completions (`rdma.read_timeout_ms`,
  default 10s, `0` disables), enforced by a poll-thread sweep instead of
  per-operation timers; timed-out connections are flushed via QP error state
  and read-held memory is only released once every completion arrived (#85)
- Per-NIC in-flight RDMA READ budget (`rdma.max_inflight_read_wrs`, default
  32): all connections on a device — server-side `remote_read` and
  client-side `pull` alike — share one FIFO semaphore, giving a single
  congestion-control knob for read traffic; a derived per-connection
  `qp.max_send_wr / 2` guard protects individual send queues (#86)
- `max_rd_atomic` / `max_dest_rd_atomic` negotiation via the endpoint
  exchange (`Endpoint.rd_atomic_cap`, min of both device caps, up to 16), so
  batched reads actually proceed in parallel inside the NIC (#85)
- Multi-SGE RDMA READ posting (`QueuePair::read_sges`) with race-free
  completion registration in `ruapc-rdma` (#85)

## [0.2.0-alpha.1] - 2026-07-23

### Added
- Remote Read/Write: server-side access to client memory with typed contracts.
  `remote_read` / `remote_read_request` let the server read a client-provided
  buffer; `ctx.remote_write(buf)` returns a `SentBuffer` witness and
  `sent.reply(rsp)` builds a `Result<WithBuffer<T>>` return value recognized by
  the type system through any alias. Transfers use data-copy over TCP or
  zero-copy RDMA READ, with request-liveness (message ID) validation for
  buffer lifetime safety (#42–#48, #61–#63, #76)
- New `ruapc-bufpool` crate: buddy memory allocator with slab layer, lazy
  merging, async waiters, subtree reservation, and transport-independent
  device registration (#44, #51, #64, #65, #72)
- RDMA multi-NIC path awareness: least-connections local NIC placement,
  power-of-two-choices remote NIC selection, per-peer blacklist with
  connect-phase failover, and a maintenance task that prunes dead stripes,
  replenishes peers, and rate-limits rebalancing migrations. Explicit control
  via `Context::with_rdma_path`, `rdma.device_filter` config, and
  `State::rdma_path_report()` introspection (#77)
- Production-readiness: per-method client/server metrics via the `metrics`
  facade, deadline propagation (`Client.timeout` → `Context::deadline()`),
  server load shedding (`max_inflight_requests`), handler panic containment,
  client retries with round-robin multi-address failover (#78, #79)
- RDMA Queue Pair config negotiation and device query handshake with
  periodic port refresh (#67, #68, #70)
- `ibv_devinfo` Rust binary in `ruapc-rdma` (behind the `bin` feature) (#66)
- End-to-end echo benchmark (`cargo bench -p ruapc --bench echo`) with usage
  and reference results in `docs/benchmark.md` (#80)
- trybuild UI tests for the `#[service]` macro (#80)

### Changed
- **BREAKING**: the `rdma` feature is no longer enabled by default; opt in
  with `features = ["rdma"]` (#80)
- **BREAKING**: `ruapc-rdma` rewritten as low-level ibverbs FFI bindings with
  type-safe device management; higher-level RDMA logic now lives in `ruapc`
  (#55, #58–#60)
- RDMA data path reworked for performance: dedicated poll-thread, fixed
  dispatch worker pool, gather-list sends, self-delimiting framed wire format,
  and flow-control hardening (#73, #75)
- Buffer pool replaced with the buddy allocator from `ruapc-bufpool` (#64)
- All workspace crates now share a single version (`workspace.package`);
  `ruapc` pins `ruapc-macro` with an exact `=` requirement

### Fixed
- TCP/WS/HTTP connection lifecycle hardening: dead sockets evicted exactly
  once, pending waiters failed eagerly on connection close (#78)
- RDMA memory registration `rkey()` bug (#46)
- crates.io publish chain: `ruapc-bufpool` is published first and internal
  dependencies carry version requirements (#80)

## [0.1.3] - 2026-04-11

### Added
- HTTP/2 h2c support with automatic HTTP/1.1 and HTTP/2 protocol negotiation (#37)
- Reverse RPC: server can call back into client services over HTTP/2 bidirectional streaming (#37)
- OpenAPI 3.0 specification auto-generation with JSON Schema support (#17)
- RapiDoc integration for interactive API documentation (#18)
- Message ID (UUID) validation on server side (#28)
- Comprehensive API documentation and doc comments (#26, #30)

### Changed
- HTTP client switched from HTTP/1.1 to HTTP/2 with single-connection multiplexing (#37)
- Message ID allocation moved to client side (#27)
- Socket module refactored for cleaner architecture (#34)

### Fixed
- Message ID leak in waiter (#15)
- RDMA `ibv_reg_mr` return value check (#24)
- HTTP socket content type (#21)
- RDMA socket periodic health check (#16)
- CI: prefer RXE device in RDMA unit tests for reliable CI (#36)

## [0.1.2] - 2025-08-26

### Added
- RDMA transport support (optional `rdma` feature) (#13)
- Unified socket pool: single port for TCP, WebSocket, and HTTP (#10)
- HTTP transport (#9)
- `MetaService::list_methods` for service discovery (#11)
- JSON Schema support for request/response types (#7)
- Task supervisor for graceful async task management (#6)
- Body-less request support (#12)

### Fixed
- MessagePack deserialization failure (#4)

## [0.1.1] - 2025-07-26

### Added
- Initial release
- TCP and WebSocket transport
- MessagePack serialization support
- RPC callback (bidirectional RPC over TCP)
- Proc macro `#[service]` for service definition

### Fixed
- Waiter cleanup on timeout (#1)
- RPC callback failure (#2)
