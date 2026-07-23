# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

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
