# AGENTS.md

## Project Overview

RuaPC ("Rua! Procedure Call") is a high-performance Rust RPC library supporting multiple transport protocols with a unified API.

## Architecture

### Workspace Structure
- `ruapc/` — Core library: server, client, router, socket abstractions, message format
- `ruapc-macro/` — Proc macro `#[service]` for service definition and code generation
- `ruapc-rdma/` — RDMA transport (optional, behind `rdma` feature flag)
- `ruapc-demo/` — Example server/client applications

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

- **Enum dispatch over `dyn Trait`**: All runtime polymorphism uses enum variants (e.g. `Socket`, `SocketPool`, `HttpSocket`) instead of trait objects. Two reasons: (1) we don't need open-ended extensibility and won't sacrifice performance for it; (2) `async`-compatible `dyn Trait` has high runtime cost and is not mature enough. When adding new transport types or socket variants, add enum variants rather than trait objects.

### RDMA Multi-NIC Path Awareness

- **Peer identity vs path**: peers are identified by their bootstrap TCP address (the socket_map key); the *path* — the (local NIC, remote NIC) pair, `RdmaPathInfo` — is a per-connection property carried on every `RdmaSocket`. Each stripe of a peer picks its own path.
- **Placement**: local NIC by least-connections over live per-device counters (`ConnCountGuard`, outbound + inbound); remote NIC by power-of-two-choices over the peer's advertised per-NIC load (`RdmaDeviceInfo.active_connections` in the `info` RPC) — P2C avoids client herding on stale snapshots.
- **Reachability**: device matching cannot verify routability; QP setup failures (e.g. no route between subnets) blacklist the NIC pair per peer for 30s and placement falls over to the next candidate (`connect_with_failover`).
- **Maintenance task** (per pool, jittered `rdma.maintenance_interval_ms`, default 5s): fails connections on downed local ports (via the 15s device refresher), prunes dead stripes, replenishes peers up to `connections_per_peer`, and migrates at most one connection per tick to a less-loaded pair (make-before-break with a `drain_timeout_ms` grace; `rebalance_threshold` gives hysteresis, scores exclude the victim's own contribution). Rate-limited migration is what makes traffic return gradually to recovered NICs.
- **Explicit control**: `Context::with_rdma_path(RdmaPathSelector)` pins requests to matching paths (creating pinned, rebalance-exempt connections on demand); `rdma.device_filter` / `rdma.remote_device_filter` for config-level constraints; `State::rdma_path_report()` / `Server::state()` for introspection (paths, direction, health, per-device load).

### Wire Format
- TCP / HTTP-2 stream: `[4B magic "RUA!"][4B total_len][4B meta_len][meta bytes][payload bytes]`
- RDMA send: a sequence of self-delimiting frames `[4B frame_len][4B meta_len][meta][payload]` (usually one). Window-blocked sends are aggregated by plain frame concatenation; one RDMA send consumes one flow-control credit (= one peer receive buffer) regardless of frame count. The poll thread never parses messages — received buffers are batched per CQ drain and routed (sticky, spilling on pressure) to a fixed pool of dispatch worker tasks (`rdma.dispatch_workers`, default 32), each owning one SPSC queue, that walk and parse the frames; when every worker is saturated the poll thread falls back to a one-shot `tokio::spawn` per batch.

### Serialization
- JSON (default) or MessagePack (via `MsgFlags::UseMessagePack`)

## Development

### Build & Test
```bash
cargo build --all-features
cargo test --all-features
cargo fmt
cargo clippy --all-features
```

### CI
- GitHub Actions: build, test (cargo-nextest with `-j 1`), clippy, rustfmt, CodeQL, codecov
- RDMA tests use `rxe_0` virtual device in CI (env var `RUAPC_PREFER_RXE=1`)

### Conventions
- Always run `cargo fmt` and `cargo clippy` before committing
- PRs target `main` branch
- All CI checks must pass before merging
