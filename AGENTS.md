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

### Wire Format (TCP/HTTP/2 Stream)
`[4B magic "RUA!"][4B total_len][4B meta_len][meta bytes][payload bytes]`

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
