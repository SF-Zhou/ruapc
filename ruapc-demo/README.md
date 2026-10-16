# RuaPC examples

This unpublished crate demonstrates service registration, typed calls and
echo/read/write workloads. Run the commands below from the workspace root. TCP, WebSocket
and HTTP builds do not need RDMA libraries.

| Binary | Purpose |
|---|---|
| `server` | Hosts `EchoService`, `GreetService` and `MemBenchService`. |
| `client` | Makes individual calls or runs fixed-concurrency echo/read/write workloads. |

## Quick start

Start a server, then run the client in a second terminal:

```bash
cargo run -p ruapc-demo --release --bin server -- 127.0.0.1:8000
```

```bash
cargo run -p ruapc-demo --release --bin client -- tcp://127.0.0.1:8000 --value Alice
```

The default client calls both `echo` and `greet`. The default server uses
`--listen-mode unified`, so the same port also accepts WebSocket and HTTP:

```bash
cargo run -p ruapc-demo --release --bin client -- ws://127.0.0.1:8000
cargo run -p ruapc-demo --release --bin client -- http://127.0.0.1:8000
```

Endpoints use an IP address and port, for example `tcp://[::1]:8000` for IPv6.
Use `--listen-mode tcp`, `ws` or `http` on the server to select one protocol.
The server stops and joins its background tasks on Ctrl-C. RPC, setup and
workload failures make the client exit with a nonzero status.

The demo client uses JSON by default. Add `--use-msgpack` to select MessagePack
payloads on any typed transport; no server change is needed. This is a demo CLI
default: the library's `Client::default()` uses MessagePack.

## HTTP and reflection

An ordinary HTTP caller can invoke public methods with JSON:

```bash
curl -s -H 'content-type: application/json' -d '"hello HTTP"' \
  http://127.0.0.1:8000/EchoService/echo
# {"Ok":"hello HTTP"}

curl -s -H 'content-type: application/json' -d '{}' \
  http://127.0.0.1:8000/_ruapc.meta/describe
```

Open `http://127.0.0.1:8000/rapidoc` for interactive documentation or fetch
`http://127.0.0.1:8000/openapi.json` for the schema. `Request(String)` serializes
as a JSON string, which is why the echo request body is `"hello HTTP"`.

To mount HTTP endpoints below a prefix, set the same base path on both peers:

```bash
# Server terminal:
cargo run -p ruapc-demo --release --bin server -- 127.0.0.1:8000 --http-base-path /api/v1
# Client terminal:
cargo run -p ruapc-demo --release --bin client -- http://127.0.0.1:8000 --http-base-path /api/v1
```

The routes then include `/api/v1/EchoService/echo`,
`/api/v1/_ruapc.meta/describe`, `/api/v1/openapi.json` and `/api/v1/rapidoc`.
Keep the endpoint itself in `http://IP:port` form; pass the prefix separately.
RuaPC's typed HTTP client connects to the HTTP/2 bidirectional stream at
`/api/v1/_rpc`, which supports reverse RPC and remote memory. Ordinary HTTP
POST calls do not support reverse RPC. See [built-in services](../docs/builtin-services.md)
for the reflection contract.

## Remote read/write

With the same server running, select `--rpc read` or `--rpc write`:

```bash
cargo run -p ruapc-demo --release --bin client -- tcp://127.0.0.1:8000 --rpc read --buffer-size 1048576
cargo run -p ruapc-demo --release --bin client -- tcp://127.0.0.1:8000 --rpc write --buffer-size 1048576
```

`read` attaches an immutable client source, asks the server to read it and checks
the returned CRC32C. `write` supplies client destinations, asks the server to fill
them and checks their CRC32C against the response. `--buffer-size` accepts 0 through
67108864 bytes (64 MiB), inclusive. Both operations work with TCP, WebSocket,
typed HTTP and RDMA.

[`client.rs`](src/bin/client.rs) shows how to allocate buffers, set their logical
lengths and reuse them across calls. Read attachments own immutable sources.
Successful `WithBuffers` responses return available write destinations only when
uniquely held; failed or cancelled DMA can leave those destinations unavailable.
Read-source recovery proves local uniqueness, not remote DMA completion: after
source-side timeout or cancellation, an already posted one-sided READ may still
be in flight. The post-READ pending check rejects expired results but cannot
prove safe source reuse. See the [ownership contract](../DESIGN.md#remote-memory)
and [safety boundaries](../docs/safe-boundaries.md).

## Load testing

`--stress` reports periodic request rates and failure counts. `--bench` additionally
records successful-call latency and reports mean, min, max and percentiles.
The modes are mutually exclusive and use the same workload implementation.

```bash
# Fixed-concurrency load, with no warmup:
cargo run -p ruapc-demo --release --bin client -- tcp://127.0.0.1:8000 \
  --stress --coroutines 128 --secs 10

# Serial echo latency, with a 16-byte payload:
cargo run -p ruapc-demo --release --bin client -- tcp://127.0.0.1:8000 \
  --bench --coroutines 1 --payload-size 16 --secs 10 --warmup-secs 2

# Verified 1 MiB writes into client buffers:
cargo run -p ruapc-demo --release --bin client -- tcp://127.0.0.1:8000 \
  --bench --rpc write --buffer-size 1048576 --coroutines 32 --secs 10 --warmup-secs 2
```

`--secs` includes warmup: both bench commands request 2 seconds of warmup
and 8 seconds of measurement. In bench mode it must exceed `--warmup-secs`.
Stress mode has no warmup. Each coroutine waits for its call before issuing
another, so this is a closed-loop test at fixed concurrency, not a fixed
arrival-rate generator. Requests are assigned to warmup or measurement by their
start time. In-flight calls can finish after the requested duration and still
contribute to the result. The final `measured` duration includes this tail and
excludes warmup; throughput divides successful measured calls by that duration.
Warmup calls are excluded from reported counts and latency, but any failed
call, including one during warmup, makes the final exit status nonzero.

Echo uses `--value` for individual calls and stress, and a generated string of
`--payload-size` bytes for bench. Read/write use `--buffer-size` per coroutine.
Workers prepare payloads and buffers before timing starts. Calls verify echo
contents or CRC32C; read/write timings include server-side CRC calculation,
write-pattern generation, client verification and per-call allocation.
Their MiB/s counts successful logical payload bytes, not network traffic.
Use equivalent payloads, concurrency, encoding, runtime settings and CPU/NUMA
placement when comparing runs. See the [benchmark guide](../docs/benchmark.md).

## Configuration and RDMA

The binaries' `--help` output is the reference for option defaults:

```bash
cargo run -p ruapc-demo --bin server -- --help
cargo run -p ruapc-demo --bin client -- --help
```

Server and client share `--worker-threads` (0 selects Tokio's CPU-based
default) and `--pool-mem-mb` (0 selects the library buffer-pool limit).
`RUST_LOG` controls tracing, with `info` as the fallback. The examples use
the system allocator. Size the pool for all workers' attachments plus transport
and server transfer buffers. Allocation size classes and 64 MiB backing blocks
determine pool use, not just logical payload lengths. Workload preparation
returns an error if its buffers cannot be allocated.

RDMA requires the `rdma` Cargo feature, a C compiler, `pkg-config`, libclang
and the libibverbs development package. At runtime it needs an active RDMA
device and sufficient locked-memory allowance. See
[CONTRIBUTING.md](../CONTRIBUTING.md#rdma-tests) for Soft-RoCE setup.

```bash
# In both terminals, increase the locked-memory limit if needed:
sudo prlimit --pid $$ -l=unlimited

# Server terminal; replace mlx5_0 with an available local device:
cargo run -p ruapc-demo --release --features rdma --bin server -- \
  0.0.0.0:8000 --listen-mode unified --rdma-devices mlx5_0

# Client terminal; use the server's bootstrap IP and a local device name:
cargo run -p ruapc-demo --release --features rdma --bin client -- \
  rdma://127.0.0.1:8000 --rdma-devices mlx5_0
```

With the feature enabled, `server` initializes RDMA by default and requires
`tcp` or `unified` listen mode for bootstrap. Add `--no-rdma` to run that build
without opening RDMA devices, including with a dedicated HTTP or WebSocket
listener:

```bash
cargo run -p ruapc-demo --features rdma --bin server -- --no-rdma --listen-mode http
```

`--no-rdma` is available only in RDMA-enabled server builds. A build without
the feature needs neither this flag nor RDMA libraries. The client enables
RDMA automatically only for an `rdma://` endpoint.

Feature-enabled server/client builds also expose `--rdma-devices` (a
comma-separated local allowlist), `--poll-threads`, `--conns-per-peer`,
`--poll-spin-us`, `--dispatch-workers`, `--recv-queue-len` and
`--traffic-class`. Inspect `--help` with `--features rdma` for their defaults.
Device names are local to each peer; omit the allowlist for automatic selection.
See [RDMA connection setup](../docs/rdma-connection.md) for path selection and
configuration details.

## Source layout and validation

| Source | Responsibility |
|---|---|
| [`src/lib.rs`](src/lib.rs) | Shared service contracts, request types, CRC and pattern helpers. |
| [`src/app.rs`](src/app.rs) | Tracing, Tokio runtime and shared pool/RDMA CLI configuration. |
| [`src/bin/server.rs`](src/bin/server.rs) | Private service implementation, registration, listener setup and shutdown. |
| [`src/bin/client.rs`](src/bin/client.rs) | CLI validation, individual calls, echo/read/write buffer reuse and verification, and load reporting. |
| [`tests/common/mod.rs`](tests/common/mod.rs) | Shared process startup and cleanup for integration tests. |
| [`tests/rpc.rs`](tests/rpc.rs) | RPC contract checks against the server binary. |
| [`tests/client_cli.rs`](tests/client_cli.rs) | Client command-line behavior and workload checks. |

For local checks without enabling RDMA:

```bash
cargo fmt --all -- --check
cargo clippy -p ruapc-demo --all-targets -- -D warnings
cargo test -p ruapc-demo
```

With the RDMA build prerequisites installed, also check the feature-enabled
build. The integration tests use stream transports and start the server with
`--no-rdma`, so these checks do not need an RDMA device:

```bash
cargo test -p ruapc-demo --all-features
cargo clippy -p ruapc-demo --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc -p ruapc-demo --all-features --no-deps
```

Exercise the client/server commands above with TCP, WebSocket and HTTP.
Use the RDMA commands on a configured device to validate that transport.
Use the [workspace validation commands](../CONTRIBUTING.md) for all-feature
tests and documentation checks.
