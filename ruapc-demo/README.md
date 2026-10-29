# RuaPC examples

This unpublished crate demonstrates service registration, typed calls and
echo/read/write workloads. Run the commands below from the workspace root.
TCP, WebSocket and HTTP need neither RDMA libraries nor a device in the default
build.

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

The client takes one endpoint: one scheme and one or more equivalent server
addresses, for example `tcp://127.0.0.1:8000` or
`tcp://[::1]:8000,[::1]:8001`. TCP, WS, HTTP and RDMA all support groups;
RDMA uses `tcp+rdma://`, `ws+rdma://` or `http+rdma://`:

```bash
# Replace both addresses with addresses served by the same server State.
cargo run -p ruapc-demo --release --bin client -- 'tcp://192.0.2.10:8000,192.0.2.11:8000'
```

Addresses are sorted and deduplicated. A group has one endpoint scheme,
including the RDMA bootstrap transport; separate endpoint arguments or mixed
schemes inside a group are rejected. See
[endpoint identity and address fallback](../docs/endpoints.md) for the same-server
contract, independent overlapping groups and canonical string format.

Use `--listen-mode tcp`, `ws` or `http` on the server to select one protocol.
The server stops and joins its background tasks on Ctrl-C. RPC, setup and
workload failures make the client exit with a nonzero status.

The demo client uses JSON by default. Add `--use-msgpack` to select MessagePack
payloads on any typed transport; no server change is needed. This is a demo CLI
default: the library's `Client::default()` uses MessagePack.

## HTTP and OpenAPI

An ordinary HTTP caller can invoke registered methods with JSON when the server
uses `--listen-mode http` or `unified` (the demo default):

```bash
curl -s -H 'content-type: application/json' -d '"hello HTTP"' \
  http://127.0.0.1:8000/EchoService/echo
# {"Ok":"hello HTTP"}

curl -s http://127.0.0.1:8000/openapi.json
```

Open `http://127.0.0.1:8000/rapidoc` for interactive documentation or fetch
`http://127.0.0.1:8000/openapi.json` for the schema of every registered method,
including built-ins. `Request(String)` serializes as a JSON string, which is why
the echo request body is `"hello HTTP"`.

With an RDMA-enabled server and an initialized RDMA pool, discover its devices
through the same HTTP listener:

```bash
curl -s -H 'content-type: application/json' -d 'null' \
  http://127.0.0.1:8000/RdmaService/discover
```

The `()` request body is JSON `null`. Other built-ins can require live request
attachments or an RDMA connection; their presence in OpenAPI does not remove
those prerequisites. See the [built-in method catalog](../docs/builtin-services.md).

To mount HTTP endpoints below a prefix, set the same base path on both peers:

```bash
# Server terminal:
cargo run -p ruapc-demo --release --bin server -- 127.0.0.1:8000 --http-base-path /api/v1
# Client terminal:
cargo run -p ruapc-demo --release --bin client -- http://127.0.0.1:8000 --http-base-path /api/v1
```

The routes then include `/api/v1/EchoService/echo`,
`/api/v1/openapi.json` and `/api/v1/rapidoc`.
Pass the prefix separately from the `http://address[,address...]` endpoint.
This group form is a RuaPC endpoint string; ordinary HTTP callers still use
a single-address URL.
RuaPC's typed HTTP client connects to the HTTP/2 bidirectional stream at
`/api/v1/_rpc`, which supports reverse RPC and remote memory. Ordinary HTTP
POST calls do not support reverse RPC.

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
lengths and reuse them across calls; see the [remote-memory API guide](../docs/remote-memory.md)
for attachments and logical copy spaces. Read attachments own immutable sources.
`BufferReply` reports destination availability explicitly; destinations are
recoverable only when uniquely held. Failed or cancelled DMA can leave them
unavailable.
RDMA validates successful reads after READ completion, but source recovery does
not wait for remote DMA. Readable destinations from a failed transfer are not
valid results. See [read validation and recovery](../docs/safe-boundaries.md#application-read-source-validation-and-recovery)
for the immutable-source/request-ID conditions and the separate `remote_write`
completion path.

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
another. Requests enter warmup or measurement by their start time; in-flight
calls can finish after `--secs` and still contribute to the result. The final
`measured` duration includes this tail and excludes warmup; throughput divides
successful measured calls by that duration.
Warmup calls are excluded from reported counts and latency, but any failed
call, including one during warmup, makes the final exit status nonzero.

Echo uses `--value` for individual calls and stress, and a generated string of
`--payload-size` bytes for bench. Read/write use `--buffer-size` per coroutine.
Load workers verify echo contents or CRC32C on every call; the default one-shot
echo/greet command only prints responses. Read/write MiB/s counts successful
logical payload bytes. See the [measurement guide](../docs/benchmark.md#demo-measurements)
for timing boundaries, fixed-concurrency interpretation and comparisons.

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
and server transfer buffers, accounting for the pool's
[size classes and backing blocks](../ruapc-bufpool/README.md#allocation).
Workload preparation fails if its buffers cannot be allocated.

RDMA connections require the `rdma` Cargo feature, a C compiler, `pkg-config`,
libclang and the libibverbs development package. At runtime it needs an active RDMA
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
  tcp+rdma://127.0.0.1:8000 --rdma-devices mlx5_0
```

With the feature enabled, `server` initializes RDMA by default. It must serve
the bootstrap transport named by the client endpoint: `tcp`, `ws`, `http` or
`unified`. Add `--no-rdma` to run that build without opening RDMA devices,
including with a dedicated HTTP or WebSocket listener:

```bash
cargo run -p ruapc-demo --features rdma --bin server -- --no-rdma --listen-mode http
```

`--no-rdma` is available only in RDMA-enabled server builds. A build without
the feature needs neither this flag nor RDMA libraries. The client enables
RDMA automatically only for a `<bootstrap>+rdma://` endpoint. Builds without
the feature still parse that endpoint format, but report an error when making
a call.

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
| [`tests/`](tests/) | Server process lifecycle, stream RPC contracts, CLI failures and workload verification. |

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
