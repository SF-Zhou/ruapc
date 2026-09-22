# RuaPC

[![Rust](https://github.com/SF-Zhou/ruapc/actions/workflows/rust.yml/badge.svg)](https://github.com/SF-Zhou/ruapc/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/SF-Zhou/ruapc/graph/badge.svg?token=G3US2MDB26)](https://codecov.io/gh/SF-Zhou/ruapc)
[![crates.io](https://img.shields.io/crates/v/ruapc.svg)](https://crates.io/crates/ruapc)
[![stability-wip](https://img.shields.io/badge/stability-wip-lightgrey.svg)](https://github.com/mkenney/software-guides/blob/master/STABILITY-BADGES.md#work-in-progress)

A Rust RPC library with a shared service API for TCP, WebSocket, HTTP and optional RDMA.

<img src="docs/logo.png" alt="RuaPC" width="256" height="256">

- `#[service]` generates typed clients, server dispatch and OpenAPI schemas.
- A unified listener accepts TCP, WebSocket and HTTP on one port, including RDMA bootstrap when enabled.
- Persistent TCP, WebSocket, HTTP/2 and RDMA connections support reverse RPC.
- Remote read/write transfers use RDMA READs or inline reverse RPC over the other transports.
- Payloads use MessagePack by default; clients can select JSON. HTTP POST endpoints accept JSON.
- RapiDoc serves interactive documentation; metrics use the `metrics` facade with an application-provided recorder.

See the [documentation index](docs/README.md), [architecture](DESIGN.md) and [contribution guide](CONTRIBUTING.md).

## Example

Add the dependencies below. TCP, WebSocket and HTTP require no RDMA libraries.

```toml
[dependencies]
ruapc = "0.2.0-alpha.5"
schemars = "1.0"
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

This program registers an echo service, makes a TCP call and shuts down the server:

```rust
use std::sync::Arc;

use ruapc::{Client, Context, Endpoint, Result, Router, Server, SocketPoolConfig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema)]
struct Request(String);

#[ruapc::service]
trait EchoService {
    async fn echo(&self, ctx: &Context, request: &Request) -> Result<String>;
}

struct Echo;

impl EchoService for Echo {
    async fn echo(&self, _ctx: &Context, request: &Request) -> Result<String> {
        Ok(request.0.clone())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let config = SocketPoolConfig::default();
    let mut router = Router::default();
    EchoService::ruapc_export(Arc::new(Echo), &mut router);
    let server = Server::create(router, &config)?;
    let addr = server.listen("127.0.0.1:0".parse()?).await?;

    let ctx = Context::create(&config)?.with_endpoint(Endpoint::tcp(addr));
    let response = Client::default().echo(&ctx, &Request("Rua!".into())).await?;
    assert_eq!(response, "Rua!");

    server.stop();
    server.join().await;
    Ok(())
}
```

The default listener accepts TCP. Use `ListenMode::UNIFIED` to accept the other
protocols, and select the outbound transport in the endpoint, for example
`"ws://127.0.0.1:8000".parse::<Endpoint>()?`.

## Run the demos

From this repository, start the server and run a client in another terminal:

```bash
cargo run -p ruapc-demo --release --bin server -- --listen-mode unified
cargo run -p ruapc-demo --release --bin client -- tcp://127.0.0.1:8000
```

The client also accepts `ws://` and `http://`. Add
`--stress --coroutines 128 --secs 10` for a load test. The demo client uses JSON
unless `--use-msgpack` is passed; the library's `Client::default()` uses MessagePack.

HTTP methods and reflection are available without a typed client:

```bash
curl -s -H 'content-type: application/json' -d '"hello HTTP"' \
  http://127.0.0.1:8000/EchoService/echo
# {"Ok":"hello HTTP"}

curl -s -H 'content-type: application/json' -d '{}' \
  http://127.0.0.1:8000/_ruapc.meta/describe
```

Open `http://127.0.0.1:8000/rapidoc` for interactive documentation.
Ordinary HTTP POST calls do not support reverse RPC; RuaPC's typed HTTP client
uses an HTTP/2 bidirectional stream.

For a base path, pass `--http-base-path /api/v1` to the demo server. Set the same
`SocketPoolConfig.http_base_path` on typed HTTP clients. RPC routes, the HTTP/2
stream and documentation then live at `/api/v1/ServiceName/method`,
`/api/v1/_rpc` and `/api/v1/rapidoc`. See [built-in services](docs/builtin-services.md)
for reflection and internal control methods.

## Remote read/write

Clients attach owned buffers; servers address their concatenated logical lengths
with `CopyOp { src_offset, dst_offset, len }`. Set each buffer's `len()` to the
intended transfer length: pool allocations initially expose their full size class.

For example, a service can read the client's source and copy it into the client's
write space, whose total logical length must cover the transfer:

```rust
use ruapc::{Context, Result, WithBuffers};

#[ruapc::service]
trait BlobService {
    async fn copy(&self, ctx: &Context, req: &()) -> Result<WithBuffers<u64>>;
}

struct Blob;

impl BlobService for Blob {
    async fn copy(&self, ctx: &Context, _req: &()) -> Result<WithBuffers<u64>> {
        let data = ctx.remote_read_all().await?;
        let total = data.iter().map(|buffer| buffer.len() as u64).sum();
        let sent = ctx.remote_write_all(data).await?;
        Ok(sent.reply(total))
    }
}
```

After registering the service, call it with allocated source and destination buffers:

```rust,ignore
let mut transfer = client.with_read_buffers(src).with_write_buffers(dst);
let (total, buffers) = transfer.copy(&ctx, &()).await?.into_parts();
let reusable_sources = transfer.take_read_buffers();
```

Read attachments can be reused across calls; attaching another source replaces
them. `read_buffers()` provides shared views, and `take_read_buffers()` returns
ownership only when no local reader or pending request holds the source. A `None`
result preserves the source for a later attempt.

Successful `WithBuffers` responses return available write destinations. After a
failed call, `take_write_buffers()` recovers only buffers already returned to the
wrapper. Cancelled or failed DMA can make recovery unavailable even after the QP
eventually recycles its memory. Posted destinations remain held until completion
or successful QP destruction. Read-source recovery does not prove that remote
one-sided DMA has completed; see the [safety boundaries](docs/safe-boundaries.md).

The [remote-memory demo](ruapc-demo/src/bin/remote_memory.rs) includes allocation,
service registration and data verification:

```bash
cargo run -p ruapc-demo --bin remote_memory -- --transport tcp
cargo run -p ruapc-demo --bin remote_memory --features rdma -- --transport rdma
```

## RDMA

Enable the `rdma` Cargo feature explicitly:

```toml
ruapc = { version = "0.2.0-alpha.5", features = ["rdma"] }
```

Building requires a C compiler, `pkg-config`, libclang and the libibverbs development
package (`libibverbs-dev` on Debian/Ubuntu). Running requires a usable RDMA device
and sufficient locked-memory allowance. With the feature enabled,
`SocketPoolConfig::default()` initializes RDMA; set `config.rdma = None` to disable
it for a particular context. RDMA servers require TCP or UNIFIED listen mode for
bootstrap.

```bash
sudo prlimit --pid $$ -l=unlimited
cargo run -p ruapc-demo --release --bin server --features rdma -- --listen-mode unified
# In another terminal with a sufficient locked-memory limit:
cargo run -p ruapc-demo --release --bin client --features rdma -- rdma://127.0.0.1:8000
```

See [RDMA connection establishment](docs/rdma-connection.md) for configuration,
path selection and diagnostics.

## Benchmarks and workspace

```bash
cargo bench -p ruapc --bench echo
cargo bench -p ruapc --bench remote_memory
```

Repository tests and RPC benchmarks enable RDMA through a development dependency.
See [benchmark instructions](docs/benchmark.md) for prerequisites, workload controls
and reproducible comparisons.

| Crate | Responsibility |
| --- | --- |
| `ruapc` | Clients, servers, routing, transports and remote-memory API |
| [`ruapc-bufpool`](ruapc-bufpool/README.md) | Buddy/slab allocation and device registration |
| `ruapc-macro` | `#[service]` code generation |
| [`ruapc-rdma`](ruapc-rdma/README.md) | libibverbs bindings and owned work requests |
| `ruapc-demo` | Example programs; not published |

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache-2.0](LICENSE-APACHE).
