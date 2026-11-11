# RuaPC

[![crates.io](https://img.shields.io/crates/v/ruapc.svg)](https://crates.io/crates/ruapc)
[![docs.rs](https://docs.rs/ruapc/badge.svg)](https://docs.rs/ruapc)

Typed, bidirectional RPC for Rust over TCP, WebSocket, HTTP/2 and optional RDMA,
using Tokio. Define a service once with `#[ruapc::service]` and use the same trait
and return types for its server implementation and generated clients.

RuaPC is under active development. **API stability is not guaranteed before the
first stable release.**

For the design and a complete service using remote reads and writes across
transports, see the [project README](https://github.com/SF-Zhou/ruapc#example).

## Quick start

Add these dependencies to your application's `Cargo.toml`. TCP, WebSocket and
HTTP are available without feature flags or RDMA libraries.

```toml
[dependencies]
ruapc = "0.2.0-alpha.8"
schemars = "1.2"
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

This complete program registers an echo service, calls it over TCP and shuts
down the server:

```rust
use std::sync::Arc;

use ruapc::{Client, Context, Endpoint, Result, Router, Server, SocketPoolConfig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema)]
struct Request {
    message: String,
}

#[ruapc::service]
trait EchoService {
    async fn echo(&self, ctx: &Context, request: &Request) -> Result<String>;
}

struct Echo;

impl EchoService for Echo {
    async fn echo(&self, _ctx: &Context, request: &Request) -> Result<String> {
        Ok(request.message.clone())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let config = SocketPoolConfig::default();
    let mut router = Router::default();
    EchoService::ruapc_export(Arc::new(Echo), &mut router);
    let server = Server::create(router, &config)?;
    let addr = server.listen("127.0.0.1:0".parse()?).await?;

    let ctx = Context::create(&config)?.with_endpoint(Endpoint::tcp([addr])?);
    let request = Request {
        message: "Rua!".into(),
    };
    let response = Client::default().echo(&ctx, &request).await?;
    assert_eq!(response, "Rua!");

    server.stop();
    server.join().await;
    Ok(())
}
```

The macro implements `EchoService` for `Client` and `ClientWithBuffers`. Import
the service trait wherever you call its generated methods. Request and response
types use Serde for serialization and `schemars::JsonSchema` for schemas.
Service methods take `&self`, `&Context` and `&Request`, and return a result;
custom error types must also implement `From<ruapc::Error>`. See the
[service macro](https://docs.rs/ruapc-macro/latest/ruapc_macro/) for declaration
rules and error handling.

`Context` holds shared runtime state and the selected endpoint. Reuse it, or derive
contexts with `with_endpoint`, to share connection pools. `Client` holds call
settings; creating another client does not create another connection pool.

## Transports and listeners

An `Endpoint` selects the outbound transport. The server's
`SocketPoolConfig::listen_mode` selects which protocols its listener accepts;
the default is TCP.

| Endpoint example | Required listener mode | Cargo feature |
|---|---|---|
| `tcp://127.0.0.1:8000` | `ListenMode::TCP` | None |
| `ws://127.0.0.1:8000` | `ListenMode::WS` | None |
| `http://127.0.0.1:8000` | `ListenMode::HTTP` | None |
| `tcp+rdma://127.0.0.1:8000` | `ListenMode::TCP` for bootstrap | `rdma` |
| `ws+rdma://127.0.0.1:8000` | `ListenMode::WS` for bootstrap | `rdma` |
| `http+rdma://127.0.0.1:8000` | `ListenMode::HTTP` for bootstrap | `rdma` |

Combine listener flags with `|`, or use `ListenMode::UNIFIED` for TCP, WebSocket
and HTTP on the same port. For example, replace the quick start's configuration
with this to enable TCP and HTTP:

```rust
use ruapc::{ListenMode, SocketPoolConfig};

let config = SocketPoolConfig {
    listen_mode: ListenMode::TCP | ListenMode::HTTP,
    ..Default::default()
};
```

Parse a string with `"http://127.0.0.1:8000".parse::<Endpoint>()?` to select HTTP.
RuaPC's typed HTTP client uses a persistent HTTP/2 stream with reverse RPC
support. Ordinary HTTP clients can instead send unary JSON requests to
`POST /EchoService/echo` with a body such as `{"message":"Rua!"}`; the successful
response is `{"Ok":"Rua!"}`. Unary HTTP does not support reverse RPC or buffer
attachments. HTTP mode also serves `/openapi.json` and `/rapidoc`.
`SocketPoolConfig::http_base_path` prefixes these routes.

Endpoint strings use numeric IP addresses and ports. A comma-separated group,
such as `tcp://127.0.0.1:8000,127.0.0.1:8001`, provides alternative addresses for
the same server. All addresses must share server state and, for RDMA, lease
records. The complete normalized group identifies a connection-pool entry;
different groups remain independent even when they overlap. See the
[endpoint guide](https://github.com/SF-Zhou/ruapc/blob/main/docs/endpoints.md)
for syntax, identity and address fallback.

## Call settings

Configure `Client::timeout` for the response budget, `connect_timeout` for
connection establishment, and `max_retries` for failures before transport
acceptance. Requests are never automatically retried after acceptance, when
execution may already have occurred. Nested calls inherit the parent context's
remaining deadline. Expiry does not cancel a running handler; long-running
handlers can check `Context::is_expired()`.

Set `Client::use_msgpack` to `false` to use JSON payloads. Set
`SocketPoolConfig::max_inflight_requests` to limit concurrent server handlers;
excess requests receive an overload error. Install a recorder for the
[`metrics` facade](https://docs.rs/metrics/) before traffic to collect metrics.
See the API documentation for
[`Client`](https://docs.rs/ruapc/latest/ruapc/struct.Client.html) and
[`SocketPoolConfig`](https://docs.rs/ruapc/latest/ruapc/struct.SocketPoolConfig.html)
for defaults and configuration details.

## Remote memory

`Client::with_read_buffers` borrows immutable sources. `with_write_buffers`
attaches owned destinations; `with_write_buffers_mut` lends caller-owned buffer
slots. Handlers use `Context::remote_read` and `remote_write` with logical
`CopyOp` ranges through the same API on every bidirectional transport.

Handlers and clients share `Result<BufferReply<T>>` for responses with write
destinations. On the client, `reply.response()` accesses the application value
and `reply.destinations()` reports owned destination availability.

The [remote-memory guide](https://github.com/SF-Zhou/ruapc/blob/main/docs/remote-memory.md)
covers usage examples, buffer sizing, transfer receipts and recovery rules.
See
[safety boundaries](https://github.com/SF-Zhou/ruapc/blob/main/docs/safe-boundaries.md)
for source validation, cancellation and ownership guarantees.

## Optional RDMA support

RDMA is disabled by default. Enable it in your dependency:

```toml
ruapc = { version = "0.2.0-alpha.8", features = ["rdma"] }
```

RDMA builds require a C compiler, `pkg-config`, libclang and libibverbs
development files. Running requires a usable RDMA device and sufficient
locked-memory allowance. With the feature enabled,
`SocketPoolConfig::default()` initializes RDMA resources; set `config.rdma = None`
to disable them for a server or context. The listener must accept the bootstrap
transport named by the RDMA endpoint.

The `rdma-ibv-query-gid-ex` feature also enables RDMA and selects libibverbs'
extended GID query backend. It requires headers and a runtime library from
rdma-core v32 or later. See
[build prerequisites](https://github.com/SF-Zhou/ruapc/blob/main/CONTRIBUTING.md#prerequisites)
and [RDMA configuration](https://github.com/SF-Zhou/ruapc/blob/main/docs/rdma-connection.md).

## Documentation

- [API reference](https://docs.rs/ruapc)
- [Guides and contracts](https://github.com/SF-Zhou/ruapc/blob/main/docs/README.md)
- [Runnable demos](https://github.com/SF-Zhou/ruapc/blob/main/ruapc-demo/README.md)
- [Changelog](https://github.com/SF-Zhou/ruapc/blob/main/CHANGELOG.md)

## License

Dual-licensed under [MIT](https://github.com/SF-Zhou/ruapc/blob/main/LICENSE-MIT)
and [Apache-2.0](https://github.com/SF-Zhou/ruapc/blob/main/LICENSE-APACHE).
