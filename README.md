# RuaPC

[![Rust](https://github.com/SF-Zhou/ruapc/actions/workflows/rust.yml/badge.svg)](https://github.com/SF-Zhou/ruapc/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/SF-Zhou/ruapc/graph/badge.svg?token=G3US2MDB26)](https://codecov.io/gh/SF-Zhou/ruapc)
[![crates.io](https://img.shields.io/crates/v/ruapc.svg)](https://crates.io/crates/ruapc)
[![stability-wip](https://img.shields.io/badge/stability-wip-lightgrey.svg)](https://github.com/mkenney/software-guides/blob/master/STABILITY-BADGES.md#work-in-progress)

A Rust RPC library.

<img src="docs/logo.png" alt="RuaPC" width="256" height="256">

## Example

Define service:

```rust
#![feature(return_type_notation)]

use ruapc::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request(pub String);

#[ruapc::service]
pub trait EchoService {
    async fn echo(&self, c: &Context, r: &Request) -> Result<String>;
}
```

Start server:

```rust
use ruapc::*;
use ruapc_demo::{EchoService, Request};
use std::{net::SocketAddr, str::FromStr, sync::Arc};

struct DemoImpl;

impl EchoService for DemoImpl {
    async fn echo(&self, _c: &Context, r: &Request) -> Result<String> {
        Ok(r.0.clone())
    }
}

#[tokio::main]
async fn main() {
    let demo = Arc::new(DemoImpl);
    let mut router = Router::default();
    router.add_methods(EchoService::ruapc_export(demo.clone()));
    let server = Server::create(router, &SocketPoolConfig::default());

    let server = Arc::new(server);
    let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
    let addr = server.listen(addr).await.unwrap();
    println!("Serving on {addr}...");
    server.join().await
}
```

Make a request:

```rust
use ruapc::*;
use ruapc_demo::{EchoService, Request};
use std::{net::SocketAddr, str::FromStr};

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
    let ctx = Context::create(&SocketPoolConfig::default()).with_addr(addr);
    let client = Client::default();

    let rsp = client.echo(&ctx, &Request("Rua!".into())).await;
    println!("echo rsp: {:?}", rsp);
}
```
