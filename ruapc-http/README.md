# ruapc-http

HTTP transport implementation for the RuaPC RPC library.

## Overview

This crate provides HTTP-based transport for RuaPC:

- `HttpSocket` - HTTP socket implementing `RuapcSocket`
- `HttpSocketPool` - HTTP socket pool implementing `RuapcSocketPool`
- `HttpRequestHandler` - Trait for delegating RPC-specific handling

## Features

- HTTP/1.1 client and server support
- Connection pooling with keep-alive
- WebSocket upgrade support (delegates to ruapc-ws)
- Built-in static file serving for API documentation:
  - `/openapi.json` - OpenAPI specification
  - `/rapidoc/` - RapiDoc API documentation UI
- JSON request/response handling

## Usage

```rust
use ruapc_http::HttpSocketPool;
use ruapc_transport::RuapcSocketPool;

let pool = HttpSocketPool::new();
```
