# ruapc-ws

WebSocket transport implementation for the RuaPC RPC library.

## Overview

This crate provides WebSocket-based transport for RuaPC:

- `WebSocket` - WebSocket implementing `RuapcSocket`
- `WebSocketPool` - WebSocket pool implementing `RuapcSocketPool`

## Features

- WebSocket protocol with binary message frames
- Connection pooling with automatic reconnection
- HTTP upgrade support for server-side connections
- Background tasks for send/receive loops
- Graceful shutdown support

## Usage

```rust
use ruapc_ws::WebSocketPool;
use ruapc_transport::RuapcSocketPool;

let pool = WebSocketPool::new();
```
