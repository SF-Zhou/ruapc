# ruapc-transport

Transport layer traits and common types for the RuaPC RPC library.

## Overview

This crate provides the trait-based abstractions for implementing transport protocols in RuaPC:

- `RuapcSocket` - Trait for socket implementations
- `RuapcSocketPool` - Trait for socket pool implementations
- `MessageHandler` - Trait for handling received messages
- `SocketType`, `SocketPoolConfig`, `RawStream` - Common types

## Architecture

The transport layer uses traits to enable different transport implementations (TCP, WebSocket, HTTP, RDMA) to be used interchangeably. This allows:

- Independent versioning of transport implementations
- Cleaner dependency management (only depend on transports you need)
- Easier testing with mock implementations

## Usage

```rust
use ruapc_transport::{RuapcSocket, RuapcSocketPool, SocketPoolConfig, SocketType};

// Create a socket pool configuration
let config = SocketPoolConfig {
    socket_type: SocketType::TCP,
};
```

## Related Crates

- `ruapc-tcp` - TCP transport implementation
- `ruapc-ws` - WebSocket transport implementation
- `ruapc-http` - HTTP transport implementation
