# ruapc-tcp

TCP transport implementation for the RuaPC RPC library.

## Overview

This crate provides TCP-based transport for RuaPC:

- `TcpSocket` - TCP socket implementing `RuapcSocket`
- `TcpSocketPool` - TCP socket pool implementing `RuapcSocketPool`

## Protocol

The TCP transport uses a simple framing protocol:
- 4-byte magic number: `RUA!` (0x52554121)
- 4-byte length: Total message length (big-endian)
- Variable-length message body

## Features

- Connection pooling with automatic reconnection
- Efficient vectored I/O for sending multiple messages
- Background tasks for send/receive loops
- Graceful shutdown support

## Usage

```rust
use ruapc_tcp::TcpSocketPool;
use ruapc_transport::RuapcSocketPool;

let pool = TcpSocketPool::new();
```
