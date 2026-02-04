# ruapc-core

Core types and traits for the RuaPC RPC library.

## Overview

This crate provides fundamental types that are used across the RuaPC ecosystem:

- **Error types**: `Error`, `ErrorKind`, `Result`
- **Message types**: `Message`, `MsgMeta`, `MsgFlags`
- **Payload abstraction**: `Payload`
- **Serialization trait**: `SendMsg`

## Features

- `rdma` - Enables RDMA buffer support in Payload

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
ruapc-core = "0.1"
```

### Example

```rust
use ruapc_core::{Error, ErrorKind, Result, MsgMeta, MsgFlags};

fn example() -> Result<()> {
    let meta = MsgMeta {
        method: "MyService/my_method".to_string(),
        flags: MsgFlags::IsReq,
        msgid: 1,
    };
    assert!(meta.is_req());
    Ok(())
}
```

## Architecture

This crate is designed to be a lightweight dependency that provides the core types without pulling in the full RPC transport layer. It's used by:

- `ruapc` - The main RPC library
- `ruapc-rdma` - RDMA support (optional)
- Custom implementations that need to work with RuaPC messages

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
