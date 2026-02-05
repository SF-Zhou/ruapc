# ruapc-async

Async utilities for the RuaPC RPC library.

## Overview

This crate provides reusable async components:

- **TaskSupervisor**: Task lifecycle management for graceful shutdown

## Features

- Zero external dependencies beyond tokio/tokio-util
- Reusable outside the RuaPC ecosystem
- Lightweight and focused

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
ruapc-async = "0.1"
```

### TaskSupervisor Example

```rust
use ruapc_async::TaskSupervisor;

#[tokio::main]
async fn main() {
    let supervisor = TaskSupervisor::create();
    let guard = supervisor.start_async_task();
    
    tokio::spawn(async move {
        // Task work here
        drop(guard); // Automatically decrements running count
    });
    
    supervisor.stop();
    supervisor.all_stopped().await;
}
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
