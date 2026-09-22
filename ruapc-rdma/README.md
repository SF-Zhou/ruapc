# ruapc-rdma

libibverbs bindings with RAII resources and buffer-owning work requests.
Part of [RuaPC](../README.md), usable independently for low-level verbs operations.
RPC bootstrap, path selection, flow control and poll threads live in `ruapc`.

## Requirements and discovery

Build on Linux with a C compiler, `pkg-config`, libclang and the libibverbs
development package (`libibverbs-dev` on Debian/Ubuntu). An RDMA NIC or Soft-RoCE
device is needed to run device operations and hardware tests, not to build.
Memory registration also needs sufficient locked-memory allowance.

```rust,no_run
fn main() -> Result<(), Box<dyn std::error::Error>> {
    for device in ruapc_rdma::ActiveDevice::available()? {
        let info = device.info();
        println!("{}: guid={} ports={}", info.name, info.guid, info.ports.len());
        for port in &info.ports {
            for flag in port.port_attr.port_cap_flags {
                println!("  port {}: {}", port.port_num, flag.name());
            }
        }
    }
    Ok(())
}
```

Inspect devices with the bundled `ibv_devinfo` implementation:

```bash
cargo run -p ruapc-rdma --features bin --bin ibv_devinfo -- -v
```

Device snapshots support serde and JSON Schema. Capability masks use typed
`enumflags2::BitFlags`; GIDs are classified as IB, RoCE v1 or RoCE v2.

## Resources and completions

```text
Context
  ├─ ProtectionDomain
  │    ├─ MemoryRegion → retains backing memory
  │    └─ QueuePair    → retains send/receive CQs and work requests
  ├─ CompChannel
  └─ CompletionQueue  → retains an optional completion channel
```

Ownership through `Arc` keeps parent resources alive until their children are
dropped. `QueuePair::send` and `recv` take ownership of pool buffers, and
`prepare_reads` takes ownership of READ destinations. The crate also supports
gather-list sends and selective signaling; a signaled completion can reclaim
preceding unsignaled SENDs.

Use `CompletionQueue::poll_batch` with reusable `CompletionBatch` storage and
pass each CQ-issued `Completion` to `QueuePair::complete`. The borrowed proof
is non-cloneable and checked against the originating CQ, hardware QPN and QP
sequence floor. Raw metadata from `CompletionQueue::poll` cannot authorize
buffer recovery. Applications sharing a CQ route completions by `qp_num()`.

WRIDs encode a two-bit type and a 62-bit sequence. SQ and RQ use independent
sequences; SEND variants and READ share SQ. CQ-owned QPN leases and a retirement
floor prevent completions from a destroyed QP from releasing a replacement's
buffers. Sequences never wrap. Posting uses per-QP counters; the registry lock
is limited to registration, retirement and explicit statistics.

`CompletionQueue::capacity()` reports the provider's CQE capacity, not a QP
count limit. Applications must bound outstanding completions themselves.
SEND/RECV buffers use a separate bounded slot array; slot exhaustion rejects
the new submission without replacing an existing buffer. See
[WRID and completion identity](../docs/wrid.md) and
[QP registry and CQ capacity](../docs/qp-registry.md) for the full invariants.

## READ lifetime

`prepare_reads` validates destination buffer indices, bounds and overlap, then
resolves local addresses and registration keys internally. Its non-cloneable
`ReadPosting` cursor posts each request once, only to the preparing QP. Dropping
the cursor accounts for its unposted suffix while the QP retains posted work.
Explicit cancellation before any post can recover the destination vector.

Timeout or connection failure can notify a receiver before DMA has stopped.
Posted destinations remain owned until all posted READs complete or the provider
successfully destroys the QP. Dropping or forgetting a future cannot release
them early; forgetting an owner can retain resources indefinitely. Destruction
also releases remaining unsignaled SENDs. Failure to destroy a QP aborts the
process before memory still accessible to DMA can be released.

These guarantees cover local destinations. Remote addresses and keys do not
provide a source-side completion lease. The RPC layer's post-READ pending check
rejects stale results but cannot delay source recovery until remote DMA has
completed. See [safety boundaries](../docs/safe-boundaries.md).

## Registration and FFI

`ActiveDevice` implements `ruapc_bufpool::MemoryRegistrar` in
`src/buffer_registration.rs`. Safe `Device` wrappers supply that audited
registrar without receiving pool backing memory. `MemoryRegion` retains memory
and its protection domain; registration grants no independent access to bytes
owned by pool buffers. Queue operations enforce their own DMA lifetimes.

`build.rs` compiles `src/shim.c` against the installed `<infiniband/verbs.h>` and
generates bindings with bindgen. Every verbs entry point used by the crate goes
through a `ruapc_ibv_*` C wrapper so header macros and static inline functions
are applied. This avoids accidentally binding only a legacy exported symbol.

| Module | Responsibility |
| --- | --- |
| `src/shim.{h,c}`, `build.rs` | C wrappers, build detection and binding generation |
| `src/ffi/` | Generated bindings and extensions |
| `src/types/` | Device snapshots, identifiers and other value types |
| `src/verbs/` | Resource wrappers, posting and completion ownership |
| `src/bin/ibv_devinfo.rs` | Device inspection CLI |

## Tests

```bash
cargo test -p ruapc-rdma
```

Device tests open real devices. Setting `RUAPC_PREFER_RXE=1` restricts their
selection to Soft-RoCE devices named `rxe*`, as in CI. See
[CONTRIBUTING.md](../CONTRIBUTING.md) for setup and workspace checks.
