# ruapc-rdma

libibverbs bindings with RAII resources and buffer-owning work requests.
Part of [RuaPC](../README.md), usable independently for low-level verbs operations.
RPC bootstrap, path selection, flow control and poll threads live in `ruapc`.
For that layer, see [RDMA connection setup](../docs/rdma-connection.md).

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
cargo build -p ruapc-rdma
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

`CompletionQueue::capacity()` reports the provider's CQE capacity, not a QP
count limit. Applications must bound outstanding completions themselves.
SEND/RECV buffers use a separate bounded slot array; slot exhaustion rejects
the new submission without replacing an existing buffer. Non-wrapping
sequences and CQ-owned QPN leases prevent stale completion reuse; see
[completion identity and posting](../docs/wrid.md) for the detailed contract and
[RuaPC's CQ budgeting](../docs/rdma-capacity.md) for the RPC layer's admission
and retirement policy.

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

These guarantees cover local destinations; remote addresses and keys provide
no source-side completion lease. RuaPC validates `remote_read` results after
READ completion under the immutable-source and original-request-ID contract.
That validation does not make source recovery wait for DMA, and readable error
destinations are not valid transfer results. See
[application read validation and recovery](../docs/safe-boundaries.md#application-read-source-validation-and-recovery)
for the proof and `remote_write`'s separate reverse-RPC completion path.

## Registration and FFI

`ActiveDevice` implements `ruapc_bufpool::MemoryRegistrar` in
[`buffer_registration.rs`](src/buffer_registration.rs). `MemoryRegion` retains
memory and its protection domain, while queue operations enforce DMA lifetimes.
Registration itself grants no independent byte access; see the
[registration contract](../docs/safe-boundaries.md#registration-and-byte-access).

`build.rs` compiles `src/shim.c` against the installed `<infiniband/verbs.h>` and
generates bindings with bindgen. Every verbs entry point used by the crate goes
through a `ruapc_ibv_*` C wrapper so header macros and static inline functions
are applied. This avoids accidentally binding only a legacy exported symbol.

| Module | Responsibility |
| --- | --- |
| [`shim.h`](src/shim.h), [`shim.c`](src/shim.c), [`build.rs`](build.rs) | C wrappers, build detection and binding generation |
| [`src/ffi/`](src/ffi/), [`src/types/`](src/types/) | Bindings, extensions, device snapshots and identifiers |
| [`src/verbs/`](src/verbs/) | Resource wrappers, posting and completion ownership |
| [`ibv_devinfo.rs`](src/bin/ibv_devinfo.rs) | Device inspection CLI |

## Tests

```bash
cargo test -p ruapc-rdma
```

Device tests open real devices. Setting `RUAPC_PREFER_RXE=1` restricts their
selection to Soft-RoCE devices named `rxe*`, as in CI. See
[CONTRIBUTING.md](../CONTRIBUTING.md) for setup and workspace checks.
