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

### GID query backends

By default, `Context::query_gid_entry` queries a GID and its type together with the
kernel's `QUERY_GID_ENTRY` ioctl. Device snapshots use the device-wide
`QUERY_GID_TABLE` ioctl, so collecting every port's GIDs needs one ioctl
instead of one per index; `query_gid` and `query_gid_type` also try the
per-index ioctl first. The implementation does not require
`ibv_query_gid_ex`, so it can use a newer kernel with older libibverbs
releases, including rdma-core v28 from Ubuntu 20.04. Both methods were
introduced in Linux 5.10; older kernels need a backport or the legacy
fallback.

The ioctl requires a driver ID. Discovery uses RDMA netlink and checks the
returned character-device identity against the opened uverbs FD. If discovery
is unavailable, or the ioctl returns `ENOTTY` / `EPROTONOSUPPORT`, queries fall
back to `ibv_query_gid` and sysfs GID types; snapshots then query each GID
index separately, and a `QUERY_GID_TABLE` response that does not fit falls
back the same way. Driver IDs and unsupported-method results are cached per
context; transient discovery failures can be retried after a one-second
cooldown. Other ioctl errors, including empty GID entries, do not fall back.
Native InfiniBand ports need no sysfs type file; Ethernet ports still require
one on the legacy path. The inspection CLI's optional `board_id` remains a
sysfs query.

Enable the optional `ibv-query-gid-ex` feature to use libibverbs' public
`ibv_query_gid_ex` and `ibv_query_gid_table` APIs directly:

```bash
cargo build -p ruapc-rdma --features ibv-query-gid-ex
```

This feature is disabled by default and requires libibverbs headers and a
runtime library from rdma-core v32 or later. Both APIs were introduced in
[rdma-core v32](https://github.com/linux-rdma/rdma-core/blob/v32.0/libibverbs/verbs.h#L2225-L2250).
Per-index queries use `ibv_query_gid_ex`; snapshots use `ibv_query_gid_table`
and fall back to per-index queries if the table query fails. This backend
does not compile in the crate's ioctl, driver discovery or sysfs fallback;
any fallback inside libibverbs is controlled by the installed library.
The public `Context` query methods remain the same in both modes.

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

`build.rs` compiles the C shim against the installed `<infiniband/verbs.h>` and
generates bindings with bindgen. Every libibverbs entry point used by the crate
goes through a `ruapc_ibv_*` C wrapper so header macros and static inline
functions are applied. This avoids accidentally binding only a legacy exported
symbol. GID backend selection happens at the Rust module and C build boundaries,
so callers share one query interface. The default direct GID query keeps its
newer UAPI method IDs and response layout private, allowing builds with old
kernel headers. Its translation unit is separate from `verbs.h` to avoid
vendor/distro header conflicts.

| Module | Responsibility |
| --- | --- |
| [`ffi/c/verbs.h`](src/ffi/c/verbs.h), [`ffi/c/verbs.c`](src/ffi/c/verbs.c), [`build.rs`](build.rs) | C wrappers, build detection and binding generation |
| [`context/gid.rs`](src/verbs/context/gid.rs), [`context/gid/`](src/verbs/context/gid/) | Shared GID query API and compile-time backend selection |
| [`ffi/c/gid/compat.c`](src/ffi/c/gid/compat.c), [`ioctl.c`](src/ffi/c/gid/ioctl.c), [`driver.c`](src/ffi/c/gid/driver.c) | Default GID ioctl and RDMA netlink driver discovery |
| [`ffi/c/gid/native.c`](src/ffi/c/gid/native.c) | Optional libibverbs GID query wrappers |
| [`src/ffi/`](src/ffi/), [`src/types/`](src/types/) | Bindings, extensions, device snapshots and identifiers |
| [`src/verbs/`](src/verbs/) | Resource wrappers, posting and completion ownership |
| [`ibv_devinfo.rs`](src/bin/ibv_devinfo.rs) | Device inspection CLI |

## Tests

```bash
cargo test -p ruapc-rdma
cargo test -p ruapc-rdma --features ibv-query-gid-ex
bash ruapc-rdma/tests/test_queries.sh
bash ruapc-rdma/tests/test_queries.sh --native
```

The C query tests in [`tests/gid/`](tests/gid/) check ioctl encoding and
netlink parsing without RDMA hardware; they still compile against the
installed libibverbs headers.
`--native` additionally checks the libibverbs GID wrappers, including flags,
entry sizes and error returns, and needs the newer headers. Set
`CC=clang CFLAGS='-fsanitize=address,undefined'` to also run them with sanitizers.

Run both Rust commands to exercise the default and libibverbs GID backends;
`--all-features` selects only the latter. Device tests open real devices.
Setting `RUAPC_PREFER_RXE=1` restricts their selection to Soft-RoCE devices
named `rxe*`, as in CI. See
[CONTRIBUTING.md](../CONTRIBUTING.md) for setup and workspace checks.

Standalone ownership benchmarks live in `benches/` and use the optional
`bench-internals` feature. See the [benchmark guide](../docs/benchmark.md#rdma-ownership-harnesses)
for commands, measurement boundaries and the allocator's cross-thread handoff
benchmark. These measurements require build dependencies but no RDMA device.
