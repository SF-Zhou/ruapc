# ruapc-rdma

Low-level FFI bindings to libibverbs (RDMA verbs) with type-safe, RAII-based
resource management. This crate is part of the [ruapc](../ruapc/) project but
is independently usable by applications that need resource ownership and
explicit DMA lifetime contracts over raw verbs.

## Features

- **RAII resource wrappers**: `Context`, `ProtectionDomain`, `CompletionQueue`,
  `CompChannel`, `MemoryRegion`, and `QueuePair` free their verbs resources on
  drop; `Arc` ownership chains guarantee parents outlive children regardless of
  user drop order
- **C shim for every verbs entry point**: Rust never binds an `ibv_*` symbol
  directly (see below)
- **Buffer-owning work requests**: `QueuePair::send`/`recv` take ownership of a
  [`ruapc-bufpool`](../ruapc-bufpool/) `Buffer`. Poll `CompletionQueue` and recover
  buffers through `QueuePair::complete` using a CQ-issued, non-cloneable
  `Completion` proof. Shared CQs route by the WRID tag.
- **Lock-free SEND/RECV tracking**: buffers of those posted work requests live in
  `WrSlots`, a fixed-size atomic slot array indexed by monotonic per-direction
  IDs — no `Mutex<HashMap>` on the completion path
- **Selective signaling** with completion-driven reclamation of unsignaled
  SEND buffers (RC send queues complete in order), plus gather-list sends and
  owned vectored RDMA READ plans (`prepare_reads` / `post_read`)
- **Typed work request IDs**: `WRID` packs a work request type, an opaque
  connection tag, and a per-direction sequence number into the 64-bit `wr_id`
- **Serializable device snapshots**: `DeviceInfo`/`Port`/`Gid` (and the raw
  `ibv_device_attr`/`ibv_port_attr`) implement serde + schemars; GID types are
  classified (IB / RoCE v1 / RoCE v2) and non-routable GIDs filtered out
- **Typed capability flags**: bindgen emits `ibv_device_cap_flags`,
  `ibv_port_cap_flags`, and `ibv_port_cap_flags2` as serializable `enumflags2`
  enums and uses `BitFlags` directly in device/port attributes; combinations
  support iteration and static names without lookup tables. The Rust
  `ibv_port_cap_flags2` uses `u16` to match its only bound struct field rather
  than the standalone C enum's ABI width

## DMA ownership

`CompletionQueue::poll_batch` fills reusable stack storage and lends a unique
proof for each CQE. `QueuePair::complete` validates the originating CQ, QP number
and permanently assigned WRID tag before returning SEND/RECV buffers or settling
READ ownership. Copying raw CQE metadata cannot authorize reclamation. Creating
and validating the borrowed proof needs no allocation or reference-count update;
READ batch accounting retains its own synchronization.

Each CQ keeps a 512 KiB bitmap of connection tags claimed during its lifetime.
`set_wr_tag` accepts a tag exactly once and prevents reuse even after QP destruction,
so a retained completion cannot release memory on a replacement QP. The core
poller retires a slot after its 256 generations instead of wrapping its tag.

`prepare_reads` takes a destination `Vec<Buffer>` and buffer-index/offset/length
descriptors. It validates local bounds and cross-request overlap, and derives
the local addresses and keys itself. The returned non-cloneable `ReadPosting`
cursor can post each request once, only to its preparing QP. Dropping the cursor
accounts its unposted suffix; the QP retains already posted destinations.
Explicit cancellation before any post can recover the vector. Timeout or
connection failure notifies the receiver without recycling memory still visible
to the NIC; dropping or forgetting a future cannot authorize early recovery.
Buffers remain owned until all posted READs complete or their QP is destroyed.
Forgetting an owner can retain resources indefinitely. Successful QP destruction
also releases any remaining unsignaled SENDs; a provider failure to destroy a QP
aborts the process because dropping its memory holds would permit ongoing DMA
into recycled storage.

These guarantees concern local destinations. Remote source addresses and keys
do not carry a source-side completion lease. The RPC layer's post-READ pending
check rejects stale results but cannot delay source recovery until an
unobservable remote DMA completion.

## Buffer-pool registration

`ActiveDevice` implements `ruapc_bufpool::MemoryRegistrar` in
`src/buffer_registration.rs`. `DeviceSet` calls this audited capability directly;
safe application `Device` wrappers supply a registrar reference without receiving
the backing allocation. Registration retains memory and its protection domain
through `MemoryRegion`, but does not grant independent access to bytes owned by
pool buffers. Queue operations separately own their DMA lifetime obligations.

## Why a C shim?

rdma-core evolves its ABI by keeping old exported symbols for already-compiled
binaries and redirecting newly compiled code to new semantics via function-like
macros or static inline wrappers in `<infiniband/verbs.h>` (`ibv_query_port`
and `ibv_reg_mr` both started life as plain functions and were later
macro-wrapped this way). bindgen binds exported symbols directly, so it would
silently keep the frozen legacy semantics forever — no compile error, just
subtly wrong behavior.

Instead, `build.rs` compiles `src/shim.c`, a C translation unit that wraps
*every* verbs entry point used by this crate (`ruapc_ibv_*`), against the
locally installed header. This guarantees "freshly compiled against this
platform's rdma-core" semantics for each call, and the C compiler type-checks
each wrapper against the real prototypes. The cost is one direct call per
invocation — not measurable even on the hottest path (empty `ibv_poll_cq` on
mlx5: 9.79 ns/op with and without the shim).

## Architecture

```text
Context (ibv_context)
  ├─ ProtectionDomain (ibv_pd)
  │    ├─ MemoryRegion (ibv_mr)      ← pins Arc<AlignedMemory>
  │    └─ QueuePair (ibv_qp)         ← + send CQ + recv CQ, WrSlots
  ├─ CompChannel (ibv_comp_channel)  ← event fd for poll(2)/epoll
  └─ CompletionQueue (ibv_cq)        ← + optional CompChannel
```

Source layout:

- `src/shim.{h,c}` — C wrappers for all verbs entry points
- `build.rs` — pkg-config probe, shim compilation, bindgen with custom type
  substitutions (`FwVer`, `Guid`, `WRID`, `LinkLayer`) and typed flag derives
- `src/ffi/` — included bindgen output plus extensions on generated types
  (`ibv_gid` ↔ IPv6, `ibv_wc` helpers, typed flag accessors, pthread wrappers)
- `src/types/` — crate-defined value types (`DeviceInfo`, `Guid`, `WRID`, ...)
- `src/verbs/` — the RAII resource wrappers listed above
- `src/bin/ibv_devinfo.rs` — reimplementation of the classic `ibv_devinfo`

## Usage

```rust,no_run
// Discover and open all usable RDMA devices (context + PD per device).
let devices = ruapc_rdma::ActiveDevice::available()?;
for dev in &devices {
    let info = dev.info();
    println!("{}: guid={} ports={}", info.name, info.guid, info.ports.len());
    for port in &info.ports {
        for flag in port.port_attr.port_cap_flags {
            println!("  port {}: {}", port.port_num, flag.name());
        }
    }
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

Higher-level connection management (bootstrap over TCP, QP negotiation,
multi-NIC path selection, completion poll threads) lives in the
[`ruapc`](../ruapc/) crate's `rdma` module; this crate deliberately stays a
thin verbs layer.

### `ibv_devinfo` binary

A drop-in style reimplementation of the classic tool, useful for checking
what this crate sees on a host:

```bash
cargo run -p ruapc-rdma --features bin --bin ibv_devinfo -- -v
```

## Requirements

- Linux with `libibverbs-dev` (rdma-core) and `pkg-config` installed
- libclang (for bindgen)
- An RDMA-capable NIC — or a Soft-RoCE (`rdma_rxe`) device — is only needed at
  runtime and for tests, not to build

## Testing

```bash
cargo test -p ruapc-rdma
```

Most tests open a real device. On machines with multiple devices, setting
`RUAPC_PREFER_RXE=1` restricts tests to a Soft-RoCE `rxe*` device (used by CI).
