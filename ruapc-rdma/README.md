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
  `Completion` proof. Shared CQs route directly by the WRID slot.
- **Lock-free SEND/RECV tracking**: buffers of those posted work requests live in
  `WrSlots`, a fixed-size atomic slot array indexed by monotonic per-direction
  IDs — no `Mutex<HashMap>` on the completion path
- **Selective signaling** with completion-driven reclamation of unsignaled
  SEND buffers (RC send queues complete in order), plus gather-list sends and
  owned vectored RDMA READ plans (`prepare_reads` / `post_read`)
- **CQ-owned work request IDs**: `WRID` packs 2 type bits, a CQ-sized route slot
  and a per-direction sequence into `wr_id`. Each CQ fixes the split when it
  is created; QPs acquire their route automatically, and slot reuse carries
  sequence watermarks across QP lifetimes.
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
and `CompletionRoute` (slot and sequence floor) before returning SEND/RECV
buffers or settling READ ownership. Copying raw CQE metadata cannot authorize
reclamation. Creating
and validating the borrowed proof needs no allocation or reference-count update;
READ batch accounting retains its own synchronization.

Each CQ leases up to its actual provider-returned CQE capacity in route slots.
`CompletionQueue::capacity()` and `route_capacity()` expose that limit.
The slot width is `ceil(log2(capacity))`; the remaining `62 - slot_bits` bits
hold the sequence, reported by `sequence_bits()`. This supports larger CQs
without a fixed 16384- or 65536-QP routing ceiling. A larger routing capacity
uses more slot bits and has a smaller sequence budget. The layout stays fixed
for the CQ's complete lifetime, including any retained completion tokens.

`WRID` independently exposes only its type and raw bits. Use
`Completion::slot()` and `Completion::sequence()` to decode with the originating
CQ's layout. Application code cannot accidentally choose another CQ's layout
through these token accessors; raw metadata still carries no recovery authority.

`QueuePair::create` acquires the lease
before creating the provider QP; it owns the lease until QP destruction succeeds.
`send_route()` and `recv_route()` expose immutable route metadata. A shared
send/receive CQ uses one route, while separate CQs each allocate their own.
The QP allocates SQ and RQ sequences independently from the lease's initial
floor. SEND, SEND-with-immediate and READ share the SQ sequence stream. Failed
posts never roll sequence allocation back.

When a lease returns its slot, the CQ retains the maximum next sequence from
both directions, with at least one step for a lease that never posted anything.
The replacement starts at that watermark, so a retained completion fails its
sequence-floor check even if the provider also reuses the QP number. There is
no separate generation field or permanent claimed-tag bitmap. The allocator's
mutex is used only for route acquisition and release; posting and completion
routing do not take it. Sequence exhaustion returns `WorkRequestIdsExhausted`
and retires the slot on release; route-capacity exhaustion returns
`CompletionRoutesExhausted`. Neither wraps an identifier.

SEND/RECV buffer tracking remains a separate bounded `WrSlots` array. A slot
collision returns ownership to the submission path immediately and produces
`WorkRequestSlotsExhausted`, preserving the previously posted buffer. See
[WRID allocation and completion routing](../docs/wrid.md) for the reuse proof,
registration ordering, memory accounting and performance evidence.

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
