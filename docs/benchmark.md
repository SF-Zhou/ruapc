# Benchmarks

This guide describes workload definitions, measurement controls and comparison
methods. For a separately running server and client, use the
[demo load commands](../ruapc-demo/README.md#load-testing).

<a id="how-to-run"></a>
<a id="remote-memory-and-allocation"></a>

## RPC harnesses

Run from the workspace root:

```bash
cargo bench -p ruapc --bench echo
cargo bench -p ruapc --bench remote_memory
```

Both harnesses run a UNIFIED server and client in one process, measuring
TCP, WebSocket, HTTP and RDMA with MessagePack payloads. They require the
[libibverbs build prerequisites](../CONTRIBUTING.md) because `ruapc`'s self
dev-dependency enables RDMA. RDMA measurements additionally need a usable device
and sufficient locked-memory allowance.

| Harness | Workload | Defaults |
|---|---|---|
| [`echo`](../ruapc/benches/echo.rs), serial | One request at a time, 16 B and 4 KiB payloads | 1,000 warmups and 5,000 measured requests per size |
| `echo`, concurrent | 16 B payload, 64 or 1,024 tasks | 256,000 requests per case, split evenly across tasks |
| [`remote_memory`](../ruapc/benches/remote_memory.rs) | Serial 64 KiB and 1 MiB reads/writes with full-pattern verification | 8 warmups; 512 measured requests for 64 KiB, 128 for 1 MiB |

Echo also runs concurrent RDMA cases with one endpoint containing two bootstrap
addresses on the same server. Both addresses share one peer's stripe set.
Its pool limit is 1 GiB per context and its Tokio worker count uses the runtime
default. Remote memory uses 512 MiB per context and four runtime workers.
Keep these settings fixed across compared versions.

The harnesses read these controls before timing:

| Environment variable | Effect |
|---|---|
| `RUAPC_BENCH_TRANSPORT` | Select `TCP`, `WS`, `HTTP` or `RDMA`; unset measures all. |
| `RUAPC_BENCH_RDMA_DEVICE` | Set both peers' `rdma.path.device_filter` to one local device; unset uses automatic selection. |
| `RUAPC_BENCH_SERIAL_ITERS` | Positive measured count for every serial case; does not change concurrent echo cases. |
| `RUAPC_BENCH_WARMUP_ITERS` | Warmup count for serial cases; echo permits zero, remote memory requires a positive count. |

For example:

```bash
RUAPC_BENCH_TRANSPORT=HTTP RUAPC_BENCH_WARMUP_ITERS=5000 \
  RUAPC_BENCH_SERIAL_ITERS=100000 cargo bench -p ruapc --bench echo
RUAPC_BENCH_WARMUP_ITERS=1000 RUAPC_BENCH_SERIAL_ITERS=5000 \
  cargo bench -p ruapc --bench remote_memory
```

Echo skips any transport whose probe fails. Remote memory skips failed RDMA
probes and aborts on TCP/WS/HTTP probe failure. Retain every skip in results:
a successful process exit does not mean every transport was measured.

Echo consumes successful responses with `black_box` without comparing contents.
Concurrent timing includes task creation; its `us/op` divides total wall time
by requests per task, not individual latency samples. Remote-memory timing
excludes setup, initial registration and warmup, and reuses sources and write
destinations. It includes full-pattern verification and per-call
`remote_read_all` destination allocation.

## RDMA ownership harnesses

```bash
cargo bench -p ruapc-rdma --features bench-internals --bench buffer_stash
cargo bench -p ruapc-rdma --features bench-internals --bench read_bookkeeping
```

These standalone harnesses require the libibverbs build prerequisites but no
RDMA device. The `bench-internals` feature exposes internal measurement
components through a hidden `bench_support` module. They own only buffers
created for the measurement; they cannot access a real QP's retained buffers
or construct CQ completion proofs. Treat these interfaces as benchmark support,
not as an application API.

| Harness | Workload | Timed operations |
|---|---|---|
| [`buffer_stash`](../ruapc-rdma/benches/buffer_stash.rs) | Actual `WrSlots`, depth 64; single buffer and prebuilt gather lists of 1/4/16 buffers | Insert/take roundtrip, empty/mismatched take, occupied-slot collision |
| `buffer_stash`, churn | Sequence strides 1/8/32; retire the oldest WR every two attempts, with at most 64 live entries | Attempt cost, observed collision rate, peak live entries and their buffer capacity bytes |
| `buffer_stash`, sweep | Actual `CompletionCursor`; dense widths 1/8/32, sparse width 32 and a 4096-sequence gap | Staging inserts, cursor traversal, takes and recycling; denominator is SQ sequences visited, including empty IDs |
| `buffer_stash`, producer/consumer | Two threads, two batches of 32 buffers; slot transfer and a handoff-only control | Ownership roundtrip including two channel messages per batch and scheduling |
| [`read_bookkeeping`](../ruapc-rdma/benches/read_bookkeeping.rs) | WR group sizes 1/8/32; prepare also varies 1/4/32 scatter segments | Separately measures plan validation, batch creation, ledger insert/remove and final completion notification |

The slot cases preallocate and reuse buffers and gather containers; they do not
measure buffer allocation/free. The collision case deliberately maps a new
sequence onto an occupied slot and checks that ownership is returned. Churn
measures collisions under a synthetic posting/retirement schedule; its peak
capacity counts only buffers actually in slots, excluding unused fixture buffers
and pool caches. Neither case estimates collision frequency in a live QP.
Sweep timings include staging
and recycling, so dense/sparse results are not pure traversal costs.

READ preparation supplies a constant lkey and excludes input-plan construction
and output destruction. Batch creation includes the actual Arc/oneshot
allocations. Ledger cases measure the real sequence-indexed slot table,
Arc traffic, the per-WR acceptance store and nonfinal completion accounting
using one long-lived batch; the two-thread version allows 64 WRs in flight and
includes atomic publication/backpressure plus the acceptance/completion
contention on that shared batch. `ledger/lifecycle` includes one short batch's
creation, pending registration/pruning and final notification while recycling
its buffers; `ledger/spill` forces the collision fallback with a two-slot table,
asserts that the fallback actually ran, and is not a normal-path cost. These
cases exclude the SQ posting mutex, provider calls and CQ polling. A sequence
reaches the fallback only when an older READ stays in flight across a full
table of later SQ sequences, so production frequency depends on the workload;
`QueuePair::read_slot_spills` reports it per QP. Final-completion cases prebuild
batches and measure the final accounting plus oneshot extraction, with either
an unpolled receiver or a counting waker; no executor runs.
The shared ledger batch concentrates reference-count and countdown contention;
its throughput is not the throughput or latency of independent READ batches.
Prebuilt chunks also affect cache residency, and loop/container/clock overhead
remains in the measurements. Do not add component timings to predict RPC cost.

Thread startup, affinity, warmup and final destruction stay outside timing.
Output reports total wall time divided by operations, **not latency samples**.
READ case names specify per-batch or per-WR accounting. Handoff controls show
harness overhead but must not be mechanically subtracted from other cases.

| Environment variable | Effect |
|---|---|
| `RUAPC_RDMA_BENCH_ITERS` | Positive measured operation/batch count, default 100000; sweep rounds up to its width. |
| `RUAPC_RDMA_BENCH_WARMUP` | Positive warmup count, default 1000. |
| `RUAPC_RDMA_BENCH_REPEATS` | Positive sample count per case, default 3. |
| `RUAPC_RDMA_BENCH_FILTER` | Case-name substring; unset runs all cases. |
| `RUAPC_RDMA_BENCH_CPU_BASE=N` | On Linux, pin the main thread to CPU N and the completion thread to N+1. |

For example, bind memory to the chosen cores' NUMA node and measure just the
READ ledger:

```bash
RUAPC_RDMA_BENCH_CPU_BASE=96 RUAPC_RDMA_BENCH_FILTER=read/ledger \
  numactl --membind=1 cargo bench -p ruapc-rdma --features bench-internals \
  --bench read_bookkeeping
```

Use the RPC harnesses above to validate effects on real hardware. In particular,
direct multi-threaded slot access does not measure contention on the real SQ
posting mutex, and device-free bookkeeping does not establish end-to-end gains.

## Allocator harnesses

```bash
cargo bench -p ruapc-bufpool --bench lazy_merge
cargo bench -p ruapc-bufpool --bench contention
cargo bench -p ruapc-bufpool --bench initialization
cargo bench -p ruapc-bufpool --bench handoff
```

These harnesses need no RDMA libraries or device:

- [`lazy_merge`](../ruapc-bufpool/benches/lazy_merge.rs) compares eager and default
  lazy merging under churn, bursts, mixed sizes and demand for larger blocks.
- [`contention`](../ruapc-bufpool/benches/contention.rs) churns 64 KiB slab buffers
  and 1 MiB buddy buffers across 1, 2, 4, 8 and 16 threads. Each worker performs
  one million allocate/free pairs after warmup. `M ops/s` counts allocation and
  free separately; `ns per alloc+free` is wall time divided by pairs per thread,
  not an individual-operation latency distribution.
- [`initialization`](../ruapc-bufpool/benches/initialization.rs) compares 64 MiB
  backing allocation and first full write. It separates demand-paged creation
  from touching all bytes; it does not measure hot pool reuse or registration.
- [`handoff`](../ruapc-bufpool/benches/handoff.rs) compares same-thread
  allocation/free, producer allocation with consumer-thread free, and a
  preallocated-buffer handoff control. Cases use 16 KiB/64 KiB/1 MiB buffers,
  groups of 1/4/16 buffers, and thread caches enabled/disabled. Each case uses a
  fresh 512 MiB pool, 128 warmup batches and 2000 measured batches. Two reusable
  batches bound in-flight memory; each bounded-channel message carries eight
  groups. Container capacity is reused. Timing waits for all measured batches
  to return and excludes thread startup, affinity, initial allocation, warmup
  and final destruction. There is no payload copying or device registration.
  `ns/buffer` is an amortized wall-time cost, not a latency distribution.

Contention accepts `RUAPC_BENCH_SIZE` (65536 or 1048576 bytes) and
`RUAPC_BENCH_THREADS` (1, 2, 4, 8 or 16) to select a case in a fresh process.
On Linux, `RUAPC_BENCH_CPU_BASE=N` pins worker `i` to CPU `N+i` and the coordinator
to `N+16`; for example, base 96 uses coordinator CPU 112. Choose available
physical cores and bind memory to their NUMA node. A process-wide CPU mask still
permits workers to migrate between cores. Keep pool lifecycle and allocation
history equivalent across runs.

Handoff accepts `RUAPC_BENCH_ITERS` (positive batch count), `RUAPC_BENCH_SIZE`
(16384, 65536 or 1048576), `RUAPC_BENCH_GATHER` (1, 4 or 16),
`RUAPC_BENCH_THREAD_CACHE` (0 or 1), and `RUAPC_BENCH_MODE` (`same_thread`,
`cross_thread` or `reuse`). Unset selectors run all combinations. Its
`RUAPC_BENCH_CPU_BASE=N` pins the producer to N and consumer to N+1, unlike
contention's coordinator convention. The `reuse` mode moves preallocated
buffers through the same channels without allocating/freeing them; it is a
synchronization control, not a quantity to subtract mechanically.

```bash
RUAPC_BENCH_ITERS=20000 RUAPC_BENCH_SIZE=65536 \
  RUAPC_BENCH_GATHER=16 RUAPC_BENCH_THREAD_CACHE=1 \
  RUAPC_BENCH_CPU_BASE=96 numactl --membind=1 \
  cargo bench -p ruapc-bufpool --bench handoff
```

## Demo measurements

The [demo client](../ruapc-demo/README.md#load-testing) reports latency
percentiles in `--bench` mode and supports remote server endpoints. Its CLI
guide defines warmup, duration, exit status and verification behavior.
Workers prepare payloads, buffers and histograms before timing. Read/write
latency includes server CRC calculation, write-pattern generation, client
verification and per-call server allocation; MiB/s counts successful logical
payload bytes, not network traffic. These costs differ from the reusable write
sources in the `remote_memory` harness.

Concurrent echo and demo loads are closed-loop: each worker waits for a response
before sending another request. They measure fixed-concurrency behavior, not
latency under an independent arrival rate. Use equivalent payloads, encoding,
concurrency and runtime settings when comparing workloads.

## CPU and NUMA placement

For RDMA, select the same device in both versions and bind CPU and memory to
its NUMA node. Automatic selection across multiple NICs or cross-node DMA can
produce differences unrelated to the code. Inspect the topology first:

```bash
cat /sys/class/infiniband/mlx5_0/device/numa_node
lscpu -e=CPU,NODE,CORE

# Example only: mlx5_0 is on node 0, with distinct physical cores at CPU IDs 0-7.
RUAPC_BENCH_TRANSPORT=RDMA RUAPC_BENCH_RDMA_DEVICE=mlx5_0 \
  numactl --physcpubind=0-7 --membind=0 cargo bench -p ruapc --bench echo
RUAPC_BENCH_TRANSPORT=RDMA RUAPC_BENCH_RDMA_DEVICE=mlx5_0 \
  numactl --physcpubind=0-7 --membind=0 cargo bench -p ruapc --bench remote_memory
```

## Comparing results

Use identical benchmark sources and dependency locks. Build both versions
before sampling, then run them sequentially in alternating order on the same
CPU cores, NUMA node and RDMA device. Retain every sample, including skipped
cases, rather than selecting only the best run.

Record the source revision, toolchain, build profile, dependency lock, hardware,
thread placement, workload settings and raw output outside `docs/`. Report
variation across repeated runs with any comparison. Historical machine-specific
samples are available in Git history; they are not current performance claims.
