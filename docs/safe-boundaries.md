# Core safety boundaries and validation

This change follows the workspace refactor committed through `1d35f32`.
The earlier refactor's evidence remains in [refactoring.md](refactoring.md).
This report compares the safety-boundary changes against that committed version.

## Responsibilities

- The `ruapc` package forbids `unsafe_code` through Cargo, including its tests
  and benchmarks. Its library also uses `#![forbid(unsafe_code)]`. No Rust source
  in `ruapc/` contains an `unsafe` keyword or an override of this lint.
- `ruapc-bufpool::DeviceSet<D>` assigns device indices and registers backing
  memory through the unsafe `MemoryRegistrar` implementation contract. Safe
  application `Device` wrappers provide a registrar reference without receiving
  backing memory. `ruapc-rdma::ActiveDevice` owns the RDMA registrar implementation.
- `ruapc-rdma::CompletionQueue` lends non-cloneable completion proofs from private
  reusable stack storage. The QP validates CQ, QPN and a permanently reserved tag
  before returning completed SEND/RECV buffers or settling a READ. Raw metadata
  and caller-supplied WR IDs do not authorize reclamation.
- `ruapc-rdma::QueuePair` accepts actual destination buffers and a READ plan of
  buffer indices, offsets and lengths. It validates local bounds, overflow,
  gather limits and global destination overlap, then derives addresses and keys
  internally. Its exclusive posting cursor records each successful post once.
  Cancellation accounts the unposted suffix; timeout notification retains posted
  memory until every completion arrives or QP destruction succeeds.
- Core `WriteTarget` transfers its complete destination vector into that owned
  operation. Its empty slot excludes CPU copies and competing transfers during
  DMA. Success restores the buffers; cancellation or failure may leave recovery
  unavailable while the dependency eventually recycles the destination.

These are ownership boundaries, not safe wrappers around a caller's promise to
keep arbitrary raw pointers alive. QP creation rejects unowned FFI resources and
foreign contexts. Memory registration rejects offset-based addressing. Failure
to destroy a QP or deregister an MR aborts before releasing device-visible memory.

One existing protocol limitation remains: the source of a remote one-sided READ
cannot observe the peer's completion after source-request expiry. Owned sources
protect local reverse-RPC CPU copies, and the existing post-READ pending check
rejects expired results, but this change does not introduce a remote DMA source
lease. See [the ownership invariants](../DESIGN.md#remote-memory).

## Correctness and structural checks

The final workspace suite passes **511 tests, zero failures**, with 13 ignored
documentation examples. Coverage includes destination overlap and overflow,
partial submission and cancelled posting cursors, dropped receivers, timeout
notification versus final flush, exclusive write targets, registration order and
rollback, immutable completion tags, generation exhaustion, and rejected QP/MR
configuration. Compile-fail documentation verifies that safe code cannot invent
a completion proof or implement an unaudited registrar.

[Checks, raw logs, binary hashes and source fingerprint](safe-boundary-data/checks.json)
record the successful commands:

```bash
cargo test --workspace --all-features --no-fail-fast
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-features
cargo check -p ruapc --lib --no-default-features
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
RUSTDOCFLAGS='-D warnings' cargo doc -p ruapc --no-default-features --no-deps
cargo fmt --all -- --check
```

The completion proof adds no allocation, Arc operation or lock to CQ polling or
proof validation. READ accounting retains the existing per-WR map/Arc and
per-batch oneshot/mutex mechanisms. Restoring a checked-out write target adds one
short mutex acquisition to its completed RDMA write path.

The implementation accepts small connection-level memory costs: a 512 KiB
per-CQ tag history prevents reuse, and a temporary `Box<LocalConnection>` holds
the local handshake resources across peer negotiation. Registration consumes the
box and moves its QP directly into the established connection; READ bookkeeping
remains inline in that QP. This keeps temporary connection state out of every
RPC task. Consuming iterator conversions let READ descriptors reuse plan
storage; the single-destination case requires no overlap-list allocation. Local
bounds and key checks remain in place for that case.

[Optimized task allocation inspection](safe-boundary-data/task-sizes.json):

| Task | Baseline future | Final future | Baseline allocation | Final allocation |
|---|---:|---:|---:|---:|
| Echo handler | 2104 B | 2104 B | 2304 B | 2304 B |
| Overload response | 760 B | 760 B | 896 B | 896 B |
| Concurrent client | 4216 B | 3992 B | 4352 B | 4096 B |

An intermediate version enlarged the client allocation to 4480 B; the final
layout removes that increase. Diagnostic results from earlier implementations
are preserved in the `*-initial.json` and `*-intermediate.json` files and are not
used as final-version measurements.

## Performance method

Both versions use the same benchmark source, dependency package versions,
optimized Cargo profile and machine: Linux x86_64, Intel Xeon 6966P-C, rustc 1.98.0 and physical
`mlx5_0` on NUMA node 0. Setup, device registration and warmup are outside timing.
Remote-memory operations verify the complete payload. No compilation or tests
run concurrently with sampling; versions alternate order between paired runs.

The full matrix has three paired runs using CPUs 0–7, NUMA node 0 memory and
`RUAPC_BENCH_RDMA_DEVICE=mlx5_0`. Echo uses 5000 warmup and 50000 serial requests;
each concurrent case issues 256000 requests. Remote memory uses 1000 warmup and
5000 measured requests per read/write and size, with four runtime workers.

Follow-up measurements pin individual threads to reduce migration and competition:

- Echo: nine paired runs, eight runtime workers on CPUs 0–7, main thread on CPU 8,
  RDMA pollers on CPUs 9–10; memory remains on NUMA node 0. Both binaries use
  `TOKIO_WORKER_THREADS=8` and otherwise the same workload as the full matrix.
- RDMA remote memory: nine paired runs, four workers on CPUs 0–3, main on CPU 4,
  and two pollers on CPUs 6–7; all on NUMA node 0, with the same iteration counts.

Every raw sample is retained. Positive latency changes are slower; positive
throughput changes are faster. Results characterize these workloads and this
machine, including measurement variability; they are not a guarantee for every
platform or application.

## Measured performance

The full matrix is retained in [echo](safe-boundary-data/echo.json) and
[remote memory](safe-boundary-data/remote_memory.json). No case was skipped.
The following tables show the additional measurements with each thread pinned.

### Echo

[Nine pairs, every sample and thread mapping](safe-boundary-data/pinned-echo.json).
Serial latency is µs/op; concurrent throughput is kreq/s.

| Transport | Workload | Baseline | Current | Change |
|---|---|---:|---:|---:|
| TCP | serial 16 B | 22.58 | 22.84 | +1.2% |
| TCP | serial 4096 B | 25.29 | 25.02 | -1.1% |
| TCP | 64 tasks | 867.60 | 834.20 | -3.8% |
| TCP | 1024 tasks | 967.70 | 933.40 | -3.5% |
| WS | serial 16 B | 26.43 | 27.36 | +3.5% |
| WS | serial 4096 B | 29.40 | 30.22 | +2.8% |
| WS | 64 tasks | 322.60 | 324.40 | +0.6% |
| WS | 1024 tasks | 304.40 | 308.80 | +1.4% |
| HTTP | serial 16 B | 26.48 | 26.24 | -0.9% |
| HTTP | serial 4096 B | 29.59 | 29.97 | +1.3% |
| HTTP | 64 tasks | 344.50 | 335.60 | -2.6% |
| HTTP | 1024 tasks | 372.00 | 372.40 | +0.1% |
| RDMA | serial 16 B | 28.36 | 28.97 | +2.2% |
| RDMA | serial 4096 B | 31.29 | 31.03 | -0.8% |
| RDMA | 64 tasks | 706.50 | 693.90 | -1.8% |
| RDMA | 1024 tasks | 1371.00 | 1371.90 | +0.1% |
| RDMA (2 endpoints) | 64 tasks | 669.90 | 648.20 | -3.2% |
| RDMA (2 endpoints) | 1024 tasks | 1427.30 | 1408.10 | -1.3% |

### RDMA remote memory

[Nine pairs, every sample and thread mapping](safe-boundary-data/pinned-rdma.json).
Latency is µs/op, including full-payload verification.

| Operation | Size | Baseline | Current | Change |
|---|---|---:|---:|---:|
| remote_read_all | 64 KiB | 39.11 | 41.46 | +6.0% |
| remote_write_all | 64 KiB | 40.28 | 39.39 | -2.2% |
| remote_read_all | 1024 KiB | 159.01 | 170.17 | +7.0% |
| remote_write_all | 1024 KiB | 176.35 | 159.40 | -9.6% |

### TCP after concurrent warmup

The shorter echo workload includes task creation and measures only 256000
concurrent requests. Its final pinned TCP medians are 3.5–3.8% below the baseline.
A separate diagnostic therefore creates the same 1024 tasks, warms each with
256 requests, synchronizes their start, then measures 8192 requests per task
(8388608 total). It retains UNIFIED configuration, RDMA support, the same device
filter and 1 GiB buffer pool; TCP is the selected transport. Runtime workers
remain on CPUs 0–7 and main on CPU 8, with NUMA node 0 memory.

[All six runs, source, runner, thread placement and binary hashes](safe-boundary-data/tcp-steady.json)
retain this separately built diagnostic. Both source trees use identical
dependency package versions and the exact same harness.

| Workload | Baseline median | Current median | Change |
|---|---:|---:|---:|
| Warmed TCP, 1024 tasks | 932.20 kreq/s | 928.12 kreq/s | -0.44% |

Individual paired changes range from -1.32% to +0.77%; the shorter workload's
roughly 3% gap does not persist in this longer measurement. This result does not
remove the shorter measurements from the report or prove all workloads identical.

RDMA bulk measurements remain variable: the pinned read medians are +6–7%,
while the full-matrix read medians are -3.5% and -22.8%. For pinned 1 MiB reads,
baseline samples span 121.49–216.52 µs and current samples 122.15–225.53 µs.
The source/destination buffers, payload verification and RPC protocol are the
same; these end-to-end runs include scheduler, cache and device variability.
Neither the favorable nor unfavorable bulk medians establish a universal
throughput change.

A separate three-pair `perf stat` run enables counters only after warmup.
[Source, provenance and complete counter output](safe-boundary-data/tcp-steady-instructions.json)
record 100% instruction-counter coverage and zero CPU migrations in all runs.
Instructions/request are 37364.02 → 37275.97 (-0.24%), with paired changes
-0.24%, +0.13% and -0.57%. Profiled throughput is 886.47 → 891.70 kreq/s
(+0.59%); it is kept separate from unprofiled throughput. These counts provide
no evidence of additional steady-state execution work in this TCP workload.
