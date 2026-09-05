# Workspace refactoring validation

Baseline: `36c8352` (`main` at the start of the refactor). The baseline was
built from a detached worktree with the original dependency lock copied in.
The comparison uses existing workloads; examples' debug-mode smoke-test
throughput is not used as performance evidence.

## Structural changes

- `ruapc` separates client configuration from execution, router dispatch from
  OpenAPI projection, and memory transfer contracts from contexts/transports.
  TCP and HTTP/2 share framing; stream transports share synchronous connection
  preparation and await their own existing queues directly. Read attachments
  own immutable sources shared with waiters and local reverse-RPC readers.
- `ruapc-bufpool` separates configuration, synchronized allocator state,
  waiter/reservation logic and small-buffer coordination. Slab backing tokens
  no longer retain the owning pool, and cache chunks transfer unique ownership.
  Immutable registrations and interior-mutable buddy state have separate access
  contracts; intrusive back-pointers are established only after aliasable ownership.
- `ruapc-rdma` centralizes SEND submission/rollback and makes caller-owned DMA
  lifetime requirements explicit in unsafe APIs. `ruapc::rdma` separates wire
  framing, READ execution, completion-batch ownership and receive credits.
- `ruapc-macro` validates a service model before generating code. Diagnostics
  retain source locations; conditional attributes propagate to generated code.
- `ruapc-demo` shares setup and verified workloads. The benchmark start barrier
  excludes setup, and dispatcher modes are represented by one enum.

The remaining modules were reviewed for ownership and responsibility boundaries.
Already cohesive configuration, endpoint-health and transport-policy modules
retain their structure. The current dependency map is in [DESIGN.md](../DESIGN.md).

## Correctness checks

The refactor adds regression coverage for:

- truncated message headers and arbitrary splits/concatenations of encoded frames;
- simultaneous server arrivals respecting the capacity limit;
- cancelled client requests releasing waiters and settling metrics;
- completion waking receivers only after releasing memory holds, checked with a
  synchronous custom waker rather than scheduler timing;
- owned read sources surviving forgotten futures and waiter expiry, with
  validated cross-buffer CPU copies and recovery after completed calls;
- HTTP body collection preceding waiter allocation and handler budget;
- RDMA connection removal failing its pending requests and settling its gauge;
- RDMA credit conservation, including the 16-bit ACK field limit;
- READ completion versus partial-post failure and DMA memory retention;
- buffer-pool destruction with cached/live slab chunks, deregistration order,
  initialization, overflow, failed growth rollback and concurrent registration
  queries while the allocator mutates its tree, and buffer drops after TLS-cache destruction;
- invalid macro signatures, conditional methods, type aliases and raw identifiers.

The initial suite passed 473 tests with 14 ignored documentation examples.
The final suite passes 498 tests with 13 ignored examples and no failures.
[Machine-readable counts](refactor-data/checks.json) record the workspace runs.
Compile-fail diagnostics are additionally exercised by the macro's 14 trybuild
cases.

Validation commands:

```bash
cargo build --workspace --all-features
cargo test --workspace --all-features --no-fail-fast
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo check -p ruapc --no-default-features --lib
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
RUSTDOCFLAGS='-D warnings' cargo doc -p ruapc --no-default-features --no-deps
```

Runtime smoke checks cover TCP/WebSocket/HTTP × echo/read/write, empty buffer
paths, the remote-memory example, client benchmark/stress operation, and all
six Tokio dispatcher modes. After the read-ownership migration, read/write and
the remote-memory example were additionally checked over physical RDMA. The
end-to-end echo benchmark also exercises RDMA
and two RDMA endpoints on physical mlx5 devices.

## Generated task memory

Static `nm`/`objdump` inspection of the optimized echo binaries checks the
payload copy and allocation constants in Tokio's `Cell::new` for each task.
The final binary includes the owned read-source and allocator-state changes.
Its shared sender performs synchronous preparation and introduces no additional
async state machine or future wrapper.

| Task | Baseline future | Refactored future | Baseline allocation | Refactored allocation |
|---|---:|---:|---:|---:|
| Echo handler | 2104 B | 2104 B | 2304 B | 2304 B |
| Overload response | 760 B | 760 B | 896 B | 896 B |
| Concurrent client | 4264 B | 4216 B | 4480 B | 4352 B |

The waiter's `WaiterEntry` also remains 48 B: compiled hash-table lookups in
both binaries use a 56 B bucket containing the 8 B message ID and the value.
Using `Option<NonZeroU64>` for the connection ID makes room for the additional
read-source pointer without enlarging the entry.

These sizes describe this compiler, feature set and echo payload. They are
structural evidence, not a substitute for throughput and latency measurements.

## Environment and comparison method

- Linux x86_64; Intel Xeon 6966P-C, 384 logical CPUs across two NUMA nodes.
- `rustc 1.98.0 (88d9e12ae 2026-08-18)`; Cargo's optimized bench profile.
- libibverbs `1.14.43.0`; physical `mlx5_0`, `mlx5_1`, `mlx5_2` available.
- Baseline and refactored binaries run sequentially with alternating order.
- Final echo runs use 5000 warmup requests, 50000 serial requests per size,
  and 256000 requests per concurrent run. Payloads and task counts are unchanged.
- RPC comparisons use `numactl --physcpubind=0-7 --membind=0` with
  `RUAPC_BENCH_RDMA_DEVICE=mlx5_0`. These are distinct physical cores and
  the NIC is on NUMA node 0. Every measured RDMA case ran successfully.
- Buffer-pool contention uses worker `i` on CPU `96+i`, coordinator CPU 112,
  and NUMA node 1 memory. The full matrix and fresh-process cases are both
  retained because process allocation history changes some results.
- Earlier RPC diagnostic runs used NUMA node 1 and automatic NIC placement.
  Those cross-node results are not used as the final comparison.

Results apply to the measured workloads and machine. Other platforms, NICs,
allocator replacements and application payloads need their own measurements.
64-bit Linux uses lazy zero-filled mappings for initial memory; other targets use
`alloc_zeroed`, whose initial-allocation cost has not been measured here. Pool
reuse never adds a clearing pass. Allocation/deallocation measurements exclude
first-touch and device-registration costs; the first-full-write workload measures
allocation together with touching the entire block.

Owned read attachments protect local CPU reverse-RPC copies across cancellation
and forgotten futures. They do not add a remote DMA source lease: after source
timeout, the source side cannot observe when an already posted one-sided READ
finishes. The existing post-READ pending check rejects expired results, while
destination batches retain memory through all completions. The source-reuse
protocol is unchanged and remains a separate limitation.

## Measured performance

All table values are medians. Positive latency changes are slower; positive
throughput changes are faster. The linked JSON files retain every sample,
minimum, maximum and original benchmark output, including unfavorable results.

### Echo

[Three paired runs and raw output](refactor-data/echo.json). Serial latency is
in microseconds; concurrent throughput is in thousands of requests per second.

| Transport | Workload | Baseline | Refactored | Change |
|---|---|---:|---:|---:|
| TCP | serial 16 B (µs) | 14.28 | 13.89 | -2.7% |
| TCP | serial 4096 B (µs) | 15.85 | 15.74 | -0.7% |
| TCP | 64 tasks (kreq/s) | 839.90 | 850.60 | +1.3% |
| TCP | 1024 tasks (kreq/s) | 940.10 | 989.50 | +5.3% |
| WS | serial 16 B (µs) | 17.57 | 18.09 | +3.0% |
| WS | serial 4096 B (µs) | 19.72 | 19.66 | -0.3% |
| WS | 64 tasks (kreq/s) | 320.50 | 324.50 | +1.2% |
| WS | 1024 tasks (kreq/s) | 299.40 | 302.80 | +1.1% |
| HTTP | serial 16 B (µs) | 18.09 | 17.97 | -0.7% |
| HTTP | serial 4096 B (µs) | 20.26 | 19.76 | -2.5% |
| HTTP | 64 tasks (kreq/s) | 334.60 | 348.20 | +4.1% |
| HTTP | 1024 tasks (kreq/s) | 362.60 | 374.10 | +3.2% |
| RDMA | serial 16 B (µs) | 15.05 | 15.11 | +0.4% |
| RDMA | serial 4096 B (µs) | 17.84 | 17.29 | -3.1% |
| RDMA | 64 tasks (kreq/s) | 926.10 | 945.50 | +2.1% |
| RDMA | 1024 tasks (kreq/s) | 1358.00 | 1376.60 | +1.4% |
| RDMA (2 endpoints) | 64 tasks (kreq/s) | 931.50 | 924.20 | -0.8% |
| RDMA (2 endpoints) | 1024 tasks (kreq/s) | 1367.90 | 1396.90 | +2.1% |

Single-endpoint concurrent throughput medians improve 1.1–5.3%. Serial
latency medians range from −3.1% to +3.0%, with overlapping run ranges for
the slower cases. Two-endpoint RDMA at 64 tasks is −0.8% with overlapping
ranges. These samples support comparable echo performance; they do not
establish a universal performance guarantee.

### Remote memory

[Long runs and raw output](refactor-data/remote_memory-long.json): three paired
runs, 1000 warmups and 5000 measured requests per case, including a full payload
comparison in each request. Values are microseconds per operation. Buffer
initialization, setup and registration are excluded; normal server-side read
allocation remains part of the workload.

| Transport | Operation | Bytes | Baseline | Refactored | Change |
|---|---|---:|---:|---:|---:|
| TCP | read | 64 KiB | 36.36 | 33.12 | -8.9% |
| TCP | write | 64 KiB | 32.97 | 30.72 | -6.8% |
| TCP | read | 1024 KiB | 219.45 | 215.16 | -2.0% |
| TCP | write | 1024 KiB | 181.55 | 181.30 | -0.1% |
| WS | read | 64 KiB | 44.91 | 47.15 | +5.0% |
| WS | write | 64 KiB | 43.54 | 44.18 | +1.5% |
| WS | read | 1024 KiB | 225.92 | 211.94 | -6.2% |
| WS | write | 1024 KiB | 186.25 | 193.13 | +3.7% |
| HTTP | read | 64 KiB | 53.96 | 52.38 | -2.9% |
| HTTP | write | 64 KiB | 50.90 | 49.18 | -3.4% |
| HTTP | read | 1024 KiB | 374.74 | 368.15 | -1.8% |
| HTTP | write | 1024 KiB | 289.00 | 294.40 | +1.9% |
| RDMA | read | 64 KiB | 38.27 | 41.26 | +7.8% |
| RDMA | write | 64 KiB | 38.43 | 38.81 | +1.0% |
| RDMA | read | 1024 KiB | 150.86 | 124.90 | -17.2% |
| RDMA | write | 1024 KiB | 136.66 | 154.52 | +13.1% |

The initial [short full-matrix runs](refactor-data/remote_memory.json) and
[short isolated-transport runs](refactor-data/remote_memory-isolated-short.json)
are also retained. Their 1 MiB cases took only tens of milliseconds: the apparent
HTTP read slowdown in the full matrix reversed in isolated runs and disappeared
with longer measurement (374.74 → 368.15 µs). RDMA still has broad run ranges;
its long full-matrix write median is slower, so a separately controlled affinity
check follows rather than discarding that result.

[Nine additional paired RDMA runs](refactor-data/remote_memory-pinned-rdma.json)
use the same long-sample binaries with one transport per process. After thread
creation, the four Tokio workers are assigned to CPUs 0–3, the main thread to
CPU 4, and the two poll threads to CPUs 6 and 7. Memory remains on NUMA node 0.
The artifact records the actual thread assignments and every sample.

| RDMA operation | Baseline median [min, max], µs | Refactored median [min, max], µs | Change |
|---|---:|---:|---:|
| read 64 KiB | 40.29 [38.52, 44.11] | 41.33 [37.98, 49.00] | +2.6% |
| write 64 KiB | 43.00 [38.89, 59.13] | 41.69 [38.82, 45.18] | -3.0% |
| read 1024 KiB | 162.40 [137.01, 290.53] | 148.34 [117.76, 323.92] | -8.7% |
| write 1024 KiB | 224.09 [159.18, 337.60] | 180.53 [148.45, 354.23] | -19.4% |

The 1 MiB write slowdown from the full matrix does not persist under this
placement. All four ranges overlap, and even fixed threads leave substantial
bulk-transfer variance on this shared machine. The 64 KiB read median is 2.6%
slower in this dataset. Taken together, these measurements do not establish a
repeatable large RDMA regression, and they also do not establish a universal
speedup. Both the full matrix and the dedicated runs are necessary context.

### Buffer pool

[Versioned measurements](refactor-data/bufpool.json) and
[original logs and diagnostic harness](refactor-data/bufpool-raw.json).
Values below are nanoseconds per allocation/free pair; the contention harness
divides elapsed time by pairs, not by twice that number. The default lazy
buddy policy and original standard-library mutex remain in production.

| Allocator | Threads | Baseline | Refactored | Change |
|---|---:|---:|---:|---:|
| buddy 1mib | 1 | 45.6 | 39.8 | -12.7% |
| buddy 1mib | 2 | 258.2 | 276.3 | +7.0% |
| buddy 1mib | 4 | 713.6 | 728.3 | +2.1% |
| buddy 1mib | 8 | 2589.4 | 2343.5 | -9.5% |
| buddy 1mib | 16 | 9753.2 | 8895.0 | -8.8% |
| slab 64kib | 1 | 58.6 | 47.5 | -18.9% |
| slab 64kib | 2 | 99.3 | 87.1 | -12.3% |
| slab 64kib | 4 | 142.6 | 153.4 | +7.6% |
| slab 64kib | 8 | 272.3 | 282.8 | +3.9% |
| slab 64kib | 16 | 692.2 | 697.8 | +0.8% |

The sequential matrix contains slower cases, notably two-thread buddy and
four-thread slab. Additional sequential buddy samples retain that difference
(260.5 → 284.5 ns median over nine pairs). Repeating each suspect case as the
first pool in a fresh process gives:

| Fresh-process case, nine pairs | Baseline median [min, max] | Refactored median [min, max] |
|---|---:|---:|
| slab 64kib 4threads | 153.3 [153.0, 153.4] | 153.3 [152.9, 153.5] |
| buddy 1mib 2threads | 330.3 [319.1, 344.3] | 273.5 [233.2, 284.1] |

The slab difference disappears and the buddy difference reverses. The pool
size and measured field offsets match across versions; address placement and
allocation history differ between sequential and fresh-process runs. This
limits attribution of the sequential differences and does not justify claiming
that every contention workload is faster. No speculative padding, custom
reference counting or mutex replacement was retained. The final TLS hit path
removes four temporary shard-reference atomic operations per allocation/free
pair without adding a lock or allocation.

The evidence also includes earlier, explicitly labeled measurements of the
final memory-initialization backend: allocating/freeing an untouched 64 MiB
mapping takes 1.32 µs median versus 3.60 µs for the old uninitialized allocator;
allocation plus writing every byte takes 3274.89 µs. These measure different
work and must not be interpreted as avoiding first-touch costs. Earlier lazy
merge results predate the TLS optimization and are preserved as supporting
history rather than relabeled as final whole-library measurements.

## API changes

Compatibility was not a constraint. In particular, mutable `AlignedMemory`
slices require `&mut self`; raw DMA/reclamation APIs require an explicit unsafe
contract (`TcpDevice::read_memory` and `QueuePair::{read_sges, take_buffer,
take_send_buffer, reclaim_send_buffers}`); unused `Completion` and
`QueuePair::{poll_send, poll_recv}` wrappers were removed; unchecked task
registration is internal; unsupported service declarations fail explicitly.
Device collections explicitly promise not to access registered bytes outside
their allocation's ownership rules through an unsafe implementation contract.
Read attachment methods now take `Buffer` / `Vec<Buffer>` by ownership and
replace the wrapper's source list. Sources stay immutable and reusable across
calls; `take_read_buffers(&mut self)` recovers ownership only when uniquely held
and otherwise preserves it for a later attempt. The internal `read_inline`
request now carries only the original request ID and logical ops, avoiding
duplicated region metadata and registration-map lookup. Public RPC method names
and transport frame formats remain unchanged.
