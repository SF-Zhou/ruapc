# Echo RPC Benchmark

End-to-end echo RPC benchmark: a single UNIFIED server serves all
protocols on one port, and each transport (TCP / WebSocket / HTTP / RDMA)
is measured with the same client-side workload:

- **serial**: round-trip latency with one in-flight request (16 B and 4 KiB payloads)
- **concurrent**: closed-loop throughput at 64 and 1024 tasks (16 B payload,
  256k total requests per run, split evenly across tasks)

Source: [`ruapc/benches/echo.rs`](../ruapc/benches/echo.rs).

## How to Run

```bash
cargo bench -p ruapc --bench echo
```

For longer latency samples or a single transport, set options before starting
the benchmark. Defaults remain 1000 warmup and 5000 serial requests; concurrent
workloads are unchanged:

```bash
RUAPC_BENCH_TRANSPORT=HTTP RUAPC_BENCH_WARMUP_ITERS=5000 \
  RUAPC_BENCH_SERIAL_ITERS=100000 cargo bench -p ruapc --bench echo
```

Both `echo` and `remote_memory` accept `RUAPC_BENCH_TRANSPORT` and
`RUAPC_BENCH_RDMA_DEVICE`. The latter sets
`SocketPoolConfig.rdma.path.device_filter` for both peers, fixing the NIC used
for the run. Unset variables preserve automatic transport/device selection;
configuration is read during setup, outside measured loops.

For RDMA comparisons, select the same device in both versions and pin CPU and
memory allocation to that device's NUMA node. Automatic placement across
multiple NICs and cross-node DMA can produce large differences unrelated to
the code being compared. Choose distinct physical cores from the topology
reported by `lscpu`:

```bash
# Find the NIC's NUMA node:
cat /sys/class/infiniband/mlx5_0/device/numa_node
lscpu -e=CPU,NODE,CORE

# Example only: mlx5_0 is on node 0, with distinct cores at CPU IDs 0-7.
RUAPC_BENCH_TRANSPORT=RDMA RUAPC_BENCH_RDMA_DEVICE=mlx5_0 \
  numactl --physcpubind=0-7 --membind=0 cargo bench -p ruapc --bench echo
RUAPC_BENCH_TRANSPORT=RDMA RUAPC_BENCH_RDMA_DEVICE=mlx5_0 \
  numactl --physcpubind=0-7 --membind=0 cargo bench -p ruapc --bench remote_memory
```

Notes:

- RDMA requires `libibverbs-dev` and a usable RDMA device; the benchmark
  reports RDMA as skipped otherwise. Make sure the memory-lock limit is
  unlimited: `sudo prlimit --pid $$ -l=unlimited`.
- The benchmark enlarges the shared buffer pool to 1 GiB
  (`SocketPoolConfig.buffer_pool_memory`); the 256 MiB default is exhausted
  by per-request send buffers at 1024 closed-loop tasks, and allocation
  waits would show up as artificial latency/timeouts.

## Remote memory and allocation

```bash
cargo bench -p ruapc --bench remote_memory
cargo bench -p ruapc-bufpool --bench lazy_merge
cargo bench -p ruapc-bufpool --bench contention
cargo bench -p ruapc-bufpool --bench initialization
```

The remote-memory benchmark measures 64 KiB and 1 MiB reads/writes over all
four transports, including complete-pattern verification. It excludes setup,
registration and warmup, and reuses read sources and write destinations.
Default measured counts are 512 for 64 KiB and 128 for 1 MiB, with 8 warmups.
For stable comparisons, increase them with the same iteration controls as echo:

```bash
RUAPC_BENCH_WARMUP_ITERS=1000 RUAPC_BENCH_SERIAL_ITERS=5000 \
  cargo bench -p ruapc --bench remote_memory
```

For Linux contention comparisons, `RUAPC_BENCH_CPU_BASE=96` binds worker `i`
to CPU `96 + i` and the coordinating thread to CPU 112. Choose a valid range
for the machine and bind memory to the corresponding NUMA node. A process-wide
CPU mask alone allows workers to move between cache clusters, which can produce
large differences in short lock benchmarks.

Compare identical harnesses and dependency locks, run versions sequentially in
alternating order, and retain every sample, including skipped transports.
The [workspace refactoring report](refactoring.md) records the current comparison
against commit `36c8352`, including task-allocation sizes and raw measurements.

## Previously recorded sample

This sample uses a different compiler and CPU placement from the refactoring
comparison linked above.

Environment:

- Intel Xeon 6966P-C, 2 NUMA nodes, 384 logical CPUs; Linux 6.8.0
- RDMA: Mellanox mlx5 (loopback through the local NIC); benchmark pinned to
  the NIC's NUMA node with `numactl -N 1 -m 1`
- rustc 1.99.0-nightly (2026-07-13), `bench` profile
- Client and server share one process and one tokio runtime; numbers
  include both sides' work

| Transport | Serial 16B | Serial 4KiB | 64 tasks | 1024 tasks |
|---|---:|---:|---:|---:|
| TCP  | 31.8 us/op | 34.9 us/op | 264 kops/s (243 us/op) | 281 kops/s (3.6 ms/op) |
| WS   | 44.4 us/op | 48.0 us/op | 149 kops/s (429 us/op) | 153 kops/s (6.7 ms/op) |
| HTTP | 36.9 us/op | 42.7 us/op | 113 kops/s (567 us/op) | 111 kops/s (9.2 ms/op) |
| RDMA | 33.1 us/op | 38.3 us/op | 401 kops/s (160 us/op) | 417 kops/s (2.5 ms/op) |

`us/op` in the concurrent rows is the average per-request latency observed
by each closed-loop task (queueing included).

Without NUMA pinning, RDMA drops to ~270 kops/s @64 / ~226 kops/s @1024 on
the same machine with high run-to-run variance; the other transports are
mostly unaffected.
