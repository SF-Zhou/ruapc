# Benchmarks

End-to-end echo RPC benchmark: a single UNIFIED server serves all
protocols on one port, and each transport (TCP / WebSocket / HTTP / RDMA)
is measured with the same client-side workload:

- **serial**: round-trip latency with one in-flight request (16 B and 4 KiB payloads)
- **concurrent**: closed-loop throughput at 64 and 1024 tasks (16 B payload,
  256k total requests per run, split evenly across tasks)

RDMA also runs the concurrent cases across two endpoints on the same server.
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

- Building `ruapc` benchmarks requires the libibverbs development package
  because the self dev-dependency enables RDMA. Echo skips transports whose
  probes fail; `remote_memory` skips only failed RDMA probes and aborts on
  TCP/WS/HTTP probe failure. RDMA measurements require a usable device and
  sufficient locked-memory allowance; see [build requirements](../CONTRIBUTING.md).
- Echo uses a 1 GiB buffer pool; remote memory uses 512 MiB. Keep these limits
  identical across compared versions so allocation pressure is comparable.

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

## Comparing results

Use identical benchmark sources and dependency locks. Build both versions
before sampling, then run them sequentially in alternating order on the same
CPU cores, NUMA node and RDMA device. Retain every sample and every skip; a
successful process exit alone does not mean all transports were measured.

Echo checks RPC success and consumes response values with `black_box`; it does
not compare response contents. Concurrent timing includes task creation and
reports aggregate throughput. Its `us/op` is elapsed wall time divided by
requests per task, not a measured latency distribution. Remote-memory timing
includes full-payload verification and normal `remote_read_all` allocation.

Record the source revision, toolchain, build profile, dependency lock, hardware,
thread placement, workload settings and raw output outside `docs/`. Report
variation across repeated runs with any comparison. Historical machine-specific
samples are available in Git history; they are not current performance claims.
