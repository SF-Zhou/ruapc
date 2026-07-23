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

On NUMA machines, pin the benchmark (CPU and memory) to the node the RDMA
NIC is attached to — cross-node traffic between the NIC, DMA buffers, and
the tokio workers otherwise costs a significant fraction of throughput and
adds run-to-run variance:

```bash
# Find the NIC's NUMA node:
cat /sys/class/infiniband/<device>/device/numa_node

# Pin CPUs and memory to that node (node 1 here):
numactl -N 1 -m 1 cargo bench -p ruapc --bench echo
```

Notes:

- RDMA requires `libibverbs-dev` and a usable RDMA device; the benchmark
  reports RDMA as skipped otherwise. Make sure the memory-lock limit is
  unlimited: `sudo prlimit --pid $$ -l=unlimited`.
- The benchmark enlarges the shared buffer pool to 1 GiB
  (`SocketPoolConfig.buffer_pool_memory`); the 256 MiB default is exhausted
  by per-request send buffers at 1024 closed-loop tasks, and allocation
  waits would show up as artificial latency/timeouts.

## Results

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
