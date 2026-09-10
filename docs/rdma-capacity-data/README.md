# RDMA capacity and performance evidence

This directory retains the **initial capacity implementation (v1)**, before
the later optimization of poller connection maintenance. Final measurements
are stored separately under `final/`; v1 samples are never overwritten.

Baseline: `601f404e3cd57b643bfae99f0cc80bdd9a5bbbf1`. The source archive and
Cargo target directory are independent of the changing workspace.
`source-metadata.json` records source/binary fingerprints. `Cargo.lock` preserves
the dependency resolution. All builds finished before sampling, and no project compilation or tests ran
during samples. After source files are fingerprinted separately from the
supplemental benchmark sources; no production source was changed for a probe.

## Hardware and resource budget

`hardware.json` records a read-only `ibv_devinfo -d mlx5_0 -v` query:
max_qp=131072, max_qp_wr=32768, max_cqe=4194303, max_cq=16777216,
max_sge=30. The selected port is ACTIVE Ethernet, 200 Gb/s, NUMA node 0.
The machine has AMD EPYC 9655 CPUs. The container's memory limit is 128 GiB;
host MemAvailable is not the effective process budget.

Measurements pin main to CPU 8, Tokio workers to CPUs 0–7 and pollers to CPUs
9–10, with NUMA node 0 memory and mlx5_0 fixed. `remote_memory` uses four
Tokio workers on CPUs 0–3. The no-RPC QP probes use only main. `pin_threads.c` affects only the launched probe process.

## Probe sources

- `capacity_qps.rs`: actual simultaneous QP creation on one CQ; asserts common
  CQ ownership and distinct QPN/route slots, records provider-returned queue
  depths, the failure index, memory observations and complete destruction.
  This probe does not post work requests.
- `capacity_exchange.rs`: creates the requested simultaneous QPs and then
  connects the final two, sends 64 bytes, and validates actual SEND/RECV CQ
  completion proofs and the complete received payload. It needs an additional
  64 MiB registered pool only after creation succeeds.
- `capacity_rpc.rs`: one client/server, one fixed NIC, default SQ/RQ depths of
  64, eight posted receives, 16 KiB receive buffers and a 512 MiB pool limit
  per peer. Maintenance is disabled, both stripe-count limits match the
  requested connection count, and setup deadlines/leases are extended.
  It retains both full path reports and validates each echoed sequence value.
  Reports from newer implementations also include their CQ accounting fields.
- `capacity-harness.patch`: adds these probes to a source snapshot. The Rust
  files here are exact copies of the probe sources.
- `run_capacity.py`: captures the archived invocations, including errors and timeouts,
  with commands, environment, timestamps, binary hashes and raw output.
- `run_multiconn.py`: five baseline/current pairs for each connection count,
  alternating AB/BA order, with setup/health/completion checks on every run.
- `run_paired.py`: the same alternating-order runner used in the previous WRID
  experiment; stock echo uses nine pairs and remote memory uses three.
- `echo-long.patch`: changes only the concurrent echo request count from
  256000 to 2560000, identically for both versions. Stock binaries were copied
  before applying this patch; separate extended binaries were then compiled.
- `capacity_rpc_large.rs`: an additional application capacity probe with
  20000 stripes, two receives, 16 KiB messages, 2 GiB pool limit per endpoint,
  CQ request 524288, a 120 s connect timeout and 300 s lease. This is a
  separate capacity workload, not part of the paired throughput comparison.

## Baseline capacity observations

A CQ requested with 262144 entries accepted 100 and 1000 QPs with returned
SQ/RQ depths of exactly 4. The 20000-QP attempt stopped at exactly 16384 live
QPs, maximum route slot 16383, with `CompletionRoutesExhausted` at index 16384.
All QPs shared the same CQ for both send and receive. Creation took 2.655 s and
complete destruction 4.930 s; live-process RSS was about 164 MiB. RSS after
destruction can retain freed userspace allocator pages and is not a QP count.
The raw low-level probes are `baseline-qps-{100,1000,20000}.json`.

Both the 512- and 1024-stripe RPC attempts failed on the 257th admission with
`65536 + 256 > 65536`. Both peers reported zero paths after rollback and stopped
successfully. The pool memory limit was not the reported failure.
The old default formula is 65536 / (2 * (64 + 64)) = 256 QPs per CQ.

## Throughput method

The shared-capacity cases compare 128 and 256 stripes with both versions using
the identical probe/configuration. Every 64- and 1024-task concurrent case
issues 2560000 requests, after setup and serial warmup. The exploratory baseline
samples are retained separately from paired comparisons.

A 512-/1024-stripe success in the revised implementation establishes additional
capacity. It is not a same-workload QPS comparison against a baseline that
rejects setup. The regular echo benchmark remains a separate one-/two-endpoint
throughput comparison, using the prior experiment's pinned-thread runner.
Stock echo uses 5000 warmup/50000 measured serial requests and 256000
concurrent requests per case. The separate extended echo experiment uses
2560000 concurrent requests per case. Remote memory uses 1000 warmup/5000
measured operations for each direction/size, including 1 MiB, and verifies
the transferred payload. Each process runs its cases sequentially; no
benchmark processes overlap. Setup and report serialization are outside the
timed request loops.

## Interpreting results

`rpc-paired.json`, `echo-stock-paired.json`, `echo-long-paired.json` and
`remote-memory-paired.json` contain all samples and per-metric summaries.
An additional `remote-memory-extended-paired.json` retains five independent
pairs with 10000 measured operations per case after the original three pairs
showed substantial 1 MiB timing variation.
The first is an index of the complete per-process `rpc-*.json` raw artifacts;
the other paired result files embed raw stdout/stderr in each run. Exploratory samples stay
in their separately named files and are not silently added to paired results.
A summary reports both the ratio of the two sample medians and the median of
within-pair ratios. Ranges are observational min/max, not confidence intervals.
No outlier deletion or retry replaces a completed sample.

The 128/256-stripe comparison uses the default SQ/RQ depths but smaller message
buffers than the stock benchmark. These are distinct workloads. The 512/1024
capacity cases use the same configuration as the paired stripe cases, with
throughput disabled; their serial warmup still validates returned values.
The large 20000-stripe probe uses reduced receive depth to limit memory.

Memory observations are `/proc/self/status` VmRSS/VmLck and, for the low-level
probe, cgroup memory. VmLck is not a complete count of RDMA-registered memory;
cgroup memory includes other processes. Host MemAvailable does not override
the container limit. RSS after QP destruction can include retained allocator
pages. The pinning shim also changes the affinity seen by
`available_parallelism()`: baseline DashMap construction on a pinned thread
can already choose four shards. Thus these pinned memory numbers do not
quantify the full benefit of fixed four-shard READ maps on an unpinned
384-CPU process. They also do not establish 20000-connection application
throughput or performance on a remote host/network.

## Reproduction

Use a host with working libibverbs and a local usable mlx5_0 port. This archive
records the exact host and compiler in `hardware.json` and the full device
query in `mlx5_0-devinfo.txt`. If affinity or device is changed for another
host, treat that as a new experiment and preserve the changed settings.

To reproduce **v1**, create two independent archives of baseline commit
`601f404`. Apply `v1-production.patch` to the v1 archive only; it reconstructs
the frozen v1 production files recorded by `source-metadata.json` without
depending on the original `/tmp` snapshot. Copy this directory's `Cargo.lock`
into each archive, apply `capacity-harness.patch`, and use a fresh target
directory for each version. For the final implementation, follow
[final/README.md](final/README.md) instead. For example, from a source archive:

```sh
git apply /path/to/rdma-capacity-data/capacity-harness.patch
CARGO_TARGET_DIR=/tmp/capacity-baseline-fresh-target cargo bench -p ruapc --locked --no-run --bench echo --bench remote_memory --bench capacity_qps --bench capacity_rpc --bench capacity_exchange
cc -O2 -shared -fPIC -o /tmp/pin_threads.so /path/to/rdma-capacity-data/pin_threads.c -ldl -pthread
```

Copy the emitted benchmark executables to immutable version-specific names
before editing the local echo source with `echo-long.patch` and rebuilding
echo in that version's target. `source-metadata.json` lists the recorded
commands, paths and expected hashes. Never reuse the changing workspace's
Cargo target for a source archive: older archive mtimes can otherwise cause
Cargo to reuse an executable from a different source tree.

Run each command serially, and keep builds/tests stopped during sampling:

```sh
python3 run_capacity.py --binary /tmp/capacity-qps-baseline --pin-library /tmp/pin_threads.so --output baseline-qps-20000.json --env RUAPC_CAPACITY_QPS=20000
python3 run_capacity.py --binary /tmp/capacity-rpc-after --pin-library /tmp/pin_threads.so --output after-rpc-1024.json --env RUAPC_CAPACITY_CONNECTIONS=1024 --env RUAPC_CAPACITY_TOTAL_OPS=0
python3 run_multiconn.py --baseline /tmp/capacity-rpc-baseline --current /tmp/capacity-rpc-after --pin-library /tmp/pin_threads.so --output-dir /tmp/capacity-results --pairs 5 --connections 128 256
python3 run_paired.py --baseline /tmp/echo-baseline-stock --current /tmp/echo-after-stock --pin-library /tmp/pin_threads.so --output /tmp/echo-stock-paired.json --pairs 9
python3 run_paired.py --baseline /tmp/echo-baseline-long --current /tmp/echo-after-long --pin-library /tmp/pin_threads.so --output /tmp/echo-long-paired.json --pairs 5
python3 run_paired.py --baseline /tmp/remote-memory-baseline --current /tmp/remote-memory-after --pin-library /tmp/pin_threads.so --output /tmp/remote-memory-paired.json --pairs 3 --kind remote_memory --warmup 1000 --serial 5000
python3 run_paired.py --baseline /tmp/remote-memory-baseline --current /tmp/remote-memory-after --pin-library /tmp/pin_threads.so --output /tmp/remote-memory-extended-paired.json --pairs 5 --kind remote_memory --warmup 1000 --serial 10000
```

To build the optional large application probe, apply `large-harness.patch`
after `capacity-harness.patch`, build `--bench capacity_rpc_large`, and run
that executable with `run_capacity.py --timeout 300`. Its defaults are
the recorded large-capacity configuration. This addition does not modify
any production source or any previously measured executable.

## Additional capacity results

The revised low-level probe created all 20000 requested QPs on one CQ. A
separate probe kept all 20000 alive while performing a verified 64-byte
SEND/RECV exchange through slots 19998 and 19999. The revised application
probe established 512 and 1024 healthy paths on both endpoints, verified all
warmup echoes, and stopped successfully. Their raw files retain the actual
provider CQ capacity (131071 after a 65536 request), reservations and layout.

The optional reduced-receive-depth 20000-stripe application experiment is
retained as `after-rpc-20000.json.gz`. It established 20000 paths per endpoint
on a CQ with actual capacity 1048575 (requested 524288), but the first echo
failed with `ConnectionClosed`. The client report had 19999 healthy paths and
one unhealthy path; the server report had 20000 healthy paths. Therefore this
experiment does **not** establish successful application traffic on 20000
stripes. Setup/report took 40.84 s, process RSS reached about 4.20 GiB, and
server stop completed in 18.02 s. All parameters, paths, stdout/stderr and the
failure remain in the compressed JSON; it can be read with `gzip.open` or
`gzip -cd`. Compression preserves the full raw artifact, not a summary.

The extended remote-memory series exhibited substantial chronological drift in
1 MiB case times for both implementations. Its ratio of version medians and
median within-pair ratios can therefore disagree considerably. Both summaries
and every chronological sample remain visible; the extension does not replace
the original three-pair result or establish an invariant remote-memory
throughput improvement. The higher-level interpretation is in
[the capacity report](../rdma-capacity.md).

A second 20000-stripe attempt adds WARN tracing and retains the same R2/pool
settings (`after-rpc-20000-r2-trace.json.gz`). Setup and the first echo succeeded,
and both reports contained 20000 healthy paths. The 40000 serial warmup echoes
did not finish before the 300 s runner timeout; process termination/driver
cleanup extended the observed runner duration to 323.45 s. There were no
logged completion/flow errors, only the advisory receive-ring memory sizing
warning. This run also does not establish completed traffic validation across
all stripes. `capacity-outcomes.json` provides small summaries linking to
these full raw archives.

`large-trace-harness.patch`, applied after `large-harness.patch`, adds the
independent WARN-logging probe. It accepts `RUAPC_CAPACITY_RECV` (default 2)
and `RUAPC_CAPACITY_POOL_GIB` (default 2); all other defaults match the original
large probe. Its benchmark name is `capacity_rpc_large_trace`. Changing these
settings for a subsequent capacity probe never changes the previously
measured stock/extended echo or remote-memory binaries.

The R8/default receive-ring follow-up (`after-rpc-20000-r8-trace.json.gz`) used
4 GiB pools per endpoint and **completed**: both endpoints had 20000 healthy
paths, all 40000 serial round-robin echoes verified, and server stop finished.
Setup/report took 32.84 s, the serial sweep 174.35 s and stop 17.91 s; process
RSS was about 7.95 GiB. Each CQ reported 1048575 actual entries and 320032
reserved credits. This proves application traffic works at that capacity, but
the approximately 229 RPC/s serial sweep also exposes the v1 poller's cost of
scanning all connections after completions. It motivated the later maintenance
optimization rather than being presented as acceptable high-connection QPS.

## Remote-memory diagnostics

`remote-diagnostic-harness.patch` adds `remote_memory_diagnostic.rs` to a fresh
source snapshot independently of the capacity patches; its manifest hunk targets
the original manifest end. For an existing capacity snapshot, copy the diagnostic
Rust file and append its `[[bench]]` entry instead of reapplying that manifest
hunk. It retains complete payload verification, selects one 1 MiB direction
per process with `RUAPC_DIAG_DIRECTION=read|write`, and measures verification
CPU wall time separately from the handler's `remote_read_all` or
`remote_write_all` await. That await includes admission, internal RPC and
completion scheduling as well as RDMA; it is **not** pure device-DMA time.
The additional atomics/clock reads make this an explanatory instrumented
workload, separate from the stock benchmark evidence.

`run_remote_diagnostic.py` executes five alternating pairs per direction with
1000 warmup and 10000 measured requests. `remote-diagnostic.json` indexes the
20 complete raw files and preserves full-process CPU usage/context switches,
which include setup and warmup. Full 1 MiB verification was stable around
37 microseconds in both versions. The baseline itself showed READ wall times
above 350 microseconds while its corresponding remote-await/verification
segments remained about 67–79/37 microseconds. This demonstrates substantial
variation outside those measured segments; it does not identify a unique
cause. The independent diagnostics did not reproduce a stable >5% paired
wall-time slowdown. Original stock and extended results are retained equally.

Kernel performance-counter tooling was unavailable (`perf-availability.log`);
no system packages were installed. The diagnostic runner can be reproduced:

```sh
python3 run_remote_diagnostic.py --baseline /tmp/remote-diagnostic-baseline --current /tmp/remote-diagnostic-after --pin-library /tmp/pin_threads.so --output-dir /tmp/remote-diagnostic-results --pairs 5
```
