# Final capacity implementation evidence

This directory measures the frozen final implementation after adding targeted
connection maintenance and raising the standalone ACK-of-ACK trigger threshold
to at least two. DATA acknowledgments and keepalives can still carry smaller
ACK deltas. The baseline
is commit `601f404`; the earlier capacity implementation and every original
sample remain in the parent directory. Do not mix the v1 and final binary
fingerprints or test results.

`checks.json` identifies the validated source with the full production digest
`af6fab2d623ebe602a5df9b17f5595964c323cf604bd9d4b2c303f6dd63af45f`.
`source-metadata.json` verifies that same complete source set in an independent
snapshot, excluding the documented benchmark-only overlays. A new independent
Cargo target built the final executables; `build.log` and immutable executable
hashes are retained. The baseline executables are the unchanged copies from
the beginning of the experiment. No builds or tests run during measurements.

## Capacity

The two compressed `rpc-20000-r{2,8}-trace.json.gz` artifacts contain the full
raw output and all path entries. Both runs used one client/server, one CQ per
endpoint requested with 524288 entries, default SQ/RQ capacities of 64,
16 KiB maximum messages, 120 s connect timeout and 300 s connection lease.
The R2 run used 2 GiB pools per endpoint; the R8 run used 4 GiB. WARN tracing
was enabled identically to the v1 diagnostic capacity probes.

Both endpoints reported 20000 healthy paths, 40000 serial round-robin echoes
were verified, and server stop completed. The actual mlx5 CQ capacity was
1048575, route capacity 1048575, and sequence width 42 bits. Reservations were
100032 entries for R2 and 320032 for R8. The full round-robin echo sweep took
1.341 s for R2 and 1.653 s for R8. The corresponding v1 R2 sweep did not finish
before its 300 s runner limit; v1 R8 needed 174.355 s. These are explicit serial
traffic validation sweeps, not concurrent throughput results.

`capacity-outcomes.json` exposes small summaries of these complete compressed
artifacts. Each artifact can be read directly with Python `gzip.open` or
`gzip -cd`; no raw path or diagnostic output was discarded.

## Paired workloads

All settings and runner source are in the parent directory's [README](../README.md).
The final comparisons use the same workload and unchanged baseline executable:

- `echo-long-paired.json`: five alternating pairs, 2.56 million requests per
  concurrent case at 64 and 1024 tasks, with one and two server endpoints.
  Serial cases retain 5000 warmup and 50000 measured requests.
- `rpc-paired.json`: five alternating pairs for each of 128 and 256 stripes;
  each process verifies exact healthy path counts, then performs 2.56 million
  RPCs at each of 64/1024 tasks. It indexes all 20 per-process raw files.
- `remote-memory-extended-paired.json`: five alternating pairs, 1000 warmup and
  10000 measured operations for both directions at 64 KiB/1 MiB, with complete
  payload verification.
- `remote-memory-confirmation-paired.json`: ten additional pairs of exactly
  the same standard remote-memory binary and workload; no rebuild or parameter
  change. `remote-memory-combined-paired.json` indexes all 15 original/confirmation
  pairs and retains their separate group summaries and original pair IDs.
- `remote-diagnostic.json`: five pairs for each isolated 1 MiB direction using
  the same instrumented diagnostic source as v1. It indexes 20 raw processes
  and separates full verification from the remote-operation await; that await
  includes admission, internal RPC and completion scheduling, not pure DMA.

No completed samples are removed. The summary includes the ratio of version
medians, median within-pair ratios and observed min/max. The environment is
local mlx5 loopback on one host; variability in remote-memory timings does not
justify claims about unmeasured networks or attribution to a unique component.

## Reproduction

Apply the parent directory's capacity, large-capacity and tracing patches to
an archive of the final implementation; copy `remote_memory_diagnostic.rs`
and append its bench entry if diagnostics are desired. Apply `echo-long.patch`
identically to baseline and final echo sources. Keep each version's Cargo
target independent and copy executables before measuring, as documented in
`source-metadata.json` and the parent README. Use the parent `Cargo.lock`.

For example, from the repository root with immutable executables available:

```sh
python3 docs/rdma-capacity-data/run_capacity.py --binary /tmp/ruapc-capacity-bench/capacity-rpc-large-trace-final --pin-library /tmp/ruapc-capacity-bench/pin_threads.so --output /tmp/final-rpc-20000-r8.json --timeout 300 --env RUAPC_CAPACITY_RECV=8 --env RUAPC_CAPACITY_POOL_GIB=4
python3 docs/rdma-capacity-data/run_paired.py --baseline /tmp/ruapc-capacity-bench/echo-baseline-long --current /tmp/ruapc-capacity-bench/echo-final-long --pin-library /tmp/ruapc-capacity-bench/pin_threads.so --output /tmp/final-echo.json --pairs 5
python3 docs/rdma-capacity-data/run_multiconn.py --baseline /tmp/ruapc-capacity-bench/capacity-rpc-baseline --current /tmp/ruapc-capacity-bench/capacity-rpc-final --pin-library /tmp/ruapc-capacity-bench/pin_threads.so --output-dir /tmp/final-rpc --pairs 5 --connections 128 256
python3 docs/rdma-capacity-data/run_paired.py --baseline /tmp/ruapc-capacity-bench/remote-memory-baseline --current /tmp/ruapc-capacity-bench/remote-memory-final --pin-library /tmp/ruapc-capacity-bench/pin_threads.so --output /tmp/final-remote.json --pairs 5 --kind remote_memory --warmup 1000 --serial 10000
python3 docs/rdma-capacity-data/run_remote_diagnostic.py --baseline /tmp/ruapc-capacity-bench/remote-diagnostic-baseline --current /tmp/ruapc-capacity-bench/remote-diagnostic-final --pin-library /tmp/ruapc-capacity-bench/pin_threads.so --output-dir /tmp/final-remote-diagnostic --pairs 5
```

The initial five standard remote-memory pairs showed lower 1 MiB throughput
(READ -11.85%, WRITE -7.57% by ratio of medians). The independent ten-pair
confirmation did not reproduce that direction: READ +12.81%, WRITE +2.30%.
Both versions exhibited slow runs in the confirmation. All 15 pairs together
showed READ +1.09% and WRITE +0.76% by version medians, with median paired
changes of +0.20% and -0.95%. These groups and their full ranges remain visible;
the combined result does not erase the first group or promise zero regression
for every workload. The separate instrumented diagnostic also is not a
replacement for the standard benchmark.

The combined index is reproducible without rerunning hardware:

```sh
python3 docs/rdma-capacity-data/combine_remote.py --initial docs/rdma-capacity-data/final/remote-memory-extended-paired.json --confirmation docs/rdma-capacity-data/final/remote-memory-confirmation-paired.json --output /tmp/remote-memory-combined.json
```
