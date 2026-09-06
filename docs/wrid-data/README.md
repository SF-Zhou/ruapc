# WRID benchmark evidence

The baseline is commit `642a7d9efd6c51c298a541b42e144ff96591ae50`.
`source-metadata.json` records source hashes; `Cargo.lock` preserves the exact
shared dependency resolution used in every build.
Each measured JSON contains every raw stdout/stderr sample, command, binary
hash, checked thread placement, elapsed time, and summary statistics.
Version order alternates baseline/current then current/baseline between pairs.

## Machine and placement

- AMD EPYC 9655 96-Core Processor; Linux x86_64, 384 logical CPUs, rustc 1.98.0.
- Physical `mlx5_0` Ethernet port, 200 Gb/s, NUMA node 0; CPU frequency governor
  `performance`. `machine.json` retains device state and CPU topology.
- Echo: eight Tokio workers on CPUs 0–7, main thread on CPU 8, RDMA pollers on 9–10.
- Remote memory: four Tokio workers on CPUs 0–3, main on 8, pollers on 9–10.
- Both versions use NUMA node 0 memory, the same Cargo.lock, and the optimized
  Cargo bench profile. No compilation, project tests, or other project
  benchmarks run during sampling.
- Echo uses 5000 warmup and 50000 measured serial requests per payload size.
  The stock concurrent case has 256000 total requests; the extended case has
  2560000. Setup and registration are outside measured loops.
- Remote memory uses 1000 warmup and 5000 measured operations per size/direction,
  with complete payload verification included in measured time.

## Recorded stages

- `exploratory.json`: all six exploratory baseline runs. The first three had an
  incorrect Tokio thread-name match, so workers inherited CPU 8. The next three
  verified placement. Neither group is included in paired comparisons.
- `echo.json`: nine pairs of the initial implementation and baseline, stock
  256000-request concurrent cases.
- `remote-memory.json`: three pairs of the initial implementation and baseline.
- `echo-initial-long.json`: five pairs of the initial implementation and baseline,
  with 2560000 concurrent requests per case.
- `initial-long.patch`: restores the measured initial implementation plus the
  extended echo harness when applied to the baseline. `long-concurrent.patch`
  changes only the concurrent request count and is shared by both versions.
- `echo-final-long.json`: nine extended echo pairs for the final implementation
  after consolidating SQ sequence allocation into the existing posting mutex.
- `remote-memory-final.json`: three remote-memory pairs for the final implementation.
- `type-sizes.json` and `type_sizes.rs`: object sizes measured against the actual
  compiled rlibs, excluding separately allocated storage.

A shared Cargo target directory initially reused baseline objects when building
an older-timestamp source snapshot. Binary-hash equality caught this before any
sample was taken. `current-long-build.log` records that rejected build;
`current-long-isolated-build.log` records the replacement in an independent
fresh target directory. No measurement uses the rejected binary.

## Final echo measurements

Nine alternating pairs, 2560000 requests per concurrent case. Values below are
medians; the change is the ratio of the two medians.

| Case | Baseline kQPS | Final kQPS | Change |
|---|---:|---:|---:|
| 1 endpoint, 64 tasks | 717.4 | 723.3 | +0.82% |
| 1 endpoint, 1024 tasks | 1366.6 | 1406.8 | +2.94% |
| 2 endpoints, 64 tasks | 678.1 | 680.5 | +0.35% |
| 2 endpoints, 1024 tasks | 1420.4 | 1451.1 | +2.16% |

Serial 16 B latency was 26.57 → 26.25 µs (-1.20%); 4 KiB was
28.37 → 28.11 µs (-0.92%). These measurements show no QPS regression in
the exercised cases.

The initial short cases showed up to -2.32% lower median throughput. Repeating
the initial implementation with ten times as many concurrent requests produced
median changes from -0.72% to +1.36%, and paired-ratio median
changes from -0.66% to +0.96%. Thus the short-case differences do not establish a
persistent regression or identify a cache-line/atomic cause. The final change
removes a redundant SQ atomic by sharing the existing posting mutex with sequence
allocation; the final measurements above compare that complete implementation.

## Final remote-memory measurements

Three alternating pairs, with every complete payload verified successfully.
Positive latency changes are slower.

| Case | Baseline µs/op | Final µs/op | Change |
|---|---:|---:|---:|
| 64 KiB read | 63.13 | 62.02 | -1.76% |
| 64 KiB write | 64.50 | 64.94 | +0.68% |
| 1 MiB read | 183.66 | 168.22 | -8.41% |
| 1 MiB write | 182.93 | 183.09 | +0.09% |

The 1 MiB cases remain variable: final-run read samples ranged from
166.00–216.93 µs for baseline and 163.81–190.86 µs for final; write ranged
175.56–187.63 µs and 173.67–214.17 µs. These three pairs do not establish
an independent bulk-transfer speedup. The initial stage's 1 MiB read median
was slower, and those samples are retained in `remote-memory.json`.

## Object memory cost

| Type | Baseline | Initial | Final |
|---|---:|---:|---:|
| QueuePair | 184 B | 224 B | 232 B |
| CompletionQueue | 40 B | 80 B | 80 B |
| CompletionCursor | 8 B | 8 B | 8 B |

All alignments are 8 B. These are object sizes, excluding dynamically allocated
route/free-slot vectors and in-flight buffer storage.

## Reproduction

Use independent source directories and target directories for baseline and
candidate. Copy `docs/wrid-data/Cargo.lock` into each workspace root before
building. For an extended run, apply `long-concurrent.patch` to both echo
harnesses before compilation.
The source copies and resulting binaries must stay fixed throughout sampling.

```sh
CARGO_TARGET_DIR=/tmp/wrid-baseline-target cargo bench --manifest-path /tmp/wrid-baseline/Cargo.toml -p ruapc --bench echo --bench remote_memory --no-run --locked
CARGO_TARGET_DIR=/tmp/wrid-candidate-target cargo bench --manifest-path /tmp/wrid-candidate/Cargo.toml -p ruapc --bench echo --bench remote_memory --no-run --locked
cc -O2 -shared -fPIC -o /tmp/wrid-pin.so docs/wrid-data/pin_threads.c -ldl -pthread
python3 docs/wrid-data/run_paired.py --baseline /path/to/baseline-echo-binary --current /path/to/candidate-echo-binary --pin-library /tmp/wrid-pin.so --output /tmp/echo.json --pairs 9
python3 docs/wrid-data/run_paired.py --baseline /path/to/baseline-remote-memory-binary --current /path/to/candidate-remote-memory-binary --pin-library /tmp/wrid-pin.so --output /tmp/remote-memory.json --kind remote_memory --pairs 3 --warmup 1000 --serial 5000
```

The binding shim acts only inside the benchmark process. The runner checks every
worker/main/poller binding from the captured stderr before accepting a sample.
Choose valid physical CPU IDs and NIC-local memory for another machine, and
update both the shim and runner placement together.

Positive QPS changes are faster; positive latency changes are slower. Medians
and observed ranges describe these workloads on this machine; the shorter
concurrent cases and 1 MiB remote-memory cases have measurable run-to-run noise.
