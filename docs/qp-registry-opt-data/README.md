# QP registry performance optimization

This experiment compares the QPN registry committed at
`b8f92fb2299719804e19d342bf4e004bd45a83bb` with three proposed performance
changes. No samples from earlier experiments are reused.

The baseline's 184-file Rust/Cargo digest is
`cf91cd3502051d436d5028abce86aa3ec550b3255048fddf864f72031ce6b985`.
The source archive, dependency lock and compiled native shim/header exactly
match the previous experiment's frozen current implementation. Its three
immutable throughput executables were checked against their archived SHA256
values and copied into this experiment's separate binary directory. Full
provenance is in [source-metadata.json](source-metadata.json); the independent
source archive is `/tmp/ruapc-qp-registry-opt-baseline`.

Each candidate was snapshotted after its production source was frozen, built
with a fresh independent Cargo target, then copied into immutable executables
with recorded hashes. Sampling began after both that build and the parent
validation completed. This avoided reuse of executables from older source
archives due to source mtimes.

Candidate A (IndexMap plus dirty indices) did not demonstrate a clear
throughput improvement and was not adopted. Its complete 20-run experiment,
three-file patch, source/binary fingerprints and validation logs are retained
in [candidate-a](candidate-a/README.md). B and C subsequently started
from the same committed baseline, with new paired samples.

Candidate B combined an activation-load fast path with maintenance timestamp
reuse. It improved the many-stripe/high-concurrency cases but reduced
two-endpoint echo throughput in all five pairs, and was not adopted as the
final default. All 40 runs, the four-file patch and validation are retained in
[candidate-b](candidate-b/README.md). Candidate C isolated activation
handling using new samples against the same baseline, but was also not adopted.

## Final decision and complete evidence

See the [full research report](../qp-registry-opt.md) for the implementation
tradeoffs. [production-state.json](production-state.json) records restoration
of the unchanged baseline and the final formatting/Clippy checks.

No candidate is adopted for the general default. The final production source
returns to baseline commit `b8f92fb` with Rust/Cargo digest `cf91cd35...`.
The experiment retains all 100 new process runs forming 50 pairs, all three
evaluated patches, source/build/binary provenance and production validation.
There are no failed processes, removed samples or repeated replacement groups.

Verification scope differs by harness: all 100 processes returned zero; all
60 many-stripe RPC runs checked exact healthy path counts, response payloads
and orderly stop; all 20 remote-memory runs verified full payloads. The 20
stock echo runs checked RPC success with `unwrap` and consumed results with
`black_box`, but did not assert response contents. Successful echo runs are
not evidence of a separate payload-content assertion.

| Candidate | Evaluated change | Process runs / pairs | Decision and evidence |
| --- | --- | ---: | --- |
| A | IndexMap and dense dirty indices | 20 / 10 | No clear throughput gain; [archive](candidate-a/README.md), [patch](candidate-a/poller.patch) |
| B | Activation load plus shared maintenance clock | 40 / 20 | Targeted many-stripe gains with two-endpoint echo regression; [archive](candidate-b/README.md), [patch](candidate-b/combined.patch) |
| C | Activation load only | 40 / 20 | Modest/mixed changes and negative 1 MiB READ paired median; [archive](candidate-c/README.md), [patch](candidate-c/activation.patch) |

B remains reviewable as a targeted candidate for workloads prioritizing many
connections; the measurements do not justify adopting it as a general default.
Its paired QPS medians at 128/256 stripes and 1024 tasks were +5.40%/+4.08%,
while two-endpoint/1024-task echo was negative in all five pairs. C removed that
consistent two-endpoint result but did not establish a broad improvement.

Large path reports are stored in 60 gzip artifacts. Their indices preserve both
compressed and original JSON SHA256 values. Echo/remote raw stdout and stderr
remain inside their paired JSON containers. Candidate results stay separate;
changes are calculated from each candidate's newly collected baseline pairs.

[Top-level integrity](integrity.json) verifies every candidate archive,
unchanged baseline, referenced harness dependencies, source/native inputs,
immutable executables, lock and all 100 complete runs. Each candidate also has
its own file-hash manifest. Source code from rejected candidates remains in
the three patches, so restoring production does not erase the experiments.

## Workloads and placement

The unchanged harnesses and runners are linked from the committed previous
experiments, with their hashes recorded as dependencies:

- [128/256-stripe RPC](../rdma-capacity-data/capacity_rpc.rs): five alternating
  pairs for each stripe count, 2.56 million verified requests at each of
  64 and 1024 tasks. Both endpoints use SQ/RQ capacities 64, R8, 16 KiB message
  buffers and 512 MiB pools. Setup and path report generation are outside
  throughput timing.
- Long echo: five alternating pairs, 2.56 million concurrent requests per
  case with 64/1024 tasks and one/two server endpoints, 5000 warmup requests
  and 50000 serial requests. Apply the same
  [echo-long.patch](../rdma-capacity-data/echo-long.patch) to both sources.
- Stock remote memory: five alternating pairs with 1000 warmup and 10000
  measured operations for each direction/size, 64 KiB and 1 MiB. Full payload
  verification remains inside the measured workload.

Each full suite contains 40 process runs forming 20 pairs. A stopped after
its 20 RPC runs; B and C each completed a full suite. All samples,
including unfavorable results and outliers, remain in the archive. A later
candidate or diagnostic group needs a separate result directory; it must
not overwrite these samples.

Hardware is fixed to mlx5_0, with NUMA node 0 memory. The existing pinning
shim places main on CPU 8, runtime workers on CPUs 0–7 (0–3 for remote
memory), and the two endpoint pollers on CPUs 9–10. Refreshed read-only
device information is in [hardware.json](hardware.json) and
[mlx5_0-devinfo.txt](mlx5_0-devinfo.txt). Builds, tests and other benchmark
processes must finish before sampling; all benchmark runs are serial.

## Reproducing

Create independent baseline/candidate source archives and copy
[Cargo.lock](Cargo.lock) into each. Copy the linked `capacity_rpc.rs` into
`ruapc/benches` and add a `[[bench]]` entry named `capacity_rpc` with
`harness = false`. Apply the linked echo request-count patch. Compile each
source tree with its own fresh target:

```sh
CARGO_TARGET_DIR=/tmp/qp-registry-opt-current-fresh-target cargo bench -p ruapc --locked --no-run --bench capacity_rpc --bench echo --bench remote_memory
cc -O2 -shared -fPIC -o /tmp/qp-registry-opt-pin.so docs/rdma-capacity-data/pin_threads.c -ldl -pthread
```

Copy emitted executables into immutable names matching `run_suite.py` and
copy the pin library as `pin_threads.so` into the same directory. The actual
source and executable hashes are recorded in the metadata before measuring.

```sh
python3 docs/qp-registry-opt-data/run_suite.py --binary-dir /tmp/ruapc-qp-registry-opt-candidate-c-bench --output-dir /tmp/qp-registry-opt-results --groups rpc echo remote --pairs 5
python3 docs/qp-registry-data/summarize.py --directory /tmp/qp-registry-opt-results
```

Use `--dry-run` to inspect commands without running benchmarks, or `--groups`
to execute one stage at a time. Existing stage result files are rejected to
avoid replacing samples. Large path reports retain their exact original
JSON inside deterministic gzip files; the multi-stripe index records both
compressed and uncompressed SHA256 values. Read them with the committed
[read_artifact.py](../qp-registry-data/read_artifact.py).
