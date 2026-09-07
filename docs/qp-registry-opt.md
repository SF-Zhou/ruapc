# QP registry performance optimization

None of the three candidates was adopted as a general default optimization.
Candidate B improved the measured 128/256-connection high-concurrency cases
by 5.40%/4.08%, but regressed two-endpoint echo by 4.81%. Candidate C had mixed
results and a -12.10% paired median for 1 MiB READ with wide variation.
Production code therefore remains identical to `b8f92fb`.

This investigation used independent source snapshots and fresh paired
measurements for every candidate against that baseline. Earlier results in
[QP registry](qp-registry.md) are historical evidence, not reused samples.
All three patches and all 100 measured runs are retained for review or
workload-specific adoption; the default implementation was not changed.

## C: activation fast path

Candidate C changes only `RdmaSocket::take_activation_request`. An empty
request takes an acquire load, avoiding the unconditional acquire/release
exchange previously performed on every eligible connection-maintenance
visit. A pending request still uses the exchange's actual result to consume
it at most once. It adds no dependency, unsafe code or configuration.

The producer stores the request before publishing the poller's maintenance
wake. If a request races an empty fast-path read, that wake schedules a later
pass; a pass that acquires the published wake also observes the preceding
request store. ACK-capacity checks still precede consumption, so a request
remains pending when the capacity is full. Existing request coalescing and
failure retry behavior are preserved.

Candidate C retains the QPN hash map, fixed **2 type bits plus 62
sequence bits**, per-QP sequences, CQ-issued completion tokens, retirement
floors and CQ credit admission. Flow-control timing still samples the clock
per connection, as in the baseline.

## Earlier candidates

### A: contiguous connection storage

Candidate A used `IndexMap`, stored dense indices in the dirty queue and
repaired those indices after connection removal. It passed 539 tests and
strict Clippy, including a regression for repeated tail movement during
maintenance. Twenty process runs compared it with the baseline.

| Connections / concurrent tasks | Paired throughput change median [range] |
| --- | ---: |
| 128 / 64 | -1.05% [-5.27%, +0.38%] |
| 128 / 1024 | +0.85% [-1.16%, +1.42%] |
| 256 / 64 | -1.45% [-5.62%, +2.33%] |
| 256 / 1024 | +0.19% [-2.95%, +2.13%] |

This did not demonstrate a consistent throughput gain, so the layout change
was removed. Its complete patch, fingerprints, validation and all samples
remain in [candidate-a](qp-registry-opt-data/candidate-a/).

### B: fast activation plus a shared maintenance timestamp

Candidate B combined the activation fast path with reusing a maintenance
pass's timestamp for flow-control timing decisions. Successful ACK posts
still recorded fresh timestamps. It passed 539 tests and strict Clippy,
including a test of timing decisions made with a timestamp earlier than a
newly posted ACK.

| Workload | Paired throughput change median [range] |
| --- | ---: |
| RDMA/concurrent_64_kops | +14.35% [-10.23%, +14.65%] |
| RDMA/concurrent_1024_kops | +0.03% [-13.24%, +6.66%] |
| RDMA (2 endpoints)/concurrent_64_kops | -2.83% [-7.82%, +1.37%] |
| RDMA (2 endpoints)/concurrent_1024_kops | -4.81% [-28.42%, -1.11%] |
| 128_connections/64_tasks_qps | -0.32% [-3.04%, +7.41%] |
| 128_connections/1024_tasks_qps | +5.40% [-4.17%, +7.43%] |
| 256_connections/64_tasks_qps | +2.58% [-2.17%, +4.88%] |
| 256_connections/1024_tasks_qps | +4.08% [+3.35%, +8.01%] |
| remote_read_all/64KiB_mibps | +9.30% [-29.84%, +11.97%] |
| remote_write_all/64KiB_mibps | -1.04% [-25.02%, +29.45%] |
| remote_read_all/1024KiB_mibps | -11.23% [-18.76%, +37.55%] |
| remote_write_all/1024KiB_mibps | -2.06% [-37.09%, +66.30%] |

Although the 128/256-connection, 1024-task cases improved by 5.40%/4.08%,
the two-endpoint echo at 1024 tasks regressed in all five pairs, with a
-4.81% paired median. This was not accepted as a general optimization.
Remote-memory samples also varied widely; per-version and paired medians
can even have opposite signs. No outliers were removed.

Source review found no change in the count-triggered DATA/ACK thresholds or
credit accounting. Sharing time can delay a purely time-triggered keepalive,
including when it is piggybacked, until a subsequent maintenance pass. The
available raw measurements do not identify the cause of the regression.
The clock-sharing change was removed for candidate C to separate the two
factors. Neither candidate was adopted in the production source. All forty
runs, the complete patch, source/binary provenance and validation remain in
[candidate-b](qp-registry-opt-data/candidate-b/).

## Validation and measurement method

Candidate C passed **538 tests with zero failures**, plus formatting and
strict Clippy (`--workspace --all-features --all-targets -- -D warnings`).
Thirteen documentation examples remain ignored. The source and validation
outputs are in [checks.json](qp-registry-opt-data/candidate-c/checks.json),
[tests.log](qp-registry-opt-data/candidate-c/tests.log) and
[clippy.log](qp-registry-opt-data/candidate-c/clippy.log).

All comparisons use independent source snapshots and immutable binaries,
identical benchmark sources and dependency locks, and fixed CPU/NUMA
placement on the same physical mlx5 local-loopback device. Verified binaries
from the preceding experiment provide the baseline executables; performance
samples are newly measured. Builds, tests and benchmark processes do not
run concurrently with measurements.

Each measured workload group has five pairs in alternating process order.
Concurrent echo cases perform 2.56 million requests each. The 128/256-
connection groups use R8, SQ/RQ depth 64, 16 KiB message buffers and 512 MiB
pools per endpoint. Ordinary echo uses 5000 warmup and 50000 measured serial
requests. Remote memory uses 1000 warmup and 10000 measured operations per
direction and size, including payload verification.

The echo subcases have fixed order within each process: the two-endpoint
1024-task case runs last and inherits the preceding load state. Those two
endpoints are two listen addresses of one server; the recorded placement
still has two poller threads, not four. The benchmark does not record
per-endpoint traffic counts, so the label does not establish equal traffic.

Per-version medians and the median paired ratio are different statistics.
Paired changes use `100 * (candidate / baseline - 1)`; ranges include every
pair and are not confidence intervals. No samples are removed. Candidate
samples are kept separate, and cross-round absolute throughput is not used
to infer gains. Results characterize this host and workload, not a remote
network. Reproduction and raw evidence are indexed in
[the archive](qp-registry-opt-data/README.md).

## Complete paired results for candidate C

The baseline and candidate columns are per-version medians; paired changes
are calculated from each baseline/candidate pair before taking the median.
Throughput increases are positive; positive latency changes mean slower
requests.

### Multiple connections

| Workload | Baseline median | C median | Paired change median [range] |
| --- | ---: | ---: | ---: |
| 128_connections/64_tasks_qps | 523.944 kops/s | 521.817 kops/s | -0.47% [-5.78%, -0.31%] |
| 128_connections/1024_tasks_qps | 698.276 kops/s | 700.227 kops/s | +0.48% [-4.43%, +1.48%] |
| 256_connections/64_tasks_qps | 514.080 kops/s | 515.854 kops/s | +2.36% [-3.72%, +3.79%] |
| 256_connections/1024_tasks_qps | 681.890 kops/s | 686.875 kops/s | +1.30% [-0.64%, +5.03%] |

### Ordinary echo

| Workload | Baseline median | C median | Paired change median [range] |
| --- | ---: | ---: | ---: |
| RDMA/serial_16B_us | 35.19 us/op | 34.16 us/op | +4.26% [-4.55%, +6.17%] |
| RDMA/serial_4096B_us | 37.63 us/op | 35.87 us/op | +0.90% [-16.19%, +9.50%] |
| RDMA/concurrent_64_kops | 635.4 kops/s | 679.2 kops/s | -1.10% [-2.85%, +15.85%] |
| RDMA/concurrent_1024_kops | 1126.8 kops/s | 1196.0 kops/s | -2.17% [-7.60%, +24.26%] |
| RDMA (2 endpoints)/concurrent_64_kops | 637.7 kops/s | 630.9 kops/s | +0.92% [-5.70%, +3.59%] |
| RDMA (2 endpoints)/concurrent_1024_kops | 1308.4 kops/s | 1338.6 kops/s | +1.76% [-1.72%, +2.99%] |

### Remote memory

| Workload | Baseline median | C median | Paired change median [range] |
| --- | ---: | ---: | ---: |
| remote_read_all/64KiB_mibps | 795.3 MiB/s | 844.3 MiB/s | +6.16% [+1.04%, +47.59%] |
| remote_write_all/64KiB_mibps | 803.0 MiB/s | 807.0 MiB/s | +2.98% [-17.08%, +64.34%] |
| remote_read_all/1024KiB_mibps | 5042.3 MiB/s | 4382.1 MiB/s | -12.10% [-34.50%, +42.11%] |
| remote_write_all/1024KiB_mibps | 3957.1 MiB/s | 4101.8 MiB/s | -1.37% [-31.11%, +40.56%] |

Candidate C did not reproduce B's consistent two-endpoint echo regression,
but single-endpoint 1024-task throughput was lower in four of five pairs.
Its multi-connection 1024-task paired medians were only +0.48%/+1.30%.
The 64 KiB READ throughput increased in all five pairs; 1 MiB READ was slower
in the last three pairs after being faster in the first two. Those larger
remote-memory ranges prevent attributing precise gains or losses to this
small code change. They do not support declaring a general improvement.

The scope is **100 process runs / 50 pairs**: 20 runs for A, 40 for B and 40
for C. Every completed sample is retained and all processes exited normally.
The multiple-connection and remote-memory workloads passed their payload
checks; the multiple-connection workloads also passed their path/lifecycle
checks. Stock echo records successful RPC returns without asserting their
contents. Each candidate passed its full workspace test suite and strict
Clippy. The candidates' source, native shim,
lock files, immutable binaries and raw artifacts are fingerprinted separately.

B remains a concrete option for deployments that prioritize the measured
many-connection/high-concurrency workload and accept its other measured
costs. Its [complete patch](qp-registry-opt-data/candidate-b/combined.patch)
is available without making that tradeoff the default for all workloads.
C's [single-change patch](qp-registry-opt-data/candidate-c/activation.patch)
and A's [layout patch](qp-registry-opt-data/candidate-a/poller.patch) are
also preserved. Further performance claims require measurements matching
the intended workload; these results alone do not justify a universal gain.
