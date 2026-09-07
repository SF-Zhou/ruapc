# Candidate B: combined candidate, not adopted

Candidate B retains the baseline HashMap and changes the empty activation
check to load before swapping, while reusing the maintenance timestamp for
ACK threshold decisions. Actual successful ACK submission timestamps remain
fresh. No low-level WRID identity, DMA lifetime or CQ admission behavior is
changed.

The baseline is commit `b8f92fb2299719804e19d342bf4e004bd45a83bb`.
The candidate's 184-file Rust/Cargo digest is
`35c505f2b65096fc8483debc814e58d8e075c17c43b85dd03464cd66e200b0cf`.
[Source metadata](source-metadata.json) records the independent snapshot,
fresh Cargo target, immutable executable hashes, native shim/header and
three unchanged benchmark input hashes. Baseline provenance and shared
harness/lock hashes are in the parent [metadata](../source-metadata.json).

The parent validated 539 tests, zero failures, 13 ignored doctests, formatting
and strict Clippy before measurements. See [checks](checks.json),
[test output](tests.log), [Clippy output](clippy.log) and the independent
[benchmark build log](current-build.log).

## Many-stripe RPC

Five fresh alternating baseline/candidate pairs were measured at each stripe
count. Every run completed healthy-path checks, 2.56 million verified requests
at each of 64/1024 tasks, and orderly shutdown. All samples are retained.

| Workload | Baseline median kQPS | Candidate B median kQPS | Paired median change | Paired range |
| --- | ---: | ---: | ---: | --- |
| 128_connections/64_tasks_qps | 516.228 | 508.866 | -0.318% | -3.037% to +7.410% |
| 128_connections/1024_tasks_qps | 712.585 | 749.743 | +5.397% | -4.167% to +7.425% |
| 256_connections/64_tasks_qps | 506.438 | 513.419 | +2.576% | -2.168% to +4.879% |
| 256_connections/1024_tasks_qps | 694.028 | 722.509 | +4.079% | +3.353% to +8.009% |

The 256-stripe/1024-task improvement is positive in all five pairs. At 128
stripes and 1024 tasks it is positive in four of five pairs. Lower-concurrency
cases have mixed signs. Paired changes compare samples collected together;
absolute throughput from separate candidates or earlier experiments is not
combined into these comparisons.

The [raw index](rpc-paired.json) includes compressed and original JSON hashes
for every large path report. [rpc-summary.json](rpc-summary.json) retains all
individual paired values and changes. The unchanged reader can print compact
counts and CQ reports without discarding the original payload:

```sh
python3 docs/qp-registry-data/read_artifact.py docs/qp-registry-opt-data/candidate-b/rpc-256-current-1.json.gz --summary
```

Reproduce in a new output directory after all builds/tests finish:

```sh
python3 docs/qp-registry-opt-data/run_suite.py --binary-dir /tmp/ruapc-qp-registry-opt-candidate-b-bench --output-dir /tmp/qp-registry-opt-b-results --groups rpc --pairs 5
```

## Echo and remote memory: candidate not adopted

The combined candidate was not selected as the final default optimization.
The two-endpoint/1024-task echo throughput was lower in all five pairs; the
remote-memory paired distributions were broad and sometimes disagreed in
direction with the ratio of the two version medians. The positive many-stripe
results therefore do not establish a general improvement.

All 40 process runs forming 20 pairs completed successfully and are preserved.
No outlier was removed. The four-file evaluated implementation is retained in
[combined.patch](combined.patch). [paired-summary.json](paired-summary.json)
contains all 22 metrics with unrounded per-pair values and changes.

| Echo workload | Baseline median | Candidate B median | Paired median change | Paired range |
| --- | ---: | ---: | ---: | --- |
| RDMA/serial_16B_us | 31.08 | 29.72 | -0.45% | -9.22% to +19.82% |
| RDMA/serial_4096B_us | 34.31 | 36.39 | -0.44% | -20.16% to +59.02% |
| RDMA/concurrent_64_kops | 619.00 | 700.00 | +14.35% | -10.23% to +14.65% |
| RDMA/concurrent_1024_kops | 1237.80 | 1246.50 | +0.03% | -13.24% to +6.66% |
| RDMA (2 endpoints)/concurrent_64_kops | 656.50 | 637.90 | -2.83% | -7.82% to +1.37% |
| RDMA (2 endpoints)/concurrent_1024_kops | 1324.80 | 1198.10 | -4.81% | -28.42% to -1.11% |

Echo throughput units are kQPS; serial latency is microseconds.

| Remote workload | Baseline median MiB/s | Candidate B median MiB/s | Paired median change | Paired range |
| --- | ---: | ---: | ---: | --- |
| remote_read_all/64KiB_mibps | 821.5 | 849.2 | +9.30% | -29.84% to +11.97% |
| remote_write_all/64KiB_mibps | 840.5 | 759.9 | -1.04% | -25.02% to +29.45% |
| remote_read_all/1024KiB_mibps | 4038.1 | 4526.7 | -11.23% | -18.76% to +37.55% |
| remote_write_all/1024KiB_mibps | 4040.4 | 3723.6 | -2.06% | -37.09% to +66.30% |

All displayed percentages are formatted directly from the full-precision
summary, without intermediate rounding. Raw echo and remote output remain in
[echo-long-paired.json](echo-long-paired.json) and
[remote-memory-extended-paired.json](remote-memory-extended-paired.json).
Reproduce these groups with the same parent runner, recorded immutable B
binaries, and `--groups echo remote --pairs 5` in a new output directory.

[Integrity verification](integrity.json) fingerprints every archived file and
checks source, binaries, lock, complete samples and placement.
