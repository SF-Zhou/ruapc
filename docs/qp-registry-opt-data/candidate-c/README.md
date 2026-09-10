# Candidate C: activation check only, not adopted for the general default

Candidate C adds only an Acquire load before the existing activation flag
swap when no activation is pending. It retains the original HashMap layout,
clock sampling, WRID identity, DMA ownership and CQ admission behavior.

The baseline is `b8f92fb2299719804e19d342bf4e004bd45a83bb`. The candidate's
184-file Rust/Cargo digest is
`908910bec4ffc49e44819c4d19b7ee4cd748acabc2b088f5be36c4572a76b1fa`.
Only `ruapc/src/rdma/rdma_socket.rs` differs from the baseline. The parent
validated 538 tests, zero failures, 13 ignored doctests, formatting and strict
Clippy before measurements. See [checks](checks.json), [tests](tests.log)
and [Clippy](clippy.log).

[Source metadata](source-metadata.json) records the independent snapshot,
fresh target, build command, immutable baseline/current executable hashes,
native shim/header and three actual benchmark source hashes. Those benchmark
sources and the dependency lock are byte-identical between measured versions.
The shared baseline and harness provenance are in the parent
[source metadata](../source-metadata.json); the independent build output is in
[current-build.log](current-build.log).

Five echo pairs were measured first to check the two-endpoint case that was
slower with combined candidate B. This was followed by five pairs at each of
128/256 RPC stripes, then five remote-memory pairs. Every group reruns the
same committed baseline alongside C; no samples from A, B or earlier
experiments are mixed into these comparisons.

Use the unchanged parent runner and a fresh output directory to reproduce:

```sh
python3 docs/qp-registry-opt-data/run_suite.py --binary-dir /tmp/ruapc-qp-registry-opt-candidate-c-bench --output-dir /tmp/qp-registry-opt-c-results --groups echo rpc remote --pairs 5
python3 docs/qp-registry-data/summarize.py --directory /tmp/qp-registry-opt-c-results
```

Raw path reports use deterministic gzip. Read them with the existing
[reader](../../qp-registry-data/read_artifact.py), optionally selecting
`--summary` to show only path counts and CQ information. The uncompressed
payload remains intact and has a separately recorded SHA256.

## Completed measurements and decision

All 40 process runs forming 20 pairs completed successfully. Every sample is
retained. Candidate C did not reproduce B's consistently lower two-endpoint
1024-task throughput, but its single-endpoint results included negative paired
changes and its 1 MiB READ throughput had a materially negative paired median
with wide variation. The collected data do not establish a general improvement;
C was not adopted for the general default. Production returns to the committed
baseline. The evaluated one-file change remains available as
[activation.patch](activation.patch).

[paired-summary.json](paired-summary.json) retains all 22 metrics, both version
medians, every paired value/change and complete ranges. Percentages below are
formatted directly from its full-precision numbers. No samples are pooled with
A or B, and no outlier is removed.

| Many-stripe workload | Baseline median kQPS | C median kQPS | Paired median change | Paired range |
| --- | ---: | ---: | ---: | --- |
| 128_connections/64_tasks_qps | 523.944 | 521.817 | -0.47% | -5.78% to -0.31% |
| 128_connections/1024_tasks_qps | 698.276 | 700.227 | +0.48% | -4.43% to +1.48% |
| 256_connections/64_tasks_qps | 514.080 | 515.854 | +2.36% | -3.72% to +3.79% |
| 256_connections/1024_tasks_qps | 681.890 | 686.875 | +1.30% | -0.64% to +5.03% |

| Echo workload | Baseline median | C median | Paired median change | Paired range |
| --- | ---: | ---: | ---: | --- |
| RDMA/serial_16B_us | 35.19 | 34.16 | +4.26% | -4.55% to +6.17% |
| RDMA/serial_4096B_us | 37.63 | 35.87 | +0.90% | -16.19% to +9.50% |
| RDMA/concurrent_64_kops | 635.40 | 679.20 | -1.10% | -2.85% to +15.85% |
| RDMA/concurrent_1024_kops | 1126.80 | 1196.00 | -2.17% | -7.60% to +24.26% |
| RDMA (2 endpoints)/concurrent_64_kops | 637.70 | 630.90 | +0.92% | -5.70% to +3.59% |
| RDMA (2 endpoints)/concurrent_1024_kops | 1308.40 | 1338.60 | +1.76% | -1.72% to +2.99% |

Echo throughput is kQPS; serial latency is microseconds.

| Remote workload | Baseline median MiB/s | C median MiB/s | Paired median change | Paired range |
| --- | ---: | ---: | ---: | --- |
| remote_read_all/64KiB_mibps | 795.3 | 844.3 | +6.16% | +1.04% to +47.59% |
| remote_write_all/64KiB_mibps | 803.0 | 807.0 | +2.98% | -17.08% to +64.34% |
| remote_read_all/1024KiB_mibps | 5042.3 | 4382.1 | -12.10% | -34.50% to +42.11% |
| remote_write_all/1024KiB_mibps | 3957.1 | 4101.8 | -1.37% | -31.11% to +40.56% |

Raw evidence: [many-stripe index](rpc-paired.json),
[echo](echo-long-paired.json), [remote memory](remote-memory-extended-paired.json).
Stage-specific [echo](echo-summary.json) and [RPC](rpc-summary.json) summaries
retain the same full-precision values reported when those stages completed.
[Integrity verification](integrity.json) checks the frozen source, executable
hashes, lock and exact sample counts. Many-stripe runs verify payloads, healthy path
counts and stop; remote-memory runs verify payloads; stock echo checks RPC
success but does not assert response contents. All runs have verified CPU placement.
