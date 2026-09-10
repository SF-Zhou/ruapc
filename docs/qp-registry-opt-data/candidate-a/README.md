# Candidate A: not adopted

Candidate A used IndexMap storage and dense indices for dirty connections.
Its source digest is `eda7ea8e3f789e4b84d98ccea82dd363f7672326ae5b3c9018a3a29e21edbfcc`.
It was compared with the unchanged committed baseline `b8f92fb` in five
alternating pairs at each of 128 and 256 stripes. All 20 process runs returned
zero and completed their healthy-path, payload and shutdown checks.

The measurements did not demonstrate a clear throughput improvement, so the
three-file change was not adopted. Echo and remote memory were not measured
for this candidate. Every completed sample, including outliers, remains here.

| Workload | Baseline median kQPS | Candidate A median kQPS | Paired median change | Paired range |
| --- | ---: | ---: | ---: | --- |
| 128_connections/64_tasks_qps | 542.534 | 535.606 | -1.051% | -5.268% to +0.382% |
| 128_connections/1024_tasks_qps | 703.527 | 709.439 | +0.848% | -1.156% to +1.416% |
| 256_connections/64_tasks_qps | 519.164 | 491.811 | -1.452% | -5.619% to +2.330% |
| 256_connections/1024_tasks_qps | 684.373 | 674.683 | +0.195% | -2.951% to +2.135% |

[The full source patch](poller.patch), [source and binary metadata](source-metadata.json),
[build log](current-build.log), [test log](tests.log), [Clippy log](clippy.log)
and [checks](checks.json) preserve the evaluated implementation. Its independent
snapshot and immutable executables remain at the paths recorded in metadata.
`relative_reference_base: ".."` means dependency and validation paths in that
metadata resolve from the parent experiment directory, matching their original
locations before this candidate was archived. The low-level C shim/header,
lock and all benchmark inputs are unchanged.

[The raw index](rpc-paired.json) records both compressed and original JSON
SHA256 values. [The summary](rpc-summary.json) retains all five paired values
and changes per workload. Reproduce the workloads with the parent
[run_suite.py](../run_suite.py), selecting `--groups rpc`, using this candidate's
recorded binaries and a new output directory. Rebuild by checking out the
baseline, applying `poller.patch`, then following the parent README's unchanged
harness and fresh-target instructions.
