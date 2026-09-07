# QP registry and fixed-width WRIDs

Completion routing now uses the provider's QP number. The poll thread owns a
`HashMap<u32, ConnState>` for its CQ; a completion looks up `qp_num`, checks
the registered incarnation's sequence floor, and passes the CQ-issued token
to the QP for memory recovery. The immediate maintenance list contains only
dirty QPNs, with no additional hash lookup when marking an already-found
connection dirty.

WRIDs always contain **2 type bits and 62 sequence bits**. Neither CQ capacity
nor connection count changes the layout. The full identity is
`(originating CQ, hardware QPN, WR type, sequence)`: different QPs can issue
identical numeric WRIDs. SQ and RQ keep independent dense sequences, with
SEND, standalone ACK and READ sharing the SQ stream. This preserves the
existing per-QP selective-SEND sweep and signaling cadence without adding
a CQ-wide atomic increment per work request.

The baseline, commit `36682ae`, used `ceil(log2(actual_cqe))` route-slot bits
and the remaining bits for sequences. Its route capacity was the actual CQ
capacity. That routing limit is removed. Software completion credits still
bound core connection admission: a small CQ can associate many idle QPs at
the verbs layer, but cannot accept arbitrarily many unconsumed completions.
The existing `sum(R + W + A) + min(H, sum(K))` budget and shard selection
remain in force.

## QPN reuse and ownership

Each CQ owns a setup-only registry of live QPN leases and one `retired_floor`.
A QP is first created by the provider, then its assigned QPN is registered.
The new lease takes the current retired floor. After successful QP destruction,
returning its lease performs the following under the same registry mutex:

```text
retired_floor = max(retired_floor, send_next, recv_next, first_sequence + 1)
remove live QPN
```

A replacement with the same QPN therefore either encounters the old live
lease and fails registration, or receives a floor above all sequences issued
by that lease. Failed posts consume sequence numbers. Even a QP that posted
nothing advances the floor. Returning a lower-use lease cannot lower the CQ
watermark, and advancing the watermark never changes a still-live QP's floor.

Retained completion tokens remain tied to their original CQ. Tokens from a
previous QPN incarnation fail the replacement's floor check even if another
batch has already polled the CQ empty. Both ordinary destruction and partial
creation rollback destroy the provider QP before releasing any identity lease;
destruction failure aborts before freeing memory accessible to DMA.

The registry retains no historical QPN entries. Its hash allocation can retain
peak live capacity, while old identities are represented by one watermark.
QP creation, destruction and explicit introspection lock this registry; WR
posting and normal completion processing do not. Core registration retains
the initial-receive/inbox publication barrier, including early error CQEs.
Removing core connection state still cannot refund CQ credits until the QP
is destroyed and a later retirement snapshot is followed by an empty CQ poll.

## Bounds and tradeoffs

The current work-request sequence never wraps. Exhaustion returns
`WorkRequestIdsExhausted`; after an exhausted lease is returned, the CQ's
retired floor also prevents new registrations. It is deliberately not reset.
A fresh 62-bit stream lasts about 146000 years at one million allocations per
second. This illustrates numeric headroom, not a guaranteed QP lifetime:
inherited floors, failed posts and repeated incarnations consume space too.

The main new hot-path cost is a QPN hash lookup instead of a compact route-slot
array lookup. The smaller fixed identity avoids carrying dynamic layout masks
in every QP. Measurements below quantify their combined effect. A numeric
WRID alone no longer identifies a QP, so diagnostics and task lookup must
retain the CQ and QPN.
The two type bits remain because error CQEs do not guarantee a valid opcode.

`rdma_path_report().completion_queues` now reports actual CQ entries, reserved
credits/connections, live registered QPs, the next registration's sequence
floor and the fixed sequence width. The removed `route_capacity` field no
longer describes a routing constraint.

## Validation and measurement method

The final source passed **538 tests with zero failures**, with 13 ignored
documentation examples, plus strict Clippy and formatting checks:

```text
cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo fmt --all -- --check
```

[Validation fingerprints](qp-registry-data/checks.json),
[test output](qp-registry-data/tests.log) and
[Clippy output](qp-registry-data/clippy.log) identify the tested source.
Regressions cover retained completion tokens after QPN reuse, separate CQs,
occupied registration and partial creation rollback, sequence exhaustion,
out-of-order lease retirement, registry churn, early-completion publication,
and actual same-WRID receive completions on two QPs in one CQ. Existing
selective-SEND, READ ownership, admission and teardown tests remain enabled.

The source, dependency resolution, benchmark overlays, executable hashes and
raw results are archived in [qp-registry-data](qp-registry-data/). The
comparison uses independent source snapshots and target directories for
baseline `36682ae` and this implementation. Benchmarks run serially after
builds and tests finish, on the same physical mlx5 local-loopback device and
fixed CPU placement used by the baseline. Results do not establish performance
on a remote machine or network.

The small-CQ probes use identical harness source for both versions. They
request eight CQ entries and 20000 simultaneous QPs, post no work while
creating them, then perform a checked SEND/RECV exchange through the final
two QPs. Actual CQ identity, distinct QPNs, both completions and payload bytes
are verified. This tests routing capacity independently of completion volume.

The application capacity probe uses one large CQ and 20000 connections per
endpoint, with default R8 receive rings, SQ/RQ depths of 64 and 16 KiB message
buffers. It checks all paths and 40000 serial round-robin echo replies.
Same-workload performance pairs cover small-connection echo, 128/256 stripes
at 64/1024 concurrent tasks, and fully verified remote READ/WRITE transfers.

## Capacity results

| Probe | Baseline `36682ae` | QP registry |
| --- | --- | --- |
| One CQ, requested 8 / actual 15 entries, 20000 idle QPs | Stops at 15; QP 16 returns `CompletionRoutesExhausted` | All 20000 simultaneously live |
| Same small CQ, exchange through the final two QPs | Cannot create the required QPs | 64-byte payload and both completion tokens verified |
| Application, actual 1048575 CQ entries per endpoint, R8 | 20000 healthy connections per endpoint | 20000 healthy connections per endpoint |
| Application round-robin payload verification | 40000 replies, then clean shutdown | 40000 replies, then clean shutdown |
| Application sequence width | 42 bits | 62 bits |

The small-CQ creation probe built all 20000 QPs in 3.354 s and destroyed them
in 5.894 s. Its exchange probe held all QPs simultaneously and generated only
two completion entries. This demonstrates removal of the routing-slot limit;
it does not make a 15-entry CQ sufficient for 20000 busy application
connections. The application probe reserved the same **320032 completion
credits per endpoint** in both versions.

Application setup took 34.171 s / 34.683 s (baseline / registry); the 40000
serial replies took 1.670 s / 1.851 s. These are single capacity-validation
runs, not repeated performance estimates. The measured post-verification
process RSS was 8866032 / 8873144 KiB, a 6.95 MiB difference for 40000 total
connections across the two endpoints; this includes the entire runtime and
does not isolate registry memory.

[Capacity summary and artifact checksums](qp-registry-data/capacity-summary.json)
link all six raw results, including the two expected baseline routing failures.

## Paired performance results

Each workload has five baseline/current pairs with alternating run order.
The baseline and registry columns show each version's median. The paired
change is the median of the five `current / baseline - 1` ratios; the range
includes every pair. These two median calculations need not agree, and the
ranges are observations rather than confidence intervals. No outliers are
discarded.

### Echo

Each concurrent sample performs 2.56 million requests per case. Serial
samples use 5000 warmup and 50000 measured requests per payload size.

| Workload | Baseline median | Registry median | Paired change median [range] |
| --- | ---: | ---: | ---: |
| 1 endpoint, 64 tasks | 721.6 kops/s | 730.1 kops/s | +0.08% [-1.56%, +8.25%] |
| 1 endpoint, 1024 tasks | 1317.6 kops/s | 1330.5 kops/s | +0.89% [-5.87%, +2.83%] |
| 2 endpoints, 64 tasks | 687.6 kops/s | 682.0 kops/s | -1.09% [-3.68%, +0.83%] |
| 2 endpoints, 1024 tasks | 1380.9 kops/s | 1400.2 kops/s | +0.73% [-0.27%, +1.59%] |
| Serial 16 B | 28.58 us/op | 28.05 us/op | -1.85% [-6.01%, +0.29%] |
| Serial 4096 B | 30.62 us/op | 29.40 us/op | -2.36% [-7.61%, -0.69%] |

Throughput's paired medians remain within 1.1% of the baseline in these echo
cases. Serial latency medians improve slightly. This does not isolate the
hash lookup's cycle cost or establish a statistically significant speedup.
[All echo samples](qp-registry-data/echo-long-paired.json) include commands,
binary fingerprints, raw stdout/stderr and per-run metrics.

### Multiple connections

These runs use R8, SQ/RQ depth 64, 16 KiB message buffers and a 512 MiB pool
per endpoint. Each run verifies its healthy connection count, payloads and
clean shutdown, and performs 2.56 million requests per concurrency case.

| Connections per endpoint / concurrent tasks | Baseline median | Registry median | Paired change median [range] |
| --- | ---: | ---: | ---: |
| 128 / 64 | 572.147 kops/s | 571.527 kops/s | +1.19% [-2.16%, +8.82%] |
| 128 / 1024 | 756.336 kops/s | 745.784 kops/s | -1.52% [-1.77%, -0.30%] |
| 256 / 64 | 543.206 kops/s | 549.548 kops/s | +1.17% [-2.14%, +2.81%] |
| 256 / 1024 | 753.044 kops/s | 727.779 kops/s | -3.91% [-6.19%, -0.69%] |

Both 1024-task cases are slower in **all five pairs**. The measured cost is
about 1.5% at 128 connections and 3.9% at 256 connections, using paired
medians. This is a repeatable cost in this workload, not a result to dismiss
as noise. QPN hashing and map storage replace direct array routing; the
end-to-end comparison measures their combined effect with the simpler WRID
layout, rather than attributing a precise cost to any one instruction.
The 64-task cases have mixed pair signs and a positive paired median.

[All multiple-connection samples](qp-registry-data/rpc-paired.json) link the
20 compressed raw reports and their compressed/uncompressed checksums.

### Remote memory

The stock remote-memory benchmark uses 1000 warmup and 10000 measured
operations for each READ/WRITE and payload-size combination. Payload
verification remains enabled, so these figures include RPC coordination and
CPU verification rather than measuring the raw RDMA link.

| Workload | Baseline median | Registry median | Paired throughput change median [range] |
| --- | ---: | ---: | ---: |
| 64 KiB READ | 913.1 MiB/s | 940.7 MiB/s | +3.02% [-13.49%, +12.04%] |
| 64 KiB WRITE | 903.7 MiB/s | 907.1 MiB/s | +1.70% [-5.73%, +15.41%] |
| 1 MiB READ | 4789.3 MiB/s | 4612.7 MiB/s | -3.79% [-14.68%, +7.15%] |
| 1 MiB WRITE | 3979.9 MiB/s | 4052.8 MiB/s | +5.83% [-15.17%, +12.01%] |

The first pair is slower for both versions at 1 MiB and remains included.
Every remote-memory case has both positive and negative pair changes. In
particular, 1 MiB READ throughput falls in four of five pairs, with a -3.79%
paired median; its latency medians are 208.80 / 216.79 us and its paired
latency change is +3.93% [-6.68%, +17.20%]. The wide ranges limit attribution
to the registry, and these results do not establish a general speedup.
[All remote-memory samples](qp-registry-data/remote-memory-extended-paired.json)
retain the raw output and placement metadata.

All **46 measured processes** exited successfully: six capacity probes and
40 performance runs forming 20 pairs. The two small-CQ baseline probes
reported their expected routing-capacity failures as structured output;
all remaining capacity checks passed, as did the multiple-connection payload
and lifecycle checks and remote-memory payload checks. Stock echo records
successful RPC returns without asserting their contents. The
[derived summary](qp-registry-data/paired-summary.json)
contains every input metric and paired ratio and can be regenerated with
[summarize.py](qp-registry-data/summarize.py).

The implementation removes WRID routing-bit limits and preserves independent
QP counters, completion ownership and CQ credit admission. It trades direct
array routing for QPN hashing. On this host the ordinary echo cases are close
to baseline, while the repeated 128/256-connection high-concurrency cases
cost approximately 1.5%/3.9% throughput. The capacity gain and fixed 62-bit
sequence therefore come with a measured workload-dependent performance cost.
