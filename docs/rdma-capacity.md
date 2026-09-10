# RDMA connection capacity

The capacity redesign removes the fixed 14-bit QP ceiling, admits connections
using software completion credits, and selects a CQ shard that has room
before creating its QP. The full allocation and retirement proof is in
[WRID allocation](wrid.md).

A CQ's actual provider capacity C determines its immutable WRID layout:
`2 type + ceil(log2(C)) slot + remaining sequence`. Routing remains a CQ-local
array lookup. Large CQs allocate more slot bits; smaller CQs retain a longer
sequence range. The route table grows with issued slots, not the full C.

Core admission uses `sum(R + W + A) + min(H, sum(K))`, including unsignaled
flush completions, capped activation ACKs and the shared READ budget. With
normal R8/W4/A4/K32/H32, C65536 supports 4094 simultaneous connections, versus
256 in the baseline. Actual provider rounding can raise this capacity.
The QP owns its reservation until destruction, followed by an observed empty
CQ after the reservation's retirement snapshot. Removing poller state alone
cannot release the reservation.

CQ shards are bounded by `poll_threads_per_device` and selected by reserved
fraction. Admission tries other shards when one cannot fit a connection,
including when creation of an additional shard fails. The path report exposes
capacity, reserved entries, reserved connections, route capacity and sequence
width for each CQ.

The READ bookkeeping map now has four shards per QP. QPs without outstanding
READ permits skip timeout-map iteration. Closing a connection cancels READ
admission waiters and retains completion routing until all per-QP READ permits
return. These changes avoid per-host-CPU empty-map costs at large connection
counts without adding a shared CQ operation per WR.

## Validation

The final source passed **534 tests, zero failures**, with 13 ignored
documentation examples, plus formatting and strict Clippy:

```text
cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo fmt --all -- --check
```

[Full test output](rdma-capacity-data/final/tests.log) and
[Clippy output](rdma-capacity-data/final/clippy.log) are retained. Tests include
65537 independently allocated routes, sequence exhaustion and stale reuse,
different CQ layouts, concurrent credit admission, retirement races,
capacity fallback across shards, failed shard creation and cancelled READ
permit waits. The bootstrap rollback test now fails capacity admission before
QP creation and verifies that established and prepared stripes are rolled back.

## Measurement method

The baseline is commit `601f404`, which already had CQ-owned route leases,
sequence watermarks and the early-completion registration barrier. Comparisons
therefore isolate this capacity redesign from the earlier ownership refactor.
Each version uses a separate source snapshot and a fresh separate Cargo target;
source and binary hashes, hardware details, harness sources and raw output are
in [rdma-capacity-data](rdma-capacity-data/).

Tests use local loopback on a physical mlx5 device, with fixed CPU affinity for
throughput comparisons. This is evidence for the measured host and workloads,
not a cross-machine RDMA or production-network benchmark. Capacity tests keep
all claimed QPs alive simultaneously and check CQ identity. Application tests
check both endpoints' actual path counts and every echoed value; setup and
report serialization are outside the timed request loops.

The baseline's 20000-QP attempt stopped at exactly 16384 live QPs despite a
larger CQ, returning `CompletionRoutesExhausted`. Its 512- and 1024-connection
RPC setups failed at the 256-connection CQ budget; both endpoints cleaned up
all prepared paths. These failures establish distinct route and credit limits,
and are not reported as throughput comparisons.

## First iteration and the scaling follow-up

The first capacity iteration removed the route/budget ceilings but retained
the old full maintenance scan after every CQ drain. Its data stays at the
archive root; final-version measurements are isolated under `final/` with
fresh source and binary fingerprints. The first iteration's 20000-stripe R8
probe verified every reply but needed 174.35 seconds for 40000 serial RPCs.
The minimum-ring R2 attempts also exposed excessive pure-ACK traffic: one
failed its initial RPC, and another did not finish warmup within 300 seconds.
All failures, timeouts and successful runs are retained, without treating path
creation alone as application capacity success.

The final implementation deduplicates CQE/registration dirty slots and
maintains those connections immediately. Full housekeeping runs every 100 ms;
undirected external wakeups still request a full scan. Receive deficits retry
without making window-blocked sends spin. Requiring at least two ACKs to
trigger a standalone ACK-of-ACK also stops the minimum-ring activation
ping-pong without changing the CQ budget or default-ring threshold. DATA and
keepalive messages can still carry a smaller accumulated ACK delta.

## Final application capacity

Both final probes kept **20000 healthy connections per endpoint on one CQ**,
verified all 40000 serial round-robin echo responses, and shut down cleanly.
The CQ request was 524288; mlx5 returned 1048575 entries, giving 20 slot bits
and 42 sequence bits. SQ/RQ capacities stayed at 64 and message buffers were
16 KiB. The R2 probe used 2 GiB buffer pools per endpoint; the default-ring R8
probe used 4 GiB per endpoint.

| Receive ring | Reserved entries per CQ | Setup | 40000 verified RPCs | Stop |
|---:|---:|---:|---:|---:|
| 2 | 100032 | 32.93 s | 1.341 s | 17.84 s |
| 8 | 320032 | 33.42 s | 1.653 s | 17.94 s |

The R8 sweep is approximately 105 times faster than the first iteration's
174.35 s under the same capacity workload. The R2 sweep now completes too.
This is a serial validation sweep through all connections, not a maximum
concurrent-QPS measurement. R8 process RSS was about 7.96 GiB at the report
and 8.46 GiB after traffic; CQ capacity does not remove receive-buffer costs.
The [capacity summaries](rdma-capacity-data/final/capacity-outcomes.json) link
to both full compressed outputs, including every path and returned CQ budget.

## Final throughput at small connection counts

Five alternating baseline/final pairs used 2560000 requests per concurrent
echo case. The following numbers compare version medians; within-pair ratios
and all raw output are in
[the final echo results](rdma-capacity-data/final/echo-long-paired.json).

| Endpoints | Concurrent tasks | Baseline kRPC/s | Final kRPC/s | Change |
|---:|---:|---:|---:|---:|
| 1 | 64 | 710.8 | 731.0 | +2.84% |
| 1 | 1024 | 1376.1 | 1334.1 | -3.05% |
| 2 | 64 | 678.5 | 694.7 | +2.39% |
| 2 | 1024 | 1423.2 | 1391.0 | -2.26% |

The high-concurrency cases show a small consistent cost: median paired
throughput changes were -3.79% and -2.60%. Serial 16-byte latency was
26.94 -> 26.83 microseconds (-0.41%; paired +1.15%); 4 KiB was
28.51 -> 30.13 microseconds (+5.68%; paired +4.97%). The implementation
therefore does not establish zero regression for every workload. Normal CQE
maintenance scales with active slots; undirected wakeups and periodic
housekeeping still scan the connection array.

## Final throughput at 128 and 256 connections

Five alternating pairs per connection count used the same configuration as
the corresponding baseline: default SQ/RQ capacities and receive ring,
16 KiB message buffers, and 2560000 checked requests per concurrency case.

| Connections | Concurrent tasks | Baseline RPC/s | Final RPC/s | Change |
|---:|---:|---:|---:|---:|
| 128 | 64 | 572219 | 569914 | -0.40% |
| 128 | 1024 | 763303 | 760950 | -0.31% |
| 256 | 64 | 514828 | 553782 | +7.57% |
| 256 | 1024 | 759619 | 753214 | -0.84% |

The corresponding median paired changes are -0.40%, -0.16%, +7.28% and
-0.07%. [Every final run](rdma-capacity-data/final/rpc-paired.json) includes
path-count, health and payload checks. Increasing route capacity has not
introduced a substantial throughput cost in these measured connection-count
workloads.

## Final remote-memory measurements

The first five alternating pairs used 1000 warmup and 10000 measured
operations per direction and size, retaining complete payload verification.
64 KiB READ/WRITE median throughput changed by +1.30%/+1.87%. At 1 MiB,
READ changed from 5591.5 to 4929.0 MiB/s (-11.85%; paired -16.41%), and
WRITE from 4605.3 to 4256.5 MiB/s (-7.57%; paired -7.57%). These slower
samples are retained in
[the standard workload results](rdma-capacity-data/final/remote-memory-extended-paired.json).
In this series, baseline READ latency ranged from 169.60 to 193.87
microseconds, while final ranged from 152.66 to 380.44 microseconds. Variation
alone does not establish that the slower final results are unrelated to the
implementation.

A separate instrumented experiment ran each 1 MiB direction in its own
process, five alternating pairs per direction. READ wall latency was
205.75 -> 204.24 microseconds (paired +0.24%); WRITE was
252.80 -> 233.70 microseconds (paired -7.04%). Full verification remained
about 37.6 microseconds in both versions. The measured remote-operation await
was 69.70 -> 63.53 microseconds for READ and 88.88 -> 76.02 for WRITE. That
await includes admission, internal RPC and completion scheduling, not pure
device DMA. These
[diagnostic results](rdma-capacity-data/final/remote-diagnostic.json) do not
reproduce the standard workload's slowdown, but instrumenting and separating
directions changes the workload. They cannot replace its negative samples
or establish a unique cause.

To check the original negative result directly, an independent confirmation
series ran ten more alternating pairs with the **same standard binary,
parameters and full payload verification**, without any intervening source
change or build. Both versions now included approximately 400-microsecond
1 MiB samples. The confirmation series and all fifteen pairs give:

| Standard workload series | 1 MiB READ throughput change | Paired READ change | 1 MiB WRITE throughput change | Paired WRITE change |
|---|---:|---:|---:|---:|
| Initial 5 pairs | -11.85% | -16.41% | -7.57% | -7.57% |
| Independent 10 pairs | +12.81% | +3.00% | +2.30% | +0.28% |
| All 15 pairs | +1.09% | +0.20% | +0.76% | -0.95% |

The initial large decrease did not persist in the independent same-workload
series. All fifteen pairs do not support a consistent substantial throughput
regression for this workload, while the observed variability prevents a
precise small-effect estimate. The
[confirmation output](rdma-capacity-data/final/remote-memory-confirmation-paired.json)
and [combined summary](rdma-capacity-data/final/remote-memory-combined-paired.json)
retain both groups and every original sample; no run was removed or replaced.
These local-loopback results do not establish performance on another host,
remote network, or unmeasured workload. Small-connection high-concurrency
echo retains the measured 2–3% throughput cost reported above.

The historical tables below describe the **first iteration**. They show which
capacity limits were removed before the maintenance change and are kept
separate from final measurements.

## First-iteration capacity results

| Probe | Baseline | Redesign |
|---|---|---|
| 20000 low-level QPs, one CQ requested at 262144 entries | Stopped at 16384, route exhaustion | 20000 simultaneously live; all destroyed |
| Actual SEND/RECV between slots 19998 and 19999 | Could not allocate these slots | 64-byte payload and both CQ completions verified |
| 512 RPC stripes per endpoint, one CQ requested at 65536 | Rejected by CQ budget | 512 healthy paths on each end; echo and shutdown passed |
| 1024 RPC stripes per endpoint, same CQ configuration | Rejected by CQ budget | 1024 healthy paths on each end; echo and shutdown passed |

The RPC probes retained the default SQ/RQ depths of 64 and receive ring of 8;
message buffers were 16 KiB to keep receive-memory demand separate from CQ
capacity. Each endpoint requested one CQ of 65536 entries; mlx5 returned
131071 entries. Reports showed 8224 reserved entries for 512 connections and
16416 for 1024, exactly `16*N + 32`, with 45 sequence bits. This particular
CQ's arithmetic admission ceiling is 8189 default-credit connections; that
ceiling is a budget calculation, not a measured maximum.

[QP capacity output](rdma-capacity-data/after-qps-20000.json),
[high-slot completion output](rdma-capacity-data/after-exchange-20000.json),
[512-stripe output](rdma-capacity-data/after-rpc-512.json) and
[1024-stripe output](rdma-capacity-data/after-rpc-1024.json) retain setup,
identity, count, payload and teardown evidence.

## First-iteration throughput at unchanged connection counts

Five alternating pairs at 128 and 256 connections used 2,560,000 requests
per concurrency case. Every response was checked. The table compares each
version's median QPS; pairwise ratios are also retained in the raw summary.

| Connections | Concurrent tasks | Baseline QPS | Redesign QPS | Change |
|---:|---:|---:|---:|---:|
| 128 | 64 | 567,596 | 558,851 | -1.54% |
| 128 | 1024 | 755,876 | 755,716 | -0.02% |
| 256 | 64 | 512,538 | 504,848 | -1.50% |
| 256 | 1024 | 747,890 | 746,171 | -0.23% |

Low concurrency showed a small, roughly 1.5% decrease; high concurrency was
close to baseline. This is not a claim of zero cost. The capacity increases
above do not rely on a new per-WR hash lookup or shared CQ admission lock.
[All paired runs and summaries](rdma-capacity-data/rpc-paired.json) are retained.
