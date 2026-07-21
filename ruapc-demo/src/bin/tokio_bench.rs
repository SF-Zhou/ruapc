//! Pure-tokio ceiling benchmark for the server message path.
//!
//! Measures how many messages per second the tokio runtime itself can move
//! through the shapes used by the ruapc server, with **no RDMA, no TCP, no
//! serde**: producer OS threads (simulating device poll threads) inject
//! message batches, tasks "handle" each message and complete it by bumping
//! a counter (simulating a synchronous response post). The difference
//! between modes isolates where the runtime overhead is:
//!
//! - `direct`: the producer thread spawns one task per message straight
//!   into the runtime (tokio's remote-inject path with its shared lock).
//!   This is the architecture ruapc had *before* the per-shard dispatcher.
//! - `spawn`:  producer sends batches over an unbounded channel to a
//!   long-lived dispatcher task, which spawns one task per message
//!   (worker-local queue, work-stealable). This mirrors the current
//!   server: per-shard dispatcher + per-request spawn.
//! - `chunk`:  the dispatcher spawns one task per chunk of messages and
//!   the chunk task handles its messages inline — one spawn amortized
//!   over `--chunk` requests.
//! - `inline`: the dispatcher handles every message inline; the serial
//!   single-task ceiling.
//! - `pool`:   `--pool-tasks` pre-spawned tasks all wait on one shared
//!   MPMC queue (mutex-protected VecDeque + semaphore, the async-channel
//!   structure); the producer pushes one boxed work item per message and
//!   adds permits. No per-message spawn: this tests whether pre-spawning
//!   removes the spawn cost or merely the task-allocation part of it
//!   (the wakeup/scheduling part remains).
//! - `pool-first`: like `pool`, but the queue holds `Pin<Box<dyn Future>>`
//!   built by the producer (the poll thread pays the allocation; routing
//!   and deserialization stay lazy inside the future). A consumer polls
//!   each future once inline; if it returns `Pending` (fraction set by
//!   `--pending-frac`, modeling handlers that await), the future is
//!   spawned as its own task — a local, in-runtime spawn paid only by
//!   suspending handlers — so pool slots are never held across awaits.
//!
//! `--handler-spin-ns` adds busy-work per message to model a real handler
//! (deserialize + user logic + response serialization). With 0 the numbers
//! are pure runtime overhead; with ~1000-2000ns they approximate the 32B
//! echo cost, letting the result be compared against RDMA bench QPS to
//! decide whether tokio or the transport is the bottleneck.
//!
//! `--heavy-frac` enables a mixed load: that fraction of messages (picked
//! pseudo-randomly) spins `--heavy-spin-ns` instead, modeling user handlers
//! with occasional expensive requests. When enabled, per-message latency
//! (batch inject -> completion) is recorded into light/heavy histograms and
//! percentiles are printed at the end. This exposes the head-of-line cost
//! of chunked dispatch — a heavy message delays every later message in its
//! chunk — while per-message spawn isolates heavy messages at the price of
//! spawn overhead.

use clap::Parser;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// Execution mode: direct | spawn | chunk | inline | pool | pool-first.
    #[arg(long, default_value = "spawn")]
    mode: Mode,

    /// Pre-spawned consumer tasks per producer (pool modes).
    #[arg(long, default_value = "10000")]
    pool_tasks: usize,

    /// Fraction of messages whose future suspends once before completing
    /// (pool-first mode; models handlers that await).
    #[arg(long, default_value = "0.0")]
    pending_frac: f64,

    /// Tokio worker threads per runtime (0 = number of CPUs).
    #[arg(long, default_value = "0")]
    worker_threads: usize,

    /// Number of independent tokio runtimes; producers are assigned
    /// round-robin (producer p -> runtime p % M). Total worker threads =
    /// runtimes * worker_threads. `1` is the single shared runtime.
    #[arg(long, default_value = "1")]
    runtimes: usize,

    /// Dispatcher tasks per producer; batches are distributed round-robin
    /// across them. Models "multiple consumer coroutines pull from the
    /// poll thread's queue and spawn per message" (an idealized version:
    /// K SPSC channels instead of one contended MPMC queue).
    #[arg(long, default_value = "1")]
    dispatchers: usize,

    /// Producer OS threads simulating device poll threads (RDMA shards).
    #[arg(long, default_value = "1")]
    producers: usize,

    /// Messages per injected batch (models one CQ drain).
    #[arg(long, default_value = "64")]
    batch: usize,

    /// Messages per spawned chunk task (chunk mode).
    #[arg(long, default_value = "64")]
    chunk: usize,

    /// In-flight message cap per producer (models the client send window /
    /// coroutine count; producers stall when it is reached).
    #[arg(long, default_value = "2048")]
    inflight: usize,

    /// Simulated connections per producer: completion counters are sharded
    /// per connection (cache-padded), mirroring the per-connection window
    /// atomics of the real transport. `1` makes every handler hammer one
    /// shared cache line, which measures counter contention rather than
    /// the runtime.
    #[arg(long, default_value = "8")]
    conns: usize,

    /// Busy-work per message in nanoseconds (models handler cost).
    #[arg(long, default_value = "0")]
    handler_spin_ns: u64,

    /// Fraction of messages that are heavy (e.g. 0.01 = 1%). 0 disables
    /// the mixed-load mode and its latency accounting.
    #[arg(long, default_value = "0.0")]
    heavy_frac: f64,

    /// Busy-work for heavy messages in nanoseconds (default 1ms).
    #[arg(long, default_value = "1000000")]
    heavy_spin_ns: u64,

    /// Benchmark duration in seconds (excluding warmup).
    #[arg(long, default_value = "10")]
    secs: u64,

    /// Warmup duration in seconds.
    #[arg(long, default_value = "2")]
    warmup_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    Direct,
    Spawn,
    Chunk,
    Inline,
    Pool,
    PoolFirst,
}

/// One simulated connection's accounting, padded to its own cache line so
/// connections do not false-share (the real transport keeps per-connection
/// window atomics in separate allocations).
#[repr(align(128))]
#[derive(Default)]
struct ConnCounters {
    completed: AtomicU64,
    inflight: AtomicU64,
}

/// Lock-free latency histogram: 8 sub-buckets per power of two (~12.5%
/// resolution), covering 0ns to ~2^44ns.
const HIST_BUCKETS: usize = 8 + 8 * 42;

struct LatHist {
    buckets: Vec<AtomicU64>,
}

impl LatHist {
    fn new() -> Self {
        Self {
            buckets: (0..HIST_BUCKETS).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn record(&self, ns: u64) {
        self.buckets[Self::index(ns)].fetch_add(1, Ordering::Relaxed);
    }

    fn index(ns: u64) -> usize {
        if ns < 8 {
            ns as usize
        } else {
            let e = 63 - ns.leading_zeros() as usize;
            let sub = ((ns >> (e - 3)) & 7) as usize;
            (8 + (e - 3) * 8 + sub).min(HIST_BUCKETS - 1)
        }
    }

    /// Lower bound of a bucket in ns.
    fn value(idx: usize) -> u64 {
        if idx < 8 {
            idx as u64
        } else {
            let e = (idx - 8) / 8 + 3;
            let sub = ((idx - 8) % 8) as u64;
            (1u64 << e) | (sub << (e - 3))
        }
    }
}

fn percentile(counts: &[u64], total: u64, p: f64) -> u64 {
    if total == 0 {
        return 0;
    }
    let target = ((total as f64) * p).ceil() as u64;
    let mut acc = 0u64;
    for (i, c) in counts.iter().enumerate() {
        acc += c;
        if acc >= target {
            return LatHist::value(i);
        }
    }
    LatHist::value(HIST_BUCKETS - 1)
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.2}s", ns as f64 / 1e9)
    } else if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.2}us", ns as f64 / 1e3)
    } else {
        format!("{ns}ns")
    }
}

/// Cheap deterministic RNG for heavy-message selection.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    /// True with probability `thr / 2^32`; `thr == 0` disables (no rng step).
    fn pick(&mut self, thr: u64) -> bool {
        thr != 0 && (self.next() >> 32) < thr
    }
}

/// `--heavy-frac` as a 32-bit threshold; 0 means mixed mode disabled.
fn heavy_threshold(frac: f64) -> u64 {
    (frac.clamp(0.0, 1.0) * 4294967296.0) as u64
}

/// Per-producer state: one counter pair per simulated connection, plus
/// latency histograms (used only when mixed mode is enabled).
struct Counters {
    conns: Vec<ConnCounters>,
    light: LatHist,
    heavy: LatHist,
}

impl Counters {
    fn new(conns: usize) -> Self {
        Self {
            conns: (0..conns.max(1)).map(|_| ConnCounters::default()).collect(),
            light: LatHist::new(),
            heavy: LatHist::new(),
        }
    }

    fn hist(&self, heavy: bool) -> &LatHist {
        if heavy { &self.heavy } else { &self.light }
    }

    fn completed(&self) -> u64 {
        self.conns
            .iter()
            .map(|c| c.completed.load(Ordering::Relaxed))
            .sum()
    }

    fn inflight(&self) -> u64 {
        self.conns
            .iter()
            .map(|c| c.inflight.load(Ordering::Relaxed))
            .sum()
    }
}

/// "Handles" one message: optional busy-work, then post the response
/// (counter bump) and release the window slot. Mirrors a handler whose
/// response send is a synchronous queue-pair post on `conn`.
#[inline]
fn handle_msg(counters: &Counters, conn: usize, spin_ns: u64) {
    if spin_ns > 0 {
        let start = Instant::now();
        while start.elapsed().as_nanos() < u128::from(spin_ns) {
            std::hint::spin_loop();
        }
    }
    let c = &counters.conns[conn];
    c.completed.fetch_add(1, Ordering::Relaxed);
    c.inflight.fetch_sub(1, Ordering::Relaxed);
}

/// One queued request for pool mode. Boxed individually, as proposed:
/// the poll thread allocates a work item per message.
struct WorkItem {
    conn: usize,
    heavy: bool,
    t0: Instant,
}

/// Shared MPMC queue for pool mode: mutex-protected deque + semaphore
/// (structurally what async-channel does internally).
struct PoolQueue {
    q: std::sync::Mutex<std::collections::VecDeque<Box<WorkItem>>>,
    sem: tokio::sync::Semaphore,
}

/// Pre-spawned consumer task: waits for a permit, pops one item, handles
/// it inline. `--pool-tasks` of these per producer.
async fn run_pool_task(pool: Arc<PoolQueue>, counters: Arc<Counters>, args: Args) {
    let heavy_thr = heavy_threshold(args.heavy_frac);
    loop {
        let Ok(permit) = pool.sem.acquire().await else {
            return;
        };
        permit.forget();
        let item = pool.q.lock().unwrap().pop_front();
        let Some(item) = item else { continue };
        let spin_ns = if item.heavy {
            args.heavy_spin_ns
        } else {
            args.handler_spin_ns
        };
        handle_msg(&counters, item.conn, spin_ns);
        if heavy_thr != 0 {
            counters
                .hist(item.heavy)
                .record(item.t0.elapsed().as_nanos() as u64);
        }
    }
}

type BoxFut = std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;

/// Shared MPMC queue of pre-built handler futures (pool-first mode).
struct PoolFutQueue {
    q: std::sync::Mutex<std::collections::VecDeque<BoxFut>>,
    sem: tokio::sync::Semaphore,
}

/// Future that suspends exactly once (self-waking), modeling a handler
/// that hits an await point.
#[derive(Default)]
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            std::task::Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

/// Pool-first consumer: pops a pre-built future, polls it once inline
/// with its own waker; on `Pending` hands it to `tokio::spawn` (a local
/// in-runtime spawn, paid only by suspending handlers). This is sound
/// because `spawn` guarantees an initial poll, which re-registers the
/// task's own waker per the `Future` contract.
async fn run_pool_first_task(pool: Arc<PoolFutQueue>) {
    loop {
        let Ok(permit) = pool.sem.acquire().await else {
            return;
        };
        permit.forget();
        let item = pool.q.lock().unwrap().pop_front();
        let Some(mut fut) = item else { continue };
        let ready =
            std::future::poll_fn(|cx| std::task::Poll::Ready(fut.as_mut().poll(cx).is_ready()))
                .await;
        if !ready {
            tokio::spawn(fut);
        }
    }
}

/// Dispatcher task: receives `(count, conn, inject_time)` batches and
/// executes them according to the mode. Messages carry no payload — a
/// batch is just a count plus its connection index, which is the cheapest
/// possible representation and therefore an upper bound for the real path.
async fn run_dispatcher(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(usize, usize, Instant)>,
    counters: Arc<Counters>,
    args: Args,
) {
    let chunk = args.chunk.max(1);
    let spin_ns = args.handler_spin_ns;
    let heavy_spin_ns = args.heavy_spin_ns;
    let heavy_thr = heavy_threshold(args.heavy_frac);
    let mut rng = Lcg::new(0x9e3779b97f4a7c15);
    while let Some((n, conn, t0)) = rx.recv().await {
        let mut n = n;
        match args.mode {
            Mode::Inline => {
                for _ in 0..n {
                    let heavy = rng.pick(heavy_thr);
                    handle_msg(&counters, conn, if heavy { heavy_spin_ns } else { spin_ns });
                    if heavy_thr != 0 {
                        counters.hist(heavy).record(t0.elapsed().as_nanos() as u64);
                    }
                }
            }
            Mode::Spawn => {
                for _ in 0..n {
                    let heavy = rng.pick(heavy_thr);
                    let counters = counters.clone();
                    tokio::spawn(async move {
                        handle_msg(&counters, conn, if heavy { heavy_spin_ns } else { spin_ns });
                        if heavy_thr != 0 {
                            counters.hist(heavy).record(t0.elapsed().as_nanos() as u64);
                        }
                    });
                }
            }
            Mode::Chunk => {
                while n > 0 {
                    let c = n.min(chunk);
                    n -= c;
                    let counters = counters.clone();
                    let seed = rng.next();
                    tokio::spawn(async move {
                        let mut rng = Lcg::new(seed);
                        for _ in 0..c {
                            let heavy = rng.pick(heavy_thr);
                            handle_msg(
                                &counters,
                                conn,
                                if heavy { heavy_spin_ns } else { spin_ns },
                            );
                            if heavy_thr != 0 {
                                counters.hist(heavy).record(t0.elapsed().as_nanos() as u64);
                            }
                        }
                    });
                }
            }
            Mode::Direct | Mode::Pool | Mode::PoolFirst => {
                unreachable!("mode has no dispatcher")
            }
        }
    }
}

/// Producer OS thread: keeps the window full by injecting batches, exactly
/// like a poll thread forwarding parsed messages. Batches are assigned to
/// simulated connections round-robin (one CQ drain usually carries a run
/// of completions from the same connection).
fn run_producer(
    counters: Arc<Counters>,
    tx: Option<Vec<tokio::sync::mpsc::UnboundedSender<(usize, usize, Instant)>>>,
    pool: Option<Arc<PoolQueue>>,
    pool_fut: Option<Arc<PoolFutQueue>>,
    handle: tokio::runtime::Handle,
    args: Args,
    done: Arc<AtomicBool>,
) {
    // Under saturation a poll thread drains completions in runs (up to the
    // poll array size), so batches injected into the dispatcher are large.
    // Waiting for a full batch's worth of window room models that; without
    // it this tight loop would inject 1-2 message batches as slots trickle
    // back, degenerating every mode into per-message dispatch.
    let full_batch = args.batch.min(args.inflight).max(1);
    let heavy_thr = heavy_threshold(args.heavy_frac);
    let pending_thr = heavy_threshold(args.pending_frac);
    let mut rng = Lcg::new(0x5851f42d4c957f2d);
    let mut seq = 0usize;
    while !done.load(Ordering::Relaxed) {
        let used = counters.inflight() as usize;
        let room = args.inflight.saturating_sub(used).min(full_batch);
        if room < full_batch {
            std::hint::spin_loop();
            continue;
        }
        let conn = seq % counters.conns.len();
        seq = seq.wrapping_add(1);
        counters.conns[conn]
            .inflight
            .fetch_add(room as u64, Ordering::Relaxed);
        let t0 = Instant::now();
        if let Some(pf) = &pool_fut {
            // Build one boxed handler future per message; the poll thread
            // pays the allocation, routing/deser stay lazy inside.
            {
                let mut q = pf.q.lock().unwrap();
                for _ in 0..room {
                    let heavy = rng.pick(heavy_thr);
                    let pending = rng.pick(pending_thr);
                    let counters = counters.clone();
                    let spin_ns = if heavy {
                        args.heavy_spin_ns
                    } else {
                        args.handler_spin_ns
                    };
                    q.push_back(Box::pin(async move {
                        if pending {
                            YieldOnce::default().await;
                        }
                        handle_msg(&counters, conn, spin_ns);
                        if heavy_thr != 0 {
                            counters.hist(heavy).record(t0.elapsed().as_nanos() as u64);
                        }
                    }) as BoxFut);
                }
            }
            pf.sem.add_permits(room);
            continue;
        }
        if let Some(pool) = &pool {
            {
                let mut q = pool.q.lock().unwrap();
                for _ in 0..room {
                    let heavy = rng.pick(heavy_thr);
                    q.push_back(Box::new(WorkItem { conn, heavy, t0 }));
                }
            }
            pool.sem.add_permits(room);
            continue;
        }
        match &tx {
            Some(tx) => {
                if tx[seq % tx.len()].send((room, conn, t0)).is_err() {
                    return;
                }
            }
            None => {
                // Remote-inject path: one spawn per message from outside
                // the runtime, like the pre-dispatcher architecture.
                for _ in 0..room {
                    let heavy = rng.pick(heavy_thr);
                    let counters = counters.clone();
                    let spin_ns = if heavy {
                        args.heavy_spin_ns
                    } else {
                        args.handler_spin_ns
                    };
                    handle.spawn(async move {
                        handle_msg(&counters, conn, spin_ns);
                        if heavy_thr != 0 {
                            counters.hist(heavy).record(t0.elapsed().as_nanos() as u64);
                        }
                    });
                }
            }
        }
    }
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let runtimes: Vec<tokio::runtime::Runtime> = (0..args.runtimes.max(1))
        .map(|_| {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all();
            if args.worker_threads > 0 {
                builder.worker_threads(args.worker_threads);
            }
            builder.build().expect("failed to build tokio runtime")
        })
        .collect();

    let done = Arc::new(AtomicBool::new(false));
    let mut producer_counters = Vec::new();
    let mut producer_threads = Vec::new();

    for p in 0..args.producers.max(1) {
        let runtime = &runtimes[p % runtimes.len()];
        let counters = Arc::new(Counters::new(args.conns));
        producer_counters.push(counters.clone());

        let pool = if args.mode == Mode::Pool {
            let pool = Arc::new(PoolQueue {
                q: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(
                    args.inflight.max(1),
                )),
                sem: tokio::sync::Semaphore::new(0),
            });
            for _ in 0..args.pool_tasks.max(1) {
                runtime.spawn(run_pool_task(pool.clone(), counters.clone(), args.clone()));
            }
            Some(pool)
        } else {
            None
        };

        let pool_fut = if args.mode == Mode::PoolFirst {
            let pool = Arc::new(PoolFutQueue {
                q: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(
                    args.inflight.max(1),
                )),
                sem: tokio::sync::Semaphore::new(0),
            });
            for _ in 0..args.pool_tasks.max(1) {
                runtime.spawn(run_pool_first_task(pool.clone()));
            }
            Some(pool)
        } else {
            None
        };

        let tx = if matches!(args.mode, Mode::Direct | Mode::Pool | Mode::PoolFirst) {
            None
        } else {
            let txs = (0..args.dispatchers.max(1))
                .map(|_| {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    runtime.spawn(run_dispatcher(rx, counters.clone(), args.clone()));
                    tx
                })
                .collect();
            Some(txs)
        };

        let handle = runtime.handle().clone();
        let args = args.clone();
        let done = done.clone();
        producer_threads.push(
            std::thread::Builder::new()
                .name(format!("producer-{p}"))
                .spawn(move || run_producer(counters, tx, pool, pool_fut, handle, args, done))
                .expect("failed to spawn producer thread"),
        );
    }

    let total =
        |counters: &[Arc<Counters>]| -> u64 { counters.iter().map(|c| c.completed()).sum() };

    // Warmup, then measure with per-second samples (the samples expose
    // scheduling bistability that an average would hide).
    std::thread::sleep(Duration::from_secs(args.warmup_secs));
    let start_count = total(&producer_counters);
    let start = Instant::now();
    let mut last = start_count;
    for _ in 0..args.secs {
        std::thread::sleep(Duration::from_secs(1));
        let now = total(&producer_counters);
        tracing::info!("QPS: {}/s", now - last);
        last = now;
    }
    let elapsed = start.elapsed();
    let processed = total(&producer_counters) - start_count;

    done.store(true, Ordering::Relaxed);
    for t in producer_threads {
        let _ = t.join();
    }

    tracing::info!(
        "mode={:?} runtimes={} workers={} dispatchers={} producers={} batch={} chunk={} inflight={} conns={} spin={}ns heavy={}@{}ns",
        args.mode,
        runtimes.len(),
        if args.worker_threads > 0 {
            args.worker_threads.to_string()
        } else {
            "ncpu".to_string()
        },
        args.dispatchers.max(1),
        args.producers,
        args.batch,
        args.chunk,
        args.inflight,
        args.conns,
        args.handler_spin_ns,
        args.heavy_frac,
        args.heavy_spin_ns,
    );
    tracing::info!(
        "processed {} messages in {:.2}s: {:.0} msg/s",
        processed,
        elapsed.as_secs_f64(),
        processed as f64 / elapsed.as_secs_f64(),
    );

    // Mixed-load latency report (inject -> completion, includes warmup).
    if heavy_threshold(args.heavy_frac) != 0 {
        for (name, sel) in [("light", false), ("heavy", true)] {
            let mut counts = vec![0u64; HIST_BUCKETS];
            for c in &producer_counters {
                for (dst, b) in counts.iter_mut().zip(&c.hist(sel).buckets) {
                    *dst += b.load(Ordering::Relaxed);
                }
            }
            let total: u64 = counts.iter().sum();
            let max_idx = counts.iter().rposition(|&c| c > 0).unwrap_or(0);
            tracing::info!(
                "{} latency: n={} p50={} p90={} p99={} p99.9={} max~={}",
                name,
                total,
                fmt_ns(percentile(&counts, total, 0.50)),
                fmt_ns(percentile(&counts, total, 0.90)),
                fmt_ns(percentile(&counts, total, 0.99)),
                fmt_ns(percentile(&counts, total, 0.999)),
                fmt_ns(LatHist::value(max_idx)),
            );
        }
    }
}
