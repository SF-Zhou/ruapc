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
//!
//! `--handler-spin-ns` adds busy-work per message to model a real handler
//! (deserialize + user logic + response serialization). With 0 the numbers
//! are pure runtime overhead; with ~1000-2000ns they approximate the 32B
//! echo cost, letting the result be compared against RDMA bench QPS to
//! decide whether tokio or the transport is the bottleneck.

use clap::Parser;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// Execution mode: direct | spawn | chunk | inline.
    #[arg(long, default_value = "spawn")]
    mode: Mode,

    /// Tokio worker threads (0 = number of CPUs).
    #[arg(long, default_value = "0")]
    worker_threads: usize,

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

/// Per-producer state: one counter pair per simulated connection.
struct Counters {
    conns: Vec<ConnCounters>,
}

impl Counters {
    fn new(conns: usize) -> Self {
        Self {
            conns: (0..conns.max(1)).map(|_| ConnCounters::default()).collect(),
        }
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

/// Dispatcher task: receives `(count, conn)` batches and executes them
/// according to the mode. Messages carry no payload — a batch is just a
/// count plus its connection index, which is the cheapest possible
/// representation and therefore an upper bound for the real path.
async fn run_dispatcher(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(usize, usize)>,
    counters: Arc<Counters>,
    mode: Mode,
    chunk: usize,
    spin_ns: u64,
) {
    while let Some((n, conn)) = rx.recv().await {
        let mut n = n;
        match mode {
            Mode::Inline => {
                for _ in 0..n {
                    handle_msg(&counters, conn, spin_ns);
                }
            }
            Mode::Spawn => {
                for _ in 0..n {
                    let counters = counters.clone();
                    tokio::spawn(async move { handle_msg(&counters, conn, spin_ns) });
                }
            }
            Mode::Chunk => {
                while n > 0 {
                    let c = n.min(chunk);
                    n -= c;
                    let counters = counters.clone();
                    tokio::spawn(async move {
                        for _ in 0..c {
                            handle_msg(&counters, conn, spin_ns);
                        }
                    });
                }
            }
            Mode::Direct => unreachable!("direct mode has no dispatcher"),
        }
    }
}

/// Producer OS thread: keeps the window full by injecting batches, exactly
/// like a poll thread forwarding parsed messages. Batches are assigned to
/// simulated connections round-robin (one CQ drain usually carries a run
/// of completions from the same connection).
fn run_producer(
    counters: Arc<Counters>,
    tx: Option<tokio::sync::mpsc::UnboundedSender<(usize, usize)>>,
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
        match &tx {
            Some(tx) => {
                if tx.send((room, conn)).is_err() {
                    return;
                }
            }
            None => {
                // Remote-inject path: one spawn per message from outside
                // the runtime, like the pre-dispatcher architecture.
                for _ in 0..room {
                    let counters = counters.clone();
                    let spin_ns = args.handler_spin_ns;
                    handle.spawn(async move { handle_msg(&counters, conn, spin_ns) });
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

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if args.worker_threads > 0 {
        builder.worker_threads(args.worker_threads);
    }
    let runtime = builder.build().expect("failed to build tokio runtime");

    let done = Arc::new(AtomicBool::new(false));
    let mut producer_counters = Vec::new();
    let mut producer_threads = Vec::new();

    let _guard = runtime.enter();
    for p in 0..args.producers.max(1) {
        let counters = Arc::new(Counters::new(args.conns));
        producer_counters.push(counters.clone());

        let tx = if args.mode == Mode::Direct {
            None
        } else {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            runtime.spawn(run_dispatcher(
                rx,
                counters.clone(),
                args.mode,
                args.chunk.max(1),
                args.handler_spin_ns,
            ));
            Some(tx)
        };

        let handle = runtime.handle().clone();
        let args = args.clone();
        let done = done.clone();
        producer_threads.push(
            std::thread::Builder::new()
                .name(format!("producer-{p}"))
                .spawn(move || run_producer(counters, tx, handle, args, done))
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
        "mode={:?} workers={} producers={} batch={} chunk={} inflight={} conns={} spin={}ns",
        args.mode,
        if args.worker_threads > 0 {
            args.worker_threads.to_string()
        } else {
            "ncpu".to_string()
        },
        args.producers,
        args.batch,
        args.chunk,
        args.inflight,
        args.conns,
        args.handler_spin_ns,
    );
    tracing::info!(
        "processed {} messages in {:.2}s: {:.0} msg/s",
        processed,
        elapsed.as_secs_f64(),
        processed as f64 / elapsed.as_secs_f64(),
    );
}
