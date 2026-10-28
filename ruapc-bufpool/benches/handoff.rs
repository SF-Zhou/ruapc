//! Buffer allocation and recycling on the same thread or across two threads.
//!
//! Run with `cargo bench -p ruapc-bufpool --bench handoff`.
//! Each bounded-channel message carries eight gather groups. Two reusable
//! batches bound in-flight memory and avoid channel operations per buffer.
//! Group vectors keep their capacity; this measures buffer allocation/free,
//! not allocation of gather-list containers. No payload bytes are copied.
//!
//! `same_thread` allocates and drops on the producer; `cross_thread` allocates
//! on the producer and drops on the consumer. `reuse` passes preallocated
//! buffers through the same channels without allocating or freeing them in
//! the timed region. It indicates handoff overhead, not a subtractable cost.
//!
//! Optional controls: `RUAPC_BENCH_ITERS` (measured batches, default 2000),
//! `RUAPC_BENCH_SIZE` (16384/65536/1048576), `RUAPC_BENCH_GATHER` (1/4/16),
//! `RUAPC_BENCH_THREAD_CACHE` (0/1), and `RUAPC_BENCH_MODE`
//! (same_thread/cross_thread/reuse). Unset filters run every case.
//! On Linux, `RUAPC_BENCH_CPU_BASE=N` pins the producer to N and consumer to
//! N+1. Select available physical cores and bind memory to their NUMA node.
//! Thread setup, initial allocation, warmup and final destruction are untimed.
//! Reported ns/buffer is elapsed time divided by total processed buffers,
//! not a latency distribution or a per-thread latency.

use std::hint::black_box;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use ruapc_bufpool::{Buffer, BufferPool, BufferPoolBuilder, EmptyDevices};

// Measurements pin jemalloc as the process allocator so samples are comparable
// across hosts; see docs/benchmark.md#allocator.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const MIB: usize = 1024 * 1024;
const GROUPS_PER_BATCH: usize = 8;
const BATCHES_IN_FLIGHT: usize = 2;
const WARMUP_BATCHES: usize = 128;
const SIZES: [usize; 3] = [16 * 1024, 64 * 1024, MIB];
const GATHERS: [usize; 3] = [1, 4, 16];

type Batch = Vec<Vec<Buffer>>;

#[derive(Clone, Copy)]
enum Mode {
    SameThread,
    CrossThread,
    Reuse,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::SameThread => "same_thread",
            Self::CrossThread => "cross_thread",
            Self::Reuse => "reuse",
        }
    }
}

struct Options {
    iters: usize,
    size: Option<usize>,
    gather: Option<usize>,
    thread_cache: Option<usize>,
    mode: Option<String>,
    cpu_base: Option<usize>,
}

impl Options {
    fn from_env() -> Self {
        let options = Self {
            iters: env_usize("RUAPC_BENCH_ITERS").unwrap_or(2_000),
            size: env_usize("RUAPC_BENCH_SIZE"),
            gather: env_usize("RUAPC_BENCH_GATHER"),
            thread_cache: env_usize("RUAPC_BENCH_THREAD_CACHE"),
            mode: optional_env("RUAPC_BENCH_MODE"),
            cpu_base: env_usize("RUAPC_BENCH_CPU_BASE"),
        };
        assert!(options.iters > 0, "RUAPC_BENCH_ITERS must be positive");
        assert!(
            options.size.is_none_or(|value| SIZES.contains(&value)),
            "RUAPC_BENCH_SIZE must be 16384, 65536, or 1048576"
        );
        assert!(
            options.gather.is_none_or(|value| GATHERS.contains(&value)),
            "RUAPC_BENCH_GATHER must be 1, 4, or 16"
        );
        assert!(
            options.thread_cache.is_none_or(|value| value <= 1),
            "RUAPC_BENCH_THREAD_CACHE must be 0 or 1"
        );
        assert!(
            options
                .mode
                .as_deref()
                .is_none_or(|value| { ["same_thread", "cross_thread", "reuse"].contains(&value) }),
            "RUAPC_BENCH_MODE must be same_thread, cross_thread, or reuse"
        );
        options
    }
}

fn allocate_batch(batch: &mut Batch, pool: &Arc<BufferPool>, size: usize, gather: usize) {
    for group in batch {
        assert!(group.is_empty());
        for _ in 0..gather {
            group.push(pool.allocate(size).expect("benchmark pool exhausted"));
        }
    }
}

fn recycle_batch(batch: &mut Batch) {
    for group in batch {
        black_box(group).clear();
    }
}

fn prepare_batches(pool: &Arc<BufferPool>, size: usize, gather: usize, reuse: bool) -> Vec<Batch> {
    let mut batches: Vec<_> = (0..BATCHES_IN_FLIGHT)
        .map(|_| {
            let mut batch = (0..GROUPS_PER_BATCH)
                .map(|_| Vec::with_capacity(gather))
                .collect();
            allocate_batch(&mut batch, pool, size, gather);
            batch
        })
        .collect();
    // Prime the largest live working set before timing in every mode.
    if !reuse {
        for batch in &mut batches {
            recycle_batch(batch);
        }
    }
    batches
}

fn run_local(pool: &Arc<BufferPool>, size: usize, gather: usize, iters: usize) -> Duration {
    let mut batches = prepare_batches(pool, size, gather, false);
    let mut run = |count| {
        for index in 0..count {
            let batch = &mut batches[index % BATCHES_IN_FLIGHT];
            allocate_batch(batch, pool, size, gather);
            recycle_batch(batch);
        }
    };
    run(WARMUP_BATCHES);
    let start = Instant::now();
    run(iters);
    start.elapsed()
}

fn run_handoff(
    pool: &Arc<BufferPool>,
    size: usize,
    gather: usize,
    options: &Options,
    reuse: bool,
) -> Duration {
    let mut ready = prepare_batches(pool, size, gather, reuse);
    let (send, incoming) = mpsc::sync_channel::<Batch>(BATCHES_IN_FLIGHT);
    let (recycled, receive) = mpsc::sync_channel::<Batch>(BATCHES_IN_FLIGHT);
    let consumer_cpu = options.cpu_base.map(|base| {
        base.checked_add(1)
            .expect("RUAPC_BENCH_CPU_BASE range overflow")
    });
    let consumer = std::thread::spawn(move || {
        pin_current_thread(consumer_cpu);
        while let Ok(mut batch) = incoming.recv() {
            if reuse {
                black_box(&batch);
            } else {
                recycle_batch(&mut batch);
            }
            recycled.send(batch).unwrap();
        }
    });

    // Return every batch before ending each phase. The final acknowledgments
    // include the consumer's frees, while thread teardown stays outside timing.
    let mut run = |count| {
        for _ in 0..count {
            let mut batch = ready.pop().unwrap_or_else(|| receive.recv().unwrap());
            if !reuse {
                allocate_batch(&mut batch, pool, size, gather);
            }
            send.send(batch).unwrap();
        }
        while ready.len() < BATCHES_IN_FLIGHT {
            ready.push(receive.recv().unwrap());
        }
    };
    run(WARMUP_BATCHES);
    let start = Instant::now();
    run(options.iters);
    let elapsed = start.elapsed();
    drop(send);
    consumer.join().unwrap();
    elapsed
}

#[allow(clippy::cast_precision_loss)]
fn report(mode: Mode, size: usize, gather: usize, cache: bool, iters: usize, elapsed: Duration) {
    let buffers = iters
        .checked_mul(GROUPS_PER_BATCH * gather)
        .expect("buffer count overflow");
    let seconds = elapsed.as_secs_f64();
    println!(
        "{:<12} size={size:>7} gather={gather:>2} cache={} buffers={buffers:>8} | {:>9.1} ns/buffer | {:>7.2} M buffers/s",
        mode.name(),
        usize::from(cache),
        seconds * 1e9 / buffers as f64,
        buffers as f64 / seconds / 1e6,
    );
}

#[cfg(target_os = "linux")]
fn pin_current_thread(cpu: Option<usize>) {
    let Some(cpu) = cpu else { return };
    assert!(cpu < libc::CPU_SETSIZE as usize, "CPU index is too large");
    // SAFETY: zero initializes an empty set, and the CPU index is in bounds.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_SET(cpu, &mut set) };
    // SAFETY: pid zero selects this thread; the set and its size agree.
    let result = unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) };
    assert_eq!(
        result,
        0,
        "pin to CPU {cpu}: {}",
        std::io::Error::last_os_error()
    );
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(cpu: Option<usize>) {
    assert!(cpu.is_none(), "benchmark CPU pinning requires Linux");
}

fn optional_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn env_usize(name: &str) -> Option<usize> {
    optional_env(name).map(|value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a nonnegative integer"))
    })
}

fn main() {
    if std::env::args().any(|arg| arg == "--test") {
        return;
    }
    let options = Options::from_env();
    pin_current_thread(options.cpu_base);
    println!("ruapc-bufpool: allocation and buffer handoff (no payload copies)");
    println!(
        "{} measured batches, {GROUPS_PER_BATCH} groups/batch, {BATCHES_IN_FLIGHT} reusable batches; ns/buffer is amortized wall time",
        options.iters
    );
    if let Some(base) = options.cpu_base {
        println!("producer CPU {base}, consumer CPU {}", base + 1);
    }
    for size in SIZES {
        if options.size.is_some_and(|selected| selected != size) {
            continue;
        }
        for gather in GATHERS {
            if options.gather.is_some_and(|selected| selected != gather) {
                continue;
            }
            for cache in [true, false] {
                if options
                    .thread_cache
                    .is_some_and(|selected| selected != usize::from(cache))
                {
                    continue;
                }
                for mode in [Mode::SameThread, Mode::CrossThread, Mode::Reuse] {
                    if options
                        .mode
                        .as_deref()
                        .is_some_and(|selected| selected != mode.name())
                    {
                        continue;
                    }
                    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
                        .max_memory(512 * MIB)
                        .thread_cache(cache)
                        .build();
                    let elapsed = match mode {
                        Mode::SameThread => run_local(&pool, size, gather, options.iters),
                        Mode::CrossThread => run_handoff(&pool, size, gather, &options, false),
                        Mode::Reuse => run_handoff(&pool, size, gather, &options, true),
                    };
                    report(mode, size, gather, cache, options.iters, elapsed);
                }
            }
        }
    }
}
