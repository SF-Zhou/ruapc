//! Benchmark comparing eager vs lazy buddy merging.
//!
//! Run with: `cargo bench -p ruapc-bufpool`
//!
//! Each scenario runs the exact same operation sequence against two pools:
//! - **eager**: `merge_watermarks([0, 0, 0, 0])`, the pre-lazy-merge behavior
//!   (every free immediately merges complete quads upward)
//! - **lazy**: default watermarks, deferring merges and coalescing on demand

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ruapc_bufpool::{Buffer, BufferPool, BufferPoolBuilder, EmptyDevices};

const MIB: usize = 1024 * 1024;
const LEVEL_SIZES: [usize; 4] = [MIB, 4 * MIB, 16 * MIB, 64 * MIB];

const EAGER: [usize; 4] = [0, 0, 0, 0];
const LAZY: [usize; 4] = [16, 8, 2, 0];

fn new_pool(watermarks: [usize; 4]) -> Arc<BufferPool> {
    BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(256 * MIB)
        .merge_watermarks(watermarks)
        .build()
}

/// Simple deterministic RNG (splitmix-style) so both pools see identical
/// operation sequences.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) as usize
    }
}

fn report(name: &str, ops: u64, eager: Duration, lazy: Duration) {
    #[allow(clippy::cast_precision_loss)]
    let per_op = |d: Duration| d.as_nanos() as f64 / ops as f64;
    let (e, l) = (per_op(eager), per_op(lazy));
    println!(
        "{name:<44} eager {e:>9.1} ns/op | lazy {l:>9.1} ns/op | speedup {:>5.2}x",
        e / l
    );
}

/// Worst case for eager merging: repeatedly allocate and free a single
/// buffer while the pool is otherwise empty. Eager merging pays a full
/// split + merge chain on every iteration.
fn bench_same_size_churn(level: usize, iters: u64) {
    let run = |watermarks| {
        let pool = new_pool(watermarks);
        for _ in 0..1_000 {
            black_box(pool.allocate(LEVEL_SIZES[level]).unwrap());
        }
        let start = Instant::now();
        for _ in 0..iters {
            black_box(pool.allocate(LEVEL_SIZES[level]).unwrap());
        }
        start.elapsed()
    };
    report(
        &format!(
            "same-size churn ({}MiB alloc+free)",
            LEVEL_SIZES[level] / MIB
        ),
        iters,
        run(EAGER),
        run(LAZY),
    );
}

/// Concurrency oscillating around a quad boundary: one full quad (4 x 1MiB)
/// is held, and a 5th buffer is repeatedly allocated and freed. The churn
/// buffer sits alone in the next quad, so under eager merging every free
/// completes that quad and merges it up, and every allocation splits again.
fn bench_quad_boundary(iters: u64) {
    let run = |watermarks| {
        let pool = new_pool(watermarks);
        let _held: Vec<Buffer> = (0..4).map(|_| pool.allocate(MIB).unwrap()).collect();
        for _ in 0..1_000 {
            black_box(pool.allocate(MIB).unwrap());
        }
        let start = Instant::now();
        for _ in 0..iters {
            black_box(pool.allocate(MIB).unwrap());
        }
        start.elapsed()
    };
    report(
        "quad boundary oscillation (1MiB)",
        iters,
        run(EAGER),
        run(LAZY),
    );
}

/// Bursty RPC pattern: allocate a batch of small buffers, then drop the
/// whole batch, repeatedly.
fn bench_burst(iters: u64) {
    const BATCH: u64 = 16;
    let run = |watermarks| {
        let pool = new_pool(watermarks);
        let mut batch = Vec::with_capacity(BATCH as usize);
        for _ in 0..100 {
            for _ in 0..BATCH {
                batch.push(pool.allocate(MIB).unwrap());
            }
            batch.clear();
        }
        let start = Instant::now();
        for _ in 0..iters {
            for _ in 0..BATCH {
                batch.push(pool.allocate(MIB).unwrap());
            }
            batch.clear();
        }
        start.elapsed()
    };
    report(
        "burst alloc/free (16 x 1MiB per batch)",
        iters * BATCH,
        run(EAGER),
        run(LAZY),
    );
}

/// Random mix of sizes with a steady working set, biased toward small
/// buffers as in typical RPC workloads.
fn bench_mixed(iters: u64) {
    let run = |watermarks| {
        let pool = new_pool(watermarks);
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut held: Vec<Buffer> = Vec::new();
        let op = |pool: &Arc<BufferPool>, rng: &mut Rng, held: &mut Vec<Buffer>| {
            if rng.next() % 100 < 55 || held.is_empty() {
                let level = [0, 0, 0, 0, 1, 1, 2][rng.next() % 7];
                if let Ok(buffer) = pool.allocate(LEVEL_SIZES[level]) {
                    held.push(buffer);
                }
            } else {
                let idx = rng.next() % held.len();
                held.swap_remove(idx);
            }
        };
        for _ in 0..10_000 {
            op(&pool, &mut rng, &mut held);
        }
        let start = Instant::now();
        for _ in 0..iters {
            op(&pool, &mut rng, &mut held);
        }
        start.elapsed()
    };
    report(
        "mixed random sizes (55% alloc)",
        iters,
        run(EAGER),
        run(LAZY),
    );
}

/// Demand path: small-buffer churn interleaved with an occasional 64MiB
/// allocation. The lazy pool must pay for on-demand coalescing here, so this
/// checks that the deferred work does not make large allocations regress.
fn bench_small_churn_with_large(iters: u64) {
    const SMALL_PER_LARGE: u64 = 64;
    let run = |watermarks| {
        let pool = new_pool(watermarks);
        let round = |pool: &Arc<BufferPool>| {
            for _ in 0..SMALL_PER_LARGE {
                black_box(pool.allocate(MIB).unwrap());
            }
            black_box(pool.allocate(64 * MIB).unwrap());
        };
        for _ in 0..100 {
            round(&pool);
        }
        let start = Instant::now();
        for _ in 0..iters {
            round(&pool);
        }
        start.elapsed()
    };
    report(
        "64x 1MiB churn + one 64MiB alloc",
        iters * (SMALL_PER_LARGE + 1),
        run(EAGER),
        run(LAZY),
    );
}

fn main() {
    println!("ruapc-bufpool: eager vs lazy buddy merging");
    println!("(eager = merge_watermarks [0,0,0,0], lazy = default watermarks)");
    println!();

    bench_same_size_churn(0, 2_000_000);
    bench_same_size_churn(1, 2_000_000);
    bench_quad_boundary(2_000_000);
    bench_burst(200_000);
    bench_mixed(2_000_000);
    bench_small_churn_with_large(50_000);
}
