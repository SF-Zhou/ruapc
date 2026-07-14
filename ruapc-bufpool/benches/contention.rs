//! Multi-threaded contention benchmark for the buffer pool's global mutex.
//!
//! Run with: `cargo bench -p ruapc-bufpool --bench contention`
//!
//! Every thread churns buffers (allocate + free) on a shared pool, which is
//! the worst case for lock contention: each operation takes the pool mutex
//! once. Reported numbers show how per-op latency and aggregate throughput
//! scale with thread count.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use ruapc_bufpool::{BufferPool, BufferPoolBuilder, EmptyDevices};

const MIB: usize = 1024 * 1024;

fn new_pool() -> Arc<BufferPool> {
    BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(1024 * MIB)
        .build()
}

/// Runs `threads` workers, each performing `iters` alloc+free pairs of
/// `size`-byte buffers on the shared pool. Returns elapsed wall time in
/// seconds.
fn run_churn(pool: &Arc<BufferPool>, threads: usize, iters: u64, size: usize) -> f64 {
    let barrier = Arc::new(Barrier::new(threads + 1));

    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let pool = Arc::clone(pool);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                // Warm up: fault in this thread's working set and pre-split.
                for _ in 0..1_000 {
                    black_box(pool.allocate(size).unwrap());
                }
                barrier.wait();
                for _ in 0..iters {
                    black_box(pool.allocate(size).unwrap());
                }
            })
        })
        .collect();

    barrier.wait();
    let start = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }
    start.elapsed().as_secs_f64()
}

fn scenario(name: &str, size: usize, hold: usize, iters: u64) {
    println!("{name}");
    let mut baseline_ns = 0.0;

    for threads in [1usize, 2, 4, 8, 16] {
        let pool = new_pool();
        // Hold a few buffers so the pool stays pre-grown and quads stay
        // broken, keeping the scenario steady-state.
        let _held: Vec<_> = (0..threads * hold)
            .map(|_| pool.allocate(size).unwrap())
            .collect();

        let secs = run_churn(&pool, threads, iters, size);

        #[allow(clippy::cast_precision_loss)]
        let total_ops = (iters * threads as u64 * 2) as f64; // alloc + free
        let mops = total_ops / secs / 1e6;
        #[allow(clippy::cast_precision_loss)]
        let ns_per_pair = secs * 1e9 / (iters as f64); // latency per alloc+free pair per thread
        if threads == 1 {
            baseline_ns = ns_per_pair;
        }

        println!(
            "threads {threads:>2}: {mops:>7.2} M lock-ops/s | {ns_per_pair:>8.1} ns per alloc+free \
             | scaling vs 1 thread: {:>5.2}x latency",
            ns_per_pair / baseline_ns
        );
    }
    println!();
}

fn main() {
    println!("ruapc-bufpool: multi-threaded churn on a shared pool");
    println!("(each op = allocate + free = one lock acquisition each)");
    println!();

    scenario(
        "1MiB churn (buddy path, global pool mutex)",
        MIB,
        1,
        1_000_000,
    );
    scenario(
        "64KiB churn (slab path, per-class mutex)",
        64 * 1024,
        1,
        1_000_000,
    );
}
