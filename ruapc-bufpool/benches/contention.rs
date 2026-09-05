//! Multi-threaded contention benchmark for the buffer pool's global mutex.
//!
//! Run with: `cargo bench -p ruapc-bufpool --bench contention`
//!
//! Every thread churns buffers (allocate + free) on a shared pool. Buddy
//! traffic takes the pool mutex for each operation; slab traffic uses thread
//! magazines and reaches shared class locks only on refill/overflow. Reported
//! numbers show how latency and aggregate throughput scale with thread count.
//!
//! On Linux, set `RUAPC_BENCH_CPU_BASE` to pin worker i to CPU base+i and
//! the coordinator to CPU base+16. Select 17 available physical cores on the
//! intended NUMA node; a process-wide CPU mask alone permits worker migration.
//! Set `RUAPC_BENCH_SIZE` (bytes) and `RUAPC_BENCH_THREADS` to run one case in
//! a fresh process, avoiding allocation history from earlier scenarios.

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
fn run_churn(
    pool: &Arc<BufferPool>,
    threads: usize,
    iters: u64,
    size: usize,
    cpu_base: Option<usize>,
) -> f64 {
    let barrier = Arc::new(Barrier::new(threads + 1));

    let handles: Vec<_> = (0..threads)
        .map(|worker| {
            let pool = Arc::clone(pool);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin_current_thread(cpu_base.map(|base| base + worker));
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

fn scenario(
    name: &str,
    size: usize,
    hold: usize,
    iters: u64,
    cpu_base: Option<usize>,
    selected_threads: Option<usize>,
) {
    println!("{name}");
    let mut baseline_ns = 0.0;

    for threads in [1usize, 2, 4, 8, 16] {
        if selected_threads.is_some_and(|selected| selected != threads) {
            continue;
        }
        let pool = new_pool();
        // Hold a few buffers so the pool stays pre-grown and quads stay
        // broken, keeping the scenario steady-state.
        let _held: Vec<_> = (0..threads * hold)
            .map(|_| pool.allocate(size).unwrap())
            .collect();

        let secs = run_churn(&pool, threads, iters, size, cpu_base);

        #[allow(clippy::cast_precision_loss)]
        let total_ops = (iters * threads as u64 * 2) as f64; // alloc + free
        let mops = total_ops / secs / 1e6;
        #[allow(clippy::cast_precision_loss)]
        let ns_per_pair = secs * 1e9 / (iters as f64); // latency per alloc+free pair per thread
        if threads == 1 {
            baseline_ns = ns_per_pair;
        }

        print!("threads {threads:>2}: {mops:>7.2} M ops/s | {ns_per_pair:>8.1} ns per alloc+free");
        if baseline_ns != 0.0 {
            print!(
                " | scaling vs 1 thread: {:>5.2}x latency",
                ns_per_pair / baseline_ns
            );
        }
        println!();
    }
    println!();
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

fn main() {
    let cpu_base = env_usize("RUAPC_BENCH_CPU_BASE");
    let selected_size = env_usize("RUAPC_BENCH_SIZE");
    let selected_threads = env_usize("RUAPC_BENCH_THREADS");
    assert!(
        selected_size.is_none_or(|size| [MIB, 64 * 1024].contains(&size)),
        "RUAPC_BENCH_SIZE must be 1048576 or 65536 bytes"
    );
    assert!(
        selected_threads.is_none_or(|threads| [1, 2, 4, 8, 16].contains(&threads)),
        "RUAPC_BENCH_THREADS must be 1, 2, 4, 8, or 16"
    );
    pin_current_thread(cpu_base.map(|base| base.checked_add(16).expect("CPU range overflow")));
    println!("ruapc-bufpool: multi-threaded churn on a shared pool");
    println!("(one measured pair = allocate + free)");
    if let Some(base) = cpu_base {
        println!(
            "workers pinned to CPUs {base}–{}, coordinator {}",
            base + 15,
            base + 16
        );
    }
    println!();

    for (name, size) in [
        ("1MiB churn (buddy path, global pool mutex)", MIB),
        ("64KiB churn (thread cache, shared slab refill)", 64 * 1024),
    ] {
        if selected_size.is_none_or(|selected| selected == size) {
            scenario(name, size, 1, 1_000_000, cpu_base, selected_threads);
        }
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().map(|value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an integer"))
    })
}
