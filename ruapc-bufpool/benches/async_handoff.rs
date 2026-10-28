//! Exhausted-pool waiter registration, delivery and cancellation.
//!
//! Run with `cargo bench -p ruapc-bufpool --bench async_handoff`. A 64 MiB
//! buffer occupies the entire pool. Each iteration polls an allocation to
//! Pending, then either releases the buffer to that waiter or cancels the
//! waiter before releasing and synchronously reacquiring it. No bytes are
//! touched and no executor runs; the armed waker only counts notifications.
//! Pool creation, initial allocation, warmup and final destruction are untimed.
//! `ns/op` is amortized wall time, not a latency distribution. Pin the process
//! and memory externally for comparisons. Controls: RUAPC_BENCH_ITERS (100000),
//! RUAPC_BENCH_WARMUP (1000), RUAPC_BENCH_REPEATS (5), all positive integers.
//! RUAPC_BENCH_SLAB_HISTORY=1 first allocates and frees a 64 KiB slab chunk,
//! then recovers the whole pool before warmup; the default is 0. This measures
//! waiting on a pool that has used slabs, with the same timed allocation cycle.

use std::{
    future::Future,
    hint::black_box,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Instant,
};

use ruapc_bufpool::{Buffer, BufferPool, BufferPoolBuilder, EmptyDevices};

// Measurements pin jemalloc as the process allocator so samples are comparable
// across hosts; see docs/benchmark.md#allocator.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SIZE: usize = 64 * 1024 * 1024;

#[derive(Default)]
struct Notifications(AtomicUsize);

impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn cycle(pool: &Arc<BufferPool>, held: &mut Option<Buffer>, cx: &mut Context<'_>, cancel: bool) {
    if cancel {
        {
            let mut allocation = pin!(pool.async_allocate(SIZE));
            assert!(allocation.as_mut().poll(cx).is_pending());
        }
        drop(held.take());
        *held = Some(
            pool.allocate(SIZE)
                .expect("cancelled waiter retained capacity"),
        );
    } else {
        let mut allocation = pin!(pool.async_allocate(SIZE));
        assert!(allocation.as_mut().poll(cx).is_pending());
        drop(held.take());
        let Poll::Ready(Ok(buffer)) = allocation.as_mut().poll(cx) else {
            panic!("returned buffer did not satisfy queued allocation");
        };
        *held = Some(buffer);
    }
    black_box(&held);
}

fn number(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or_else(|| panic!("{name} must be a positive integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn main() {
    let iterations = number("RUAPC_BENCH_ITERS", 100_000);
    let warmup = number("RUAPC_BENCH_WARMUP", 1_000);
    let repeats = number("RUAPC_BENCH_REPEATS", 5);
    let slab_history = match std::env::var("RUAPC_BENCH_SLAB_HISTORY") {
        Ok(value) => match value.as_str() {
            "0" => false,
            "1" => true,
            _ => panic!("RUAPC_BENCH_SLAB_HISTORY must be 0 or 1"),
        },
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => panic!("invalid RUAPC_BENCH_SLAB_HISTORY: {error}"),
    };
    println!(
        "waiter configuration: slab_history={}",
        usize::from(slab_history)
    );
    for cancel in [false, true] {
        let name = if cancel { "cancel" } else { "handoff" };
        for sample in 0..repeats {
            let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
                .max_memory(SIZE)
                .thread_cache(false)
                .build();
            if slab_history {
                drop(pool.allocate(64 * 1024).unwrap());
            }
            // This allocation also drains any cached slab created above.
            let mut held = Some(pool.allocate(SIZE).unwrap());
            let notifications = Arc::new(Notifications::default());
            let waker = Waker::from(notifications.clone());
            let mut cx = Context::from_waker(&waker);
            for _ in 0..warmup {
                cycle(&pool, &mut held, &mut cx, cancel);
            }
            notifications.0.store(0, Ordering::Relaxed);
            let start = Instant::now();
            for _ in 0..iterations {
                cycle(&pool, &mut held, &mut cx, cancel);
            }
            let elapsed = start.elapsed();
            let wakes = notifications.0.load(Ordering::Relaxed);
            assert_eq!(wakes, if cancel { 0 } else { iterations });
            println!(
                "waiter/{name}: sample={} operations={iterations} elapsed_ms={:.3} ns/op={:.2} wakes={wakes}",
                sample + 1,
                elapsed.as_secs_f64() * 1e3,
                elapsed.as_secs_f64() * 1e9 / iterations as f64,
            );
        }
    }
}
