//! CPU-only READ ownership benchmarks, deliberately separate from provider I/O.
//!
//! Run with `cargo bench -p ruapc-rdma --features bench-internals --bench read_bookkeeping`.
//! `prepare` excludes input-plan construction and output destruction and supplies
//! a constant lkey instead of consulting a registered device. `batch/create`
//! measures the real Arc/oneshot allocation, excluding destination allocation.
//! `ledger` measures real slot-table insertion/removal, Arc traffic, the per-WR
//! acceptance store and nonfinal batch accounting. Its posting groups share one
//! long-lived batch, so batch creation and final notification are excluded. The
//! two-thread case includes bounded atomic publication/backpressure and the
//! shared batch's acceptance/completion contention, but no per-WR channel or
//! thread startup. `ledger/lifecycle` includes one short batch's creation,
//! ownership accounting and final notification, excluding the core deadline
//! registry; `ledger/spill` forces the collision fallback with a two-slot table.
//! `finish` measures the last completion plus receiver extraction; its optional
//! counting waker measures invocation, not executor scheduling or task resumption.
//! No case posts work, creates a QP, or fabricates a CQ completion token.

use std::{
    future::Future,
    hint::{black_box, spin_loop},
    pin::Pin,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Wake, Waker},
    time::{Duration, Instant},
};

use ruapc_bufpool::{Buffer, BufferPool, BufferPoolBuilder, EmptyDevices};

use ruapc_rdma::{
    ReadCompletion, ReadFailure, ReadRequest, ReadSegment, WRID,
    bench_support::read::{Batch, Ledger, prepare},
};

mod common;
use common::Config;

const CHUNK: u64 = 64;
const SEGMENT_LEN: usize = 64;
const LEDGER_WINDOW: u64 = 64;

fn pool() -> Arc<BufferPool> {
    BufferPoolBuilder::new(Arc::new(EmptyDevices)).build()
}

fn destination(pool: &Arc<BufferPool>, len: usize) -> Buffer {
    let mut buffer = pool.allocate(len).unwrap();
    buffer.set_len(len);
    buffer
}

fn requests(wrs: usize, scatter: usize) -> Vec<ReadRequest> {
    (0..wrs)
        .map(|wr| ReadRequest {
            remote_addr: 0x1000 + (wr * scatter * SEGMENT_LEN) as u64,
            rkey: 7,
            segments: (0..scatter)
                .map(|buffer| ReadSegment {
                    buffer,
                    offset: wr * SEGMENT_LEN,
                    len: SEGMENT_LEN,
                })
                .collect(),
        })
        .collect()
}

fn prepare_run(iterations: u64, buffers: &[Buffer], wrs: usize, scatter: usize) -> Duration {
    let mut elapsed = Duration::ZERO;
    let mut remaining = iterations;
    while remaining != 0 {
        let count = remaining.min(CHUNK);
        let mut plans = (0..count)
            .map(|_| requests(wrs, scatter))
            .collect::<Vec<_>>()
            .into_iter();
        let mut prepared = Vec::with_capacity(count as usize);
        let start = Instant::now();
        for plan in plans.by_ref() {
            prepared.push(prepare(black_box(buffers), plan, scatter, 42).unwrap());
        }
        elapsed += start.elapsed();
        black_box(&prepared);
        remaining -= count;
    }
    elapsed
}

fn create_run(iterations: u64, pool: &Arc<BufferPool>, wrs: usize) -> Duration {
    let mut elapsed = Duration::ZERO;
    let mut remaining = iterations;
    while remaining != 0 {
        let count = remaining.min(CHUNK);
        let mut destinations = (0..count)
            .map(|_| vec![destination(pool, SEGMENT_LEN)])
            .collect::<Vec<_>>()
            .into_iter();
        let mut batches = Vec::with_capacity(count as usize);
        let start = Instant::now();
        for buffers in destinations.by_ref() {
            batches.push(Batch::new(black_box(wrs), buffers));
        }
        elapsed += start.elapsed();
        black_box(&batches);
        remaining -= count;
    }
    elapsed
}

fn insert_group(state: &Ledger, batch: &Batch, sequence: u64, wrs: u64) {
    for sequence in sequence..sequence + wrs {
        assert!(state.insert(sequence, batch));
        // Production stores the accepted count after the provider post. Include
        // it so the batch's atomic line sees the same producer traffic.
        batch.mark_accepted(usize::try_from(sequence + 1).expect("sequence fits usize"));
    }
}

fn complete_group(state: &Ledger, sequence: u64, wrs: u64) {
    for sequence in sequence..sequence + wrs {
        assert!(state.complete(sequence));
    }
}

fn phases(config: &Config) -> impl Iterator<Item = u64> + '_ {
    std::iter::once(config.warmup).chain(std::iter::repeat_n(config.iters, config.repeats))
}

fn ledger_batch(config: &Config, pool: &Arc<BufferPool>, wrs: u64) -> (Batch, ReadCompletion) {
    let count = config
        .iters
        .checked_mul(config.repeats as u64)
        .and_then(|count| count.checked_add(config.warmup))
        .and_then(|count| count.checked_mul(wrs))
        .filter(|&count| count <= WRID::MAX_SEQUENCE)
        .and_then(|count| usize::try_from(count).ok())
        .and_then(|count| count.checked_add(1))
        .expect("READ benchmark iteration count exceeds sequence/counter range");
    // One unposted suffix keeps the measured completions on the nonfinal path.
    // No hardware owns these buffers; cancellation below settles that suffix.
    Batch::new(count, vec![destination(pool, SEGMENT_LEN)])
}

fn finish_ledger(state: &Ledger, batch: Batch, mut receiver: ReadCompletion) {
    assert!(state.is_empty());
    assert_eq!(batch.remaining(), 1);
    batch.abort_unposted(1);
    assert_eq!(
        receiver.try_recv().unwrap().unwrap_err(),
        ReadFailure::Completion
    );
}

fn ledger_serial(config: &Config, name: &str, pool: &Arc<BufferPool>, wrs: u64) {
    ledger_serial_with(config, name, pool, wrs, &Ledger::new());
}

/// Forces sequence collisions into the slot table's fallback map and asserts
/// that the measured case actually took that path.
fn ledger_spill(config: &Config, name: &str, pool: &Arc<BufferPool>, wrs: u64) {
    let state = Ledger::with_depth(1);
    ledger_serial_with(config, name, pool, wrs, &state);
    assert!(
        state.spills() != 0,
        "spill case never reached the fallback map"
    );
}

fn ledger_serial_with(
    config: &Config,
    name: &str,
    pool: &Arc<BufferPool>,
    wrs: u64,
    state: &Ledger,
) {
    let (batch, receiver) = ledger_batch(config, pool, wrs);
    let mut sequence = 0;
    for (phase, iterations) in phases(config).enumerate() {
        let start = Instant::now();
        for _ in 0..iterations {
            insert_group(state, &batch, sequence, wrs);
            complete_group(state, sequence, wrs);
            sequence += wrs;
        }
        let elapsed = start.elapsed();
        if phase != 0 {
            config.report(name, elapsed, iterations * wrs);
        }
    }
    finish_ledger(state, batch, receiver);
}

/// One complete low-level batch lifetime: creation, one insert/complete per WR
/// and final notification. Core deadline-registry work is excluded. Buffers are
/// recycled through the receiver, so no pool allocation is measured between batches.
fn lifecycle_run(iterations: u64, pool: &Arc<BufferPool>, wrs: u64) -> Duration {
    let state = Ledger::new();
    let mut buffers = vec![destination(pool, SEGMENT_LEN)];
    let mut sequence = 0u64;
    let mut elapsed = Duration::ZERO;
    let mut remaining = iterations;
    while remaining != 0 {
        let count = remaining.min(CHUNK);
        let start = Instant::now();
        for _ in 0..count {
            let (batch, mut receiver) = Batch::new(wrs as usize, std::mem::take(&mut buffers));
            let first = sequence;
            for accepted in 1..=wrs as usize {
                assert!(state.insert(sequence, &batch));
                batch.mark_accepted(accepted);
                sequence += 1;
            }
            for sequence in first..first + wrs {
                assert!(state.complete(sequence));
            }
            buffers = receiver.try_recv().unwrap().unwrap();
        }
        elapsed += start.elapsed();
        remaining -= count;
    }
    assert!(state.is_empty());
    elapsed
}

fn ledger_lifecycle(config: &Config, name: &str, pool: &Arc<BufferPool>, wrs: u64) {
    lifecycle_run(config.warmup, pool, wrs);
    for _ in 0..config.repeats {
        config.report(name, lifecycle_run(config.iters, pool, wrs), config.iters);
    }
}

// Keep publication and completion counters on distinct cache lines. Their
// synchronization is harness overhead, included in the two-thread result.
#[repr(align(128))]
struct Progress(AtomicU64);

fn wait_until(counter: &AtomicU64, expected: u64) {
    let mut spins = 0u32;
    while counter.load(Ordering::Acquire) < expected {
        spin_loop();
        spins += 1;
        if spins == 256 {
            std::thread::yield_now();
            spins = 0;
        }
    }
}

fn ledger_threads(config: &Config, name: &str, pool: &Arc<BufferPool>, wrs: u64) {
    let state = Ledger::new();
    let (batch, receiver) = ledger_batch(config, pool, wrs);
    let published = Progress(AtomicU64::new(0));
    let completed = Progress(AtomicU64::new(0));
    let barrier = Barrier::new(2);
    std::thread::scope(|scope| {
        // A failed affinity request must not strand the poster at the barrier.
        // This handshake is used once, outside warmup and measured work.
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
        scope.spawn(|| {
            common::pin_thread(1);
            ready_tx.send(()).unwrap();
            drop(ready_tx);
            let mut sequence = 0;
            for iterations in phases(config) {
                barrier.wait();
                for _ in 0..iterations {
                    wait_until(&published.0, sequence + wrs);
                    complete_group(&state, sequence, wrs);
                    sequence += wrs;
                    completed.0.store(sequence, Ordering::Release);
                }
                barrier.wait();
            }
        });
        ready_rx.recv().expect("READ completer failed during setup");
        let mut sequence = 0;
        for (phase, iterations) in phases(config).enumerate() {
            barrier.wait();
            let start = Instant::now();
            for _ in 0..iterations {
                wait_until(&completed.0, (sequence + wrs).saturating_sub(LEDGER_WINDOW));
                insert_group(&state, &batch, sequence, wrs);
                sequence += wrs;
                published.0.store(sequence, Ordering::Release);
            }
            wait_until(&completed.0, sequence);
            let elapsed = start.elapsed();
            barrier.wait();
            if phase != 0 {
                config.report(name, elapsed, iterations * wrs);
            }
        }
    });
    finish_ledger(&state, batch, receiver);
}

#[derive(Default)]
struct CountWake(AtomicU64);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn finish_run(iterations: u64, pool: &Arc<BufferPool>, wrs: usize, armed: bool) -> Duration {
    let wake_count = Arc::new(CountWake::default());
    let waker = Waker::from(wake_count.clone());
    let mut context = Context::from_waker(&waker);
    let mut elapsed = Duration::ZERO;
    let mut remaining = iterations;
    while remaining != 0 {
        let count = remaining.min(CHUNK);
        let mut batches = (0..count)
            .map(|_| {
                let (batch, mut receiver) = Batch::new(wrs, vec![destination(pool, SEGMENT_LEN)]);
                for _ in 1..wrs {
                    batch.complete_one(true);
                }
                if armed {
                    assert!(Pin::new(&mut receiver).poll(&mut context).is_pending());
                }
                (batch, receiver)
            })
            .collect::<Vec<_>>();
        let mut returned = Vec::with_capacity(count as usize);
        let start = Instant::now();
        for (batch, receiver) in &mut batches {
            batch.complete_one(true);
            returned.push(receiver.try_recv().unwrap().unwrap());
        }
        elapsed += start.elapsed();
        black_box(&returned);
        remaining -= count;
    }
    assert_eq!(
        wake_count.0.load(Ordering::Relaxed),
        if armed { iterations } else { 0 }
    );
    elapsed
}

fn main() {
    if std::env::args().any(|arg| arg == "--test") {
        return;
    }
    let config = Config::from_env();
    common::pin_thread(0);
    let pool = pool();
    println!(
        "READ benchmarks: no QP/provider/CQ polling; prepare uses a constant lkey; \
         ledger includes per-WR acceptance and nonfinal accounting; \
         finish excludes executor scheduling"
    );
    for wrs in [1, 8, 32] {
        for scatter in [1, 4, 32] {
            let name = format!("read/prepare/wrs={wrs}/sge={scatter}/per-batch");
            if config.selected(&name) {
                let buffers = (0..scatter)
                    .map(|_| destination(&pool, wrs * SEGMENT_LEN))
                    .collect::<Vec<_>>();
                prepare_run(config.warmup, &buffers, wrs, scatter);
                for _ in 0..config.repeats {
                    let elapsed = prepare_run(config.iters, &buffers, wrs, scatter);
                    config.report(&name, elapsed, config.iters);
                }
            }
        }
        let name = format!("read/batch/create/wrs={wrs}/per-batch");
        if config.selected(&name) {
            create_run(config.warmup, &pool, wrs);
            for _ in 0..config.repeats {
                let elapsed = create_run(config.iters, &pool, wrs);
                config.report(&name, elapsed, config.iters);
            }
        }
        let name = format!("read/ledger/serial/wrs={wrs}/per-wr");
        if config.selected(&name) {
            ledger_serial(&config, &name, &pool, wrs as u64);
        }
        let name = format!("read/ledger/threads/wrs={wrs}/window={LEDGER_WINDOW}/per-wr");
        if config.selected(&name) {
            ledger_threads(&config, &name, &pool, wrs as u64);
        }
        if wrs == 32 {
            let name = format!("read/ledger/spill/wrs={wrs}/per-wr");
            if config.selected(&name) {
                ledger_spill(&config, &name, &pool, wrs as u64);
            }
        }
        let name = format!("read/ledger/lifecycle/wrs={wrs}/per-batch");
        if config.selected(&name) {
            ledger_lifecycle(&config, &name, &pool, wrs as u64);
        }
        for armed in [false, true] {
            let mode = if armed { "counting-waker" } else { "unpolled" };
            let name = format!("read/finish/{mode}/wrs={wrs}/per-batch");
            if config.selected(&name) {
                finish_run(config.warmup, &pool, wrs, armed);
                for _ in 0..config.repeats {
                    let elapsed = finish_run(config.iters, &pool, wrs, armed);
                    config.report(&name, elapsed, config.iters);
                }
            }
        }
    }
}
