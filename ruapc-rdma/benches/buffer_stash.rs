//! Device-free measurements of the actual SEND/RECV ownership table.
//!
//! Run with `cargo bench -p ruapc-rdma --features bench-internals --bench buffer_stash`.
//! Buffers are allocated before timing and recycled without pool allocation or
//! destruction in the measured loops. No WR is posted and no completion proof
//! is fabricated: the sweep cases only exercise the real cursor and slot table.
//! Reported times are wall time divided by operations, not latency samples.

use std::{
    collections::VecDeque,
    hint::black_box,
    sync::{Arc, mpsc},
    time::Instant,
};

use ruapc_bufpool::{BufferPool, BufferPoolBuilder, EmptyDevices};

use ruapc_rdma::{
    CompletionCursor, WrBuffers,
    bench_support::{self, WrSlots},
};

mod common;
use common::{Config, pin_thread};

const DEPTH: u32 = 64;
const BATCH: usize = 32;
const PIPELINE: usize = 2;

fn pool() -> Arc<BufferPool> {
    BufferPoolBuilder::new(Arc::new(EmptyDevices)).build()
}

fn buffers(pool: &Arc<BufferPool>, gather: Option<usize>) -> WrBuffers {
    match gather {
        None => WrBuffers::One(pool.allocate(64).unwrap()),
        Some(count) => WrBuffers::Many(
            (0..count)
                .map(|_| pool.allocate(64).unwrap())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ),
    }
}

fn roundtrip(slots: &WrSlots, buffer: &mut Option<WrBuffers>, next: &mut u64, count: u64) {
    for _ in 0..count {
        let id = *next;
        *next += 1;
        slots.insert(id, black_box(buffer.take().unwrap())).unwrap();
        *buffer = Some(black_box(slots.take(id).unwrap()));
    }
}

fn benchmark_wr_slots(config: &Config) {
    let capacity = WrSlots::new(DEPTH).capacity();
    println!(
        "WrSlots depth={DEPTH}, capacity={}, slot_bytes={}, table_bytes={}; \
         roundtrip op=insert+take; no buffer allocation/drop in timed loops",
        capacity,
        WrSlots::slot_bytes(),
        capacity * WrSlots::slot_bytes(),
    );
    for (name, gather) in [
        ("slots/roundtrip/one", None),
        ("slots/roundtrip/gather_1", Some(1)),
        ("slots/roundtrip/gather_4", Some(4)),
        ("slots/roundtrip/gather_16", Some(16)),
    ] {
        if !config.selected(name) {
            continue;
        }
        for _ in 0..config.repeats {
            let pool = pool();
            let slots = WrSlots::new(DEPTH);
            let mut buffer = Some(buffers(&pool, gather));
            let mut next = 0;
            roundtrip(&slots, &mut buffer, &mut next, config.warmup);
            let start = Instant::now();
            roundtrip(&slots, &mut buffer, &mut next, config.iters);
            config.report(name, start.elapsed(), config.iters);
        }
    }

    for name in ["slots/take_empty", "slots/take_mismatch", "slots/collision"] {
        if !config.selected(name) {
            continue;
        }
        for _ in 0..config.repeats {
            let pool = pool();
            let slots = WrSlots::new(DEPTH);
            let mut returned = Some(buffers(&pool, None));
            if name != "slots/take_empty" {
                slots.insert(0, buffers(&pool, None)).unwrap();
            }
            let capacity = slots.capacity() as u64;
            let mut next = capacity;
            let step = if name == "slots/take_empty" {
                1
            } else {
                capacity
            };
            let mut run = |count| {
                if name == "slots/collision" {
                    for _ in 0..count {
                        let id = black_box(next);
                        next += step;
                        // The failed insert returns the untouched owned buffer.
                        returned = Some(black_box(
                            slots.insert(id, returned.take().unwrap()).unwrap_err(),
                        ));
                    }
                } else {
                    for _ in 0..count {
                        let id = black_box(next);
                        next += step;
                        assert!(black_box(slots.take(id)).is_none());
                    }
                }
            };
            run(config.warmup);
            let start = Instant::now();
            run(config.iters);
            config.report(name, start.elapsed(), config.iters);
        }
    }
}

fn recycle_oldest(
    slots: &WrSlots,
    live: &mut VecDeque<(u64, usize)>,
    available: &mut Vec<WrBuffers>,
    live_bytes: &mut usize,
) {
    if let Some((id, capacity)) = live.pop_front() {
        available.push(slots.take(id).unwrap());
        *live_bytes -= capacity;
    }
}

fn benchmark_wr_slots_churn(config: &Config) {
    println!(
        "WrSlots churn op=one insert attempt; includes harness recycling/accounting; \
         reclaim oldest every 2 attempts, additionally before posting when depth is full; \
         peaks count only buffers currently owned by slots"
    );
    for (name, stride) in [
        ("slots/churn/stride_1", 1),
        ("slots/churn/stride_8", 8),
        ("slots/churn/stride_32", 32),
    ] {
        if !config.selected(name) {
            continue;
        }
        for _ in 0..config.repeats {
            let pool = pool();
            let slots = WrSlots::new(DEPTH);
            let mut available = (0..=DEPTH)
                .map(|_| buffers(&pool, None))
                .collect::<Vec<_>>();
            let mut live = VecDeque::with_capacity(DEPTH as usize);
            let mut live_bytes = 0;
            let mut attempt = 0u64;
            let mut run = |count| {
                let (mut collisions, mut peak_entries, mut peak_bytes) =
                    (0u64, live.len(), live_bytes);
                for _ in 0..count {
                    if live.len() == DEPTH as usize {
                        recycle_oldest(&slots, &mut live, &mut available, &mut live_bytes);
                    }
                    let buffer = available.pop().unwrap();
                    let capacity = match &buffer {
                        WrBuffers::One(buffer) => buffer.capacity(),
                        WrBuffers::Many(_) => unreachable!("churn preallocates single buffers"),
                    };
                    let id = attempt * stride;
                    attempt += 1;
                    match slots.insert(id, buffer) {
                        Ok(()) => {
                            live.push_back((id, capacity));
                            live_bytes += capacity;
                            peak_entries = peak_entries.max(live.len());
                            peak_bytes = peak_bytes.max(live_bytes);
                        }
                        Err(buffer) => {
                            available.push(buffer);
                            collisions += 1;
                        }
                    }
                    if attempt.is_multiple_of(2) {
                        recycle_oldest(&slots, &mut live, &mut available, &mut live_bytes);
                    }
                }
                (collisions, peak_entries, peak_bytes)
            };
            black_box(run(config.warmup));
            let start = Instant::now();
            let (collisions, peak_entries, peak_bytes) = run(config.iters);
            config.report(name, start.elapsed(), config.iters);
            println!(
                "  attempts={} collisions={collisions} collision_pct={:.3} \
                 peak_live_slots={peak_entries} peak_buffer_capacity_bytes={peak_bytes}",
                config.iters,
                collisions as f64 * 100.0 / config.iters as f64,
            );
            assert_eq!(available.len() + live.len(), DEPTH as usize + 1);
        }
    }
}

fn sweep(
    slots: &WrSlots,
    cursor: &mut CompletionCursor,
    available: &mut Vec<WrBuffers>,
    next: &mut u64,
    count: u64,
    width: u64,
    stride: usize,
) {
    for _ in 0..count {
        let begin = *next;
        *next += width;
        for id in (begin..*next).step_by(stride) {
            slots.insert(id, available.pop().unwrap()).unwrap();
        }
        let completed = *next - 1;
        for id in bench_support::sweep(cursor, 0, completed) {
            if let Some(buffer) = black_box(slots.take(id)) {
                available.push(buffer);
            }
        }
        if let Some(buffer) = black_box(slots.take(completed)) {
            available.push(buffer);
        }
    }
}

fn benchmark_wr_slots_sweep(config: &Config) {
    println!(
        "WrSlots sweep op=SQ sequence visited, including empty IDs; \
         includes insert staging and buffer recycling; synthetic sequence gaps \
         exercise CompletionCursor, not a CQ/provider"
    );
    for (name, width, stride) in [
        ("slots/sweep/dense_1", 1, 1),
        ("slots/sweep/dense_8", 8, 1),
        ("slots/sweep/dense_32", 32, 1),
        ("slots/sweep/sparse_32", 32, 8),
        ("slots/sweep/gap_4096", 4096, 4096),
    ] {
        if !config.selected(name) {
            continue;
        }
        let iterations = config.iters.div_ceil(width);
        let warmup = config.warmup.div_ceil(width);
        for _ in 0..config.repeats {
            let pool = pool();
            let slots = WrSlots::new(DEPTH);
            let mut cursor = CompletionCursor::default();
            let mut available = (0..width.div_ceil(stride as u64))
                .map(|_| buffers(&pool, None))
                .collect::<Vec<_>>();
            let mut next = 0;
            sweep(
                &slots,
                &mut cursor,
                &mut available,
                &mut next,
                warmup,
                width,
                stride,
            );
            let start = Instant::now();
            sweep(
                &slots,
                &mut cursor,
                &mut available,
                &mut next,
                iterations,
                width,
                stride,
            );
            config.report(name, start.elapsed(), iterations * width);
            assert_eq!(available.len() as u64, width.div_ceil(stride as u64));
        }
    }
}

struct Batch {
    first: u64,
    count: usize,
    buffers: Vec<WrBuffers>,
}

fn post_batches(
    slots: &WrSlots,
    ready: &mpsc::SyncSender<Batch>,
    returned: &mpsc::Receiver<Vec<WrBuffers>>,
    available: &mut VecDeque<Vec<WrBuffers>>,
    next: &mut u64,
    count: u64,
    move_buffers: bool,
) {
    let mut remaining = count;
    while remaining != 0 {
        let mut buffers = available
            .pop_front()
            .unwrap_or_else(|| returned.recv().unwrap());
        let count = remaining.min(BATCH as u64) as usize;
        let first = *next;
        *next += count as u64;
        if move_buffers {
            for id in first..*next {
                slots.insert(id, buffers.pop().unwrap()).unwrap();
            }
        }
        ready
            .send(Batch {
                first,
                count,
                buffers,
            })
            .unwrap();
        remaining -= count as u64;
    }
    // Stop timing only after all buffers have crossed back. Thread startup,
    // joining and buffer destruction stay outside the measured interval.
    while available.len() < PIPELINE {
        available.push_back(returned.recv().unwrap());
    }
}

fn benchmark_wr_slots_producer_consumer(config: &Config) {
    println!(
        "WrSlots producer/consumer op=one ownership roundtrip; \
         pipeline={PIPELINE}, buffers/batch={BATCH}; includes two channel messages \
         per batch and scheduling; handoff_only is a control, not a subtractable latency"
    );
    for (name, move_buffers) in [
        ("slots/producer_consumer/2x32", true),
        ("slots/handoff_only/2x32", false),
    ] {
        if !config.selected(name) {
            continue;
        }
        for _ in 0..config.repeats {
            let pool = pool();
            let slots = Arc::new(WrSlots::new(DEPTH));
            let mut available = (0..PIPELINE)
                .map(|_| (0..BATCH).map(|_| buffers(&pool, None)).collect::<Vec<_>>())
                .collect::<VecDeque<_>>();
            let (ready_tx, ready_rx) = mpsc::sync_channel::<Batch>(PIPELINE);
            let (returned_tx, returned_rx) = mpsc::sync_channel(PIPELINE);
            let (started_tx, started_rx) = mpsc::sync_channel(0);
            let worker_slots = Arc::clone(&slots);
            let worker = std::thread::spawn(move || {
                pin_thread(1);
                started_tx.send(()).unwrap();
                while let Ok(mut batch) = ready_rx.recv() {
                    if move_buffers {
                        for id in batch.first..batch.first + batch.count as u64 {
                            batch
                                .buffers
                                .push(black_box(worker_slots.take(id).unwrap()));
                        }
                    }
                    returned_tx.send(batch.buffers).unwrap();
                }
            });
            // A startup failure (for example invalid worker affinity) closes
            // this channel instead of leaving the other thread at a barrier.
            started_rx.recv().unwrap();
            let mut next = 0;
            post_batches(
                &slots,
                &ready_tx,
                &returned_rx,
                &mut available,
                &mut next,
                config.warmup,
                move_buffers,
            );
            let start = Instant::now();
            post_batches(
                &slots,
                &ready_tx,
                &returned_rx,
                &mut available,
                &mut next,
                config.iters,
                move_buffers,
            );
            config.report(name, start.elapsed(), config.iters);
            drop(ready_tx);
            worker.join().unwrap();
            assert_eq!(available.len(), PIPELINE);
            assert!(available.iter().all(|buffers| buffers.len() == BATCH));
        }
    }
}

fn main() {
    if std::env::args().any(|arg| arg == "--test") {
        return;
    }
    let config = Config::from_env();
    pin_thread(0);
    benchmark_wr_slots(&config);
    benchmark_wr_slots_churn(&config);
    benchmark_wr_slots_sweep(&config);
    benchmark_wr_slots_producer_consumer(&config);
}
