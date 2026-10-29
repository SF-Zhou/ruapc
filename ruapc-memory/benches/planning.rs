//! Device-free copy planning through the memory crate's public APIs.
//!
//! Each timed batch creates a validated operation token, plans chunks with it
//! and destroys the resulting plan, matching the runtime's transfer pipeline.
//! Layout and operation construction, plan verification and warmup are untimed.
//! The output is amortized wall time per whole batch, not transfer latency.

use std::{hint::black_box, time::Instant};

use ruapc_memory::{CopyBatch, CopyOp, SpaceLayout, SrcChunk};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SPACE_BYTES: u64 = 1024 * 1024;

struct Config {
    iters: usize,
    warmup: usize,
    repeats: usize,
    filter: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        let iters = read_usize("RUAPC_MEMORY_BENCH_ITERS", 10_000);
        let repeats = read_usize("RUAPC_MEMORY_BENCH_REPEATS", 5);
        assert!(iters > 0, "RUAPC_MEMORY_BENCH_ITERS must be nonzero");
        assert!(repeats > 0, "RUAPC_MEMORY_BENCH_REPEATS must be nonzero");
        Self {
            iters,
            repeats,
            warmup: read_usize("RUAPC_MEMORY_BENCH_WARMUP", 500),
            filter: optional_env("RUAPC_MEMORY_BENCH_FILTER"),
        }
    }

    fn selects(&self, name: &str) -> bool {
        self.filter
            .as_deref()
            .is_none_or(|filter| name.contains(filter))
    }

    fn measure(&self, name: &str, mut operation: impl FnMut()) {
        for repeat in 1..=self.repeats {
            for _ in 0..self.warmup {
                operation();
            }
            let start = Instant::now();
            for _ in 0..self.iters {
                operation();
            }
            let elapsed = start.elapsed();
            #[allow(clippy::cast_precision_loss)]
            let ns_per_batch = elapsed.as_secs_f64() * 1e9 / self.iters as f64;
            println!(
                "case={name} repeat={repeat} iterations={} elapsed_ns={} ns_per_batch={ns_per_batch:.2}",
                self.iters,
                elapsed.as_nanos(),
            );
        }
    }
}

fn optional_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn read_usize(name: &str, default: usize) -> usize {
    optional_env(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a nonnegative integer"))
    })
}

struct Fixture {
    source: SpaceLayout,
    destination: SpaceLayout,
    source_lens: Vec<u64>,
    destination_lens: Vec<u64>,
    ops: Vec<CopyOp>,
}

impl Fixture {
    fn new(op_count: usize, source_regions: usize, destination_regions: usize) -> Self {
        let source_lens = vec![SPACE_BYTES / source_regions as u64; source_regions];
        // Offset destination boundaries from the operation and source boundaries
        // so the fragmented cases exercise scatter lists even for short ops.
        let mut destination_lens =
            vec![SPACE_BYTES / destination_regions as u64 - 1; destination_regions];
        *destination_lens.last_mut().unwrap() += destination_regions as u64;
        let source = SpaceLayout::from_lens(source_lens.iter().copied()).unwrap();
        let destination = SpaceLayout::from_lens(destination_lens.iter().copied()).unwrap();
        let len = SPACE_BYTES / op_count as u64;
        let ops = (0..op_count)
            .map(|index| {
                // 17 is coprime with every selected operation count. The ranges
                // cover the destination once, in a non-sorted order for >1 op.
                CopyOp::new(
                    index as u64 * len,
                    ((index * 17) % op_count) as u64 * len,
                    len,
                )
            })
            .collect();
        Self {
            source,
            destination,
            source_lens,
            destination_lens,
            ops,
        }
    }

    fn verify(&self, max_sge: usize) -> usize {
        assert_eq!(self.source.total(), SPACE_BYTES);
        assert_eq!(self.destination.total(), SPACE_BYTES);
        let batch = CopyBatch::new(&self.ops).unwrap();
        assert_eq!(batch.byte_count(), SPACE_BYTES);
        let chunks = batch
            .plan_chunks(&self.source, &self.destination, max_sge)
            .unwrap();
        self.verify_chunks(&chunks, max_sge);
        chunks.len()
    }

    fn verify_chunks(&self, chunks: &[SrcChunk], max_sge: usize) {
        let mut op_index = 0;
        let mut consumed = 0;
        for chunk in chunks {
            let op = self.ops[op_index];
            assert!(chunk.len > 0 && chunk.len <= op.len - consumed);
            assert!(chunk.off + chunk.len <= self.source_lens[chunk.seg]);
            let source_start: u64 = self.source_lens[..chunk.seg].iter().sum();
            assert_eq!(source_start + chunk.off, op.src_offset + consumed);
            assert!(!chunk.dst.is_empty() && chunk.dst.len() <= max_sge);
            let mut scattered = 0;
            for slice in &chunk.dst {
                assert!(slice.len > 0);
                assert!(slice.off + slice.len <= self.destination_lens[slice.seg]);
                let destination_start: u64 = self.destination_lens[..slice.seg].iter().sum();
                assert_eq!(
                    destination_start + slice.off,
                    op.dst_offset + consumed + scattered,
                );
                scattered += slice.len;
            }
            assert_eq!(scattered, chunk.len);
            consumed += chunk.len;
            if consumed == op.len {
                op_index += 1;
                consumed = 0;
            }
        }
        assert_eq!(op_index, self.ops.len());
        assert_eq!(consumed, 0);
    }

    fn run(&self, max_sge: usize) {
        let batch = CopyBatch::new(black_box(&self.ops)).unwrap();
        black_box(batch.byte_count());
        drop(black_box(
            batch
                .plan_chunks(
                    black_box(&self.source),
                    black_box(&self.destination),
                    black_box(max_sge),
                )
                .unwrap(),
        ));
    }
}

fn main() {
    let config = Config::from_env();
    println!(
        "memory planning: allocator=jemalloc iterations={} warmup={} repeats={} filter={:?}",
        config.iters, config.warmup, config.repeats, config.filter,
    );
    println!("ns_per_batch is amortized wall time for validation, planning and plan destruction");
    let mut cases = 0;
    for op_count in [1, 64, 1024] {
        for (source_regions, destination_regions) in [(1, 1), (4, 64)] {
            for max_sge in [1, 16] {
                let name = format!(
                    "validate_plan/ops={op_count}/regions={source_regions}x{destination_regions}/sge={max_sge}"
                );
                if !config.selects(&name) {
                    continue;
                }
                let fixture = Fixture::new(op_count, source_regions, destination_regions);
                let chunks = fixture.verify(max_sge);
                println!("fixture={name} logical_bytes={SPACE_BYTES} planned_chunks={chunks}");
                config.measure(&name, || fixture.run(max_sge));
                cases += 1;
            }
        }
    }
    assert!(cases > 0, "RUAPC_MEMORY_BENCH_FILTER matched no cases");
    println!("measured_cases={cases}");
}
