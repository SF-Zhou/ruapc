//! Actual CPU gather/scatter through the memory crate's public APIs.
//!
//! Prepared cases reuse a structurally checked batch; validate_copy cases
//! create it for every copy. Raw gather/scatter includes layout construction;
//! cached cases prepare BufferSet layouts before timing. Gather includes output
//! allocation/destruction; scatter reuses destinations. Receiver cases model
//! one mutex acquisition per batch and reuse a cached destination layout.
//! Every byte, including untouched gaps, is checked before and after timing.

use std::{
    hint::black_box,
    sync::{Arc, Mutex},
    time::Instant,
};

use ruapc_bufpool::{Buffer, BufferPool, BufferPoolBuilder, EmptyDevices};
use ruapc_memory::{BufferSet, CopyBatch, CopyOp, SpaceLayout};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const GAP_BYTES: usize = 16;
const SENTINEL: u8 = 0xa5;

struct Config {
    iters: usize,
    warmup: usize,
    repeats: usize,
    filter: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        let iters = read_usize("RUAPC_COPY_BENCH_ITERS", 1_000);
        let repeats = read_usize("RUAPC_COPY_BENCH_REPEATS", 5);
        assert!(iters > 0, "RUAPC_COPY_BENCH_ITERS must be nonzero");
        assert!(repeats > 0, "RUAPC_COPY_BENCH_REPEATS must be nonzero");
        Self {
            iters,
            repeats,
            warmup: read_usize("RUAPC_COPY_BENCH_WARMUP", 100),
            filter: optional_env("RUAPC_COPY_BENCH_FILTER"),
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

fn segment_lengths(total: usize, regions: usize, uneven: bool) -> Vec<usize> {
    let base = total / regions - usize::from(uneven);
    let mut lens = vec![base; regions];
    *lens.last_mut().unwrap() = total - base * (regions - 1);
    lens
}

fn buffers(pool: &Arc<BufferPool>, lens: &[usize], bytes: &[u8]) -> Vec<Buffer> {
    let mut cursor = 0;
    let result = lens
        .iter()
        .map(|&len| {
            let mut buffer = pool.allocate(len).unwrap();
            buffer.set_len(len);
            buffer.copy_from_slice(&bytes[cursor..cursor + len]);
            cursor += len;
            buffer
        })
        .collect();
    assert_eq!(cursor, bytes.len());
    result
}

fn verify_bytes(buffers: &[Buffer], expected: &[u8]) {
    let mut cursor = 0;
    for buffer in buffers {
        assert_eq!(&buffer[..], &expected[cursor..cursor + buffer.len()]);
        cursor += buffer.len();
    }
    assert_eq!(cursor, expected.len());
}

struct Fixture {
    source: Vec<Buffer>,
    destination: Vec<Buffer>,
    ops: Vec<CopyOp>,
    packed: Vec<u8>,
    expected_destination: Vec<u8>,
    total: u64,
}

impl Fixture {
    fn new(
        pool: &Arc<BufferPool>,
        bytes: usize,
        op_count: usize,
        source_regions: usize,
        destination_regions: usize,
    ) -> Self {
        let len = bytes / op_count;
        let stride = len + GAP_BYTES;
        let total = stride * op_count;
        let original: Vec<u8> = (0..total)
            .map(|offset| ((offset * 131 + offset / 251) % 251) as u8)
            .collect();
        let mut packed = Vec::with_capacity(bytes);
        let mut expected_destination = vec![SENTINEL; total];
        let ops = (0..op_count)
            .map(|index| {
                let src = index * stride + GAP_BYTES / 2;
                // The multiplier permutes every selected power-of-two count.
                let dst = ((index * 17) % op_count) * stride + GAP_BYTES / 2;
                packed.extend_from_slice(&original[src..src + len]);
                expected_destination[dst..dst + len].copy_from_slice(&original[src..src + len]);
                CopyOp::new(src as u64, dst as u64, len as u64)
            })
            .collect();
        assert_eq!(packed.len(), bytes);
        let source = buffers(
            pool,
            &segment_lengths(total, source_regions, false),
            &original,
        );
        let destination = buffers(
            pool,
            &segment_lengths(total, destination_regions, true),
            &vec![SENTINEL; total],
        );
        assert_eq!(
            SpaceLayout::from_buffers(&source).unwrap().total(),
            total as u64
        );
        assert_eq!(
            SpaceLayout::from_buffers(&destination).unwrap().total(),
            total as u64,
        );
        Self {
            source,
            destination,
            ops,
            packed,
            expected_destination,
            total: total as u64,
        }
    }
}

// A local uncontended receiver model; production request lookup, admission and
// completion ownership are measured only by the RPC benchmark.
struct ReceiverTarget {
    buffers: Mutex<BufferSet>,
}

impl ReceiverTarget {
    fn new(buffers: Vec<Buffer>) -> Self {
        Self {
            buffers: Mutex::new(BufferSet::try_new(buffers).unwrap()),
        }
    }

    fn copy(&self, ops: &[CopyOp], bytes: &[u8]) {
        // Packed bytes expose destination bounds, not the original source space.
        let batch = CopyBatch::new(ops).unwrap();
        let mut buffers = self.buffers.lock().unwrap();
        buffers.scatter(&batch, bytes).unwrap();
    }
}

fn run_case(config: &Config, name: &str, kind: &str, mut fixture: Fixture) {
    let prepared = CopyBatch::new(&fixture.ops).unwrap();
    prepared.check_bounds(fixture.total, fixture.total).unwrap();
    assert_eq!(prepared.gather(&fixture.source).unwrap(), fixture.packed);
    let batch = || {
        if kind.ends_with("validate_copy") {
            CopyBatch::new(black_box(&fixture.ops)).unwrap()
        } else {
            black_box(prepared)
        }
    };
    if kind.starts_with("gather") {
        if kind.contains("cached") {
            let source = BufferSet::try_new(fixture.source).unwrap();
            assert_eq!(source.gather(&prepared).unwrap(), fixture.packed);
            config.measure(name, || {
                drop(black_box(black_box(&source).gather(&batch()).unwrap()));
            });
            assert_eq!(source.gather(&prepared).unwrap(), fixture.packed);
        } else {
            config.measure(name, || {
                drop(black_box(
                    batch().gather(black_box(&fixture.source)).unwrap(),
                ));
            });
            assert_eq!(prepared.gather(&fixture.source).unwrap(), fixture.packed);
        }
    } else if kind.starts_with("scatter") {
        if kind.contains("cached") {
            let mut destination = BufferSet::try_new(fixture.destination).unwrap();
            destination.scatter(&prepared, &fixture.packed).unwrap();
            verify_bytes(destination.buffers(), &fixture.expected_destination);
            config.measure(name, || {
                black_box(&mut destination)
                    .scatter(&batch(), black_box(&fixture.packed))
                    .unwrap();
                black_box(&destination);
            });
            verify_bytes(destination.buffers(), &fixture.expected_destination);
        } else {
            prepared
                .scatter(&fixture.packed, &mut fixture.destination)
                .unwrap();
            verify_bytes(&fixture.destination, &fixture.expected_destination);
            config.measure(name, || {
                batch()
                    .scatter(
                        black_box(&fixture.packed),
                        black_box(&mut fixture.destination),
                    )
                    .unwrap();
                black_box(&fixture.destination);
            });
            verify_bytes(&fixture.destination, &fixture.expected_destination);
        }
    } else {
        let target = ReceiverTarget::new(fixture.destination);
        target.copy(&fixture.ops, &fixture.packed);
        verify_bytes(
            target.buffers.lock().unwrap().buffers(),
            &fixture.expected_destination,
        );
        config.measure(name, || {
            black_box(&target).copy(black_box(&fixture.ops), black_box(&fixture.packed));
        });
        verify_bytes(
            target.buffers.lock().unwrap().buffers(),
            &fixture.expected_destination,
        );
    }
}

fn main() {
    let config = Config::from_env();
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(64 * 1024 * 1024)
        .build();
    println!(
        "memory copy: allocator=jemalloc iterations={} warmup={} repeats={} filter={:?}",
        config.iters, config.warmup, config.repeats, config.filter,
    );
    println!(
        "ns_per_batch is amortized wall time; fixture allocation and byte verification are untimed"
    );
    let mut cases = 0;
    for bytes in [64 * 1024, 1024 * 1024] {
        for op_count in [1, 64, 1024] {
            for (source_regions, destination_regions) in [(1, 1), (4, 64)] {
                for kind in [
                    "gather/prepared",
                    "gather/validate_copy",
                    "scatter/prepared",
                    "scatter/validate_copy",
                    "gather/cached_prepared",
                    "gather/cached_validate_copy",
                    "scatter/cached_prepared",
                    "scatter/cached_validate_copy",
                    "receiver/validate_copy",
                ] {
                    let name = format!(
                        "{kind}/bytes={bytes}/ops={op_count}/regions={source_regions}x{destination_regions}"
                    );
                    if !config.selects(&name) {
                        continue;
                    }
                    let fixture =
                        Fixture::new(&pool, bytes, op_count, source_regions, destination_regions);
                    println!(
                        "fixture={name} copied_bytes={} logical_bytes={} gap_bytes_per_op={GAP_BYTES}",
                        fixture.packed.len(),
                        fixture.total,
                    );
                    run_case(&config, &name, kind, fixture);
                    cases += 1;
                }
            }
        }
    }
    assert!(cases > 0, "RUAPC_COPY_BENCH_FILTER matched no cases");
    println!("measured_cases={cases}");
}
