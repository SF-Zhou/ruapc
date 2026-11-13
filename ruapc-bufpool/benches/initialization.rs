//! Compare first allocation cost without charging initialization to the hot pool
//! reuse path. The control cases allocate through `std::alloc::Global`, which
//! every harness backs with jemalloc, and never expose uninitialized memory as
//! byte slices. `AlignedMemory` allocates through the same global allocator.
//! See `docs/benchmark.md#allocator`.
//!
//! Run with: cargo bench -p ruapc-bufpool --bench initialization

use std::alloc::{Allocator, Global, Layout};
use std::hint::black_box;
use std::time::Instant;

use ruapc_bufpool::AlignedMemory;

// Measurements pin jemalloc as the process allocator so samples are comparable
// across hosts; see docs/benchmark.md#allocator.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SIZE: usize = 64 * 1024 * 1024;
const ITERATIONS: u32 = 64;
#[cfg(target_pointer_width = "64")]
const ALIGN: usize = 2 * 1024 * 1024;
#[cfg(not(target_pointer_width = "64"))]
const ALIGN: usize = 4096;

fn measure(name: &str, mut allocate: impl FnMut()) {
    for _ in 0..4 {
        allocate();
    }
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        allocate();
    }
    let micros = start.elapsed().as_secs_f64() * 1e6 / f64::from(ITERATIONS);
    println!("{name:<44} {micros:>9.2} us per 64 MiB block");
}

fn main() {
    let layout = Layout::from_size_align(SIZE, ALIGN).unwrap();
    for zeroed in [false, true] {
        let name = if zeroed {
            "Global allocate_zeroed + deallocate"
        } else {
            "Global allocate + deallocate (uninitialized)"
        };
        measure(name, || {
            let block = if zeroed {
                Global.allocate_zeroed(layout)
            } else {
                Global.allocate(layout)
            }
            .unwrap();
            assert!(block.len() >= layout.size());
            black_box(block);
            // SAFETY: `block` was allocated by `Global` with `layout` above.
            // This benchmark does not read the uninitialized control.
            unsafe {
                Global.deallocate(block.cast(), layout);
            }
        });
    }
    measure("AlignedMemory::new + drop", || {
        black_box(AlignedMemory::new(SIZE).unwrap());
    });
    measure("AlignedMemory + first full write", || {
        let mut memory = AlignedMemory::new(SIZE).unwrap();
        memory.as_mut_slice().fill(1);
        black_box(memory);
    });
}
