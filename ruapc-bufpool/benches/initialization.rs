//! Compare first allocation cost without charging initialization to the hot pool
//! reuse path. The raw controls never expose uninitialized memory as byte slices.
//!
//! Run with: cargo bench -p ruapc-bufpool --bench initialization

use std::alloc::{Layout, alloc, alloc_zeroed, dealloc};
use std::hint::black_box;
use std::time::Instant;

use ruapc_bufpool::AlignedMemory;

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
    println!("{name:<36} {micros:>9.2} us per 64 MiB block");
}

fn main() {
    let layout = Layout::from_size_align(SIZE, ALIGN).unwrap();
    for zeroed in [false, true] {
        let name = if zeroed {
            "system alloc_zeroed + free"
        } else {
            "system alloc + free (uninitialized)"
        };
        measure(name, || {
            // SAFETY: valid nonzero layout, checked allocation, matching free.
            // This benchmark does not read the raw uninitialized control.
            unsafe {
                let ptr = if zeroed {
                    alloc_zeroed(layout)
                } else {
                    alloc(layout)
                };
                assert!(!ptr.is_null());
                black_box(ptr);
                dealloc(ptr, layout);
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
