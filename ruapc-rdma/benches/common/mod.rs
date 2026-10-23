//! Shared controls for device-free ownership microbenchmarks.

use std::time::Duration;

pub(crate) struct Config {
    pub iters: u64,
    pub warmup: u64,
    pub repeats: usize,
    filter: String,
}

impl Config {
    pub fn from_env() -> Self {
        if cfg!(debug_assertions) {
            eprintln!("debug build: use cargo bench for performance measurements");
        }
        Self {
            iters: number("RUAPC_RDMA_BENCH_ITERS", 100_000),
            warmup: number("RUAPC_RDMA_BENCH_WARMUP", 1_000),
            repeats: usize::try_from(number("RUAPC_RDMA_BENCH_REPEATS", 3)).unwrap(),
            filter: std::env::var("RUAPC_RDMA_BENCH_FILTER").unwrap_or_default(),
        }
    }

    pub fn selected(&self, case: &str) -> bool {
        self.filter.is_empty() || case.contains(&self.filter)
    }

    pub fn report(&self, case: &str, elapsed: Duration, operations: u64) {
        assert!(operations > 0);
        println!(
            "{case}: operations={operations} elapsed_ms={:.3} ns/op={:.2} Mops/s={:.3}",
            elapsed.as_secs_f64() * 1e3,
            elapsed.as_secs_f64() * 1e9 / operations as f64,
            operations as f64 / elapsed.as_secs_f64() / 1e6,
        );
    }
}

fn number(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or_else(|| panic!("{name} must be a positive integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

pub(crate) fn pin_thread(offset: usize) {
    let Ok(base) = std::env::var("RUAPC_RDMA_BENCH_CPU_BASE") else {
        return;
    };
    let cpu = base
        .parse::<usize>()
        .expect("RUAPC_RDMA_BENCH_CPU_BASE must be a CPU index")
        .checked_add(offset)
        .expect("CPU index overflow");
    #[cfg(target_os = "linux")]
    {
        assert!(cpu < libc::CPU_SETSIZE as usize, "CPU index out of range");
        // SAFETY: a zeroed cpu_set_t is valid; the checked CPU fits its bitset.
        // sched_setaffinity with pid zero affects only the calling thread.
        let result = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(cpu, &mut set);
            libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set)
        };
        assert_eq!(
            result,
            0,
            "CPU affinity: {}",
            std::io::Error::last_os_error()
        );
    }
    #[cfg(not(target_os = "linux"))]
    panic!("CPU affinity is only supported on Linux (requested CPU {cpu})");
}
