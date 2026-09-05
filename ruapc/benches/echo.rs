//! End-to-end echo RPC benchmark across transports.
//!
//! Run with: `cargo bench -p ruapc --bench echo`
//! Optional environment settings: `RUAPC_BENCH_SERIAL_ITERS` (default 5000),
//! `RUAPC_BENCH_WARMUP_ITERS` (default 1000), and `RUAPC_BENCH_TRANSPORT`
//! (`TCP`, `WS`, `HTTP`, or `RDMA`; unset measures every transport).
//! `RUAPC_BENCH_RDMA_DEVICE` restricts both peers to one named RDMA device.
//!
//! A single UNIFIED server serves all protocols on one port; each transport
//! is measured with the same client-side workload:
//! - serial round-trip latency (one in-flight request)
//! - concurrent throughput (many tasks issuing requests in a closed loop)
//!
//! Transports whose setup fails in the current environment (e.g. RDMA
//! without a usable device) are reported as skipped instead of aborting
//! the whole run.

// `#[service]` request types must be owned deserializable types behind a
// reference (`&String`), so `&str` is not an option here.
#![allow(clippy::ptr_arg)]

use std::{str::FromStr, sync::Arc, time::Instant};

use ruapc::{Client, Context, Endpoint, ListenMode, Router, Server, SocketPoolConfig, Transport};

const WARMUP_ITERS: usize = 1_000;
const SERIAL_ITERS: usize = 5_000;
/// Concurrency levels for the closed-loop throughput benchmark.
const CONCURRENT_TASKS: [usize; 2] = [64, 1024];
/// Total number of requests per concurrent run, split evenly across tasks
/// so every concurrency level issues the same amount of work.
const CONCURRENT_TOTAL_OPS: usize = 256_000;

struct BenchOptions {
    warmup_iters: usize,
    serial_iters: usize,
    transport: Option<Transport>,
    rdma_device: Option<String>,
}

impl BenchOptions {
    fn from_env() -> Self {
        let serial_iters = env_iters("RUAPC_BENCH_SERIAL_ITERS", SERIAL_ITERS);
        assert!(serial_iters > 0, "RUAPC_BENCH_SERIAL_ITERS must be nonzero");
        Self {
            warmup_iters: env_iters("RUAPC_BENCH_WARMUP_ITERS", WARMUP_ITERS),
            serial_iters,
            transport: optional_env("RUAPC_BENCH_TRANSPORT")
                .map(|value| value.parse().expect("invalid RUAPC_BENCH_TRANSPORT")),
            rdma_device: optional_env("RUAPC_BENCH_RDMA_DEVICE"),
        }
    }

    fn includes(&self, transport: Transport) -> bool {
        self.transport.is_none_or(|selected| selected == transport)
    }
}

fn optional_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn env_iters(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a nonnegative integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

#[ruapc::service]
trait EchoService {
    async fn echo(&self, ctx: &ruapc::Context, req: &String) -> ruapc::Result<String>;
}

struct EchoImpl;

impl EchoService for EchoImpl {
    async fn echo(&self, _ctx: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        Ok(req.clone())
    }
}

/// Serial round-trip latency: one request in flight at a time.
async fn bench_serial(
    ctx: &Context,
    payload_size: usize,
    warmup_iters: usize,
    serial_iters: usize,
) {
    let client = Client::default();
    let req = "x".repeat(payload_size);

    for _ in 0..warmup_iters {
        client.echo(ctx, &req).await.unwrap();
    }

    let start = Instant::now();
    for _ in 0..serial_iters {
        std::hint::black_box(client.echo(ctx, &req).await.unwrap());
    }
    let secs = start.elapsed().as_secs_f64();

    #[allow(clippy::cast_precision_loss)]
    let us_per_op = secs * 1e6 / serial_iters as f64;
    println!("  serial     {payload_size:>5}B: {us_per_op:>8.2} us/op");
}

/// Concurrent closed-loop throughput at the given concurrency level.
async fn bench_concurrent(ctx: &Context, payload_size: usize, num_tasks: usize) {
    let req = Arc::new("x".repeat(payload_size));
    let iters_per_task = CONCURRENT_TOTAL_OPS / num_tasks;

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(num_tasks);
    for _ in 0..num_tasks {
        let ctx = ctx.clone();
        let req = req.clone();
        tasks.push(tokio::spawn(async move {
            let client = Client::default();
            for _ in 0..iters_per_task {
                std::hint::black_box(client.echo(&ctx, &req).await.unwrap());
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let secs = start.elapsed().as_secs_f64();

    let total_ops = num_tasks * iters_per_task;
    #[allow(clippy::cast_precision_loss)]
    let kops = total_ops as f64 / secs / 1e3;
    #[allow(clippy::cast_precision_loss)]
    let us_per_op = secs * 1e6 / iters_per_task as f64;
    println!(
        "  concurrent {payload_size:>5}B: {kops:>8.1} kops/s | {us_per_op:>8.2} us/op \
         ({num_tasks} tasks)"
    );
}

async fn run(options: BenchOptions) {
    let echo = Arc::new(EchoImpl);
    let mut router = Router::default();
    echo.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        // The default pool (256 MiB) is sized for regular workloads; at
        // 1024 closed-loop tasks the per-request send buffers exhaust it
        // and allocation waits show up as artificial latency/timeouts.
        buffer_pool_memory: 1 << 30,
        ..Default::default()
    };
    #[cfg(feature = "rdma")]
    let config = {
        let mut config = config;
        if let Some(device) = &options.rdma_device {
            config
                .rdma
                .get_or_insert_with(Default::default)
                .path
                .device_filter = vec![device.clone()];
        }
        config
    };
    let server = Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("127.0.0.1:0").unwrap();
    let addr = server.listen(addr).await.unwrap();
    #[cfg(feature = "rdma")]
    let second_addr = if options.includes(Transport::RDMA) {
        Some(server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap())
    } else {
        None
    };

    let base_ctx = Context::create(&config).unwrap();

    let transports = [
        Transport::TCP,
        Transport::WS,
        Transport::HTTP,
        #[cfg(feature = "rdma")]
        Transport::RDMA,
    ];
    for transport in transports {
        if !options.includes(transport) {
            continue;
        }
        println!("{transport:?}");
        let ctx = base_ctx.with_endpoint(Endpoint::new(transport, addr));

        // Probe the transport once: environments without e.g. a usable RDMA
        // device should skip instead of failing the whole benchmark.
        let probe_client = Client::default();
        if let Err(err) = probe_client.echo(&ctx, &"probe".to_string()).await {
            println!("  skipped: {err}");
            println!();
            continue;
        }

        for payload_size in [16, 4096] {
            bench_serial(
                &ctx,
                payload_size,
                options.warmup_iters,
                options.serial_iters,
            )
            .await;
        }
        for num_tasks in CONCURRENT_TASKS {
            bench_concurrent(&ctx, 16, num_tasks).await;
        }
        println!();
    }

    #[cfg(feature = "rdma")]
    if let Some(second_addr) = second_addr {
        println!("RDMA (2 endpoints)");
        let ctx = base_ctx.with_endpoints(vec![
            Endpoint::new(Transport::RDMA, addr),
            Endpoint::new(Transport::RDMA, second_addr),
        ]);
        let probe_client = Client::default();
        if let Err(err) = probe_client.echo(&ctx, &"probe".to_string()).await {
            println!("  skipped: {err}");
        } else {
            for num_tasks in CONCURRENT_TASKS {
                bench_concurrent(&ctx, 16, num_tasks).await;
            }
        }
        println!();
    }

    server.stop();
    server.join().await;
}

fn main() {
    let options = BenchOptions::from_env();
    println!("ruapc: end-to-end echo RPC benchmark (unified server, one port)");
    println!(
        "serial iterations: {}; warmup iterations: {}; transport: {:?}; RDMA device: {:?}",
        options.serial_iters, options.warmup_iters, options.transport, options.rdma_device,
    );
    println!();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(options));
}
