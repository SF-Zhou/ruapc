//! End-to-end echo RPC benchmark across transports.
//!
//! Run with: `cargo bench -p ruapc --bench echo`
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

use ruapc::{Client, Context, Router, Server, SocketPoolConfig, SocketType};

const WARMUP_ITERS: usize = 1_000;
const SERIAL_ITERS: usize = 5_000;
/// Concurrency levels for the closed-loop throughput benchmark.
const CONCURRENT_TASKS: [usize; 2] = [64, 1024];
/// Total number of requests per concurrent run, split evenly across tasks
/// so every concurrency level issues the same amount of work.
const CONCURRENT_TOTAL_OPS: usize = 256_000;

fn bench_client(socket_type: SocketType) -> Client {
    Client {
        socket_type: Some(socket_type),
        ..Default::default()
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
async fn bench_serial(ctx: &Context, socket_type: SocketType, payload_size: usize) {
    let client = bench_client(socket_type);
    let req = "x".repeat(payload_size);

    for _ in 0..WARMUP_ITERS {
        client.echo(ctx, &req).await.unwrap();
    }

    let start = Instant::now();
    for _ in 0..SERIAL_ITERS {
        std::hint::black_box(client.echo(ctx, &req).await.unwrap());
    }
    let secs = start.elapsed().as_secs_f64();

    #[allow(clippy::cast_precision_loss)]
    let us_per_op = secs * 1e6 / SERIAL_ITERS as f64;
    println!("  serial     {payload_size:>5}B: {us_per_op:>8.2} us/op");
}

/// Concurrent closed-loop throughput at the given concurrency level.
async fn bench_concurrent(
    ctx: &Context,
    socket_type: SocketType,
    payload_size: usize,
    num_tasks: usize,
) {
    let req = Arc::new("x".repeat(payload_size));
    let iters_per_task = CONCURRENT_TOTAL_OPS / num_tasks;

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(num_tasks);
    for _ in 0..num_tasks {
        let ctx = ctx.clone();
        let req = req.clone();
        tasks.push(tokio::spawn(async move {
            let client = bench_client(socket_type);
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

async fn run() {
    let echo = Arc::new(EchoImpl);
    let mut router = Router::default();
    echo.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        // The default pool (256 MiB) is sized for regular workloads; at
        // 1024 closed-loop tasks the per-request send buffers exhaust it
        // and allocation waits show up as artificial latency/timeouts.
        buffer_pool_memory: 1 << 30,
        ..Default::default()
    };
    let server = Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("127.0.0.1:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let ctx = Context::create(&config).unwrap().with_addr(addr);

    let socket_types = [
        SocketType::TCP,
        SocketType::WS,
        SocketType::HTTP,
        #[cfg(feature = "rdma")]
        SocketType::RDMA,
    ];
    for socket_type in socket_types {
        println!("{socket_type:?}");

        // Probe the transport once: environments without e.g. a usable RDMA
        // device should skip instead of failing the whole benchmark.
        let probe_client = Client {
            socket_type: Some(socket_type),
            ..Default::default()
        };
        if let Err(err) = probe_client.echo(&ctx, &"probe".to_string()).await {
            println!("  skipped: {err}");
            println!();
            continue;
        }

        for payload_size in [16, 4096] {
            bench_serial(&ctx, socket_type, payload_size).await;
        }
        for num_tasks in CONCURRENT_TASKS {
            bench_concurrent(&ctx, socket_type, 16, num_tasks).await;
        }
        println!();
    }

    server.stop();
    server.join().await;
}

fn main() {
    println!("ruapc: end-to-end echo RPC benchmark (unified server, one port)");
    println!();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}
