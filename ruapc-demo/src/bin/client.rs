use clap::Parser;
use ruapc::*;
use ruapc_demo::{EchoService, GreetService, Request};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Listen address.
    #[arg(default_value = "127.0.0.1:8000")]
    pub addr: std::net::SocketAddr,

    /// Socket type.
    #[arg(long, default_value = "tcp")]
    pub socket_type: SocketType,

    /// Request value.
    #[arg(short, long, default_value = "alice")]
    pub value: String,

    /// Use MessagePack
    #[arg(long, default_value_t = false)]
    pub use_msgpack: bool,

    /// Enable stress testing.
    #[arg(long, default_value_t = false)]
    pub stress: bool,

    /// Enable latency benchmark (reports throughput and latency percentiles).
    /// Use `--coroutines 1` for pure ping-pong latency.
    #[arg(long, default_value_t = false)]
    pub bench: bool,

    /// Stress/bench testing duration.
    #[arg(long, default_value = "60")]
    pub secs: u64,

    /// The number of coroutines.
    #[arg(long, default_value = "32")]
    pub coroutines: usize,

    /// Request payload size in bytes (bench mode).
    #[arg(long, default_value = "1024")]
    pub payload_size: usize,

    /// Warmup duration in seconds before recording latency (bench mode).
    #[arg(long, default_value = "3")]
    pub warmup_secs: u64,

    /// RDMA: number of (CQ + poll thread) shards per device.
    #[arg(long, default_value = "1")]
    pub poll_threads: u32,

    /// RDMA: number of connections (QPs) per peer; requests are striped.
    #[arg(long, default_value = "1")]
    pub conns_per_peer: u32,

    /// RDMA: comma-separated device allowlist (e.g. "mlx5_0").
    #[arg(long, value_delimiter = ',')]
    pub rdma_devices: Vec<String>,

    /// Buffer pool memory limit in MiB (0 = library default).
    #[arg(long, default_value = "0")]
    pub pool_mem_mb: usize,

    /// Tokio worker threads (0 = number of CPUs).
    #[arg(long, default_value = "0")]
    pub worker_threads: usize,

    /// RDMA: poll-thread busy-poll window in microseconds.
    #[arg(long, default_value = "50")]
    pub poll_spin_us: u64,

    /// RDMA: number of dispatch worker tasks shared by all poll threads.
    #[arg(long, default_value = "32")]
    pub dispatch_workers: u32,

    /// RDMA: receive ring depth per connection (negotiated to the minimum
    /// of both sides); the send window is half of it. Small values force
    /// aggregation under load; raise for large-message pipelines.
    #[arg(long, default_value = "8")]
    pub recv_queue_len: u32,
}

fn socket_pool_config(args: &Args) -> SocketPoolConfig {
    #[allow(unused_mut)]
    let mut config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        buffer_pool_memory: args.pool_mem_mb * 1024 * 1024,
        ..Default::default()
    };
    #[cfg(feature = "rdma")]
    {
        config.rdma.poll_threads_per_device = args.poll_threads;
        config.rdma.connections_per_peer = args.conns_per_peer;
        config.rdma.device_filter = args.rdma_devices.clone();
        config.rdma.poll_spin_us = args.poll_spin_us;
        config.rdma.dispatch_workers = args.dispatch_workers;
        config.rdma.recv_queue_len = args.recv_queue_len;
    }
    config
}

#[derive(Default)]
struct State {
    total: AtomicUsize,
    fails: AtomicUsize,
}

async fn stress_test(args: Args) {
    let state = Arc::new(State::default());
    let start_time = std::time::Instant::now();
    let mut tasks = vec![];
    let ctx = Context::create(&socket_pool_config(&args))
        .unwrap()
        .with_addr(args.addr);
    for _ in 0..args.coroutines {
        let value = Request(args.value.clone());
        let state = state.clone();
        let ctx = ctx.clone();
        tasks.push(tokio::spawn(async move {
            while start_time.elapsed().as_secs() < args.secs {
                let client = Client {
                    timeout: Duration::from_secs(5),
                    use_msgpack: args.use_msgpack,
                    socket_type: Some(args.socket_type),
                };
                for _ in 0..256 {
                    let result = client.echo(&ctx, &value).await;
                    state.total.fetch_add(1, Ordering::AcqRel);
                    if result.is_err() {
                        state.fails.fetch_add(1, Ordering::AcqRel);
                    }
                }
            }
        }));
    }
    tokio::select! {
        _ = async {
            for task in tasks {
                task.await.unwrap();
            }
        } => {
        }
        _ = async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let total = state.total.swap(0, Ordering::AcqRel);
                let fails = state.fails.swap(0, Ordering::AcqRel);
                tracing::info!("QPS: {total}/s, fails: {fails}/s");
            }
        } => {
        }
    }
}

async fn bench_test(args: Args) {
    let ctx = Context::create(&socket_pool_config(&args))
        .unwrap()
        .with_addr(args.addr);

    let payload = Request("x".repeat(args.payload_size));
    let warmup = Duration::from_secs(args.warmup_secs);
    let total = Duration::from_secs(args.secs);
    assert!(total > warmup, "--secs must be larger than --warmup-secs");
    let start = std::time::Instant::now();

    let mut tasks = Vec::with_capacity(args.coroutines);
    for _ in 0..args.coroutines {
        let ctx = ctx.clone();
        let payload = payload.clone();
        let args = args.clone();
        tasks.push(tokio::spawn(async move {
            // Latency in nanoseconds, 1ns..60s, 3 significant digits.
            let mut hist = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
                .expect("failed to create histogram");
            let client = Client {
                timeout: Duration::from_secs(5),
                use_msgpack: args.use_msgpack,
                socket_type: Some(args.socket_type),
            };
            let mut fails = 0u64;
            loop {
                let elapsed = start.elapsed();
                if elapsed >= total {
                    break;
                }
                let t = std::time::Instant::now();
                let result = client.echo(&ctx, &payload).await;
                let nanos = t.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                if result.is_err() {
                    fails += 1;
                    continue;
                }
                if elapsed >= warmup {
                    let _ = hist.record(nanos.max(1));
                }
            }
            (hist, fails)
        }));
    }

    let mut merged = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
        .expect("failed to create histogram");
    let mut fails = 0u64;
    for task in tasks {
        let (hist, f) = task.await.unwrap();
        merged.add(hist).unwrap();
        fails += f;
    }

    let measured_secs = (args.secs - args.warmup_secs) as f64;
    let p = |q: f64| merged.value_at_quantile(q) as f64 / 1_000.0;
    tracing::info!(
        "bench: socket_type={:?} payload={}B coroutines={} duration={}s (warmup {}s)",
        args.socket_type,
        args.payload_size,
        args.coroutines,
        args.secs,
        args.warmup_secs,
    );
    tracing::info!(
        "requests: {} ok, {} fails, {:.0} req/s",
        merged.len(),
        fails,
        merged.len() as f64 / measured_secs,
    );
    tracing::info!(
        "latency(µs): mean={:.1} min={:.1} p50={:.1} p90={:.1} p99={:.1} p99.9={:.1} p99.99={:.1} max={:.1}",
        merged.mean() / 1_000.0,
        merged.min() as f64 / 1_000.0,
        p(0.50),
        p(0.90),
        p(0.99),
        p(0.999),
        p(0.9999),
        merged.max() as f64 / 1_000.0,
    );
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if args.worker_threads > 0 {
        builder.worker_threads(args.worker_threads);
    }
    let runtime = builder.build().expect("failed to build tokio runtime");
    runtime.block_on(async_main(args));
}

async fn async_main(args: Args) {
    if args.bench {
        bench_test(args).await;
    } else if args.stress {
        stress_test(args).await;
    } else {
        let ctx = Context::create(&socket_pool_config(&args))
            .unwrap()
            .with_addr(args.addr);
        let client = Client {
            use_msgpack: args.use_msgpack,
            socket_type: Some(args.socket_type),
            ..Default::default()
        };
        let rsp = client.echo(&ctx, &Request(args.value.clone())).await;
        tracing::info!("echo rsp: {:?}", rsp);

        let rsp = client.greet(&ctx, &Request(args.value.clone())).await;
        tracing::info!("greet rsp: {:?}", rsp);
    }
}
